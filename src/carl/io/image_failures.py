"""Compact refresh image diagnostics, including older retained acquisitions."""

import json
from collections import Counter
from typing import cast

from carl.core.image_failures import image_acquisition_failure_kind
from carl.core.models import JsonValue
from carl.core.review import WorkFailureReasonCount
from carl.io.sqlite import Database


def _mapping(value: JsonValue) -> dict[str, JsonValue]:
    return cast(dict[str, JsonValue], value) if isinstance(value, dict) else {}


def _strings(value: JsonValue) -> tuple[str, ...]:
    return (
        tuple(item for item in cast(list[JsonValue], value) if isinstance(item, str))
        if isinstance(value, list)
        else ()
    )


async def refresh_image_failure_reason_counts(
    database: Database, work_identifier: str
) -> tuple[WorkFailureReasonCount, ...]:
    """Read current failed image children without changing retained work.

    Request edges include children not yet checkpointed; checkpoint identifiers
    include historical refreshes and resumed extractions. Read acquisition
    metadata so older generic validation errors still reveal their HTTP cause.
    """
    work = await database.work(work_identifier)
    result = work.get("result")
    checkpoint = _mapping(result)
    identifiers = [
        identifier
        for key in (
            "image_collection_work_identifiers",
            "image_extraction_work_identifiers",
            "resumed_image_extraction_work_identifiers",
        )
        for identifier in _strings(checkpoint.get(key))
    ]
    async with database._connections.reader() as connection:  # pyright: ignore[reportPrivateUsage]
        cursor = await connection.execute(
            """
            WITH selected AS (
                SELECT value AS id FROM json_each(?)
                UNION
                SELECT request.work_item_id FROM work_requests AS request
                WHERE request.requester_kind_parts_json IN (
                    '["carl","facebook","search_refresh","image"]',
                    '["carl","ebay","search_refresh","image"]'
                ) AND (
                    json_extract(request.context_json, '$.search_refresh_work_identifier') = ?
                    OR (? IS NOT NULL AND json_extract(request.context_json,
                        '$.refreshed_search_run_record_identifier') = ?)
                )
            ), children AS (
                SELECT id FROM selected
                UNION
                SELECT json_extract(work.result_json, '$.extraction_work_identifier')
                FROM work_items AS work JOIN selected ON selected.id = work.id
            )
            SELECT work.error_json, work.result_json, acquisition.value_json
            FROM work_items AS work JOIN children ON children.id = work.id
            LEFT JOIN records AS image_result ON image_result.object_id =
                json_extract(work.result_json, '$.image_result_record_identifier')
            LEFT JOIN records AS acquisition ON acquisition.object_id = COALESCE(
                json_extract(work.result_json, '$.acquisition_record_identifier'),
                json_extract(image_result.value_json, '$.acquisition_record_identifier')
            )
            WHERE work.state = 'terminal_failure'
               OR (work.state = 'completed' AND
                   json_extract(work.result_json, '$.state') = 'failed')
            """,
            (
                json.dumps(identifiers),
                work_identifier,
                checkpoint.get("refreshed_search_run_record_identifier"),
                checkpoint.get("refreshed_search_run_record_identifier"),
            ),
        )
        rows = await cursor.fetchall()
    counts: Counter[str] = Counter()
    for error_json, result_json, acquisition_json in rows:
        error = _mapping(json.loads(str(error_json))) if error_json else {}
        result = _mapping(json.loads(str(result_json))) if result_json else {}
        acquisition = (
            json.loads(str(acquisition_json)) if acquisition_json else result.get("acquisition")
        )
        kind = image_acquisition_failure_kind(_mapping(acquisition))
        if kind is None:
            failure_kind = error.get("failure_kind")
            exception = error.get("type")
            kind = (
                failure_kind
                if isinstance(failure_kind, str)
                else "transport_timeout"
                if isinstance(exception, str) and "timeout" in exception.lower()
                else str(error.get("kind") or "unknown")
            )
        counts[kind] += 1
    return tuple(
        WorkFailureReasonCount(kind=kind, count=count) for kind, count in sorted(counts.items())
    )
