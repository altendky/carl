"""Atomic recovery of failed item pages without repeating a refresh's search."""

import json
from collections.abc import Callable
from typing import Literal, cast

from carl.core.json import encode_json
from carl.core.models import JsonValue
from carl.core.refresh_recovery import RetryItemFailuresRequest, RetryItemFailuresResult
from carl.core.review_errors import ReviewInputError
from carl.io.sqlite import Database

_REFRESH_KINDS = {
    ("carl", "facebook", "work", "refresh_search"): "facebook",
    ("carl", "ebay", "work", "refresh_search"): "ebay",
}


async def retry_refresh_item_failures(
    database: Database,
    request: RetryItemFailuresRequest,
    *,
    utc_ns: int,
    new_identifier: Callable[[], str],
) -> RetryItemFailuresResult:
    """Select linked transient failures, reset their budgets, and resume downstream stages."""
    async with database._connections.writer() as connection:  # pyright: ignore[reportPrivateUsage]
        cursor = await connection.execute(
            "SELECT kind_parts_json,state,result_json FROM work_items WHERE id=?",
            (request.refresh_work_identifier,),
        )
        source = await cursor.fetchone()
        if source is None or tuple(json.loads(str(source[0]))) not in _REFRESH_KINDS:
            raise ReviewInputError(
                "Item recovery requires an exact Facebook or eBay refresh work ID"
            )
        marketplace = _REFRESH_KINDS[tuple(json.loads(str(source[0])))]
        if source[1] not in ("completed", "terminal_failure"):
            raise ReviewInputError(
                "Wait for the refresh to settle before retrying its failed items"
            )
        cursor = await connection.execute(
            """SELECT active.id FROM work_items AS active JOIN work_items AS original
                ON active.kind_parts_json=original.kind_parts_json
                AND active.deduplication_identity_json=original.deduplication_identity_json
                WHERE original.id=? AND active.id<>original.id
                AND active.state IN ('pending','leased') LIMIT 1""",
            (request.refresh_work_identifier,),
        )
        if await cursor.fetchone() is not None:
            raise ReviewInputError("An equivalent refresh is already active; wait for it to settle")
        result = json.loads(str(source[2])) if source[2] is not None else {}
        refreshed = result.get("refreshed_search_run_record_identifier")
        if not isinstance(refreshed, str):
            raise ReviewInputError(
                "The refresh has no completed search to resume without searching"
            )
        item_kind = (
            ["carl", "ebay", "collect", "item"]
            if marketplace == "ebay"
            else ["carl", "facebook", "work", "collect_item"]
        )
        parameters = (
            encode_json(item_kind),
            request.refresh_work_identifier,
            encode_json(["carl", marketplace, "search_refresh", "item"]),
        )
        selection = """
            FROM work_items AS work
            WHERE work.kind_parts_json=? AND work.state='terminal_failure'
              AND EXISTS (
                  SELECT 1 FROM work_requests AS requester
                  WHERE requester.work_item_id=work.id
                    AND json_extract(requester.context_json,'$.search_refresh_work_identifier')=?
                    AND requester.requester_kind_parts_json=?
              )
        """
        retryable = """
            AND (
                json_extract(work.result_json,'$.acquisition.stopping_condition')='transport_failure'
                OR json_extract(work.error_json,'$.code') IN (
                    'proton_transport_invalidated','proton_session_not_available',
                    'decodo_session_not_available','decodo_session_duration_expired'
                )
            )
            AND COALESCE(json_extract(work.result_json,'$.acquisition.network_provider_result.http_status'),0)
                NOT IN (401,407)
            AND NOT EXISTS (
                SELECT 1 FROM work_items AS active
                WHERE active.kind_parts_json=work.kind_parts_json
                  AND active.deduplication_identity_json=work.deduplication_identity_json
                  AND active.state IN ('pending','leased')
            )
        """
        cursor = await connection.execute("SELECT COUNT(*) " + selection, parameters)
        matched_row = await cursor.fetchone()
        assert matched_row is not None
        matched = int(str(matched_row[0]))
        cursor = await connection.execute("SELECT COUNT(*) " + selection + retryable, parameters)
        retryable_row = await cursor.fetchone()
        assert retryable_row is not None
        eligible = int(str(retryable_row[0]))
        cursor = await connection.execute(
            "SELECT work.id,work.attempt "
            + selection
            + retryable
            + " ORDER BY work.created_at_utc_ns,work.id LIMIT ?",
            (*parameters, request.maximum_items),
        )
        rows = await cursor.fetchall()
        identifiers = tuple(str(row[0]) for row in rows)
        if identifiers:
            await connection.execute(
                """UPDATE work_items SET state='pending',eligible_at_utc_ns=?,result_json=NULL,error_json=NULL,
                    lease_token=NULL,lease_owner=NULL,lease_expires_at_utc_ns=NULL
                    WHERE id IN (SELECT value FROM json_each(?)) AND state='terminal_failure'""",
                (utc_ns, encode_json(list(identifiers))),
            )
            if await connection.changes() != len(identifiers):
                raise RuntimeError("Item recovery selection changed during its transaction")
            generation = new_identifier()
            checkpoint: dict[str, JsonValue] = {
                "stage": "collecting_items",
                "refreshed_search_run_record_identifier": refreshed,
                "item_retry_generation_identifier": generation,
                "prior_image_collection_count": int(result.get("prior_image_collection_count", 0))
                + int(result.get("new_image_collections", 0)),
            }
            await connection.execute(
                """UPDATE work_items SET state='pending',eligible_at_utc_ns=?,result_json=?,error_json=NULL,
                    lease_token=NULL,lease_owner=NULL,lease_expires_at_utc_ns=NULL WHERE id=?""",
                (utc_ns, encode_json(checkpoint), request.refresh_work_identifier),
            )
            events = [
                (
                    new_identifier(),
                    str(row[0]),
                    utc_ns,
                    encode_json(
                        {
                            "reason": {
                                "kind": "retry_item_failures",
                                "refresh_work_identifier": request.refresh_work_identifier,
                            },
                            "previous_attempt": int(str(row[1])),
                            "retry_budget_start_attempt": int(str(row[1])),
                        }
                    ),
                )
                for row in rows
            ]
            events.append(
                (
                    new_identifier(),
                    request.refresh_work_identifier,
                    utc_ns,
                    encode_json(
                        {
                            "reason": {
                                "kind": "resume_refresh_items_without_search",
                                "item_retry_generation_identifier": generation,
                            },
                            "retried": len(identifiers),
                        }
                    ),
                )
            )
            await connection.executemany(
                "INSERT INTO work_events(id,work_item_id,event_kind,recorded_at_utc_ns,data_json) VALUES (?,?,'enqueued',?,?)",
                tuple(events),
            )
    return RetryItemFailuresResult(
        refresh_work_identifier=request.refresh_work_identifier,
        marketplace=cast(Literal["facebook", "ebay"], marketplace),
        matched_terminal_failures=matched,
        retryable_terminal_failures=eligible,
        retried=len(identifiers),
        remaining_terminal_failures=matched - len(identifiers),
        refresh_resumed=bool(identifiers),
        retried_work_identifier_sample=identifiers[:20],
    )
