"""Atomic, evidence-scoped retry of retained terminal eBay image work."""

from collections.abc import Callable

from carl.core.ebay_items import COLLECT_EBAY_IMAGE_WORK_KIND
from carl.core.ebay_refresh import REFRESH_EBAY_SEARCH_WORK_KIND
from carl.core.json import encode_json
from carl.io.sqlite import Database


async def retry_ebay_image_failures(
    database: Database,
    source_identifier: str,
    maximum_items: int,
    utc_ns: int,
    new_identifier: Callable[[], str],
) -> tuple[int, tuple[str, ...]]:
    """Requeue bounded image failures, retaining attempts and immutable evidence.

    A refresh selects precisely its retained observations. A source search run
    selects its item identities and only image work bound to real observations
    and real gallery references for those identities. No provider is contacted.
    """

    if not source_identifier or maximum_items < 1 or utc_ns < 0:
        raise ValueError("Image retry selection arguments are invalid")
    # Database's serialized writer owns the transaction: selection, updates,
    # and events must not be split across independent public API transactions.
    async with database._connections.writer() as connection:  # pyright: ignore[reportPrivateUsage]
        cursor = await connection.execute(
            """
            WITH source_refresh AS (
                SELECT result_json FROM work_items
                WHERE id = ? AND kind_parts_json = ?
            ), refresh_observations AS (
                SELECT selected.value AS observation_id
                FROM source_refresh, json_each(
                    source_refresh.result_json, '$.observation_record_identifiers'
                ) AS selected
            ), run_members AS (
                SELECT json_extract(occurrence.value_json, '$.item_identifier') AS item_id
                FROM objects AS source
                JOIN records AS run ON run.object_id = source.id
                JOIN json_each(run.value_json, '$.listing_occurrence_record_identifiers') AS selected
                JOIN objects AS occurrence_object ON occurrence_object.id = selected.value
                JOIN records AS occurrence ON occurrence.object_id = occurrence_object.id
                WHERE source.id = ?
                  AND source.kind_parts_json = '["carl","ebay","search_run"]'
                  AND occurrence_object.kind_parts_json =
                      '["carl","ebay","search_listing_occurrence"]'
            )
            SELECT work.id, work.attempt, work_operation.operation_id,
                   count(*) OVER () AS matched
            FROM work_items AS work
            JOIN work_operations AS work_operation
              ON work_operation.work_item_id = work.id
             AND work_operation.attempt = work.attempt
            JOIN operations AS operation
              ON operation.id = work_operation.operation_id
             AND operation.result_json IS NOT NULL
            JOIN objects AS reference_object
              ON reference_object.id = json_extract(work.payload_json, '$.reference_record_identifier')
             AND reference_object.kind_parts_json = '["carl","ebay","gallery_image_reference"]'
            JOIN records AS reference ON reference.object_id = reference_object.id
            JOIN objects AS observation_object
              ON observation_object.id = json_extract(work.payload_json, '$.observation_record_identifier')
             AND observation_object.kind_parts_json = '["carl","ebay","listing_observation"]'
            JOIN records AS observation ON observation.object_id = observation_object.id
            WHERE work.kind_parts_json = ?
              AND work.state = 'terminal_failure'
              AND json_extract(reference.value_json, '$.observation_record_identifier') = observation_object.id
              AND json_extract(reference.value_json, '$.item_identifier') = json_extract(work.payload_json, '$.item_identifier')
              AND json_extract(observation.value_json, '$.item_identifier') = json_extract(work.payload_json, '$.item_identifier')
              AND json_extract(reference.value_json, '$.url') = json_extract(work.payload_json, '$.url')
              AND (
                  EXISTS (SELECT 1 FROM refresh_observations WHERE observation_id = observation_object.id)
                  OR (
                      NOT EXISTS (SELECT 1 FROM source_refresh)
                      AND EXISTS (SELECT 1 FROM run_members WHERE item_id = json_extract(work.payload_json, '$.item_identifier'))
                  )
              )
              AND NOT EXISTS (
                  SELECT 1 FROM work_items AS active
                  WHERE active.kind_parts_json = work.kind_parts_json
                    AND active.deduplication_identity_json = work.deduplication_identity_json
                    AND active.state IN ('pending', 'leased')
              )
            ORDER BY work.created_at_utc_ns, work.id
            LIMIT ?
            """,
            (
                source_identifier,
                encode_json(list(REFRESH_EBAY_SEARCH_WORK_KIND)),
                source_identifier,
                encode_json(list(COLLECT_EBAY_IMAGE_WORK_KIND)),
                maximum_items,
            ),
        )
        rows = await cursor.fetchall()
        if not rows:
            return 0, ()
        identifiers = tuple(str(row[0]) for row in rows)
        matched = int(str(rows[0][3]))
        _ = await connection.execute(
            """
            UPDATE work_items
            SET state = 'pending', eligible_at_utc_ns = ?,
                result_json = NULL, error_json = NULL,
                lease_token = NULL, lease_owner = NULL, lease_expires_at_utc_ns = NULL
            WHERE id IN (SELECT value FROM json_each(?)) AND state = 'terminal_failure'
            """,
            (utc_ns, encode_json(list(identifiers))),
        )
        if await connection.changes() != len(identifiers):
            raise RuntimeError("Terminal image work changed during bulk recovery")
        _ = await connection.executemany(
            """
            INSERT INTO work_events(id, work_item_id, event_kind, recorded_at_utc_ns, data_json)
            VALUES (?, ?, 'enqueued', ?, ?)
            """,
            tuple(
                (
                    new_identifier(),
                    str(row[0]),
                    utc_ns,
                    encode_json(
                        {
                            "reason": {
                                "kind": "retry_image_failures",
                                "source_identifier": source_identifier,
                            },
                            "previous_attempt": int(str(row[1])),
                            "recovered_checkpoint_operation_identifier": str(row[2]),
                        }
                    ),
                )
                for row in rows
            ),
        )
    return matched, identifiers
