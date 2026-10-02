"""Read-only composed projections across retained marketplace sources.

eBay identities are qualified; legacy unqualified identities remain Facebook IDs.
The adapter never changes evidence kinds or publishes derived records.
"""

# This adapter deliberately reuses ReviewApplication's existing projection primitives.
# pyright: reportImportCycles=false, reportPrivateUsage=false

from __future__ import annotations

import base64
import hashlib
from contextlib import suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from carl.core.composed_projection import (
    AnalysisApplicability,
    ComposedAnalysis,
    ComposedField,
    ComposedGallery,
    ComposedGalleryImage,
    ComposedListingPage,
    ComposedListingProjection,
    ComposedPreviewImage,
    ComposedStatus,
    GetComposedListingRequest,
    ListComposedSearchRequest,
    ListingStatus,
    ProjectionAnalysisDescriptor,
    ProjectionEvidence,
    ProjectionGalleryImageDescriptor,
    ProjectionSourceKind,
    SearchComparisonCoverage,
    SearchMembershipProjection,
    StatusObservationCandidate,
    composed_listing_matches_filters,
    projection_revision,
    select_status,
    status_candidate_from_item_observation,
    truncate_composed_gallery,
)
from carl.core.ebay_price import ebay_price_value
from carl.core.json import decode_json, encode_json
from carl.core.models import JsonValue
from carl.core.review import AnalysisDescriptor, CandidateAvailability, CandidateSource
from carl.core.review_workspace import ReviewWorkspace
from carl.io.sqlite import Database, _analysis_descriptor

if TYPE_CHECKING:
    from carl.review import ReviewApplication


@dataclass(frozen=True)
class _Record:
    identifier: str
    value: dict[str, JsonValue]
    rowid: int
    sequence: int
    acquisition_sequence: int
    operation: str
    completed_at: str | None


async def _boundary(database: Database) -> int:
    async with database._connections.reader() as connection:
        cursor = await connection.execute("SELECT COALESCE(MAX(rowid), 0) FROM objects")
        row = await cursor.fetchone()
    assert row is not None
    return int(row[0])


async def _records(
    database: Database,
    kind: tuple[str, ...],
    *,
    boundary: int,
    as_of: int,
    where: str = "1",
    parameters: tuple[str, ...] = (),
    limit: int = 10_001,
    order_by: str = "object.rowid DESC",
    partition_by: str | None = None,
) -> tuple[_Record, ...]:
    # SQL predicates are internal constants; source IDs are always bound parameters.
    state_predicate = "operation.state = 'completed'"
    if kind == ("carl", "ebay", "gallery_image_reference"):
        # Refresh checkpoints are immutable published evidence even when that
        # waiting attempt is subsequently released rather than completed.
        state_predicate = """(operation.state='completed' OR (
            operation.state='failed'
            AND json_extract(operation.error_json,'$.kind')='retry_scheduled'
            AND EXISTS (SELECT 1 FROM work_operations AS owner
              JOIN work_items AS coordinator ON coordinator.id=owner.work_item_id
              JOIN work_events AS release ON release.work_item_id=coordinator.id
                AND release.event_kind='released'
                AND json_extract(release.data_json,'$.operation_identifier')=operation.id
              JOIN operation_outputs AS published ON published.operation_id=operation.id
                AND published.object_id=object.id
                AND json_extract(published.name_parts_json,'$[0]')='gallery_reference'
              WHERE owner.operation_id=operation.id
                AND coordinator.kind_parts_json='["carl","ebay","work","refresh_search"]'
            )))"""
    partition_rank = (
        f", ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY {order_by}) AS partition_rank"
        if partition_by is not None
        else ""
    )
    query = f"""
            SELECT object.id, record.value_json, object.rowid AS object_rowid,
                   COALESCE((SELECT MAX(event.sequence)
                     FROM work_operations AS work
                     JOIN work_events AS event ON event.work_item_id = work.work_item_id
                     WHERE work.operation_id = object.created_by_operation_id
                       AND event.event_kind IN ('completed','released','terminal_failure')
                       AND json_extract(event.data_json,'$.operation_identifier')=object.created_by_operation_id), 0),
                   object.created_by_operation_id, operation.ended_at_utc,
                   COALESCE((SELECT MAX(event.sequence)
                     FROM objects AS acquisition
                     JOIN work_operations AS work ON work.operation_id=acquisition.created_by_operation_id
                     JOIN work_events AS event ON event.work_item_id=work.work_item_id
                       AND event.event_kind='completed'
                     WHERE acquisition.id=json_extract(record.value_json,'$.acquisition_record_identifier')), 0)
                   {partition_rank}
            FROM objects AS object
            JOIN records AS record ON record.object_id = object.id
            JOIN operations AS operation ON operation.id = object.created_by_operation_id
            WHERE object.kind_parts_json = ? AND object.rowid <= ?
              AND {state_predicate} AND ({where})
              AND COALESCE((SELECT MAX(event.sequence)
                FROM work_operations AS work
                JOIN work_events AS event ON event.work_item_id = work.work_item_id
                WHERE work.operation_id = object.created_by_operation_id
                  AND event.event_kind IN ('completed','released','terminal_failure')
                  AND json_extract(event.data_json,'$.operation_identifier')=object.created_by_operation_id), 0) <= ?
            """
    if partition_by is None:
        query += f" ORDER BY {order_by} LIMIT ?"
    else:
        query = f"SELECT * FROM ({query}) WHERE partition_rank <= ? ORDER BY object_rowid DESC"
    async with database._connections.reader() as connection:
        cursor = await connection.execute(
            query,
            (encode_json(list(kind)), boundary, *parameters, as_of, limit),
        )
        rows = await cursor.fetchall()
    result: list[_Record] = []
    for row in reversed(rows):
        value = decode_json(str(row[1]))
        if isinstance(value, dict):
            result.append(
                _Record(
                    str(row[0]),
                    cast(dict[str, JsonValue], value),
                    int(cast(int, row[2])),
                    int(cast(int, row[3])),
                    int(cast(int, row[6])),
                    str(row[4]),
                    cast(str | None, row[5]),
                )
            )
    return tuple(result)


async def expand_search_runs(
    database: Database,
    roots: tuple[str, ...],
    *,
    as_of_completion_sequence: int,
    maximum_runs: int = 100,
    maximum_object_rowid: int | None = None,
) -> tuple[str, ...]:
    """Expand groups into completed enabled-target runs, without provider access."""

    result: list[str] = []
    boundary = await _boundary(database) if maximum_object_rowid is None else maximum_object_rowid
    pending = list(roots)
    visited: set[str] = set()
    while pending and len(result) < maximum_runs:
        root = pending.pop(0)
        if root in visited:
            continue
        visited.add(root)
        async with database._connections.reader() as connection:
            object_cursor = await connection.execute(
                "SELECT rowid FROM objects WHERE id=?", (root,)
            )
            object_row = await object_cursor.fetchone()
        if object_row is None:
            raise KeyError(root)
        if int(cast(int, object_row[0])) > boundary:
            raise ValueError("Search scope source lies after the cursor snapshot")
        kind, _, _ = await database.get_record(root)
        if kind in (("carl", "facebook", "search_run"), ("carl", "ebay", "search_run")):
            result.append(root)
            async with database._connections.reader() as connection:
                parent_cursor = await connection.execute(
                    """
                    SELECT json_extract(work.payload_json,'$.base_search_run_record_identifier')
                    FROM work_items AS work JOIN work_events AS event ON event.work_item_id=work.id
                      AND event.event_kind='completed' AND event.sequence<=?
                    WHERE work.kind_parts_json IN (?,?)
                      AND json_extract(work.result_json,'$.refreshed_search_run_record_identifier')=?
                    ORDER BY event.sequence DESC LIMIT 1
                    """,
                    (
                        as_of_completion_sequence,
                        encode_json(["carl", "facebook", "work", "refresh_search"]),
                        encode_json(["carl", "ebay", "work", "refresh_search"]),
                        root,
                    ),
                )
                parent_row = await parent_cursor.fetchone()
            if parent_row and isinstance(parent_row[0], str):
                pending.append(parent_row[0])
            continue
        if kind != ("carl", "marketplace", "search"):
            raise ValueError("Projection source is not a search run or search group")
        states = await _records(
            database,
            ("carl", "marketplace", "search_target_state"),
            boundary=boundary,
            as_of=as_of_completion_sequence,
            where="json_extract(record.value_json, '$.search_record_identifier') = ?",
            parameters=(root,),
            limit=10_000,
        )
        enabled = {r.value.get("target_record_identifier"): r.value.get("enabled") for r in states}
        executions = await _records(
            database,
            ("carl", "marketplace", "search_execution"),
            boundary=boundary,
            as_of=as_of_completion_sequence,
            where="json_extract(record.value_json, '$.search_record_identifier') = ?",
            parameters=(root,),
            limit=10_000,
        )
        for execution in reversed(executions):
            if enabled.get(execution.value.get("target_record_identifier"), True) is False:
                continue
            work_id = execution.value.get("work_identifier")
            if not isinstance(work_id, str):
                continue
            async with database._connections.reader() as connection:
                work_cursor = await connection.execute(
                    """SELECT work.result_json FROM work_items AS work
                    JOIN work_events AS event ON event.work_item_id=work.id AND event.event_kind='completed'
                    WHERE work.id=? AND event.sequence<=? ORDER BY event.sequence LIMIT 1""",
                    (work_id, as_of_completion_sequence),
                )
                work_row = await work_cursor.fetchone()
            value = decode_json(str(work_row[0])) if work_row else None
            if isinstance(value, dict):
                run = cast(dict[str, JsonValue], value).get("search_run_record_identifier")
                if isinstance(run, str):
                    pending.append(run)
    return tuple(dict.fromkeys(result))[:maximum_runs]


async def scope_contains_ebay(database: Database, roots: tuple[str, ...]) -> bool:
    """Whether a scope requires the marketplace adapter (groups included)."""
    for identifier in roots:
        kind, _, _ = await database.get_record(identifier)
        if kind in (("carl", "ebay", "search_run"), ("carl", "marketplace", "search")):
            return True
    return False


async def run_listing_identifiers(database: Database, run_identifier: str) -> tuple[str, ...]:
    """Return exact immutable membership, qualifying only eBay identities."""
    kind, _, value = await database.get_record(run_identifier)
    if kind == ("carl", "facebook", "search_run") and isinstance(value, dict):
        traversal = cast(dict[str, JsonValue], value).get("traversal")
        raw_identifiers = (
            cast(dict[str, JsonValue], traversal).get("unique_listing_identifiers")
            if isinstance(traversal, dict)
            else None
        )
        if not isinstance(raw_identifiers, list) or not all(
            isinstance(item, str) for item in cast(list[JsonValue], raw_identifiers)
        ):
            raise ValueError("Retained Facebook search membership is malformed")
        return tuple(cast(list[str], raw_identifiers))
    if kind == ("carl", "ebay", "search_run"):
        records = await _records(
            database,
            ("carl", "ebay", "search_listing_occurrence"),
            boundary=await _boundary(database),
            as_of=await database.current_completion_boundary(),
            where="json_extract(record.value_json, '$.search_run_record_identifier') = ?",
            parameters=(run_identifier,),
            limit=100_001,
        )
        if len(records) > 100_000:
            raise ValueError("Retained eBay membership exceeds the supported safety bound")
        return tuple(
            dict.fromkeys(
                "ebay:" + str(record.value["item_identifier"])
                for record in records
                if isinstance(record.value.get("item_identifier"), str)
            )
        )
    if kind == ("carl", "marketplace", "search"):
        runs = await expand_search_runs(
            database,
            (run_identifier,),
            as_of_completion_sequence=await database.current_completion_boundary(),
        )
        identifiers: list[str] = []
        for run in runs:
            identifiers.extend(await run_listing_identifiers(database, run))
        return tuple(dict.fromkeys(identifiers))
    raise ValueError("Membership source is not a supported search run or group")


async def ebay_analysis_descriptor(
    database: Database, record_identifier: str
) -> AnalysisDescriptor:
    """Read an exact retained analysis attempt, including failure outcomes."""
    async with database._connections.reader() as connection:
        cursor = await connection.execute(
            """
            SELECT analysis.id, report.value_json, work.payload_json,
                   input.object_id, outcome.sequence, operation.ended_at_utc
            FROM objects AS analysis JOIN records AS report ON report.object_id=analysis.id
            JOIN operations AS operation ON operation.id=analysis.created_by_operation_id
            JOIN work_operations AS link ON link.operation_id=operation.id
            JOIN work_items AS work ON work.id=link.work_item_id
            JOIN objects AS evidence ON evidence.id=json_extract(work.payload_json,'$.evidence_set_record_identifier')
            JOIN operation_inputs AS input ON input.operation_id=evidence.created_by_operation_id
              AND input.name_parts_json=?
            JOIN work_events AS outcome ON outcome.work_item_id=work.id
              AND outcome.event_kind IN ('completed','released','terminal_failure')
              AND json_extract(outcome.data_json,'$.operation_identifier')=operation.id
            WHERE analysis.kind_parts_json=? AND analysis.id=?
            ORDER BY outcome.sequence DESC LIMIT 1
            """,
            (
                encode_json(["listing_observation"]),
                encode_json(["carl", "ebay", "item_analysis"]),
                record_identifier,
            ),
        )
        row = await cursor.fetchone()
    if row is None:
        raise KeyError(record_identifier)
    return _analysis_descriptor(row)


def _evidence(record: _Record, source: ProjectionSourceKind) -> ProjectionEvidence:
    acquisition = record.value.get("acquisition_record_identifier")
    return ProjectionEvidence(
        evidence_record_identifier=record.identifier,
        observation_record_identifier=record.identifier,
        acquisition_record_identifier=acquisition if isinstance(acquisition, str) else None,
        producing_operation_identifier=record.operation,
        acquisition_completion_sequence=record.acquisition_sequence or record.sequence,
        observation_completion_sequence=record.sequence,
        completed_at_utc=record.completed_at,
        source_kind=source,
    )


def _text(value: JsonValue) -> str | None:
    return value if isinstance(value, str) else None


async def _ebay_records(
    database: Database,
    kind: str,
    item: str,
    *,
    boundary: int,
    as_of: int,
    limit: int = 101,
) -> tuple[_Record, ...]:
    return await _records(
        database,
        ("carl", "ebay", kind),
        boundary=boundary,
        as_of=as_of,
        where="json_extract(record.value_json, '$.item_identifier') = ?",
        parameters=(item,),
        limit=limit,
        order_by="COALESCE((SELECT acquisition.rowid FROM objects AS acquisition WHERE acquisition.id=json_extract(record.value_json,'$.acquisition_record_identifier')),object.rowid) DESC, object.rowid DESC"
        if kind in ("listing_observation", "search_listing_occurrence")
        else "object.rowid DESC",
    )


async def _analyses(
    database: Database,
    observations: tuple[_Record, ...],
    *,
    boundary: int,
    as_of: int,
    maximum_results: int = 10_001,
) -> tuple[AnalysisDescriptor, ...]:
    identifiers = encode_json([record.identifier for record in observations])
    async with database._connections.reader() as connection:
        cursor = await connection.execute(
            """
            SELECT analysis.id, report.value_json, work.payload_json,
                   input.object_id, outcome.sequence, operation.ended_at_utc
            FROM objects AS analysis JOIN records AS report ON report.object_id=analysis.id
            JOIN operations AS operation ON operation.id=analysis.created_by_operation_id
            JOIN work_operations AS link ON link.operation_id=operation.id
            JOIN work_items AS work ON work.id=link.work_item_id
            JOIN objects AS evidence ON evidence.id=json_extract(work.payload_json,'$.evidence_set_record_identifier')
            JOIN operation_inputs AS input ON input.operation_id=evidence.created_by_operation_id
              AND input.name_parts_json=?
            JOIN work_events AS outcome ON outcome.work_item_id=work.id
              AND outcome.event_kind IN ('completed','released','terminal_failure')
              AND json_extract(outcome.data_json,'$.operation_identifier')=operation.id
            WHERE analysis.kind_parts_json=? AND analysis.rowid <= ?
              AND outcome.sequence<=? AND input.object_id IN (SELECT value FROM json_each(?))
            ORDER BY outcome.sequence DESC, analysis.id DESC LIMIT ?
            """,
            (
                encode_json(["listing_observation"]),
                encode_json(["carl", "ebay", "item_analysis"]),
                boundary,
                as_of,
                identifiers,
                maximum_results,
            ),
        )
        rows = await cursor.fetchall()
    if len(rows) >= maximum_results:
        raise ValueError("Retained analysis history exceeds the supported safety bound")
    return tuple(_analysis_descriptor(row) for row in reversed(rows))


@dataclass(frozen=True)
class _EbayProjectionEvidence:
    observations: tuple[_Record, ...]
    cards: tuple[_Record, ...]
    sold_cards: tuple[_Record, ...]
    scoped_cards: tuple[_Record, ...]
    acquisition_rows: dict[str, int]
    references: tuple[_Record, ...]
    image_results: tuple[_Record, ...]
    descriptions: tuple[_Record, ...]
    analyses: tuple[AnalysisDescriptor, ...]
    work_states: dict[str, str]


async def _ebay_bulk_evidence(
    database: Database,
    items: tuple[str, ...],
    runs: tuple[str, ...],
    *,
    boundary: int,
    as_of: int,
) -> dict[str, _EbayProjectionEvidence]:
    """Read one bounded item chunk without a query for each listing or gallery image."""
    item_json = encode_json(list(items))
    item_field = "json_extract(record.value_json, '$.item_identifier')"
    item_where = f"{item_field} IN (SELECT value FROM json_each(?))"
    acquisition_order = (
        "COALESCE((SELECT acquisition.rowid FROM objects AS acquisition "
        "WHERE acquisition.id=json_extract(record.value_json,'$.acquisition_record_identifier')),"
    )
    acquisition_order += "object.rowid) DESC, object.rowid DESC"

    async def histories(
        kind: str, *, sold: bool = False, scoped: bool = False
    ) -> tuple[_Record, ...]:
        where = item_where
        parameters = (item_json,)
        if sold:
            where += " AND json_extract(record.value_json, '$.listing_state') = 'sold'"
        if scoped:
            where += " AND json_extract(record.value_json, '$.search_run_record_identifier') IN (SELECT value FROM json_each(?))"
            parameters += (encode_json(list(runs)),)
        return await _records(
            database,
            ("carl", "ebay", kind),
            boundary=boundary,
            as_of=as_of,
            where=where,
            parameters=parameters,
            limit=1 if sold else 101,
            order_by="object.rowid DESC" if scoped else acquisition_order,
            partition_by=item_field,
        )

    observations = await histories("listing_observation")
    cards = await histories("search_listing_occurrence")
    sold_cards = await histories("search_listing_occurrence", sold=True)
    scoped_cards = await histories("search_listing_occurrence", scoped=True) if runs else ()

    def by_item(records: tuple[_Record, ...]) -> dict[str, tuple[_Record, ...]]:
        grouped: dict[str, list[_Record]] = {}
        for record in records:
            item = record.value.get("item_identifier")
            if isinstance(item, str):
                grouped.setdefault(item, []).append(record)
        return {item: tuple(values) for item, values in grouped.items()}

    observation_groups = by_item(observations)
    card_groups = by_item(cards)
    sold_groups = by_item(sold_cards)
    scoped_groups = by_item(scoped_cards)
    acquisitions = tuple(
        dict.fromkeys(
            acquisition
            for record in (*observations, *cards, *sold_cards)
            if isinstance(acquisition := record.value.get("acquisition_record_identifier"), str)
        )
    )
    acquisition_rows: dict[str, int] = {}
    if acquisitions:
        async with database._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT id, rowid FROM objects WHERE id IN (SELECT value FROM json_each(?))",
                (encode_json(list(acquisitions)),),
            )
            acquisition_rows = {
                str(row[0]): int(cast(int, row[1])) for row in await cursor.fetchall()
            }
    item_acquisition_rows: dict[str, dict[str, int]] = {}
    equivalent_by_item: dict[str, tuple[_Record, ...]] = {}
    for item in items:
        rows = dict(acquisition_rows)
        for record in (
            *observation_groups.get(item, ()),
            *card_groups.get(item, ()),
            *sold_groups.get(item, ()),
        ):
            acquisition = record.value.get("acquisition_record_identifier")
            if isinstance(acquisition, str) and acquisition not in acquisition_rows:
                rows[acquisition] = record.rowid
        item_acquisition_rows[item] = rows
        usable = tuple(
            record
            for record in observation_groups.get(item, ())
            if record.value.get("classification") == "detail"
        )
        if usable:
            detail = max(
                usable,
                key=lambda r: (
                    rows.get(str(r.value.get("acquisition_record_identifier")), r.rowid),
                    r.rowid,
                ),
            )
            equivalent_by_item[item] = tuple(
                record
                for record in usable
                if record.value.get("acquisition_record_identifier")
                == detail.value.get("acquisition_record_identifier")
            )

    observation_items = {
        record.identifier: item
        for item, records in equivalent_by_item.items()
        for record in records
    }
    equivalent_json = encode_json(list(observation_items))
    # Derive grouping from the retained observation, not optional item fields on a followup.
    observation_item = "(SELECT json_extract(source.value_json, '$.item_identifier') FROM records AS source WHERE source.object_id=json_extract(record.value_json,'$.observation_record_identifier'))"

    async def followups(kind: str, limit: int) -> dict[str, tuple[_Record, ...]]:
        if not observation_items:
            return {}
        records = await _records(
            database,
            ("carl", "ebay", kind),
            boundary=boundary,
            as_of=as_of,
            where="json_extract(record.value_json, '$.observation_record_identifier') IN (SELECT value FROM json_each(?))",
            parameters=(equivalent_json,),
            limit=limit,
            partition_by=observation_item,
        )
        grouped: dict[str, list[_Record]] = {}
        for record in records:
            item = observation_items.get(str(record.value.get("observation_record_identifier")))
            if item is not None:
                grouped.setdefault(item, []).append(record)
        return {item: tuple(values) for item, values in grouped.items()}

    references = await followups("gallery_image_reference", 5100)
    image_results = await followups("image_result", 5100)
    descriptions = await followups("description_result", 101)
    all_analyses = (
        await _analyses(
            database,
            observations,
            boundary=boundary,
            as_of=as_of,
            maximum_results=10_000 * len(items) + 1,
        )
        if observations
        else ()
    )
    item_by_observation = {
        record.identifier: str(record.value.get("item_identifier")) for record in observations
    }
    analysis_groups: dict[str, list[AnalysisDescriptor]] = {}
    for analysis in all_analyses:
        item = item_by_observation.get(analysis.listing_observation_record_identifier)
        if item is not None:
            analysis_groups.setdefault(item, []).append(analysis)
    if any(len(group) > 10_000 for group in analysis_groups.values()):
        raise ValueError("Retained analysis history exceeds the supported safety bound")
    work_identifiers = tuple(
        dict.fromkeys(
            identifier
            for records in references.values()
            for record in records
            if isinstance(identifier := record.value.get("work_identifier"), str)
        )
    )
    work_states: dict[str, str] = {}
    if work_identifiers:
        async with database._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT id, state FROM work_items WHERE id IN (SELECT value FROM json_each(?))",
                (encode_json(list(work_identifiers)),),
            )
            work_states = {str(row[0]): str(row[1]) for row in await cursor.fetchall()}
    return {
        item: _EbayProjectionEvidence(
            observations=observation_groups.get(item, ()),
            cards=card_groups.get(item, ()),
            sold_cards=sold_groups.get(item, ()),
            scoped_cards=scoped_groups.get(item, ()),
            acquisition_rows=item_acquisition_rows[item],
            references=references.get(item, ()),
            image_results=image_results.get(item, ()),
            descriptions=descriptions.get(item, ()),
            analyses=tuple(analysis_groups.get(item, ())),
            work_states=work_states,
        )
        for item in items
    }


async def _ebay_projection(
    database: Database,
    request: GetComposedListingRequest,
    runs: tuple[str, ...],
    *,
    boundary: int,
    as_of: int,
    prefetched: _EbayProjectionEvidence | None = None,
) -> ComposedListingProjection:
    item = request.listing_identifier.removeprefix("ebay:")
    observations = (
        prefetched.observations
        if prefetched is not None
        else await _ebay_records(
            database, "listing_observation", item, boundary=boundary, as_of=as_of
        )
    )
    cards = (
        prefetched.cards
        if prefetched is not None
        else await _ebay_records(
            database, "search_listing_occurrence", item, boundary=boundary, as_of=as_of
        )
    )
    sold_cards = (
        prefetched.sold_cards
        if prefetched is not None
        else await _records(
            database,
            ("carl", "ebay", "search_listing_occurrence"),
            boundary=boundary,
            as_of=as_of,
            where="json_extract(record.value_json, '$.item_identifier') = ? AND json_extract(record.value_json, '$.listing_state') = 'sold'",
            parameters=(item,),
            limit=1,
            order_by="COALESCE((SELECT acquisition.rowid FROM objects AS acquisition WHERE acquisition.id=json_extract(record.value_json,'$.acquisition_record_identifier')),object.rowid) DESC, object.rowid DESC",
        )
    )
    # Acquisition ordering, not offline re-extraction time, determines freshness.
    acquisition_rows: dict[str, int] = prefetched.acquisition_rows if prefetched is not None else {}
    for observation in () if prefetched is not None else (*observations, *cards, *sold_cards):
        acquisition = observation.value.get("acquisition_record_identifier")
        if isinstance(acquisition, str):
            async with database._connections.reader() as connection:
                cursor = await connection.execute(
                    "SELECT rowid FROM objects WHERE id = ?", (acquisition,)
                )
                row = await cursor.fetchone()
            acquisition_rows[acquisition] = int(row[0]) if row else observation.rowid
    observations = tuple(
        sorted(
            observations,
            key=lambda r: (
                acquisition_rows.get(str(r.value.get("acquisition_record_identifier")), r.rowid),
                r.rowid,
            ),
        )
    )
    cards = tuple(
        sorted(
            cards,
            key=lambda r: (
                acquisition_rows.get(str(r.value.get("acquisition_record_identifier")), r.rowid),
                r.rowid,
            ),
        )
    )
    latest = observations[-1] if observations else None
    usable = tuple(
        record for record in observations if record.value.get("classification") == "detail"
    )
    detail = usable[-1] if usable else None
    card = cards[-1] if cards else None
    if not observations and card is None:
        raise KeyError(request.listing_identifier)
    status_candidates = tuple(
        record
        for record in observations
        if record.value.get("classification") in ("detail", "unavailable")
    ) + ((card,) if card else ())
    status_record = max(
        status_candidates,
        key=lambda r: (
            acquisition_rows.get(str(r.value.get("acquisition_record_identifier")), r.rowid),
            r.rowid,
        ),
        default=latest,
    )
    assert status_record is not None
    sold_card = sold_cards[-1] if sold_cards else None
    # A generic unavailable item page confirms closure, not absence of a sale.
    # Retain the latest card's positive sold evidence through detail refreshes.
    if (
        status_record.value.get("classification") == "unavailable"
        and card is not None
        and card.value.get("listing_state") == "sold"
        and not any(
            (
                acquisition_rows.get(
                    str(record.value.get("acquisition_record_identifier")), record.rowid
                ),
                record.rowid,
            )
            > (
                acquisition_rows.get(
                    str(card.value.get("acquisition_record_identifier")), card.rowid
                ),
                card.rowid,
            )
            for record in usable
        )
    ):
        status_record = card
    status = ComposedStatus(
        value=(
            ListingStatus.SOLD
            if status_record.value.get("listing_state") == "sold"
            else ListingStatus.UNAVAILABLE
            if status_record.value.get("classification") == "unavailable"
            or status_record.value.get("listing_state") == "completed"
            else ListingStatus.UNKNOWN
            if status_record is card
            and "listing_state" in status_record.value
            and status_record.value["listing_state"] is None
            else ListingStatus.AVAILABLE
            if detail or card
            else ListingStatus.UNKNOWN
        ),
        raw_flags={
            "response_classification": status_record.value.get("classification")
            if status_record not in cards
            else "search_card"
        },
        evidence=_evidence(
            status_record,
            ProjectionSourceKind.SEARCH_CARD
            if status_record in cards
            else ProjectionSourceKind.ITEM_PAGE,
        ),
    )

    def scalar(key: str) -> ComposedField | None:
        for record, source_kind in (
            *((record, ProjectionSourceKind.ITEM_PAGE) for record in reversed(usable)),
            (card, ProjectionSourceKind.SEARCH_CARD),
        ):
            if (
                key == "displayed_price"
                and record in cards
                and record.value.get("listing_state") != "active"
                and "listing_state" in record.value
            ):
                continue
            if record and record.value.get(key) is not None:
                return ComposedField(
                    value=(
                        ebay_price_value(record.value[key], currency=record.value.get("currency"))
                        if key == "displayed_price"
                        else record.value[key]
                    ),
                    evidence=_evidence(record, source_kind),
                )
        return None

    description = scalar("description")
    gallery = None
    if detail:
        acquisition = detail.value.get("acquisition_record_identifier")
        equivalent = tuple(
            record
            for record in usable
            if record.value.get("acquisition_record_identifier") == acquisition
        )
        ids = encode_json([record.identifier for record in equivalent])
        references = (
            prefetched.references
            if prefetched is not None
            else await _records(
                database,
                ("carl", "ebay", "gallery_image_reference"),
                boundary=boundary,
                as_of=as_of,
                where="json_extract(record.value_json, '$.observation_record_identifier') IN (SELECT value FROM json_each(?))",
                parameters=(ids,),
                limit=5100,
            )
        )
        results = (
            prefetched.image_results
            if prefetched is not None
            else await _records(
                database,
                ("carl", "ebay", "image_result"),
                boundary=boundary,
                as_of=as_of,
                where="json_extract(record.value_json, '$.observation_record_identifier') IN (SELECT value FROM json_each(?))",
                parameters=(ids,),
                limit=5100,
            )
        )
        descriptions = (
            prefetched.descriptions
            if prefetched is not None
            else await _records(
                database,
                ("carl", "ebay", "description_result"),
                boundary=boundary,
                as_of=as_of,
                where="json_extract(record.value_json, '$.observation_record_identifier') IN (SELECT value FROM json_each(?))",
                parameters=(ids,),
                limit=101,
            )
        )
        for record in descriptions:
            if record.value.get("state") == "saved" and (
                record.value.get("observation_record_identifier") == detail.identifier
                or record.value.get("url") == detail.value.get("description_url")
            ):
                description = ComposedField(
                    value=record.value.get("description"),
                    evidence=_evidence(record, ProjectionSourceKind.ITEM_PAGE),
                )
        urls = detail.value.get("gallery_urls")
        urls = (
            tuple(url for url in cast(list[JsonValue], urls) if isinstance(url, str))
            if isinstance(urls, list)
            else ()
        )
        images: list[ComposedGalleryImage] = []
        for index, url in enumerate(urls):
            reference = next((r for r in reversed(references) if r.value.get("url") == url), None)
            valid_references = {r.identifier for r in references if r.value.get("url") == url}
            matching = tuple(
                r
                for r in results
                if r.value.get("url") == url
                and r.value.get("reference_record_identifier") in valid_references
            )
            saved = tuple(r for r in matching if r.value.get("state") == "saved")
            result = saved[-1] if saved else matching[-1] if matching else None
            value = result.value if result else {}
            download_state = str(value.get("state", "not_saved"))
            if not result and reference and isinstance(reference.value.get("work_identifier"), str):
                if prefetched is not None:
                    download_state = prefetched.work_states.get(
                        str(reference.value["work_identifier"]), "not_saved"
                    )
                else:
                    with suppress(KeyError):
                        download_state = str(
                            (await database.work(str(reference.value["work_identifier"]))).get(
                                "state", "not_saved"
                            )
                        )
            descriptor = ProjectionGalleryImageDescriptor(
                gallery_order=index,
                gallery_reference_record_identifier=reference.identifier if reference else None,
                original_url=url,
                photo_identifier=None,
                declared_width=None,
                declared_height=None,
                image_result_record_identifier=result.identifier if result else None,
                image_artifact_identifier=_text(value.get("image_artifact_identifier")),
                download_state=download_state,
                sha256=_text(value.get("sha256")),
                media_type=_text(value.get("media_type")),
                width=value.get("width"),
                height=value.get("height"),
            )
            images.append(
                ComposedGalleryImage(
                    descriptor=descriptor,
                    evidence=_evidence(result, ProjectionSourceKind.IMAGE_DOWNLOAD)
                    if result
                    else None,
                )
            )
        saved_count = sum(image.descriptor.download_state == "saved" for image in images)
        gallery = ComposedGallery(
            referenced_image_count=len(images),
            saved_image_count=saved_count,
            all_referenced_images_saved=saved_count == len(images),
            images=tuple(images),
            images_truncated=False,
            reference_set_truncated=False,
            reference_set_evidence=_evidence(detail, ProjectionSourceKind.ITEM_PAGE),
        )
    preview = None
    if card and isinstance(card.value.get("image_url"), str):
        preview = ComposedPreviewImage(
            descriptor=ProjectionGalleryImageDescriptor(
                gallery_order=0,
                gallery_reference_record_identifier=None,
                original_url=str(card.value["image_url"]),
                photo_identifier=None,
                declared_width=None,
                declared_height=None,
                image_result_record_identifier=None,
                image_artifact_identifier=None,
                download_state="not_saved",
                sha256=None,
                media_type=None,
                width=None,
                height=None,
            ),
            evidence=_evidence(card, ProjectionSourceKind.SEARCH_CARD),
        )
    analyses = (
        prefetched.analyses
        if prefetched is not None
        else await _analyses(database, observations, boundary=boundary, as_of=as_of)
    )
    analyses = tuple(
        a
        for a in reversed(analyses)
        if a.state == "completed"
        and (
            request.product_guide_record_identifier is None
            or a.product_guide_record_identifier == request.product_guide_record_identifier
        )
    )
    composed_analyses = tuple(
        ComposedAnalysis(
            descriptor=ProjectionAnalysisDescriptor.model_validate(a.model_dump()),
            applicability=AnalysisApplicability.EXACT
            if detail and a.listing_observation_record_identifier == detail.identifier
            else AnalysisApplicability.STALE,
            applicability_reasons=()
            if detail and a.listing_observation_record_identifier == detail.identifier
            else ("different_listing_observation",),
        )
        for a in analyses
    )
    scoped_cards = (
        prefetched.scoped_cards
        if prefetched is not None
        else await _records(
            database,
            ("carl", "ebay", "search_listing_occurrence"),
            boundary=boundary,
            as_of=as_of,
            where="json_extract(record.value_json, '$.item_identifier') = ? AND json_extract(record.value_json, '$.search_run_record_identifier') IN (SELECT value FROM json_each(?))",
            parameters=(item, encode_json(list(runs))),
            limit=101,
        )
        if runs
        else ()
    )
    membership = None
    if runs and scoped_cards:
        first, last = scoped_cards[0], scoped_cards[-1]
        membership = SearchMembershipProjection(
            lineage_root_search_run_record_identifier=None,
            selected_search_run_record_identifier=runs[0],
            oldest_included_search_run_record_identifier=runs[-1],
            included_ancestry_run_count=len(runs),
            older_ancestry_truncated=False,
            first_seen_search_run_record_identifier=str(
                first.value["search_run_record_identifier"]
            ),
            last_seen_search_run_record_identifier=str(last.value["search_run_record_identifier"]),
            first_seen_at_utc=first.completed_at,
            last_seen_at_utc=last.completed_at,
            seen_in_selected_run=any(
                r.value.get("search_run_record_identifier") == runs[0] for r in scoped_cards
            ),
            seen_run_count=len(
                {str(r.value.get("search_run_record_identifier")) for r in scoped_cards}
            ),
            selected_run_stopping_reason=None,
            comparison_coverage=SearchComparisonCoverage.BOUNDED,
            absence_comparison_valid=False,
        )
    title, price = scalar("title"), scalar("displayed_price")
    condition, shipping = scalar("condition"), scalar("shipping_text")
    last_sale = (
        ComposedField(
            value={
                key: sold_card.value.get(key)
                for key in ("sold_price", "sold_date", "sold_date_text", "sold_price_status")
            }
            | (
                {"sold_price_value": normalized_sold_price}
                if isinstance(
                    normalized_sold_price := ebay_price_value(sold_card.value.get("sold_price")),
                    dict,
                )
                else {}
            ),
            evidence=_evidence(sold_card, ProjectionSourceKind.SEARCH_CARD),
        )
        if sold_card
        else None
    )
    revision = projection_revision(
        listing_identifier=request.listing_identifier,
        status=status,
        title=title,
        price=price,
        location=None,
        description=description,
        seller=None,
        last_sale=last_sale,
        condition=condition,
        shipping=shipping,
        preview_image=preview,
        gallery=gallery,
        analyses=composed_analyses[:20],
        analyses_truncated=len(composed_analyses) > 20,
        search_membership=membership,
    )
    return ComposedListingProjection(
        listing_identifier=request.listing_identifier,
        canonical_source_url=f"https://www.ebay.com/itm/{item}",
        as_of_completion_sequence=as_of,
        projection_revision=revision,
        status=status,
        title=title,
        price=price,
        location=None,
        description=description,
        seller=None,
        last_sale=last_sale,
        condition=condition,
        shipping=shipping,
        preview_image=preview,
        gallery=truncate_composed_gallery(gallery, maximum_images=request.maximum_gallery_images),
        analyses=composed_analyses[: request.maximum_analyses],
        analyses_truncated=len(composed_analyses) > request.maximum_analyses,
        search_membership=membership,
        warnings=tuple(
            warning
            for condition, warning in (
                (len(observations) > 100, "item_observation_history_truncated"),
                (
                    latest is not None
                    and latest.value.get("classification") not in ("detail", "unavailable"),
                    "latest_item_response_" + str(latest.value.get("classification"))
                    if latest
                    else "",
                ),
            )
            if condition
        ),
    )


async def get_marketplace_composed_listing(
    application: ReviewApplication,
    request: GetComposedListingRequest,
) -> ComposedListingProjection:
    if request.product_guide_record_identifier is not None:
        _ = await application._active_product_guide(request.product_guide_record_identifier)
    as_of = await application.database.current_completion_boundary()
    runs = await expand_search_runs(
        application.database,
        (request.search_run_record_identifier, *request.additional_search_run_record_identifiers)
        if request.search_run_record_identifier
        else (),
        as_of_completion_sequence=as_of,
        maximum_runs=request.maximum_ancestry_runs,
    )
    if request.listing_identifier.startswith("ebay:"):
        projection = await _ebay_projection(
            application.database,
            request,
            runs,
            boundary=await _boundary(application.database),
            as_of=as_of,
        )
        if runs and projection.search_membership is None:
            raise KeyError(request.listing_identifier)
        return projection
    facebook_runs = tuple(
        [
            r
            for r in runs
            if (await application.database.get_record(r))[0] == ("carl", "facebook", "search_run")
        ]
    )
    if runs and not facebook_runs:
        raise KeyError(request.listing_identifier)
    return await application.get_composed_listing(
        request.model_copy(
            update={
                "search_run_record_identifier": facebook_runs[0] if facebook_runs else None,
                "additional_search_run_record_identifiers": facebook_runs[1:],
            }
        )
    )


async def marketplace_selected_projections(
    application: ReviewApplication,
    runs: tuple[str, ...],
    listing_identifiers: tuple[str, ...],
    product_guide_record_identifier: str | None = None,
) -> tuple[int, int, tuple[ComposedListingProjection, ...]]:
    """Compose a bounded exact selection at one boundary, never scan the workspace."""
    if len(listing_identifiers) > 10_000:
        raise ValueError("Explicit projection selection exceeds 10,000 listings")
    database = application.database
    as_of = await database.current_completion_boundary()
    boundary = await _boundary(database)
    expanded = await expand_search_runs(database, runs, as_of_completion_sequence=as_of)
    facebook_runs = tuple(
        [
            r
            for r in expanded
            if (await database.get_record(r))[0] == ("carl", "facebook", "search_run")
        ]
    )
    selected: list[ComposedListingProjection] = []
    for identifier in dict.fromkeys(listing_identifiers):
        request = GetComposedListingRequest(
            listing_identifier=identifier,
            search_run_record_identifier=expanded[0] if expanded else None,
            additional_search_run_record_identifiers=expanded[1:21],
            product_guide_record_identifier=product_guide_record_identifier,
            maximum_gallery_images=0,
            maximum_analyses=0,
        )
        try:
            if identifier.startswith("ebay:"):
                projection = await _ebay_projection(
                    database, request, expanded, boundary=boundary, as_of=as_of
                )
            elif facebook_runs:
                projection = await _facebook_projection(application, request, facebook_runs, as_of)
            else:
                continue
        except KeyError:
            continue
        if projection.search_membership is not None:
            selected.append(projection)
    return as_of, len(expanded), tuple(selected)


async def marketplace_bulk_review_projections(
    application: ReviewApplication,
    workspace: ReviewWorkspace,
    *,
    maximum_candidate_listings_examined: int,
    selected_listing_identifiers: tuple[str, ...] | None = None,
) -> tuple[int, int, tuple[ComposedListingProjection, ...]]:
    """Compose a bulk review once, at one evidence boundary, in bounded source batches."""
    database = application.database
    as_of = await database.current_completion_boundary()
    boundary = await _boundary(database)
    expanded = await expand_search_runs(
        database,
        application._workspace_current_search_runs(workspace),
        as_of_completion_sequence=as_of,
        maximum_object_rowid=boundary,
        maximum_runs=101,
    )
    if len(expanded) > 100:
        raise ValueError("Workspace candidate/ancestry limit reached; no mutation performed")
    facebook_runs: list[str] = []
    ebay_runs: list[str] = []
    for run in expanded:
        kind, _, _ = await database.get_record(run)
        (ebay_runs if kind == ("carl", "ebay", "search_run") else facebook_runs).append(run)
    if not expanded:
        raise ValueError("Search scope has no completed runs")
    selected = (
        None
        if selected_listing_identifiers is None
        else tuple(dict.fromkeys(selected_listing_identifiers))
    )
    if selected is not None and len(selected) > maximum_candidate_listings_examined:
        raise ValueError("Bulk review selection exceeds the candidate limit")
    if selected is None:
        cards = (
            await _records(
                database,
                ("carl", "ebay", "search_listing_occurrence"),
                boundary=boundary,
                as_of=as_of,
                where="json_extract(record.value_json, '$.search_run_record_identifier') IN (SELECT value FROM json_each(?))",
                parameters=(encode_json(ebay_runs),),
                limit=100_001,
            )
            if ebay_runs
            else ()
        )
        if len(cards) > 100_000:
            raise ValueError("Retained eBay membership exceeds the supported safety bound")
        ebay_identifiers = tuple(
            dict.fromkeys(
                "ebay:" + item
                for card in cards
                if isinstance(item := card.value.get("item_identifier"), str)
            )
        )
        if len(ebay_identifiers) > maximum_candidate_listings_examined:
            raise ValueError("Bulk review candidate limit reached; no reviews were recorded")
        facebook_identifiers = None
        facebook_limit = maximum_candidate_listings_examined - len(ebay_identifiers)
    else:
        ebay_identifiers = tuple(
            identifier for identifier in selected if identifier.startswith("ebay:")
        )
        facebook_identifiers = tuple(
            identifier for identifier in selected if not identifier.startswith("ebay:")
        )
        facebook_limit = maximum_candidate_listings_examined
    facebook_projections: tuple[ComposedListingProjection, ...] = ()
    facebook_examined = 0
    if facebook_runs:
        (
            _,
            facebook_examined,
            facebook_projections,
        ) = await application._facebook_workspace_bulk_review_projections(
            workspace,
            current_runs=tuple(facebook_runs),
            as_of=as_of,
            maximum_candidate_listings_examined=facebook_limit,
            selected_listing_identifiers=facebook_identifiers,
            # The global expanded-scope limit above already fences ancestry. Reusing
            # expanded Facebook roots must preserve the legacy adapter's truncation metadata.
            require_complete_ancestry=False,
        )
    projections = list(facebook_projections)
    for start in range(0, len(ebay_identifiers), 100):
        chunk = ebay_identifiers[start : start + 100]
        evidence = await _ebay_bulk_evidence(
            database,
            tuple(identifier.removeprefix("ebay:") for identifier in chunk),
            expanded,
            boundary=boundary,
            as_of=as_of,
        )
        for identifier in chunk:
            try:
                projection = await _ebay_projection(
                    database,
                    GetComposedListingRequest(
                        listing_identifier=identifier,
                        product_guide_record_identifier=workspace.product_guide_record_identifier,
                        maximum_gallery_images=0,
                        maximum_analyses=0,
                    ),
                    expanded,
                    boundary=boundary,
                    as_of=as_of,
                    prefetched=evidence[identifier.removeprefix("ebay:")],
                )
            except KeyError:
                continue
            if projection.search_membership is not None:
                projections.append(projection)
    examined = len(selected) if selected is not None else facebook_examined + len(ebay_identifiers)
    if examined > maximum_candidate_listings_examined:
        raise ValueError("Bulk review candidate limit reached; no reviews were recorded")
    return as_of, examined, tuple(projections)


async def list_marketplace_composed_search(
    application: ReviewApplication,
    request: ListComposedSearchRequest,
) -> ComposedListingPage:
    database = application.database
    if request.filters.product_guide_record_identifier is not None:
        await database.require_product_guide_record(request.filters.product_guide_record_identifier)
    # Batch scans shrink page size as slots fill; it is not part of the search scope.
    scope = hashlib.sha256(
        request.model_dump_json(exclude={"cursor", "page_size"}).encode()
    ).hexdigest()
    cursor: dict[str, JsonValue] = {}
    if request.cursor:
        try:
            # Preserve already-issued cursors when resumed at their original page size.
            legacy_scope = hashlib.sha256(
                request.model_dump_json(exclude={"cursor"}).encode()
            ).hexdigest()
            value = decode_json(
                base64.b64decode(
                    request.cursor + "=" * (-len(request.cursor) % 4), altchars=b"-_", validate=True
                ).decode()
            )
            if not isinstance(value, dict) or cast(dict[str, JsonValue], value).get(
                "scope"
            ) not in (scope, legacy_scope):
                raise ValueError("Marketplace cursor belongs to another projection scope")
            cursor = cast(dict[str, JsonValue], value)
            for key in ("as_of", "boundary", "after_sequence"):
                if (
                    not isinstance(cursor.get(key), int)
                    or isinstance(cursor.get(key), bool)
                    or cast(int, cursor[key]) < 0
                ):
                    raise ValueError("Invalid marketplace cursor boundary")
            if (
                not isinstance(cursor.get("runs"), list)
                or not all(isinstance(run, str) for run in cast(list[JsonValue], cursor["runs"]))
                or not isinstance(cursor.get("after"), str)
            ):
                raise ValueError("Invalid marketplace cursor position")
        except (UnicodeDecodeError, ValueError) as error:
            raise ValueError("Invalid marketplace projection cursor") from error
    as_of = int(cursor["as_of"]) if cursor else await database.current_completion_boundary()
    boundary = int(cursor["boundary"]) if cursor else await _boundary(database)
    if as_of > await database.current_completion_boundary() or boundary > await _boundary(database):
        raise ValueError("Marketplace cursor boundary is beyond retained evidence")
    expanded = await expand_search_runs(
        database,
        (
            request.search_run_record_identifier,
            *request.additional_search_run_record_identifiers,
        ),
        as_of_completion_sequence=as_of,
        maximum_runs=request.maximum_ancestry_runs + 1,
        maximum_object_rowid=boundary,
    )
    ancestry_truncated = len(expanded) > request.maximum_ancestry_runs
    runs = expanded[: request.maximum_ancestry_runs]
    if cursor and tuple(cast(list[str], cursor["runs"])) != runs:
        raise ValueError("Marketplace cursor run scope is inconsistent with its source")
    if not runs:
        raise ValueError("Search scope has no completed runs")
    facebook_runs = tuple(
        [r for r in runs if (await database.get_record(r))[0] == ("carl", "facebook", "search_run")]
    )
    ebay_runs = tuple(
        [r for r in runs if (await database.get_record(r))[0] == ("carl", "ebay", "search_run")]
    )
    positions: dict[str, int] = {}
    if facebook_runs:
        ancestry = await application._projection_search_scope(
            facebook_runs[0],
            facebook_runs[1:],
            as_of_completion_sequence=as_of,
            maximum_runs=request.maximum_ancestry_runs,
        )
        memberships = await database.facebook_projection_membership_candidates(
            tuple((r.record_identifier, r.internal_search_run_identifier) for r in ancestry.runs),
            as_of_completion_sequence=as_of,
            maximum_listings=request.maximum_candidate_listings_examined + 1,
            after_position=(cast(int, cursor["after_sequence"]), str(cursor["after"]))
            if cursor
            else None,
        )
        for membership in memberships:
            positions[membership.candidate.listing_identifier] = (
                membership.candidate.search_run_completion_sequence
            )
    if ebay_runs:
        cards = await _records(
            database,
            ("carl", "ebay", "search_listing_occurrence"),
            boundary=boundary,
            as_of=as_of,
            where="json_extract(record.value_json, '$.search_run_record_identifier') IN (SELECT value FROM json_each(?))",
            parameters=(encode_json(list(ebay_runs)),),
            limit=100_001,
        )
        if len(cards) > 100_000:
            raise ValueError("Retained eBay membership exceeds the supported safety bound")
        for card in cards:
            item = card.value.get("item_identifier")
            if isinstance(item, str):
                identifier = "ebay:" + item
                positions[identifier] = max(positions.get(identifier, 0), card.sequence)
    ordered = sorted(positions, key=lambda identifier: (-positions[identifier], identifier))
    after = cursor.get("after")
    if after:
        after_key = (-cast(int, cursor["after_sequence"]), str(after))
        ordered = [
            identifier for identifier in ordered if (-positions[identifier], identifier) > after_key
        ]
    selected: list[ComposedListingProjection] = []
    examined = 0
    candidates = ordered[: request.maximum_candidate_listings_examined]
    facebook_statuses: dict[str, ListingStatus] = {}
    for position, identifier in enumerate(candidates):
        examined += 1
        # Rare status filters must not hydrate every nonmatching Facebook listing.
        # Preserve candidate counts/cursor positions, and use exactly the status
        # evidence and history bounds of the full projection at this snapshot.
        if request.filters.statuses and not identifier.startswith("ebay:"):
            if identifier not in facebook_statuses:
                facebook_statuses.update(
                    await _facebook_listing_statuses(
                        application,
                        tuple(
                            item
                            for item in candidates[position : position + 100]
                            if not item.startswith("ebay:")
                        ),
                        as_of=as_of,
                    )
                )
            if facebook_statuses[identifier] not in request.filters.statuses:
                continue
        get_request = GetComposedListingRequest(
            listing_identifier=identifier,
            search_run_record_identifier=runs[0],
            additional_search_run_record_identifiers=runs[1:21],
            product_guide_record_identifier=request.filters.product_guide_record_identifier,
            maximum_gallery_images=request.maximum_gallery_images_per_listing,
            maximum_analyses=request.maximum_analyses_per_listing,
        )
        if identifier.startswith("ebay:"):
            projection = await _ebay_projection(
                database, get_request, runs, boundary=boundary, as_of=as_of
            )
        else:
            projection = await _facebook_projection(application, get_request, facebook_runs, as_of)
        # Truncation retains presence when the compact transport requests zero reports.
        present = bool(projection.analyses) or projection.analyses_truncated
        if composed_listing_matches_filters(
            projection, request.filters, matching_analysis_present=present
        ):
            selected.append(projection)
            if len(selected) == request.page_size:
                break
    more = examined < len(ordered)
    next_cursor = None
    if more and examined:
        payload = {
            "scope": scope,
            "as_of": as_of,
            "boundary": boundary,
            "runs": list(runs),
            "after": ordered[examined - 1],
            "after_sequence": positions[ordered[examined - 1]],
        }
        next_cursor = base64.urlsafe_b64encode(encode_json(payload).encode()).decode().rstrip("=")
    return ComposedListingPage(
        as_of_completion_sequence=as_of,
        selected_search_run_record_identifier=request.search_run_record_identifier,
        included_ancestry_run_count=len(runs),
        older_ancestry_truncated=ancestry_truncated,
        examined_candidate_listing_count=examined,
        candidate_examination_limit_reached=more
        and examined == request.maximum_candidate_listings_examined,
        listings=tuple(selected),
        next_cursor=next_cursor,
    )


async def _facebook_listing_statuses(
    application: ReviewApplication,
    listing_identifiers: tuple[str, ...],
    *,
    as_of: int,
) -> dict[str, ListingStatus]:
    """Batch exact status selection without scalar, ancestry, image, or AI reads."""
    observations = await application.database.facebook_projection_item_observations(
        listing_identifiers,
        as_of_completion_sequence=as_of,
        maximum_per_listing=101,
        status_only=True,
    )
    bounded, _ = application._bounded_observations_by_listing(observations, maximum_per_listing=100)
    candidates: dict[str, list[StatusObservationCandidate]] = {}
    for identifier, history in bounded.items():
        candidates[identifier] = [
            status
            for observation in history
            if (status := status_candidate_from_item_observation(observation)) is not None
        ]
    for status in await application.database.facebook_projection_search_occurrences(
        listing_identifiers, as_of_completion_sequence=as_of
    ):
        candidates.setdefault(status.listing_identifier, []).append(status)
    return {
        identifier: select_status(
            candidates.get(identifier, ()), as_of_completion_sequence=as_of
        ).value
        for identifier in listing_identifiers
    }


async def _facebook_projection(
    application: ReviewApplication,
    request: GetComposedListingRequest,
    runs: tuple[str, ...],
    as_of: int,
) -> ComposedListingProjection:
    """Compose legacy Facebook evidence at the mixed cursor's immutable boundary."""
    database = application.database
    observations = await database.facebook_projection_item_observations(
        (request.listing_identifier,), as_of_completion_sequence=as_of, maximum_per_listing=101
    )
    observations_by_listing, truncated = application._bounded_observations_by_listing(
        observations, maximum_per_listing=100
    )
    cards = await database.facebook_projection_search_cards(
        (request.listing_identifier,), as_of_completion_sequence=as_of, maximum_per_listing=101
    )
    cards_by_listing, truncated_cards = application._bounded_search_cards_by_listing(
        cards, maximum_per_listing=100
    )
    statuses = await database.facebook_projection_search_occurrences(
        (request.listing_identifier,), as_of_completion_sequence=as_of
    )
    analyses = await application._projection_analyses_by_listing(
        (request.listing_identifier,),
        product_guide_record_identifier=request.product_guide_record_identifier,
        maximum_per_listing=21,
        as_of_completion_sequence=as_of,
    )
    ancestry = await application._projection_search_scope(
        runs[0],
        runs[1:],
        as_of_completion_sequence=as_of,
        maximum_runs=request.maximum_ancestry_runs,
    )
    occurrences = await database.facebook_projection_membership_occurrences(
        (request.listing_identifier,),
        tuple((r.record_identifier, r.internal_search_run_identifier) for r in ancestry.runs),
        as_of_completion_sequence=as_of,
    )
    from carl.core.composed_projection import compose_search_membership

    membership = compose_search_membership(
        listing_identifier=request.listing_identifier, ancestry=ancestry, occurrences=occurrences
    )
    galleries = await application._projection_galleries(
        {request.listing_identifier: observations_by_listing.get(request.listing_identifier, ())},
        as_of_completion_sequence=as_of,
    )
    projection, _ = application._compose_listing_projection(
        request.listing_identifier,
        observations=observations_by_listing.get(request.listing_identifier, ()),
        search_cards=cards_by_listing.get(request.listing_identifier, ()),
        search_statuses=statuses,
        analyses=analyses.get(request.listing_identifier, ()),
        as_of_completion_sequence=as_of,
        product_guide_record_identifier=request.product_guide_record_identifier,
        maximum_analyses=request.maximum_analyses,
        maximum_gallery_images=request.maximum_gallery_images,
        gallery=galleries.get(request.listing_identifier),
        membership=membership,
        warnings=tuple(
            w
            for flag, w in (
                (request.listing_identifier in truncated, "item_observation_history_truncated"),
                (request.listing_identifier in truncated_cards, "search_card_history_truncated"),
            )
            if flag
        ),
    )
    return projection


async def ebay_candidate_sources(
    database: Database,
    listing_identifiers: tuple[str, ...],
    *,
    as_of_completion_sequence: int | None = None,
) -> tuple[CandidateSource, ...]:
    """Expose source-qualified retained observations to shared batch selection."""
    as_of = (
        await database.current_completion_boundary()
        if as_of_completion_sequence is None
        else as_of_completion_sequence
    )
    boundary = await _boundary(database)
    result: list[CandidateSource] = []
    for identifier in listing_identifiers:
        if not identifier.startswith("ebay:"):
            continue
        records = await _ebay_records(
            database,
            "listing_observation",
            identifier.removeprefix("ebay:"),
            boundary=boundary,
            as_of=as_of,
            limit=10_001,
        )
        if len(records) > 10_000:
            raise ValueError(
                "Retained listing analysis-selection history exceeds the supported safety bound"
            )
        analyses = await _analyses(database, records, boundary=boundary, as_of=as_of)
        for record in records:
            acquisition = record.value.get("acquisition_record_identifier")
            classification = record.value.get("classification")
            if not isinstance(acquisition, str) or classification not in ("detail", "unavailable"):
                continue
            fields: dict[str, JsonValue] = {
                key: {"evidence": [{"state": "present", "normalized": record.value.get(source)}]}
                for key, source in (
                    ("title", "title"),
                    ("price", "displayed_price"),
                    ("description", "description"),
                )
                if record.value.get(source) is not None
            }
            result.append(
                CandidateSource(
                    listing_identifier=identifier,
                    observation_record_identifier=record.identifier,
                    acquisition_record_identifier=acquisition,
                    availability=CandidateAvailability.FULL_LISTING
                    if classification == "detail"
                    else CandidateAvailability.LISTING_UNAVAILABLE,
                    acquisition_completion_sequence=record.acquisition_sequence or record.sequence,
                    observation_completion_sequence=record.sequence,
                    observation={"fields": fields, "marketplace": "ebay"},
                    analyses=tuple(
                        a
                        for a in analyses
                        if a.listing_observation_record_identifier == record.identifier
                        and a.state == "completed"
                    ),
                )
            )
    return tuple(result)
