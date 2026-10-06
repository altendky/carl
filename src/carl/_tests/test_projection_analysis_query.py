"""Analysis projection windows use a single indexed scan, not one scan per listing."""

# pyright: reportPrivateUsage=false

from pathlib import Path
from time import time_ns
from typing import cast

import apsw
import pytest

from carl._tests.test_ebay_analysis import _publish
from carl._tests.test_mixed_workspace import _complete
from carl.core.item_analysis import AnalyzeItemPayload, analyze_item_work
from carl.core.models import NamedInput, RecordDraft
from carl.core.work import WorkRequester
from carl.io.sqlite import Database


async def _analysis(
    database: Database,
    identifier: str,
    observation: str,
    guide: str,
    *,
    schema_version: int = 4,
) -> None:
    evidence = f"evidence-{identifier}"
    await _publish(
        database,
        f"publish-{evidence}",
        (
            RecordDraft(
                identifier=evidence,
                kind=("carl", "facebook", "listing_analysis_evidence"),
                schema_version=1,
                value={},
            ),
        ),
        inputs=(NamedInput(name=("listing_observation",), object_identifier=observation),),
    )
    definition = analyze_item_work(
        identifier=f"work-{identifier}",
        payload=AnalyzeItemPayload(
            evidence_set_record_identifier=evidence, product_guide_record_identifier=guide
        ),
    ).model_copy(update={"payload_schema_version": schema_version})
    await database.enqueue_work(
        definition,
        WorkRequester(
            request_identifier=f"request-{identifier}",
            kind=("test", "analysis"),
            identifier=identifier,
            context={},
        ),
        event_identifier=f"enqueued-{identifier}",
        enqueued_at_utc_ns=time_ns(),
    )
    await _complete(
        database,
        definition.identifier,
        records=(
            RecordDraft(
                identifier=identifier,
                kind=("carl", "facebook", "item_analysis"),
                schema_version=1,
                value={"state": "completed", "warnings": []},
            ),
        ),
        result={"state": "completed"},
    )


@pytest.mark.anyio
async def test_projection_analysis_latest_per_guide_bounds_and_frozen_completion(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(
            database,
            "observations",
            tuple(
                RecordDraft(
                    identifier=identifier,
                    kind=("carl", "facebook", "listing_observation"),
                    schema_version=1,
                    value={"listing_id": listing},
                )
                for identifier, listing in (("old", "123"), ("new", "123"), ("other", "456"))
            ),
        )
        await _analysis(database, "old-a", "old", "guide-a", schema_version=3)
        await _analysis(database, "old-b", "old", "guide-b")
        await _analysis(database, "new-a", "new", "guide-a")
        await _analysis(database, "other-a", "other", "guide-a")
        boundary = await database.current_completion_boundary()
        await _analysis(database, "later-b", "new", "guide-b")
        descriptors = await database.facebook_projection_analysis_descriptors(
            ("123", "456", "999"),
            product_guide_record_identifier=None,
            maximum_per_listing=21,
            as_of_completion_sequence=boundary,
        )
        assert [
            (listing, report.analysis_record_identifier) for listing, report in descriptors
        ] == [
            ("123", "new-a"),
            ("123", "old-b"),
            ("456", "other-a"),
        ]
        filtered = await database.facebook_projection_analysis_descriptors(
            ("123",),
            product_guide_record_identifier="guide-b",
            maximum_per_listing=1,
            as_of_completion_sequence=boundary,
        )
        assert [report.analysis_record_identifier for _, report in filtered] == ["old-b"]
        latest = await database.facebook_projection_analysis_descriptors(
            ("123",),
            product_guide_record_identifier=None,
            maximum_per_listing=1,
            as_of_completion_sequence=await database.current_completion_boundary(),
        )
        assert [report.analysis_record_identifier for _, report in latest] == ["later-b"]
        historical = await database.facebook_projection_analysis_descriptors(
            ("123",),
            product_guide_record_identifier="guide-a",
            maximum_per_listing=1,
            as_of_completion_sequence=descriptors[1][1].completion_sequence - 1,
        )
        assert [report.analysis_record_identifier for _, report in historical] == ["old-a"]


@pytest.mark.anyio
async def test_projection_analysis_plan_scans_analysis_work_once(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(
            database,
            "observations",
            (
                RecordDraft(
                    identifier="observation",
                    kind=("carl", "facebook", "listing_observation"),
                    schema_version=1,
                    value={"listing_id": "123"},
                ),
            ),
        )
        await _analysis(database, "analysis", "observation", "guide")
        query: tuple[str, tuple[apsw.SQLiteValue, ...]] | None = None

        def trace(_cursor: object, statement: str, bindings: object) -> bool:
            nonlocal query
            if "latest_per_guide AS MATERIALIZED" in statement:
                query = statement, cast("tuple[apsw.SQLiteValue, ...]", bindings)
            return True

        async with database._connections.reader() as connection:
            connection.set_exec_trace(trace)
            try:
                await database.facebook_projection_analysis_descriptors(
                    tuple(str(123 + index) for index in range(1000)),
                    product_guide_record_identifier=None,
                    maximum_per_listing=21,
                    as_of_completion_sequence=await database.current_completion_boundary(),
                )
            finally:
                connection.set_exec_trace(None)
            assert query is not None
            cursor = await connection.execute("EXPLAIN QUERY PLAN " + query[0], query[1])
            plan = [str(row[3]) for row in await cursor.fetchall()]
        assert any("SEARCH work USING INDEX work_items_analysis_lookup" in step for step in plan)
        assert any(
            "SEARCH analysis USING COVERING INDEX objects_operation_kind" in step for step in plan
        )
        assert not any("SEARCH analysis USING INDEX objects_kind" in step for step in plan)
        assert not any("SCAN requested" in step for step in plan[:3])
