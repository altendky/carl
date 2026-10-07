from pathlib import Path
from time import time_ns
from typing import TypedDict, cast
from uuid import uuid4

import pytest

from carl._tests.test_ebay_workers import _directories, _provenance
from carl.core.components import Component, ComponentId
from carl.core.ebay import CollectEbaySearchPayload, EbaySearchRequest
from carl.core.models import JsonValue, NamedInput, NamedOutput, RecordDraft
from carl.core.work import (
    SchedulingScope,
    SchedulingScopeKind,
    WorkCapability,
    WorkDefinition,
    WorkRequester,
)
from carl.core.worker import AttemptContext
from carl.ebay_workers import EbaySearchWorkerDependencies, build_ebay_worker_registry
from carl.io.sqlite import Database


async def _noop() -> None:
    pass


_COMPONENT = Component(ComponentId(("test", "pipeline")), 1, _noop)


class _OccurrenceScope(TypedDict):
    search_work_identifiers: tuple[str, ...]
    search_run_record_identifiers: tuple[str, ...]


async def _attempt(
    database: Database,
    identifier: str,
    marketplace: str,
    *,
    work_kind: tuple[str, ...] | None = None,
    payload: JsonValue = None,
) -> AttemptContext:
    kind = work_kind or (
        ("carl", "facebook", "work", "collect_search")
        if marketplace == "facebook"
        else ("carl", "ebay", "collect", "search")
    )
    await database.enqueue_work(
        WorkDefinition(
            identifier=identifier,
            kind=kind,
            payload_schema_version=1,
            payload={} if payload is None else payload,
            deduplication_identity=(identifier,),
            scopes=(
                SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=kind),
            ),
            not_before_utc_ns=0,
        ),
        WorkRequester(
            request_identifier=str(uuid4()), kind=("test",), identifier=identifier, context={}
        ),
        event_identifier=str(uuid4()),
        enqueued_at_utc_ns=time_ns(),
    )
    claimed = await database.claim_work(
        supported_capabilities=(WorkCapability(kind=kind, payload_schema_version=1),),
        worker_identifier="worker",
        lease_token=str(uuid4()),
        lease_duration_ns=60_000_000_000,
        utc_now_ns=time_ns,
        event_identifier=str(uuid4()),
        eligible_identifiers=(identifier,),
    )
    assert claimed.lease is not None
    context = AttemptContext(
        work_item_identifier=identifier,
        lease_token=claimed.lease.token,
        worker_identifier="worker",
        attempt=claimed.lease.attempt,
        operation_identifier=str(uuid4()),
    )
    await database.begin_leased_operation(
        work_item_identifier=identifier,
        lease_token=context.lease_token,
        worker_identifier="worker",
        lease_duration_ns=60_000_000_000,
        utc_now_ns=time_ns,
        event_identifier=str(uuid4()),
        operation_id=context.operation_identifier,
        component=_COMPONENT,
        provenance=await _provenance(),
        invocation={},
        configuration={},
        started_at_utc="2026-10-06T00:00:00+00:00",
    )
    return context


async def _checkpoint(
    database: Database, context: AttemptContext, records: tuple[RecordDraft, ...]
) -> None:
    await database.publish_leased_operation_checkpoint(
        work_item_identifier=context.work_item_identifier,
        lease_token=context.lease_token,
        worker_identifier=context.worker_identifier,
        utc_now_ns=time_ns,
        operation_id=context.operation_identifier,
        records=records,
        artifacts=(),
        outputs=tuple(
            NamedOutput(name=("record", record.identifier), object_identifier=record.identifier)
            for record in records
        ),
        checkpoint_result={"state": "collecting"},
    )


async def _publish(
    database: Database, records: tuple[RecordDraft, ...], *, inputs: tuple[NamedInput, ...] = ()
) -> None:
    await database.publish_records_operation(
        component=_COMPONENT,
        operation_identifier=str(uuid4()),
        records=records,
        inputs=inputs,
        outputs=tuple(
            NamedOutput(name=("record", record.identifier), object_identifier=record.identifier)
            for record in records
        ),
        provenance=await _provenance(),
        invocation={},
        started_at_utc="2026-10-06T00:00:00+00:00",
        ended_at_utc="2026-10-06T00:00:00+00:00",
        duration_ns=0,
        result={},
    )


def _card(identifier: str, marketplace: str, run: str) -> RecordDraft:
    return RecordDraft(
        identifier=identifier,
        kind=("carl", marketplace, "search_listing_occurrence"),
        schema_version=1,
        value=(
            {
                "listing_identifier": "123456789",
                "search_run_identifier": run,
                "page_ordinal": 1,
                "edge_index": 0,
                "original": {"marketplace_listing_title": "Scope"},
            }
            if marketplace == "facebook"
            else {
                "item_identifier": "123456789",
                "search_run_record_identifier": run,
                "title": "Scope",
                "position": 1,
            }
        ),
    )


@pytest.mark.anyio
async def test_incremental_cards_are_scoped_paged_and_deduplicated(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "evidence.sqlite3", initialize=True) as database:
        facebook = await _attempt(database, "fb-work", "facebook")
        unrelated = await _attempt(database, "other-work", "facebook")
        ebay = await _attempt(database, "ebay-work", "ebay")
        await _checkpoint(database, unrelated, (_card("unrelated", "facebook", "other-run"),))
        await _checkpoint(database, facebook, (_card("facebook-card", "facebook", "fb-run"),))
        await _checkpoint(
            database,
            ebay,
            (
                RecordDraft(
                    identifier="binding",
                    kind=("carl", "ebay", "search_attempt"),
                    schema_version=1,
                    value={"search_run_record_identifier": "ebay-run"},
                ),
            ),
        )
        await _publish(
            database,
            (_card("ebay-card", "ebay", "ebay-run"), _card("other-ebay", "ebay", "unrelated-run")),
        )
        await _checkpoint(
            database, unrelated, (_card("unfinished-extraction", "ebay", "ebay-run"),)
        )
        await _publish(
            database,
            (
                RecordDraft(
                    identifier="fb-final-run",
                    kind=("carl", "facebook", "search_run"),
                    schema_version=1,
                    value={"search_run_identifier": "fb-run"},
                ),
            ),
        )
        scope: _OccurrenceScope = {
            "search_work_identifiers": ("fb-work", "ebay-work"),
            "search_run_record_identifiers": ("fb-final-run",),
        }
        rows = await database.pipeline_search_occurrences(**scope)
        assert [row["occurrence_record_identifier"] for row in rows] == [
            "facebook-card",
            "ebay-card",
        ]
        assert [row["marketplace"] for row in rows] == ["facebook", "ebay"]
        assert all(row["title"] == "Scope" for row in rows)
        first = await database.pipeline_search_occurrences(**scope, limit=1)
        assert first == rows[:1]
        last_rowid = first[0]["object_rowid"]
        assert isinstance(last_rowid, int)
        assert (
            await database.pipeline_search_occurrences(**scope, after_object_rowid=last_rowid)
            == rows[1:]
        )
        assert (await database.work("fb-work"))["state"] == "leased"
        assert (await database.work("ebay-work"))["state"] == "leased"


@pytest.mark.anyio
async def test_explicit_ebay_seed_and_legacy_completed_work(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "evidence.sqlite3", initialize=True) as database:
        context = await _attempt(database, "legacy", "ebay")
        await _publish(
            database,
            (
                RecordDraft(
                    identifier="run",
                    kind=("carl", "ebay", "search_run"),
                    schema_version=1,
                    value={},
                ),
                _card("card", "ebay", "run"),
            ),
        )
        await database.complete_leased_operation(
            work_item_identifier="legacy",
            lease_token=context.lease_token,
            worker_identifier="worker",
            utc_now_ns=time_ns,
            operation_id=context.operation_identifier,
            records=(),
            artifacts=(),
            inputs=(),
            outputs=(),
            result={"search_run_record_identifier": "run"},
            ended_at_utc="2026-10-06T00:00:00+00:00",
            duration_ns=0,
            event_identifier=str(uuid4()),
        )
        legacy = await database.pipeline_search_occurrences(search_work_identifiers=("legacy",))
        seed = await database.pipeline_search_occurrences(
            search_work_identifiers=(), search_run_record_identifiers=("run",)
        )
        assert legacy == seed
        assert len(seed) == 1


@pytest.mark.anyio
async def test_ebay_worker_binds_run_before_collector_and_retains_retry_cards(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "evidence.sqlite3", initialize=True) as database:
        context = await _attempt(database, "ebay", "ebay")
        seen: list[str] = []

        async def collect(
            database: Database, *, search_run_identifier: str, **_kwargs: object
        ) -> dict[str, JsonValue]:
            bindings = await database.records_by_kind(("carl", "ebay", "search_attempt"))
            assert any(
                isinstance(value, dict)
                and cast(dict[str, JsonValue], value)["search_run_record_identifier"]
                == search_run_identifier
                for _, value in bindings
            )
            seen.append(search_run_identifier)
            await _publish(database, (_card("attempt-card", "ebay", search_run_identifier),))
            assert (
                len(await database.pipeline_search_occurrences(search_work_identifiers=("ebay",)))
                == 1
            )
            return {
                "state": "response_failed",
                "response_classification": {"kind": "challenge", "evidence": []},
                "search_run_record_identifier": search_run_identifier,
            }

        registry = build_ebay_worker_registry(
            EbaySearchWorkerDependencies(
                database=database,
                directories=_directories(tmp_path),
                new_identifier=lambda: str(uuid4()),
                collector=collect,
            )
        )
        outcome = await registry.handlers[0].execute(
            CollectEbaySearchPayload(request=EbaySearchRequest(query="scope")).as_json(), context
        )
        await database.retry_leased_operation(
            work_item_identifier="ebay",
            lease_token=context.lease_token,
            worker_identifier="worker",
            utc_now_ns=time_ns,
            operation_id=context.operation_identifier,
            records=(),
            artifacts=(),
            inputs=(),
            outputs=(),
            result=outcome.result,
            reason={"kind": "test"},
            delay_ns=0,
            ended_at_utc="2026-10-06T00:00:00+00:00",
            duration_ns=0,
            event_identifier=str(uuid4()),
        )
        assert (
            len(await database.pipeline_search_occurrences(search_work_identifiers=("ebay",))) == 1
        )
        assert len(seen) == 1
        second = await _attempt(database, "ebay", "ebay")
        assert second.attempt == 2
        await _checkpoint(
            database,
            second,
            (
                RecordDraft(
                    identifier="second-binding",
                    kind=("carl", "ebay", "search_attempt"),
                    schema_version=1,
                    value={"search_run_record_identifier": "second-run"},
                ),
            ),
        )
        await _publish(database, (_card("second-attempt-card", "ebay", "second-run"),))
        assert [
            row["occurrence_record_identifier"]
            for row in await database.pipeline_search_occurrences(search_work_identifiers=("ebay",))
        ] == ["attempt-card", "second-attempt-card"]


@pytest.mark.anyio
async def test_pipeline_transaction_rolls_back_and_work_rows_follow_caller_order(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "evidence.sqlite3", initialize=True) as database:
        with pytest.raises(RuntimeError, match="abort"):
            async with database.transaction():
                await _attempt(database, "rolled-back", "facebook")
                raise RuntimeError("abort")
        with pytest.raises(KeyError):
            await database.work("rolled-back")
        await _attempt(database, "first", "facebook")
        await _attempt(database, "second", "ebay")
        rows = await database.pipeline_work_rows(("second", "first", "second"))
        assert [row["identifier"] for row in rows] == ["second", "first", "second"]
        assert all(
            set(row) == {"identifier", "state", "payload", "result", "error"} for row in rows
        )
        with pytest.raises(KeyError):
            await database.pipeline_work_rows(("missing",))


@pytest.mark.anyio
@pytest.mark.parametrize(
    "identifiers,after_rowid,limit",
    [((), -1, 100), ((), 0, 0), ((), 0, 1001), (("",), 0, 100)],
)
async def test_pipeline_occurrence_bounds_are_validated(
    tmp_path: Path, identifiers: tuple[str, ...], after_rowid: int, limit: int
) -> None:
    async with Database.managed(tmp_path / "evidence.sqlite3", initialize=True) as database:
        with pytest.raises(ValueError):
            await database.pipeline_search_occurrences(
                search_work_identifiers=identifiers, after_object_rowid=after_rowid, limit=limit
            )


@pytest.mark.anyio
@pytest.mark.parametrize("marketplace", ["facebook", "ebay"])
@pytest.mark.parametrize("analysis_state", ["completed", "failed"])
async def test_completed_analysis_history_requires_success_and_exact_evidence(
    tmp_path: Path, marketplace: str, analysis_state: str
) -> None:
    async with Database.managed(tmp_path / "evidence.sqlite3", initialize=True) as database:
        await _publish(
            database,
            (
                RecordDraft(
                    identifier="observation",
                    kind=("carl", marketplace, "listing_observation"),
                    schema_version=1,
                    value={
                        "listing_id"
                        if marketplace == "facebook"
                        else "item_identifier": "123456789"
                    },
                ),
            ),
        )
        await _publish(
            database,
            (
                RecordDraft(
                    identifier="evidence",
                    kind=("carl", marketplace, "listing_analysis_evidence"),
                    schema_version=1,
                    value={},
                ),
            ),
            inputs=(NamedInput(name=("listing_observation",), object_identifier="observation"),),
        )
        context = await _attempt(
            database,
            "analysis-work",
            marketplace,
            work_kind=("carl", marketplace, "work", "analyze_item"),
            payload={
                "evidence_set_record_identifier": "evidence",
                "product_guide_record_identifier": "retired-guide",
            },
        )
        assert not await database.pipeline_completed_analysis_exists(marketplace, "123456789")
        await database.complete_leased_operation(
            work_item_identifier="analysis-work",
            lease_token=context.lease_token,
            worker_identifier="worker",
            utc_now_ns=time_ns,
            operation_id=context.operation_identifier,
            records=(
                RecordDraft(
                    identifier="analysis-result",
                    kind=("carl", marketplace, "item_analysis"),
                    schema_version=5,
                    value={"state": analysis_state},
                ),
            ),
            artifacts=(),
            inputs=(NamedInput(name=("listing_analysis_evidence",), object_identifier="evidence"),),
            outputs=(NamedOutput(name=("item_analysis",), object_identifier="analysis-result"),),
            result={},
            ended_at_utc="2026-10-06T00:00:00+00:00",
            duration_ns=0,
            event_identifier=str(uuid4()),
        )
        assert await database.pipeline_completed_analysis_exists(marketplace, "123456789") == (
            analysis_state == "completed"
        )
        assert await database.pipeline_completed_analysis_exists(
            marketplace, "123456789", "retired-guide"
        ) == (analysis_state == "completed")
        assert not await database.pipeline_completed_analysis_exists(
            marketplace, "123456789", "other-guide"
        )
        assert not await database.pipeline_completed_analysis_exists(marketplace, "999999999")
