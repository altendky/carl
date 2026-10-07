import json
import sqlite3
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from time import perf_counter_ns, time_ns
from typing import Any
from uuid import uuid4

import pytest

from carl.core.components import Component, ComponentId
from carl.core.ebay import (
    COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION,
    COLLECT_EBAY_SEARCH_WORK_KIND,
    LEGACY_COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION,
    CollectEbaySearchPayload,
    EbaySearchRequest,
    collect_ebay_search_work,
)
from carl.core.models import CodeProvenance, JsonValue, NamedOutput, RecordDraft
from carl.core.work import WorkCapability, WorkRequester
from carl.core.worker import (
    AttemptContext,
    CompletedWork,
    RetryWork,
    TerminalFailureWork,
    WorkerSettings,
)
from carl.ebay_workers import EbaySearchWorkerDependencies, build_ebay_worker_registry
from carl.io.httpx import AcquisitionFailure
from carl.io.paths import CarlDirectories
from carl.io.sqlite import Database
from carl.io.worker import WorkerRuntimeServices, execute_lease

_SEARCH_REFERENCES = (
    (("search", "page", "1", "acquisition"), "acquisition-1"),
    (("search", "page", "2", "acquisition"), "acquisition-2"),
    (("search", "page", "1", "extraction"), "extraction-1"),
    (("search", "page", "2", "extraction"), "extraction-2"),
    (("search", "run"), "search-run-1"),
)


@pytest.fixture(autouse=True)
def _stub_checkpoint_for_synthetic_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    """Policy unit tests call handlers without creating their synthetic leases."""

    checkpoint = Database.publish_leased_operation_checkpoint

    async def publish(database: Database, **kwargs: Any) -> None:
        try:
            await database.work(kwargs["work_item_identifier"])
        except KeyError:
            return
        await checkpoint(database, **kwargs)

    monkeypatch.setattr(Database, "publish_leased_operation_checkpoint", publish)


async def _provenance() -> CodeProvenance:
    return CodeProvenance(
        repository_url=None,
        commit_hash=None,
        worktree_state="dirty",
        package_version="test",
        python_implementation="test",
        python_version="test",
        dependencies=(),
        lockfile_sha256=None,
    )


def _directories(root: Path) -> CarlDirectories:
    return CarlDirectories(
        config=root / "config",
        data=root / "data",
        cache=root / "cache",
        state=root / "state",
        runtime=root / "runtime",
    )


@pytest.mark.anyio
async def test_worker_links_child_search_records_as_work_inputs(tmp_path: Path) -> None:
    observed: list[EbaySearchRequest] = []

    async def collect(
        _database: Database,
        *,
        directories: CarlDirectories,
        request: EbaySearchRequest,
        new_identifier: Callable[[], str],
        search_run_identifier: str,
    ) -> dict[str, object]:
        del directories, new_identifier, search_run_identifier
        observed.append(request)
        return {
            "state": "completed",
            "acquisition_record_identifier": "acquisition-1",
            "acquisition_record_identifiers": ["acquisition-1", "acquisition-2"],
            "extraction_record_identifier": "extraction-1",
            "extraction_record_identifiers": ["extraction-1", "extraction-2"],
            "search_run_record_identifier": "search-run-1",
            "listing_count": 5,
        }

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = build_ebay_worker_registry(
            EbaySearchWorkerDependencies(
                database=database,
                directories=_directories(tmp_path),
                new_identifier=lambda: "unused",
                collector=collect,
            )
        )
        outcome = await registry.handlers[0].execute(
            CollectEbaySearchPayload(request=EbaySearchRequest(query="oscilloscope")).as_json(),
            AttemptContext(
                work_item_identifier="work-1",
                lease_token="lease-1",
                worker_identifier="worker-1",
                attempt=1,
                operation_identifier="operation-1",
            ),
        )

    assert isinstance(outcome, CompletedWork)
    assert observed == [EbaySearchRequest(query="oscilloscope")]
    assert tuple((value.name, value.object_identifier) for value in outcome.inputs) == (
        _SEARCH_REFERENCES
    )
    assert outcome.outputs == ()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "classification", ["challenge", "unrecognized", "error_page", "http_error"]
)
@pytest.mark.parametrize("attempt", [1, 2, 3])
async def test_worker_bounds_response_failure_retries(
    tmp_path: Path, classification: str, attempt: int
) -> None:
    now_utc_ns = 1_000_000_000_000
    result: dict[str, JsonValue] = {
        "state": "completed",
        "acquisition_record_identifiers": ["acquisition-1", "acquisition-2"],
        "extraction_record_identifiers": ["extraction-1", "extraction-2"],
        "search_run_record_identifier": "search-run-1",
        "response_classification": {"kind": classification, "evidence": ["retained marker"]},
        "stopping_reason": "unrecognized_response"
        if classification == "unrecognized"
        else classification,
        "listing_count": 0,
    }

    async def collect(*_args: object, **_kwargs: object) -> dict[str, JsonValue]:
        return result

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = build_ebay_worker_registry(
            EbaySearchWorkerDependencies(
                database=database,
                directories=_directories(tmp_path),
                new_identifier=lambda: "unused",
                collector=collect,
                utc_now_ns=lambda: now_utc_ns,
            )
        )
        outcome = await registry.handlers[0].execute(
            CollectEbaySearchPayload(request=EbaySearchRequest(query="oscilloscope")).as_json(),
            AttemptContext(
                work_item_identifier="work-1",
                lease_token="lease-1",
                worker_identifier="worker-1",
                attempt=attempt,
                operation_identifier="operation-1",
            ),
        )

    delay_ns = (120 if classification == "challenge" else 1) * 2 ** (attempt - 1) * 1_000_000_000
    reason: dict[str, JsonValue] = {
        "kind": "ebay_search_response_failure",
        "response_classification": result["response_classification"],
        "stopping_reason": result["stopping_reason"],
        "attempt": attempt,
        "policy_attempt": attempt,
        "retry_attempt_offset": 0,
        "maximum_attempts": 3,
    }
    if classification == "challenge":
        reason["cooldown"] = {
            "stack_identifier": "ebay_anonymous",
            "until_utc_ns": now_utc_ns + delay_ns,
        }
    if attempt < 3:
        assert isinstance(outcome, RetryWork)
        assert outcome.delay_ns == delay_ns
        assert outcome.reason == reason
    else:
        assert isinstance(outcome, TerminalFailureWork)
        assert outcome.error == reason
    assert outcome.result == {
        **result,
        "state": "retryable_failure" if attempt < 3 else "terminal_failure",
    }
    assert tuple((value.name, value.object_identifier) for value in outcome.inputs) == (
        _SEARCH_REFERENCES
    )
    assert outcome.outputs == ()
    assert outcome.records == ()


@pytest.mark.anyio
@pytest.mark.parametrize("evidence", ["http_status_403", "http_status_429"])
@pytest.mark.parametrize("policy_attempt", [1, 2, 3])
async def test_blocked_search_response_emits_stack_cooldown_with_fresh_retry_budget(
    tmp_path: Path, evidence: str, policy_attempt: int
) -> None:
    now_utc_ns = 10_000_000_000_000
    offset = 6
    result: dict[str, JsonValue] = {
        "state": "response_failed",
        "response_classification": {"kind": "error_page", "evidence": [evidence]},
        "stopping_reason": "error_page",
        "listing_count": 0,
    }

    async def collect(*_args: object, **_kwargs: object) -> dict[str, JsonValue]:
        return result

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = build_ebay_worker_registry(
            EbaySearchWorkerDependencies(
                database,
                _directories(tmp_path),
                lambda: "unused",
                collector=collect,
                utc_now_ns=lambda: now_utc_ns,
            )
        )
        outcome = await registry.handlers[0].execute(
            CollectEbaySearchPayload(
                request=EbaySearchRequest(query="scope", stack_identifier="alternate"),
                retry_attempt_offset=offset,
            ).as_json(),
            AttemptContext(
                work_item_identifier="work",
                lease_token="lease",
                worker_identifier="worker",
                attempt=offset + policy_attempt,
                operation_identifier="operation",
            ),
        )

    assert isinstance(outcome, RetryWork if policy_attempt < 3 else TerminalFailureWork)
    reason = outcome.reason if isinstance(outcome, RetryWork) else outcome.error
    assert isinstance(reason, dict)
    delay_ns = 120_000_000_000 * 2 ** (policy_attempt - 1)
    assert reason["cooldown"] == {
        "stack_identifier": "alternate",
        "until_utc_ns": now_utc_ns + delay_ns,
    }
    assert reason["attempt"] == offset + policy_attempt
    assert reason["policy_attempt"] == policy_attempt
    assert reason["retry_attempt_offset"] == offset
    assert reason["maximum_attempts"] == 3
    if isinstance(outcome, RetryWork):
        assert outcome.delay_ns == delay_ns
    assert outcome.result == {
        **result,
        "state": "retryable_failure" if policy_attempt < 3 else "terminal_failure",
    }


@pytest.mark.anyio
@pytest.mark.parametrize("offset", [0, 6])
@pytest.mark.parametrize("policy_attempt", [1, 2, 3])
async def test_transport_acquisition_failure_has_bounded_fresh_retry_budget(
    tmp_path: Path, offset: int, policy_attempt: int
) -> None:
    acquisition: dict[str, JsonValue] = {
        "stopping_condition": "transport_failure",
        "exception_type": "ProxyConnectionError",
    }

    async def collect(*_args: object, **_kwargs: object) -> dict[str, JsonValue]:
        raise AcquisitionFailure("test transport failure", result=acquisition)

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = build_ebay_worker_registry(
            EbaySearchWorkerDependencies(
                database, _directories(tmp_path), lambda: "unused", collector=collect
            )
        )
        outcome = await registry.handlers[0].execute(
            CollectEbaySearchPayload(
                request=EbaySearchRequest(query="scope"), retry_attempt_offset=offset
            ).as_json(),
            AttemptContext(
                work_item_identifier="work",
                lease_token="lease",
                worker_identifier="worker",
                attempt=offset + policy_attempt,
                operation_identifier="operation",
            ),
        )

    assert isinstance(outcome, RetryWork if policy_attempt < 3 else TerminalFailureWork)
    reason = outcome.reason if isinstance(outcome, RetryWork) else outcome.error
    assert reason == {
        "kind": "ebay_search_acquisition_failure",
        "attempt": offset + policy_attempt,
        "policy_attempt": policy_attempt,
        "retry_attempt_offset": offset,
        "maximum_attempts": 3,
    }
    if isinstance(outcome, RetryWork):
        assert outcome.delay_ns == 2 ** (policy_attempt - 1) * 1_000_000_000
    assert outcome.result == {
        "state": "retryable_failure" if policy_attempt < 3 else "acquisition_failed",
        "acquisition": acquisition,
    }
    assert outcome.inputs == ()


@pytest.mark.anyio
async def test_nontransport_acquisition_failure_remains_terminal(tmp_path: Path) -> None:
    acquisition: dict[str, JsonValue] = {"stopping_condition": "response_too_large"}

    async def collect(*_args: object, **_kwargs: object) -> dict[str, JsonValue]:
        raise AcquisitionFailure("test acquisition failure", result=acquisition)

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = build_ebay_worker_registry(
            EbaySearchWorkerDependencies(
                database, _directories(tmp_path), lambda: "unused", collector=collect
            )
        )
        outcome = await registry.handlers[0].execute(
            CollectEbaySearchPayload(request=EbaySearchRequest(query="scope")).as_json(),
            AttemptContext(
                work_item_identifier="work",
                lease_token="lease",
                worker_identifier="worker",
                attempt=1,
                operation_identifier="operation",
            ),
        )

    assert isinstance(outcome, TerminalFailureWork)
    assert outcome.error == {"kind": "ebay_search_acquisition_failure"}
    assert outcome.result == {"state": "acquisition_failed", "acquisition": acquisition}


@pytest.mark.anyio
@pytest.mark.parametrize("policy_attempt", [1, 2, 3])
@pytest.mark.parametrize("stopping_reason", ["http_error", "invalid_next_page"])
async def test_operator_retry_retains_attempt_counter_and_has_fresh_bounded_budget(
    tmp_path: Path, policy_attempt: int, stopping_reason: str
) -> None:
    result: dict[str, JsonValue] = {
        "state": "response_failed",
        "response_classification": {
            "kind": "http_error" if stopping_reason == "http_error" else "usable_results",
            "evidence": [],
        },
        "stopping_reason": stopping_reason,
        "listing_count": 7 if stopping_reason == "invalid_next_page" else 0,
    }

    async def collect(*_args: object, **_kwargs: object) -> dict[str, JsonValue]:
        return result

    offset = 6
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = build_ebay_worker_registry(
            EbaySearchWorkerDependencies(
                database, _directories(tmp_path), lambda: "unused", collector=collect
            )
        )
        payload = CollectEbaySearchPayload(
            request=EbaySearchRequest(query="oscilloscope"), retry_attempt_offset=offset
        )
        handler = next(
            handler
            for handler in registry.handlers
            if handler.capability.payload_schema_version
            == COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION
        )
        outcome = await handler.execute(
            payload.as_json(),
            AttemptContext(
                work_item_identifier="work",
                lease_token="lease",
                worker_identifier="worker",
                attempt=offset + policy_attempt,
                operation_identifier="operation",
            ),
        )
        assert isinstance(outcome, RetryWork if policy_attempt < 3 else TerminalFailureWork)
        reason = outcome.reason if isinstance(outcome, RetryWork) else outcome.error
        assert isinstance(reason, dict)
        assert reason["attempt"] == offset + policy_attempt
        assert reason["policy_attempt"] == policy_attempt
        assert reason["retry_attempt_offset"] == offset
        assert reason["maximum_attempts"] == 3
        if isinstance(outcome, RetryWork):
            assert outcome.delay_ns == 2 ** (policy_attempt - 1) * 1_000_000_000
        assert outcome.result["listing_count"] == result["listing_count"]


@pytest.mark.anyio
async def test_invalid_retry_offset_does_not_make_provider_request(tmp_path: Path) -> None:
    async def collect(*_args: object, **_kwargs: object) -> dict[str, JsonValue]:
        pytest.fail("An invalid retry offset must not make a provider request")

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = build_ebay_worker_registry(
            EbaySearchWorkerDependencies(
                database, _directories(tmp_path), lambda: "unused", collector=collect
            )
        )
        outcome = await registry.handlers[0].execute(
            CollectEbaySearchPayload(
                request=EbaySearchRequest(query="scope"), retry_attempt_offset=3
            ).as_json(),
            AttemptContext(
                work_item_identifier="work",
                lease_token="lease",
                worker_identifier="worker",
                attempt=3,
                operation_identifier="operation",
            ),
        )
        assert isinstance(outcome, TerminalFailureWork)
        assert outcome.error["kind"] == "invalid_retry_attempt_offset"


@pytest.mark.anyio
async def test_retry_payload_schema_keeps_legacy_capability_and_search_identity(
    tmp_path: Path,
) -> None:
    request = EbaySearchRequest(query="scope")
    payload = CollectEbaySearchPayload(request=request)
    retried = payload.model_copy(update={"retry_attempt_offset": 3})
    first = collect_ebay_search_work(identifier="first", payload=payload, not_before_utc_ns=0)
    second = collect_ebay_search_work(identifier="second", payload=retried, not_before_utc_ns=0)
    assert first.deduplication_identity == second.deduplication_identity
    assert first.payload_schema_version == second.payload_schema_version == 2
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        dependencies = EbaySearchWorkerDependencies(
            database=database, directories=_directories(tmp_path), new_identifier=lambda: "unused"
        )
        assert build_ebay_worker_registry(dependencies).capabilities == (
            WorkCapability(
                kind=COLLECT_EBAY_SEARCH_WORK_KIND,
                payload_schema_version=LEGACY_COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION,
            ),
            WorkCapability(
                kind=COLLECT_EBAY_SEARCH_WORK_KIND,
                payload_schema_version=COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION,
            ),
        )


@pytest.mark.anyio
@pytest.mark.parametrize("classification", ["usable_results", "empty_results"])
async def test_worker_response_retry_can_complete_with_results_or_explicit_empty(
    tmp_path: Path, classification: str
) -> None:
    calls = 0

    async def collect(*_args: object, **_kwargs: object) -> dict[str, JsonValue]:
        nonlocal calls
        calls += 1
        kind = "unrecognized" if calls == 1 else classification
        return {
            "state": "completed",
            "response_classification": {"kind": kind, "evidence": []},
            "stopping_reason": "unrecognized_response"
            if calls == 1
            else "empty_results"
            if kind == "empty_results"
            else "no_next_page",
            "listing_count": 1 if kind == "usable_results" else 0,
        }

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = build_ebay_worker_registry(
            EbaySearchWorkerDependencies(
                database=database,
                directories=_directories(tmp_path),
                new_identifier=lambda: "unused",
                collector=collect,
            )
        )
        payload = CollectEbaySearchPayload(
            request=EbaySearchRequest(query="oscilloscope")
        ).as_json()
        for attempt in (1, 2):
            outcome = await registry.handlers[0].execute(
                payload,
                AttemptContext(
                    work_item_identifier="work-1",
                    lease_token=f"lease-{attempt}",
                    worker_identifier="worker-1",
                    attempt=attempt,
                    operation_identifier=f"operation-{attempt}",
                ),
            )
            if attempt == 1:
                assert isinstance(outcome, RetryWork)
            else:
                assert isinstance(outcome, CompletedWork)
                assert outcome.result == {
                    "state": "completed",
                    "response_classification": {"kind": classification, "evidence": []},
                    "stopping_reason": "empty_results"
                    if classification == "empty_results"
                    else "no_next_page",
                    "listing_count": 1 if classification == "usable_results" else 0,
                }
    assert calls == 2


@pytest.mark.anyio
@pytest.mark.parametrize("classification", [None, "unrecognized"])
async def test_worker_completes_with_child_records_already_published(
    tmp_path: Path, classification: str | None
) -> None:
    """Collector outputs retain their owner on completion and durable retry."""

    path = tmp_path / "carl.sqlite3"
    result: dict[str, JsonValue] = {
        "state": "completed",
        "acquisition_record_identifiers": ["acquisition-1", "acquisition-2"],
        "extraction_record_identifiers": ["extraction-1", "extraction-2"],
        "search_run_record_identifier": "search-run-1",
        "listing_count": 5,
    }
    if classification is not None:
        result.update(
            response_classification={"kind": classification, "evidence": []},
            stopping_reason="unrecognized_response",
            listing_count=0,
        )

    async def collect(
        database: Database,
        *,
        directories: CarlDirectories,
        request: EbaySearchRequest,
        new_identifier: Callable[[], str],
        search_run_identifier: str,
    ) -> dict[str, JsonValue]:
        del directories, new_identifier, search_run_identifier
        assert request.query == "oscilloscope"
        # Model the real collector: each child has already published its outputs.
        for _, identifier in _SEARCH_REFERENCES:
            operation_identifier = f"collector-{identifier}"
            await database.begin_operation(
                operation_id=operation_identifier,
                component=Component(ComponentId(("test", "ebay", "collector")), 1, collect),
                provenance=await _provenance(),
                invocation={},
                configuration={},
                started_at_utc="2026-09-30T00:00:00+00:00",
            )
            await database.complete_operation(
                operation_id=operation_identifier,
                records=(
                    RecordDraft(
                        identifier=identifier,
                        kind=("test", "ebay", "child"),
                        schema_version=1,
                        value={},
                    ),
                ),
                artifacts=(),
                outputs=(NamedOutput(name=("record",), object_identifier=identifier),),
                result={},
                ended_at_utc="2026-09-30T00:00:00+00:00",
                duration_ns=0,
            )
        return result

    settings = WorkerSettings(
        worker_count=1,
        lease_duration_ns=60_000_000_000,
        renewal_interval_ns=10_000_000_000,
        idle_poll_interval_ns=1_000_000,
    )
    async with Database.managed(path, initialize=True) as database:
        registry = build_ebay_worker_registry(
            EbaySearchWorkerDependencies(
                database=database,
                directories=_directories(tmp_path),
                new_identifier=lambda: str(uuid4()),
                collector=collect,
            )
        )
        await database.enqueue_work(
            collect_ebay_search_work(
                identifier="work-1",
                payload=CollectEbaySearchPayload(request=EbaySearchRequest(query="oscilloscope")),
                not_before_utc_ns=0,
            ),
            WorkRequester(
                request_identifier="request-1",
                kind=("test", "ebay", "request"),
                identifier="work-1",
                context={},
            ),
            event_identifier=str(uuid4()),
            enqueued_at_utc_ns=time_ns(),
        )
        claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker-1",
            lease_token="lease-1",
            lease_duration_ns=settings.lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=str(uuid4()),
        )
        assert claim.lease is not None
        outcome = await execute_lease(
            database=database,
            registry=registry,
            settings=settings,
            services=WorkerRuntimeServices(
                new_identifier=lambda: str(uuid4()),
                utc_now_ns=time_ns,
                monotonic_ns=perf_counter_ns,
                code_provenance=_provenance,
                invocation=lambda: {},
            ),
            lease=claim.lease,
        )
        assert isinstance(outcome, CompletedWork if classification is None else RetryWork)
        work = await database.work("work-1")
        assert work["state"] == ("completed" if classification is None else "pending")
        assert work["result"] == outcome.result
        assert outcome.result == {
            **result,
            "state": "completed" if classification is None else "retryable_failure",
        }

    with closing(sqlite3.connect(path)) as connection:
        operation_identifier, operation_state = connection.execute(
            """
            SELECT operations.id, operations.state FROM operations
            JOIN work_operations ON work_operations.operation_id = operations.id
            WHERE work_operations.work_item_id = 'work-1'
            """
        ).fetchone()
        assert operation_state == ("completed" if classification is None else "failed")
        inputs = connection.execute(
            "SELECT name_parts_json, object_id FROM operation_inputs WHERE operation_id = ?",
            (operation_identifier,),
        ).fetchall()
        assert {(tuple(json.loads(name)), identifier) for name, identifier in inputs} == set(
            _SEARCH_REFERENCES
        )
        outputs = connection.execute(
            """
            SELECT operation_id, object_id FROM operation_outputs
            JOIN objects ON objects.id = operation_outputs.object_id
            WHERE objects.kind_parts_json != '["carl","ebay","search_attempt"]'
            """
        ).fetchall()
        assert set(outputs) == {
            (f"collector-{identifier}", identifier) for _, identifier in _SEARCH_REFERENCES
        }
        assert connection.execute(
            """
            SELECT count(*) FROM objects
            WHERE kind_parts_json = '["carl","ebay","search_attempt"]'
              AND created_by_operation_id = ?
            """,
            (operation_identifier,),
        ).fetchone() == (1,)
