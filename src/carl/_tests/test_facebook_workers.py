import sqlite3
from contextlib import closing
from itertools import count
from pathlib import Path
from time import perf_counter_ns, time_ns

import httpx
import pytest

from carl._tests.test_facebook import HTML
from carl.core.facebook_work import (
    CollectItemPayload,
    ExtractItemPayload,
    collect_item_work,
    extract_item_work,
)
from carl.core.http import RequestPlan
from carl.core.json import decode_json, encode_json
from carl.core.models import CodeProvenance, Header
from carl.core.review import ListCandidatesRequest, candidate_page
from carl.core.work import WorkRequester, WorkState
from carl.core.worker import WorkerSettings
from carl.facebook_workers import (
    ACQUISITION_RETRY_DELAY_NS,
    COLLECT_FACEBOOK_ITEM,
    EXTRACT_FACEBOOK,
    MAX_ACQUISITION_ATTEMPTS,
    FacebookWorkerDependencies,
    build_facebook_worker_registry,
)
from carl.io.httpx import DirectHttpxAcquirer
from carl.io.sqlite import Database
from carl.io.worker import WorkerRuntimeServices, execute_lease


class _CompleteStream(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield HTML.encode()


class _BytesStream(httpx.AsyncByteStream):
    def __init__(self, content: bytes):
        self.content = content

    async def __aiter__(self):
        yield self.content


class _BrokenStream(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield b"partial"
        raise httpx.ReadError("fixture interrupted")


def _provenance() -> CodeProvenance:
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


async def _async_provenance() -> CodeProvenance:
    return _provenance()


def _settings() -> WorkerSettings:
    return WorkerSettings(
        worker_count=1,
        lease_duration_ns=1_000_000_000,
        renewal_interval_ns=100_000_000,
        idle_poll_interval_ns=10_000_000,
    )


def _requester(identifier: str) -> WorkRequester:
    return WorkRequester(
        request_identifier=f"request-{identifier}",
        kind=("carl", "test", "requester"),
        identifier=identifier,
        context={},
    )


@pytest.mark.anyio
async def test_durable_collection_enqueues_repeatable_offline_extraction(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    identifiers = count()
    request_count = 0

    def new_identifier() -> str:
        return f"id-{next(identifiers)}"

    def handle_request(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        assert request.headers["x-test-order"] == "one, two"
        return httpx.Response(
            200,
            headers=[
                ("Content-Type", "text/html; charset=utf-8"),
                ("Date", "Fri, 18 Sep 2026 20:00:00 GMT"),
            ],
            stream=_CompleteStream(),
        )

    services = WorkerRuntimeServices(
        new_identifier=new_identifier,
        utc_now_ns=time_ns,
        monotonic_ns=perf_counter_ns,
        code_provenance=_async_provenance,
        invocation=lambda: {"kind": "test"},
    )
    payload = CollectItemPayload(
        listing_id="123",
        request_plan=RequestPlan(
            url="https://www.facebook.com/marketplace/item/123/",
            headers=(
                Header(name=b"X-Test-Order", value=b"one"),
                Header(name=b"X-Test-Order", value=b"two"),
            ),
            routing=("test", "mock-transport"),
        ),
    )

    async with Database.managed(path, initialize=True) as database:
        registry = build_facebook_worker_registry(
            FacebookWorkerDependencies(
                database=database,
                acquirer=DirectHttpxAcquirer(httpx.MockTransport(handle_request)),
                new_identifier=new_identifier,
            )
        )
        await database.enqueue_work(
            collect_item_work(
                identifier="collect-work",
                payload=payload,
                not_before_utc_ns=0,
            ),
            _requester("collect"),
            event_identifier=new_identifier(),
            enqueued_at_utc_ns=time_ns(),
        )
        collection_claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="collection-lease",
            lease_duration_ns=_settings().lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=new_identifier(),
        )
        assert collection_claim.lease is not None
        await execute_lease(
            database=database,
            registry=registry,
            settings=_settings(),
            services=services,
            lease=collection_claim.lease,
        )

        extraction_claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="extraction-lease",
            lease_duration_ns=_settings().lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=new_identifier(),
        )
        assert extraction_claim.lease is not None
        extraction_payload = ExtractItemPayload.model_validate_json(
            encode_json(extraction_claim.lease.payload)
        )
        await execute_lease(
            database=database,
            registry=registry,
            settings=_settings(),
            services=services,
            lease=extraction_claim.lease,
        )

        repeated = await database.enqueue_work(
            extract_item_work(
                identifier="repeat-extraction",
                payload=extraction_payload,
                extractor_identifier=EXTRACT_FACEBOOK.parts,
                extractor_schema_version=1,
                not_before_utc_ns=0,
            ),
            _requester("repeat-extraction"),
            event_identifier=new_identifier(),
            enqueued_at_utc_ns=time_ns(),
        )
        assert repeated.created
        repeat_claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="repeat-lease",
            lease_duration_ns=_settings().lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=new_identifier(),
        )
        assert repeat_claim.lease is not None
        await execute_lease(
            database=database,
            registry=registry,
            settings=_settings(),
            services=services,
            lease=repeat_claim.lease,
        )

        assert await database.work_state("collect-work") is WorkState.COMPLETED
        assert await database.work_state("repeat-extraction") is WorkState.COMPLETED
        successful_results = await database.successful_facebook_item_page_results(("123",))
        assert len(successful_results) == 2
        assert {result.acquisition_record_identifier for result in successful_results} == {
            extraction_payload.acquisition_record_identifier
        }
        assert [result.extraction_completion_sequence for result in successful_results] == sorted(
            result.extraction_completion_sequence for result in successful_results
        )
        assert (
            successful_results[0].extraction_completion_sequence
            < successful_results[1].extraction_completion_sequence
        )
        review_sources = await database.facebook_review_candidate_sources()
        assert tuple(source.listing_identifier for source in review_sources) == ("123", "123")
        assert await database.facebook_review_candidate_sources(("123",)) == review_sources
        assert await database.facebook_review_candidate_sources(("999",)) == ()
        assert {source.observation_record_identifier for source in review_sources} == {
            result.observation_record_identifier for result in successful_results
        }
        assert all(source.analyses == () for source in review_sources)
        page = candidate_page(review_sources, ListCandidatesRequest())
        assert len(page.candidates) == 1
        assert (
            page.candidates[0].observation_record_identifier
            == successful_results[-1].observation_record_identifier
        )
    assert request_count == 1
    with closing(sqlite3.connect(path)) as connection:
        work_states = connection.execute(
            "SELECT state FROM work_items ORDER BY created_at_utc_ns, id"
        ).fetchall()
        observations = connection.execute(
            """
            SELECT records.value_json
            FROM records JOIN objects ON objects.id = records.object_id
            WHERE objects.kind_parts_json = '["carl","facebook","listing_observation"]'
            """
        ).fetchall()
        extraction_inputs = connection.execute(
            """
            SELECT operation_inputs.name_parts_json
            FROM operation_inputs
            JOIN operations ON operations.id = operation_inputs.operation_id
            WHERE operations.component_parts_json = ?
            ORDER BY operation_inputs.operation_id, operation_inputs.name_parts_json
            """,
            (encode_json(list(EXTRACT_FACEBOOK.parts)),),
        ).fetchall()
        collection_configuration = connection.execute(
            """
            SELECT configuration_json FROM operations
            WHERE component_parts_json = ?
            """,
            (encode_json(list(COLLECT_FACEBOOK_ITEM.parts)),),
        ).fetchone()

    assert work_states == [("completed",), ("completed",), ("completed",)]
    assert len(observations) == 2
    assert all(decode_json(row[0])["listing_id"] == "123" for row in observations)
    assert all(
        decode_json(row[0])["response_classification"]["kind"] == "full_listing"
        for row in observations
    )
    assert extraction_inputs == [
        ('["acquisition"]',),
        ('["terminal_response","body"]',),
        ('["acquisition"]',),
        ('["terminal_response","body"]',),
    ]
    assert collection_configuration is not None
    configuration = decode_json(collection_configuration[0])
    assert configuration["payload"]["request_plan"]["routing"] == ["test", "mock-transport"]
    assert configuration["payload"]["request_plan"]["headers"] == [
        {"name_latin1": "X-Test-Order", "value_latin1": "one"},
        {"name_latin1": "X-Test-Order", "value_latin1": "two"},
    ]


@pytest.mark.anyio
async def test_login_response_is_retained_as_terminal_extraction_failure(tmp_path: Path) -> None:
    identifiers = count()

    def new_identifier() -> str:
        return f"id-{next(identifiers)}"

    def login_response(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            request=httpx.Request(
                "GET",
                "https://www.facebook.com/login/?next=%2Fmarketplace%2Fitem%2F123%2F",
            ),
            headers={"Content-Type": "text/html; charset=utf-8"},
            stream=_BytesStream(b'<html><form id="login_form"></form></html>'),
        )

    services = WorkerRuntimeServices(
        new_identifier=new_identifier,
        utc_now_ns=time_ns,
        monotonic_ns=perf_counter_ns,
        code_provenance=_async_provenance,
        invocation=lambda: {"kind": "test"},
    )
    payload = CollectItemPayload(
        listing_id="123",
        request_plan=RequestPlan(
            url="https://www.facebook.com/marketplace/item/123/",
            routing=("test", "login-response"),
        ),
    )
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        registry = build_facebook_worker_registry(
            FacebookWorkerDependencies(
                database=database,
                acquirer=DirectHttpxAcquirer(httpx.MockTransport(login_response)),
                new_identifier=new_identifier,
            )
        )
        await database.enqueue_work(
            collect_item_work(identifier="collect-work", payload=payload, not_before_utc_ns=0),
            _requester("collect"),
            event_identifier=new_identifier(),
            enqueued_at_utc_ns=time_ns(),
        )
        collection_claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="collection-lease",
            lease_duration_ns=_settings().lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=new_identifier(),
        )
        assert collection_claim.lease is not None
        await execute_lease(
            database=database,
            registry=registry,
            settings=_settings(),
            services=services,
            lease=collection_claim.lease,
        )
        extraction_claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="extraction-lease",
            lease_duration_ns=_settings().lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=new_identifier(),
        )
        assert extraction_claim.lease is not None
        extraction_identifier = extraction_claim.lease.work_item_identifier
        await execute_lease(
            database=database,
            registry=registry,
            settings=_settings(),
            services=services,
            lease=extraction_claim.lease,
        )
        extraction = await database.work(extraction_identifier)
        assert extraction["state"] == WorkState.TERMINAL_FAILURE.value
        assert extraction["error"] == {
            "kind": "unusable_item_response",
            "response_classification": "login_page",
        }
        result = extraction["result"]
        assert isinstance(result, dict)
        _, _, observation = await database.get_record(result["observation_record_identifier"])
        assert isinstance(observation, dict)
        assert observation["response_classification"]["kind"] == "login_page"


@pytest.mark.anyio
async def test_incomplete_worker_acquisition_is_retryable_without_partial_body(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    identifiers = count()
    now = 1_000_000_000
    request_count = 0

    def new_identifier() -> str:
        return f"id-{next(identifiers)}"

    def current_time_ns() -> int:
        return now

    def broken_response(request: httpx.Request) -> httpx.Response:
        nonlocal request_count
        request_count += 1
        return httpx.Response(
            200,
            headers={"Content-Type": "text/html"},
            stream=_BrokenStream(),
        )

    services = WorkerRuntimeServices(
        new_identifier=new_identifier,
        utc_now_ns=current_time_ns,
        monotonic_ns=current_time_ns,
        code_provenance=_async_provenance,
        invocation=lambda: {"kind": "test"},
    )
    payload = CollectItemPayload(
        listing_id="123",
        request_plan=RequestPlan(
            url="https://www.facebook.com/marketplace/item/123/",
            routing=("test", "broken-transport"),
        ),
    )

    async with Database.managed(path, initialize=True) as database:
        registry = build_facebook_worker_registry(
            FacebookWorkerDependencies(
                database=database,
                acquirer=DirectHttpxAcquirer(httpx.MockTransport(broken_response)),
                new_identifier=new_identifier,
            )
        )
        await database.enqueue_work(
            collect_item_work(identifier="collect-work", payload=payload, not_before_utc_ns=0),
            _requester("collect"),
            event_identifier=new_identifier(),
            enqueued_at_utc_ns=now,
        )
        for attempt in range(1, MAX_ACQUISITION_ATTEMPTS + 1):
            claim = await database.claim_work(
                supported_capabilities=registry.capabilities,
                worker_identifier="worker",
                lease_token=f"lease-{attempt}",
                lease_duration_ns=_settings().lease_duration_ns,
                utc_now_ns=current_time_ns,
                event_identifier=new_identifier(),
            )
            assert claim.lease is not None
            assert claim.lease.attempt == attempt
            await execute_lease(
                database=database,
                registry=registry,
                settings=_settings(),
                services=services,
                lease=claim.lease,
            )
            expected_state = (
                WorkState.PENDING
                if attempt < MAX_ACQUISITION_ATTEMPTS
                else WorkState.TERMINAL_FAILURE
            )
            assert await database.work_state("collect-work") is expected_state
            now += ACQUISITION_RETRY_DELAY_NS

    with closing(sqlite3.connect(path)) as connection:
        operations = connection.execute(
            "SELECT state, result_json, error_json FROM operations ORDER BY rowid"
        ).fetchall()
        artifacts = connection.execute("SELECT count(*) FROM artifacts").fetchone()
        work = connection.execute(
            "SELECT state, attempt FROM work_items WHERE id = 'collect-work'"
        ).fetchone()

    assert request_count == MAX_ACQUISITION_ATTEMPTS
    assert len(operations) == MAX_ACQUISITION_ATTEMPTS
    assert all(operation[0] == "failed" for operation in operations)
    for operation in operations:
        result = decode_json(operation[1])
        assert result["acquisition"]["stopping_condition"] == "transport_failure"
        assert result["acquisition"]["hops"][-1]["response"]["body"] == {
            "state": "unavailable",
            "reason": "incomplete_transfer",
        }
    assert all('"decision":"retry"' in operation[2] for operation in operations[:-1])
    assert '"decision":"terminal"' in operations[-1][2]
    assert artifacts == (0,)
    assert work == ("terminal_failure", MAX_ACQUISITION_ATTEMPTS)


@pytest.mark.anyio
async def test_completed_redirect_body_is_retained_when_later_hop_fails(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    identifiers = count()

    def new_identifier() -> str:
        return f"id-{next(identifiers)}"

    def redirect_then_fail(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/123/"):
            return httpx.Response(
                302,
                headers={"Location": "/marketplace/item/123/next"},
                stream=_BytesStream(b"complete redirect body"),
            )
        return httpx.Response(
            200,
            headers={"Content-Type": "text/html"},
            stream=_BrokenStream(),
        )

    services = WorkerRuntimeServices(
        new_identifier=new_identifier,
        utc_now_ns=time_ns,
        monotonic_ns=perf_counter_ns,
        code_provenance=_async_provenance,
        invocation=lambda: {"kind": "test"},
    )
    payload = CollectItemPayload(
        listing_id="123",
        request_plan=RequestPlan(
            url="https://www.facebook.com/marketplace/item/123/",
            routing=("test", "redirect-then-fail"),
        ),
    )

    async with Database.managed(path, initialize=True) as database:
        registry = build_facebook_worker_registry(
            FacebookWorkerDependencies(
                database=database,
                acquirer=DirectHttpxAcquirer(httpx.MockTransport(redirect_then_fail)),
                new_identifier=new_identifier,
            )
        )
        await database.enqueue_work(
            collect_item_work(identifier="collect-work", payload=payload, not_before_utc_ns=0),
            _requester("collect"),
            event_identifier=new_identifier(),
            enqueued_at_utc_ns=time_ns(),
        )
        claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="lease",
            lease_duration_ns=_settings().lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=new_identifier(),
        )
        assert claim.lease is not None
        await execute_lease(
            database=database,
            registry=registry,
            settings=_settings(),
            services=services,
            lease=claim.lease,
        )
        assert await database.work_state("collect-work") is WorkState.PENDING

    with closing(sqlite3.connect(path)) as connection:
        operation = connection.execute(
            "SELECT result_json FROM operations WHERE component_parts_json = ?",
            (encode_json(list(COLLECT_FACEBOOK_ITEM.parts)),),
        ).fetchone()
        artifact = connection.execute(
            """
            SELECT artifacts.object_id, content.inline_bytes
            FROM artifacts JOIN content USING (sha256, size)
            """
        ).fetchone()
        output = connection.execute(
            "SELECT name_parts_json, object_id FROM operation_outputs"
        ).fetchone()

    assert operation is not None
    result = decode_json(operation[0])
    body_reference = result["acquisition"]["hops"][0]["response"]["body"]
    assert body_reference["state"] == "available"
    assert artifact == (body_reference["artifact_id"], b"complete redirect body")
    assert output == ('["response","0","body"]', body_reference["artifact_id"])


@pytest.mark.anyio
async def test_extraction_failure_retains_known_input_provenance(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    identifiers = count()

    def new_identifier() -> str:
        return f"id-{next(identifiers)}"

    def complete_response(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": "text/html; charset=utf-8"},
            stream=_CompleteStream(),
        )

    services = WorkerRuntimeServices(
        new_identifier=new_identifier,
        utc_now_ns=time_ns,
        monotonic_ns=perf_counter_ns,
        code_provenance=_async_provenance,
        invocation=lambda: {"kind": "test"},
    )
    payload = CollectItemPayload(
        listing_id="123",
        request_plan=RequestPlan(
            url="https://www.facebook.com/marketplace/item/123/",
            routing=("test", "corrupt-saved-body"),
        ),
    )

    async with Database.managed(path, initialize=True) as database:
        registry = build_facebook_worker_registry(
            FacebookWorkerDependencies(
                database=database,
                acquirer=DirectHttpxAcquirer(httpx.MockTransport(complete_response)),
                new_identifier=new_identifier,
            )
        )
        await database.enqueue_work(
            collect_item_work(identifier="collect-work", payload=payload, not_before_utc_ns=0),
            _requester("collect"),
            event_identifier=new_identifier(),
            enqueued_at_utc_ns=time_ns(),
        )
        collection_claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="collection-lease",
            lease_duration_ns=_settings().lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=new_identifier(),
        )
        assert collection_claim.lease is not None
        await execute_lease(
            database=database,
            registry=registry,
            settings=_settings(),
            services=services,
            lease=collection_claim.lease,
        )

    with closing(sqlite3.connect(path)) as connection:
        connection.execute(
            "UPDATE content SET inline_bytes = ?",
            (b"X" * len(HTML.encode()),),
        )
        connection.commit()

    async with Database.managed(path) as database:
        registry = build_facebook_worker_registry(
            FacebookWorkerDependencies(
                database=database,
                acquirer=DirectHttpxAcquirer(httpx.MockTransport(complete_response)),
                new_identifier=new_identifier,
            )
        )
        extraction_claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="extraction-lease",
            lease_duration_ns=_settings().lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=new_identifier(),
        )
        assert extraction_claim.lease is not None
        extraction_work_identifier = extraction_claim.lease.work_item_identifier
        await execute_lease(
            database=database,
            registry=registry,
            settings=_settings(),
            services=services,
            lease=extraction_claim.lease,
        )
        assert await database.work_state(extraction_work_identifier) is WorkState.TERMINAL_FAILURE

    with closing(sqlite3.connect(path)) as connection:
        operation = connection.execute(
            """
            SELECT operations.error_json
            FROM operations
            WHERE operations.component_parts_json = ?
            """,
            (encode_json(list(EXTRACT_FACEBOOK.parts)),),
        ).fetchone()
        inputs = connection.execute(
            """
            SELECT operation_inputs.name_parts_json
            FROM operation_inputs
            JOIN operations ON operations.id = operation_inputs.operation_id
            WHERE operations.component_parts_json = ?
            ORDER BY operation_inputs.name_parts_json
            """,
            (encode_json(list(EXTRACT_FACEBOOK.parts)),),
        ).fetchall()

    assert operation is not None
    assert decode_json(operation[0]) == {
        "kind": "extraction_failure",
        "type": "ValueError",
    }
    assert inputs == [('["acquisition"]',), ('["terminal_response","body"]',)]
