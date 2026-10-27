"""Dataset setup is durable, serialized, and cancellable without masking errors."""

import anyio
import pytest
from pydantic import ValidationError

from carl.core.travel_data import (
    TRAVEL_DATA_SETUP_WORK_KIND,
    TravelDataFailure,
    TravelDataSetupRequest,
    TravelDataSetupResult,
    travel_data_constraint,
    travel_data_work,
    travel_data_work_identifier,
)
from carl.core.work import SchedulingScopeKind
from carl.core.worker import AttemptContext, CompletedWork, RetryWork, TerminalFailureWork
from carl.travel_data_workers import build_travel_data_worker_registry


def _request() -> TravelDataSetupRequest:
    return TravelDataSetupRequest(request_identifier="build-map-1", provider="osrm")


def _context() -> AttemptContext:
    return AttemptContext(
        work_item_identifier="dataset-work",
        lease_token="lease",
        worker_identifier="worker",
        attempt=1,
        operation_identifier="operation",
    )


@pytest.mark.anyio
async def test_setup_publishes_manager_result() -> None:
    result = TravelDataSetupResult(
        provider="osrm",
        dataset_identifier="map-v1",
        manifest_path="/data/routing/map-v1/manifest.json",
        activated=True,
        message="Local routing data is ready.",
    )
    calls: list[TravelDataSetupRequest] = []

    class Service:
        async def setup(self, request: TravelDataSetupRequest) -> TravelDataSetupResult:
            calls.append(request)
            return result

    handler = build_travel_data_worker_registry(Service()).handlers[0]
    assert handler.capability.kind == TRAVEL_DATA_SETUP_WORK_KIND
    outcome = await handler.execute(_request().model_dump(mode="json"), _context())
    assert calls == [_request()]
    assert isinstance(outcome, CompletedWork)
    assert outcome.result == result.model_dump(mode="json")


@pytest.mark.anyio
async def test_expected_setup_failure_preserves_only_manager_sanitized_message() -> None:
    class Service:
        async def setup(self, request: TravelDataSetupRequest) -> TravelDataSetupResult:
            try:
                raise OSError("private details must not enter the work result")
            except OSError as error:
                raise TravelDataFailure(
                    "insufficient_disk_space", "Free disk space and request setup again."
                ) from error

    handler = build_travel_data_worker_registry(Service()).handlers[0]
    outcome = await handler.execute(_request().model_dump(mode="json"), _context())
    assert isinstance(outcome, TerminalFailureWork)
    assert outcome.error == {
        "kind": "insufficient_disk_space",
        "message": "Free disk space and request setup again.",
    }
    assert outcome.result == {"state": "failed", "provider": "osrm"}


@pytest.mark.anyio
async def test_foreground_build_contention_sleeps_instead_of_failing_or_spinning() -> None:
    class Service:
        async def setup(self, request: TravelDataSetupRequest) -> TravelDataSetupResult:
            raise TravelDataFailure("travel_data_busy", "Another explicit build is running.")

    handler = build_travel_data_worker_registry(Service()).handlers[0]
    outcome = await handler.execute(_request().model_dump(mode="json"), _context())
    assert isinstance(outcome, RetryWork)
    assert outcome.delay_ns == 60_000_000_000
    assert outcome.result == {"state": "waiting_for_dataset_build", "provider": "osrm"}


@pytest.mark.anyio
async def test_programming_errors_are_not_disguised_as_setup_failures() -> None:
    class Service:
        async def setup(self, request: TravelDataSetupRequest) -> TravelDataSetupResult:
            raise ValueError("unexpected implementation error")

    handler = build_travel_data_worker_registry(Service()).handlers[0]
    with pytest.raises(ValueError, match="unexpected implementation error"):
        await handler.execute(_request().model_dump(mode="json"), _context())


@pytest.mark.anyio
async def test_setup_cancellation_propagates() -> None:
    class Service:
        async def setup(self, request: TravelDataSetupRequest) -> TravelDataSetupResult:
            await anyio.sleep_forever()
            raise AssertionError("Cancelled setup must not complete")

    handler = build_travel_data_worker_registry(Service()).handlers[0]
    with anyio.CancelScope() as scope:
        scope.cancel()
        with pytest.raises(anyio.get_cancelled_exc_class()):
            await handler.execute(_request().model_dump(mode="json"), _context())


def test_setup_definition_has_stable_request_identity_and_build_constraint_scope() -> None:
    request = _request()
    definition = travel_data_work(request)
    assert definition.identifier == travel_data_work_identifier(request.request_identifier)
    assert len(definition.identifier) == len("travel-data-") + 64
    assert definition.identifier != travel_data_work_identifier("build-map-2")
    assert definition.kind == TRAVEL_DATA_SETUP_WORK_KIND
    assert definition.deduplication_identity == (request.request_identifier,)
    assert definition.payload == request.model_dump(mode="json")
    assert definition.scopes[0].kind == SchedulingScopeKind.OVERALL
    constraint = travel_data_constraint()
    assert constraint.maximum_active == 1
    assert constraint.scope in definition.scopes
    assert constraint.scope.kind == SchedulingScopeKind.WORK_KIND


@pytest.mark.parametrize("identifier", ["", "   ", " leading", "trailing ", "x" * 201])
def test_setup_request_rejects_blank_untrimmed_or_oversized_identity(identifier: str) -> None:
    with pytest.raises(ValidationError):
        TravelDataSetupRequest(request_identifier=identifier, provider="geonames")


@pytest.mark.parametrize("radius", [0, 1001, float("inf"), float("nan")])
def test_setup_request_requires_finite_bounded_radius(radius: float) -> None:
    with pytest.raises(ValidationError):
        TravelDataSetupRequest(
            request_identifier="radius-check", provider="osrm", radius_miles=radius
        )
