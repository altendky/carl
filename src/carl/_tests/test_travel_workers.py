"""Route refreshes preserve exact inputs and never publish under a changed origin."""

from collections.abc import Sequence
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from carl.core.json import encode_json
from carl.core.travel import TravelCoordinate, TravelDestination, TravelEstimate
from carl.core.travel_refresh import (
    REFRESH_LISTING_ROUTE_WORK_KIND,
    TRAVEL_ESTIMATE_RECORD_KIND,
    RequestListingRouteRefresh,
    TravelEstimateRecord,
    TravelRefreshPayload,
    refresh_listing_route_work,
    travel_destination_identity,
    travel_refresh_work_identifier,
)
from carl.core.worker import AttemptContext, CompletedWork, TerminalFailureWork
from carl.travel_workers import TravelWorkerDependencies, build_travel_worker_registry


class Service:
    def __init__(self, estimate: TravelEstimate):
        self.estimate = estimate
        self.calls: list[tuple[tuple[TravelDestination, ...], str]] = []

    async def refresh_many(
        self, destinations: Sequence[TravelDestination], *, expected_origin_identity: str
    ) -> tuple[TravelEstimate, ...]:
        self.calls.append((tuple(destinations), expected_origin_identity))
        return (self.estimate,)


def _payload(listing_identifier: str = "1234567890") -> TravelRefreshPayload:
    destination = TravelDestination(
        coordinate=TravelCoordinate(latitude=41.5, longitude=-78.2),
        label="Example destination",
        precision="source_approximate",
    )
    return TravelRefreshPayload(
        request=RequestListingRouteRefresh(
            request_identifier="route-attempt-1", listing_identifier=listing_identifier
        ),
        destination=destination,
        origin_identity="origin-fingerprint",
        destination_identity=travel_destination_identity(destination),
        source_record_identifiers=("observation-record", "search-card-record"),
    )


def _context() -> AttemptContext:
    return AttemptContext(
        work_item_identifier="route-work",
        lease_token="lease",
        worker_identifier="worker",
        attempt=1,
        operation_identifier="operation",
    )


@pytest.mark.anyio
@pytest.mark.parametrize("listing_identifier", ["1234567890", "ebay:1234567890"])
@pytest.mark.parametrize("unavailable", [False, True])
async def test_route_refresh_publishes_independent_ready_or_unknown_record(
    listing_identifier: str, unavailable: bool
) -> None:
    payload = _payload(listing_identifier)
    estimate = (
        TravelEstimate(status="unavailable", unavailable_reason="provider_http_502")
        if unavailable
        else TravelEstimate(
            status="ready",
            distance_meters=12345.0,
            duration_seconds=987.0,
            destination_label=payload.destination.label,
            location_precision="source_approximate",
        )
    )
    service = Service(estimate)
    identifiers = iter(("estimate-one", "estimate-two"))
    registry = build_travel_worker_registry(
        TravelWorkerDependencies(
            service=service,
            new_identifier=lambda: next(identifiers),
            utc_now=lambda: datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
        )
    )
    handler = registry.handlers[0]
    assert handler.capability.kind == REFRESH_LISTING_ROUTE_WORK_KIND
    outcomes = [
        await handler.execute(payload.model_dump(mode="json"), _context()) for _ in range(2)
    ]
    assert service.calls == [((payload.destination,), "origin-fingerprint")] * 2
    for index, outcome in enumerate(outcomes):
        assert isinstance(outcome, CompletedWork)
        draft = outcome.records[0]
        assert draft.kind == TRAVEL_ESTIMATE_RECORD_KIND
        record = TravelEstimateRecord.model_validate(draft.value)
        assert record.record_identifier == ("estimate-one", "estimate-two")[index]
        assert record.listing_id == listing_identifier
        assert record.origin_identity == payload.origin_identity
        assert record.destination_identity == payload.destination_identity
        assert record.destination == payload.destination
        assert record.estimate.model_copy(update={"expires_at_utc": None}) == estimate
        assert record.estimate.expires_at_utc == (
            "2026-01-02T03:05:05+00:00" if unavailable else "2026-01-09T03:04:05+00:00"
        )
        assert record.recorded_at_utc == "2026-01-02T03:04:05+00:00"
        assert (
            tuple(item.object_identifier for item in outcome.inputs)
            == payload.source_record_identifiers
        )
        assert outcome.outputs[0].object_identifier == record.record_identifier
        assert outcome.result == {
            "state": estimate.status,
            "estimate_record_identifier": record.record_identifier,
            "listing_identifier": listing_identifier,
        }


@pytest.mark.anyio
async def test_origin_change_never_publishes_estimate_record() -> None:
    service = Service(TravelEstimate(status="unavailable", unavailable_reason="origin_changed"))

    def unexpected_identifier() -> str:
        raise AssertionError("A changed origin must not create an estimate identifier")

    handler = build_travel_worker_registry(
        TravelWorkerDependencies(service=service, new_identifier=unexpected_identifier)
    ).handlers[0]
    outcome = await handler.execute(_payload().model_dump(mode="json"), _context())
    assert isinstance(outcome, TerminalFailureWork)
    assert outcome.error == {"kind": "origin_changed"}
    assert outcome.records == ()
    assert outcome.outputs == ()


def test_destination_identity_changes_only_with_exact_destination_values() -> None:
    destination = _payload().destination
    same = TravelDestination.model_validate_json(destination.model_dump_json())
    assert travel_destination_identity(same) == travel_destination_identity(destination)
    assert travel_destination_identity(
        destination.model_copy(update={"label": "Other locality"})
    ) != travel_destination_identity(destination)
    changed = destination.model_copy(
        update={"coordinate": TravelCoordinate(latitude=41.6, longitude=-78.2)}
    )
    assert travel_destination_identity(changed) != travel_destination_identity(destination)
    with pytest.raises(ValidationError, match="does not match"):
        TravelRefreshPayload.model_validate_json(
            encode_json(
                {
                    **_payload().model_dump(mode="json"),
                    "destination": changed.model_dump(mode="json"),
                }
            )
        )


def test_request_work_identifier_is_stable_and_request_scoped() -> None:
    payload = _payload()
    definition = refresh_listing_route_work(payload)
    assert definition.identifier == travel_refresh_work_identifier("route-attempt-1")
    assert len(definition.identifier) == len("travel-refresh-") + 64
    assert definition.identifier != travel_refresh_work_identifier("route-attempt-2")
    assert definition.deduplication_identity == ("route-attempt-1",)
    assert TravelRefreshPayload.model_validate_json(encode_json(definition.payload)) == payload
