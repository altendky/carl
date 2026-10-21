"""Durable route-only refreshes publish independent immutable listing evidence."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from carl.core.components import Component, ComponentId
from carl.core.models import NamedInput, NamedOutput, RecordDraft
from carl.core.travel_refresh import (
    REFRESH_LISTING_ROUTE_WORK_KIND,
    TRAVEL_ESTIMATE_RECORD_KIND,
    TravelEstimateRecord,
    TravelRefreshPayload,
    TravelRefreshService,
)
from carl.core.work import WorkCapability
from carl.core.worker import AttemptContext, CompletedWork, TerminalFailureWork, WorkOutcome
from carl.io.worker import TypedWorkHandler, WorkHandlerRegistry


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class TravelWorkerDependencies:
    service: TravelRefreshService
    new_identifier: Callable[[], str]
    utc_now: Callable[[], datetime] = _utc_now


async def _refresh(
    payload: TravelRefreshPayload,
    _context: AttemptContext,
    dependencies: TravelWorkerDependencies,
) -> WorkOutcome:
    estimates = await dependencies.service.refresh_many(
        (payload.destination,), expected_origin_identity=payload.origin_identity
    )
    if len(estimates) != 1:
        raise ValueError("Route refresh service returned a mismatched estimate count")
    estimate = estimates[0]
    if estimate.unavailable_reason == "origin_changed":
        return TerminalFailureWork(
            error={"kind": "origin_changed"},
            result={
                "state": "origin_changed",
                "listing_identifier": payload.request.listing_identifier,
            },
        )
    record_identifier = dependencies.new_identifier()
    recorded_at = dependencies.utc_now().astimezone(UTC)
    if estimate.expires_at_utc is None:
        estimate = estimate.model_copy(
            update={
                "expires_at_utc": (
                    recorded_at + timedelta(seconds=604800 if estimate.status == "ready" else 60)
                ).isoformat()
            }
        )
    record = TravelEstimateRecord(
        record_identifier=record_identifier,
        listing_id=payload.request.listing_identifier,
        origin_identity=payload.origin_identity,
        destination_identity=payload.destination_identity,
        destination=payload.destination,
        estimate=estimate,
        recorded_at_utc=recorded_at.isoformat(),
    )
    return CompletedWork(
        inputs=tuple(
            NamedInput(name=("listing_location", str(index)), object_identifier=identifier)
            for index, identifier in enumerate(payload.source_record_identifiers)
        ),
        records=(
            RecordDraft(
                identifier=record_identifier,
                kind=TRAVEL_ESTIMATE_RECORD_KIND,
                schema_version=1,
                value=record.model_dump(mode="json"),
            ),
        ),
        outputs=(NamedOutput(name=("route_estimate",), object_identifier=record_identifier),),
        result={
            "state": estimate.status,
            "estimate_record_identifier": record_identifier,
            "listing_identifier": payload.request.listing_identifier,
        },
    )


def build_travel_worker_registry(dependencies: TravelWorkerDependencies) -> WorkHandlerRegistry:
    return WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(
                    kind=REFRESH_LISTING_ROUTE_WORK_KIND, payload_schema_version=1
                ),
                component=Component(
                    identifier=ComponentId(("carl", "travel", "refresh_listing")),
                    output_schema_version=1,
                    implementation=_refresh,
                ),
                payload_type=TravelRefreshPayload,
                handler=lambda payload, context: _refresh(payload, context, dependencies),
            ),
        )
    )
