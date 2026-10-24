"""Durable setup jobs use the same dataset manager as explicit CLI operations."""

from carl.core.components import Component, ComponentId
from carl.core.travel_data import (
    TRAVEL_DATA_SETUP_WORK_KIND,
    TravelDataFailure,
    TravelDataService,
    TravelDataSetupRequest,
)
from carl.core.work import WorkCapability
from carl.core.worker import (
    AttemptContext,
    CompletedWork,
    RetryWork,
    TerminalFailureWork,
    WorkOutcome,
)
from carl.io.worker import TypedWorkHandler, WorkHandlerRegistry


async def _setup(
    request: TravelDataSetupRequest,
    _context: AttemptContext,
    service: TravelDataService,
) -> WorkOutcome:
    try:
        result = await service.setup(request)
    except TravelDataFailure as error:
        if error.reason == "travel_data_busy":
            return RetryWork(
                delay_ns=60_000_000_000,
                reason={"kind": error.reason, "message": error.message},
                result={"state": "waiting_for_dataset_build", "provider": request.provider},
            )
        return TerminalFailureWork(
            error={"kind": error.reason, "message": error.message},
            result={"state": "failed", "provider": request.provider},
        )
    return CompletedWork(result=result.model_dump(mode="json"))


def build_travel_data_worker_registry(service: TravelDataService) -> WorkHandlerRegistry:
    return WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(
                    kind=TRAVEL_DATA_SETUP_WORK_KIND, payload_schema_version=1
                ),
                component=Component(
                    identifier=ComponentId(TRAVEL_DATA_SETUP_WORK_KIND),
                    output_schema_version=1,
                    implementation=_setup,
                ),
                payload_type=TravelDataSetupRequest,
                handler=lambda request, context: _setup(request, context, service),
            ),
        )
    )
