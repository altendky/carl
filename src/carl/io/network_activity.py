"""Cancellation-safe admission for durable network activities."""

from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass

import anyio

from carl.core.models import JsonValue
from carl.core.work import (
    NetworkActivityAdmission,
    NetworkActivityDefinition,
    NetworkActivityState,
)
from carl.io.sqlite import Database


@dataclass(slots=True)
class NetworkActivityPermit:
    admission: NetworkActivityAdmission
    dispatched: bool = False

    def mark_dispatched(self) -> None:
        if self.dispatched:
            raise RuntimeError("Network activity was already marked as dispatched")
        self.dispatched = True


@dataclass(frozen=True, slots=True)
class NetworkActivityScheduler:
    database: Database
    new_identifier: Callable[[], str]
    utc_now_ns: Callable[[], int]
    sample_uniform_holdoff_ns: Callable[[int, int], int]
    permit_duration_ns: int

    def __post_init__(self) -> None:
        if self.permit_duration_ns <= 0:
            raise ValueError("Network activity permit duration must be positive")

    async def _finish(
        self,
        *,
        definition: NetworkActivityDefinition,
        admission: NetworkActivityAdmission | None,
        state: NetworkActivityState,
        result: JsonValue,
    ) -> None:
        await self.database.finish_network_activity(
            network_activity_identifier=definition.identifier,
            admission_token=None if admission is None else admission.token,
            state=state,
            ended_at_utc_ns=self.utc_now_ns(),
            result=result,
            event_identifier=self.new_identifier(),
        )

    async def _finish_if_present(
        self,
        *,
        definition: NetworkActivityDefinition,
        state: NetworkActivityState,
        result: JsonValue,
    ) -> None:
        """Finish a pre-admission activity when its creation committed before failure."""

        # Cancellation or another failure may have rolled back creation. If the
        # transaction committed before the exception became visible, _finish()
        # instead records the terminal state.
        with suppress(KeyError):
            await self._finish(
                definition=definition,
                admission=None,
                state=state,
                result=result,
            )

    @asynccontextmanager
    async def admit(
        self,
        definition: NetworkActivityDefinition,
    ) -> AsyncGenerator[NetworkActivityPermit]:
        """Wait for durable admission and release concurrency during shielded cleanup."""

        admission: NetworkActivityAdmission | None = None
        permit: NetworkActivityPermit | None = None
        try:
            await self.database.create_network_activity(
                definition,
                created_at_utc_ns=self.utc_now_ns(),
                event_identifier=self.new_identifier(),
                sample_uniform_holdoff_ns=self.sample_uniform_holdoff_ns,
            )
            admission_token = self.new_identifier()
            while admission is None:
                now_utc_ns = self.utc_now_ns()
                result = await self.database.try_admit_network_activity(
                    network_activity_identifier=definition.identifier,
                    admission_token=admission_token,
                    permit_duration_ns=self.permit_duration_ns,
                    now_utc_ns=now_utc_ns,
                    event_identifier=self.new_identifier(),
                )
                admission = result.admission
                if admission is not None:
                    break
                if result.next_eligible_at_utc_ns is None:
                    raise RuntimeError("Blocked network activity has no eligibility time")
                await anyio.sleep(
                    max(0, result.next_eligible_at_utc_ns - now_utc_ns) / 1_000_000_000
                )
            permit = NetworkActivityPermit(admission=admission)
            try:
                yield permit
            except anyio.get_cancelled_exc_class():
                with anyio.move_on_after(10, shield=True):
                    await self._finish(
                        definition=definition,
                        admission=admission,
                        state=NetworkActivityState.CANCELLED,
                        result={"kind": "cancelled", "possibly_dispatched": True},
                    )
                raise
            except BaseException as error:
                with anyio.move_on_after(10, shield=True):
                    await self._finish(
                        definition=definition,
                        admission=admission,
                        state=NetworkActivityState.FAILED,
                        result={"kind": "exception", "type": type(error).__name__},
                    )
                raise
            else:
                if permit.dispatched:
                    await self._finish(
                        definition=definition,
                        admission=admission,
                        state=NetworkActivityState.COMPLETED,
                        result={"kind": "request_attempt_completed"},
                    )
                else:
                    await self._finish(
                        definition=definition,
                        admission=admission,
                        state=NetworkActivityState.SKIPPED,
                        result={"kind": "not_dispatched"},
                    )
        except anyio.get_cancelled_exc_class():
            if admission is None:
                with anyio.move_on_after(10, shield=True):
                    await self._finish_if_present(
                        definition=definition,
                        state=NetworkActivityState.CANCELLED,
                        result={"kind": "cancelled", "possibly_dispatched": False},
                    )
            raise
        except BaseException as error:
            if admission is None:
                with anyio.move_on_after(10, shield=True):
                    await self._finish_if_present(
                        definition=definition,
                        state=NetworkActivityState.FAILED,
                        result={"kind": "admission_failure", "type": type(error).__name__},
                    )
            raise
