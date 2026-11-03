"""Parallel claims and effective route bindings have no built-in route ceiling."""

# pyright: reportPrivateUsage=false

import json
import sys
from pathlib import Path
from typing import Literal, cast

import anyio
import pytest
from anyio.streams.buffered import BufferedByteReceiveStream

from carl._tests.test_effective_search_admission import (
    CAPABILITY,
    DATACENTER,
    TARGET,
    _bind,
    _claim,
    _scopes,
)
from carl._tests.test_facebook_work import _payload
from carl.core.facebook_refresh import (
    REFRESH_SEARCH_PAYLOAD_SCHEMA_VERSION,
    REFRESH_SEARCH_WORK_KIND,
    RefreshSearchPayload,
    refresh_search_work,
)
from carl.core.facebook_work import FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY, collect_search_work
from carl.core.models import JsonValue
from carl.core.work import WorkCapability, WorkLease, WorkRequester, WorkState
from carl.io.sqlite import Database, WorkCancellationRequestedError

_PROCESS_CODE = """
import json,sys
from pathlib import Path
import anyio
from carl.core.work import WorkCapability,WorkLease
from carl.io.sqlite import Database

async def main():
    action=json.loads(sys.argv[2])
    async with Database.managed(Path(sys.argv[1]),reader_count=1,prepare_schema=False) as database:
        print('ready',flush=True)
        await anyio.to_thread.run_sync(sys.stdin.buffer.readline)
        if action['kind']=='claim':
            result=await database.claim_work(
                supported_capabilities=(WorkCapability.model_validate_json(json.dumps(action['capability'])),),
                worker_identifier=action['identifier'],lease_token=action['identifier'],
                lease_duration_ns=1000,utc_now_ns=lambda:10,event_identifier=action['identifier'])
            value=None if result.lease is None else result.lease.model_dump(mode='json')
        else:
            lease=WorkLease.model_validate_json(json.dumps(action['lease']))
            value=await database.bind_facebook_search_route(
                work_item_identifier=lease.work_item_identifier,lease_token=lease.token,
                worker_identifier=lease.worker_identifier,routing=tuple(action['route']),
                utc_now_ns=lambda:10)
        print(json.dumps(value),flush=True)
anyio.run(main,backend='trio')
"""


async def _parallel_processes(
    path: Path, actions: tuple[dict[str, JsonValue], ...]
) -> tuple[JsonValue, ...]:
    """Hold every subprocess at an IPC barrier before any writer can claim."""
    ready = tuple(anyio.Event() for _ in actions)
    start = anyio.Event()
    results: list[tuple[int, JsonValue]] = []

    async def run(index: int) -> None:
        async with await anyio.open_process(
            (sys.executable, "-c", _PROCESS_CODE, str(path), json.dumps(actions[index]))
        ) as process:
            assert process.stdout is not None and process.stdin is not None
            output = BufferedByteReceiveStream(process.stdout)
            assert await output.receive_until(b"\n", 100) == b"ready"
            ready[index].set()
            await start.wait()
            await process.stdin.send(b"go\n")
            await process.stdin.aclose()
            try:
                value = json.loads(await output.receive_until(b"\n", 100_000))
            except anyio.IncompleteRead:
                status = await process.wait()
                assert process.stderr is not None
                error = await process.stderr.receive()
                raise AssertionError(f"Worker exited {status}: {error.decode()}") from None
            assert await process.wait() == 0
            results.append((index, cast(JsonValue, value)))

    with anyio.fail_after(45):
        async with anyio.create_task_group() as tasks:
            for index in range(len(actions)):
                tasks.start_soon(run, index)
            for event in ready:
                await event.wait()
            start.set()
    return tuple(value for _, value in sorted(results))


@pytest.mark.anyio
@pytest.mark.parametrize("route", (TARGET, DATACENTER))
@pytest.mark.parametrize("work_kind", ("search", "refresh"))
@pytest.mark.parametrize("execution", ("tasks", "processes"))
async def test_eight_parallel_claimers_do_not_have_a_builtin_route_limit(
    tmp_path: Path,
    route: tuple[str, ...],
    work_kind: Literal["search", "refresh"],
    execution: Literal["tasks", "processes"],
) -> None:
    path = tmp_path / "carl.sqlite3"
    count = 8
    async with Database.managed(path, initialize=True) as database:
        capability = CAPABILITY
        if work_kind == "refresh":
            capability = WorkCapability(
                kind=REFRESH_SEARCH_WORK_KIND,
                payload_schema_version=REFRESH_SEARCH_PAYLOAD_SCHEMA_VERSION,
            )
        for index in range(count):
            payload = _payload().model_copy(
                update={
                    "routing": route,
                    "request": _payload().request.model_copy(update={"query": f"query-{index}"}),
                }
            )
            definition = (
                collect_search_work(
                    identifier=f"search-{index}", payload=payload, not_before_utc_ns=0
                )
                if work_kind == "search"
                else refresh_search_work(
                    identifier=f"search-{index}",
                    payload=RefreshSearchPayload(
                        base_search_run_record_identifier=f"base-{index}",
                        search_work_identifier=f"child-{index}",
                        search=payload,
                        item_routing=route,
                        image_routing=route,
                    ),
                    not_before_utc_ns=0,
                )
            )
            await database.enqueue_work(
                definition,
                WorkRequester(
                    request_identifier=f"request-{index}",
                    kind=("test", "claim"),
                    identifier=f"requester-{index}",
                    context={},
                ),
                event_identifier=f"enqueue-{index}",
                enqueued_at_utc_ns=1,
            )
        results: list[WorkLease | None] = []
        if execution == "processes":
            values = await _parallel_processes(
                path,
                tuple(
                    {
                        "kind": "claim",
                        "identifier": f"claim-{index}",
                        "capability": capability.model_dump(mode="json"),
                    }
                    for index in range(count)
                ),
            )
            results.extend(
                None if value is None else WorkLease.model_validate_json(json.dumps(value))
                for value in values
            )
        else:
            ready = tuple(anyio.Event() for _ in range(count))
            start = anyio.Event()

            async def claim(index: int) -> None:
                ready[index].set()
                await start.wait()
                result = await database.claim_work(
                    supported_capabilities=(capability,),
                    worker_identifier=f"worker-{index}",
                    lease_token=f"lease-{index}",
                    lease_duration_ns=1000,
                    utc_now_ns=lambda: 10,
                    event_identifier=f"claim-{index}",
                )
                results.append(result.lease)

            with anyio.fail_after(15):
                async with anyio.create_task_group() as tasks:
                    for index in range(count):
                        tasks.start_soon(claim, index)
                    for event in ready:
                        await event.wait()
                    start.set()
        winners = [lease for lease in results if lease is not None]
        assert len(results) == count and len(winners) == count
        assert len({lease.work_item_identifier for lease in winners}) == count
        states = await database.work_states(tuple(f"search-{index}" for index in range(count)))
        assert states.count(WorkState.LEASED) == count
        if work_kind == "search":
            other = await _claim(database, "other", (*route[:-1], "other"))
            assert other.work_item_identifier == "other"


@pytest.mark.anyio
@pytest.mark.parametrize("route", (TARGET, DATACENTER))
async def test_eight_parallel_alias_processes_bind_the_same_effective_route(
    tmp_path: Path, route: tuple[str, ...]
) -> None:
    path = tmp_path / "carl.sqlite3"
    count = 8
    async with Database.managed(path, initialize=True) as database:
        leases = tuple(
            [
                await _claim(database, f"alias-{index}", ("proton", "personal", f"alias-{index}"))
                for index in range(count)
            ]
        )
        values = await _parallel_processes(
            path,
            tuple(
                {"kind": "admit", "lease": lease.model_dump(mode="json"), "route": list(route)}
                for lease in leases
            ),
        )
        assert values == (None,) * count
        for lease in leases:
            assert (FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY, *route) in await _scopes(
                database, lease.work_item_identifier
            )
        first, second = leases[:2]
        await _bind(database, first, route)
        await _bind(database, second, route)
        await database.cancel_work(
            first.work_item_identifier,
            reason="stop",
            caller="test",
            requested_at_utc_ns=11,
            include_descendants=False,
        )
        with pytest.raises(WorkCancellationRequestedError):
            await _bind(database, first, route, now=11)
        await _bind(database, second, route, now=11)
