"""Physical resource ownership survives cancellation and stale durable leases."""

# pyright: reportPrivateUsage=false

import fcntl
import os
import signal
import subprocess
import sys
import threading
from contextlib import suppress
from pathlib import Path
from time import time_ns
from typing import override
from uuid import uuid4

import anyio
import httpx
import pytest

import carl.facebook_image_workers as image_workers
from carl._tests.test_facebook_images import _png, _reference
from carl._tests.test_wreq import _acquirer, _Factory, _response
from carl.core.facebook_images import CollectImagePayload, VerifiedImage, verify_image
from carl.core.http import RequestPlan
from carl.core.models import JsonValue
from carl.core.resources import ResourceRequest
from carl.core.work import SchedulingScope, SchedulingScopeKind, WorkDefinition, WorkRequester
from carl.core.worker import AttemptContext, CompletedWork
from carl.io.claude import ClaudeCli
from carl.io.httpx import AcquiredBody, Acquisition, DirectHttpxAcquirer, IdentifierFactory
from carl.io.image_files import ImageFileStore
from carl.io.resource_permits import (
    ResourceLeaseLost,
    ResourceManager,
    current_resource_manager,
    run_cpu,
)
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio


def _manager() -> ResourceManager:
    manager = current_resource_manager()
    assert manager is not None
    manager.poll_seconds = 0.01
    return manager


async def _enqueue(database: Database, identifier: str) -> None:
    kind = ("test", "resource_publication")
    _ = await database.enqueue_work(
        WorkDefinition(
            identifier=identifier,
            kind=kind,
            payload_schema_version=1,
            payload={},
            deduplication_identity=(identifier,),
            not_before_utc_ns=0,
            scopes=(
                SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=kind),
            ),
        ),
        WorkRequester(
            request_identifier=str(uuid4()), kind=("test",), identifier=identifier, context={}
        ),
        event_identifier=str(uuid4()),
        enqueued_at_utc_ns=time_ns(),
    )


@pytest.mark.parametrize("fails", [False, True])
async def test_success_and_failure_release_capacity(tmp_path: Path, fails: bool) -> None:
    async with Database.managed(
        tmp_path / "evidence.sqlite", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        manager = _manager()
        first_token = ""
        try:
            async with manager.admit(ResourceRequest(name="cpu")) as permit:
                first_token = permit.lease.token
                assert await database.validate_resource_permit(
                    token=first_token, owner=permit.lease.owner, now_utc_ns=time_ns()
                )
                if fails:
                    raise ValueError("operation failed")
        except ValueError as error:
            assert fails
            assert str(error) == "operation failed"
        with anyio.fail_after(2):
            async with manager.admit(ResourceRequest(name="cpu")) as replacement:
                assert replacement.lease.token != first_token


async def test_cancelled_waiter_never_enters_or_reserves_capacity(tmp_path: Path) -> None:
    async with Database.managed(
        tmp_path / "evidence.sqlite", initialize=True, resource_capacities={"cpu": 1}
    ):
        manager = _manager()
        started = anyio.Event()
        finished = anyio.Event()
        entered = False
        scope = anyio.CancelScope()

        async def waiter() -> None:
            nonlocal entered
            with scope:
                started.set()
                async with manager.admit(ResourceRequest(name="cpu")):
                    entered = True
            finished.set()

        async with anyio.create_task_group() as tasks:
            async with manager.admit(ResourceRequest(name="cpu")):
                _ = tasks.start_soon(waiter)
                await started.wait()
                await anyio.sleep(0.05)
                scope.cancel()
                with anyio.fail_after(2):
                    await finished.wait()
                assert not entered
            with anyio.fail_after(2):
                async with manager.admit(ResourceRequest(name="cpu")):
                    pass


async def test_cancelled_admitted_operation_releases_capacity(tmp_path: Path) -> None:
    async with Database.managed(
        tmp_path / "evidence.sqlite", initialize=True, resource_capacities={"cpu": 1}
    ):
        manager = _manager()
        entered = anyio.Event()
        finished = anyio.Event()
        scope = anyio.CancelScope()

        async def operation() -> None:
            with scope:
                async with manager.admit(ResourceRequest(name="cpu")):
                    entered.set()
                    await anyio.sleep_forever()
            finished.set()

        async with anyio.create_task_group() as tasks:
            _ = tasks.start_soon(operation)
            await entered.wait()
            scope.cancel()
            with anyio.fail_after(2):
                await finished.wait()
                async with manager.admit(ResourceRequest(name="cpu")):
                    pass


async def test_cpu_cancellation_does_not_release_a_running_thread(tmp_path: Path) -> None:
    async with Database.managed(
        tmp_path / "evidence.sqlite", initialize=True, resource_capacities={"cpu": 1}
    ):
        _ = _manager()
        started = threading.Event()
        release = threading.Event()
        second_started = threading.Event()
        first_finished = anyio.Event()
        second_finished = anyio.Event()
        scope = anyio.CancelScope()

        def blocking_cpu() -> None:
            started.set()
            assert release.wait(5), "test did not release its CPU thread"

        async def first() -> None:
            with scope:
                await run_cpu(blocking_cpu)
            first_finished.set()

        async def second() -> None:
            await run_cpu(second_started.set)
            second_finished.set()

        async with anyio.create_task_group() as tasks:
            try:
                _ = tasks.start_soon(first)
                with anyio.fail_after(2):
                    while not started.is_set():
                        await anyio.sleep(0.01)
                scope.cancel()
                _ = tasks.start_soon(second)
                await anyio.sleep(0.1)
                assert not second_started.is_set()
                assert not first_finished.is_set()
            finally:
                release.set()
            with anyio.fail_after(2):
                await first_finished.wait()
                await second_finished.wait()


async def test_long_operation_renews_its_lease(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        manager = _manager()
        manager.lease_duration_ns = 300_000_000
        async with manager.admit(ResourceRequest(name="cpu")) as permit:
            first_expiry = permit.lease.expires_at_utc_ns
            await anyio.sleep(0.8)
            assert time_ns() > first_expiry
            assert permit.lease.expires_at_utc_ns > first_expiry
            assert await database.validate_resource_permit(
                token=permit.lease.token, owner=permit.lease.owner, now_utc_ns=time_ns()
            )


async def test_expired_live_guard_blocks_reclamation_until_execution_ends(tmp_path: Path) -> None:
    async with Database.managed(
        tmp_path / "evidence.sqlite", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        manager = _manager()
        token = str(uuid4())
        descriptor = manager._guard(token)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            lease = await database.try_acquire_resource_permit(
                scope=manager.scope,
                token=token,
                owner="paused-owner",
                requests=(ResourceRequest(name="cpu"),),
                capacities=manager.capacities,
                now_utc_ns=time_ns() - 100,
                lease_duration_ns=1,
            )
            assert lease is not None
            await manager._recover()
            assert await database.expired_resource_permits(
                scope=manager.scope, now_utc_ns=time_ns()
            ) == (lease,)
            with anyio.move_on_after(0.1) as timeout:
                async with manager.admit(ResourceRequest(name="cpu")):
                    pytest.fail("expired live execution was overcommitted")
            assert timeout.cancel_called
        finally:
            os.close(descriptor)
        with anyio.fail_after(2):
            async with manager.admit(ResourceRequest(name="cpu")):
                pass
        assert (
            await database.expired_resource_permits(scope=manager.scope, now_utc_ns=time_ns()) == ()
        )


async def test_dead_process_guard_allows_expired_capacity_recovery(tmp_path: Path) -> None:
    async with Database.managed(
        tmp_path / "evidence.sqlite", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        manager = _manager()
        token = str(uuid4())
        descriptor = manager._guard(token)
        os.close(descriptor)
        guard = manager.guard_directory / f"{token}.lock"
        code = (
            "import fcntl,os,sys,time; "
            "fd=os.open(sys.argv[1],os.O_RDWR); fcntl.flock(fd,fcntl.LOCK_EX); "
            "print('locked',flush=True); time.sleep(30)"
        )
        process = subprocess.Popen(
            (sys.executable, "-c", code, str(guard)), stdout=subprocess.PIPE, text=True
        )
        try:
            assert process.stdout is not None
            assert await anyio.to_thread.run_sync(process.stdout.readline) == "locked\n"
            lease = await database.try_acquire_resource_permit(
                scope=manager.scope,
                token=token,
                owner="dead-owner",
                requests=(ResourceRequest(name="cpu"),),
                capacities=manager.capacities,
                now_utc_ns=time_ns() - 100,
                lease_duration_ns=1,
            )
            assert lease is not None
            await manager._recover()
            assert (
                len(
                    await database.expired_resource_permits(
                        scope=manager.scope, now_utc_ns=time_ns()
                    )
                )
                == 1
            )
            process.kill()
            _ = await anyio.to_thread.run_sync(process.wait)
            with anyio.fail_after(2):
                async with manager.admit(ResourceRequest(name="cpu")):
                    pass
            assert (
                await database.expired_resource_permits(scope=manager.scope, now_utc_ns=time_ns())
                == ()
            )
        finally:
            if process.poll() is None:
                process.kill()
            _ = await anyio.to_thread.run_sync(process.wait)
            if process.stdout is not None:
                process.stdout.close()


async def test_context_exit_keeps_capacity_while_inherited_executor_guard_is_alive(
    tmp_path: Path,
) -> None:
    async with Database.managed(
        tmp_path / "evidence.sqlite", initialize=True, resource_capacities={"analysis": 1}
    ) as database:
        manager = _manager()
        manager.lease_duration_ns = 300_000_000
        process: subprocess.Popen[str] | None = None
        try:
            async with manager.admit(ResourceRequest(name="analysis")) as permit:
                # Model a surviving executor after its supervisor returned or died.
                # This child inherits the original open file description, so closing
                # Carl's descriptor does not prove that physical execution ended.
                process = subprocess.Popen(
                    (
                        sys.executable,
                        "-c",
                        "import time; print('running',flush=True); time.sleep(30)",
                    ),
                    pass_fds=(permit.guard_descriptor,),
                    stdout=subprocess.PIPE,
                    text=True,
                )
                assert process.stdout is not None
                assert await anyio.to_thread.run_sync(process.stdout.readline) == "running\n"

            with anyio.move_on_after(0.5) as blocked:
                async with manager.admit(ResourceRequest(name="analysis")):
                    pytest.fail(
                        "context exit released capacity still used by an inherited executor"
                    )
            assert blocked.cancel_called
            expired = await database.expired_resource_permits(
                scope=manager.scope, now_utc_ns=time_ns()
            )
            assert tuple(lease.token for lease in expired) == (permit.lease.token,)

            process.kill()
            _ = await anyio.to_thread.run_sync(process.wait)
            with anyio.fail_after(2):
                async with manager.admit(ResourceRequest(name="analysis")) as replacement:
                    assert replacement.lease.token != permit.lease.token
            assert (
                await database.expired_resource_permits(scope=manager.scope, now_utc_ns=time_ns())
                == ()
            )
        finally:
            if process is not None:
                if process.poll() is None:
                    process.kill()
                _ = await anyio.to_thread.run_sync(process.wait)
                if process.stdout is not None:
                    process.stdout.close()


async def test_publication_is_transactional_and_stale_permit_cannot_publish(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "evidence.sqlite", initialize=True) as database:
        manager = _manager()
        async with manager.admit(ResourceRequest(name="cpu")) as permit:
            with pytest.raises(ValueError, match="rollback"):
                async with permit.publishing():
                    await _enqueue(database, "rolled-back")
                    raise ValueError("rollback")
            with pytest.raises(KeyError):
                _ = await database.work("rolled-back")
            async with permit.publishing():
                await _enqueue(database, "published")
        assert (await database.work("published"))["identifier"] == "published"
        async with manager.admit(ResourceRequest(name="cpu")):
            with pytest.raises(ResourceLeaseLost, match="expired"):
                async with permit.publishing():
                    await _enqueue(database, "stale-publication")
            with pytest.raises(KeyError):
                _ = await database.work("stale-publication")


class _BytesStream(httpx.AsyncByteStream):
    @override
    async def __aiter__(self):
        yield b"offline response"


async def test_http_adapter_does_not_send_before_resource_admission(tmp_path: Path) -> None:
    sent = anyio.Event()
    finished = anyio.Event()

    def respond(_request: httpx.Request) -> httpx.Response:
        sent.set()
        return httpx.Response(200, stream=_BytesStream())

    acquirer = DirectHttpxAcquirer(transport=httpx.MockTransport(respond))

    async with Database.managed(
        tmp_path / "evidence.sqlite", initialize=True, resource_capacities={"http": 1}
    ):
        manager = _manager()

        async def acquire() -> None:
            _ = await acquirer.acquire(
                RequestPlan(url="https://example.test/"), lambda: str(uuid4())
            )
            finished.set()

        async with anyio.create_task_group() as tasks:
            async with manager.admit(ResourceRequest(name="http")):
                _ = tasks.start_soon(acquire)
                await anyio.sleep(0.1)
                assert not sent.is_set()
            with anyio.fail_after(2):
                await finished.wait()
            assert sent.is_set()


async def test_wreq_adapter_does_not_create_client_or_send_before_admission(tmp_path: Path) -> None:
    response = _response()
    factory = _Factory([response])
    acquirer = _acquirer(factory)
    finished = anyio.Event()
    async with Database.managed(
        tmp_path / "evidence.sqlite", initialize=True, resource_capacities={"http": 1}
    ):
        manager = _manager()

        async def acquire() -> None:
            _ = await acquirer.acquire(
                RequestPlan(url=response.url, routing=("proton", "personal", "carl")),
                lambda: str(uuid4()),
            )
            finished.set()

        async with anyio.create_task_group() as tasks:
            async with manager.admit(ResourceRequest(name="http")):
                _ = tasks.start_soon(acquire)
                await anyio.sleep(0.1)
                assert factory.thread is None
                assert not factory.calls
            with anyio.fail_after(2):
                await finished.wait()
            assert len(factory.calls) == 1


async def test_image_worker_validates_only_after_cpu_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    validating = threading.Event()
    finished = anyio.Event()
    outcomes: list[object] = []
    reference = _reference()
    representation: dict[str, JsonValue] = {
        "kind": "content_decoded_http_body",
        "content_decoded": True,
    }

    class Acquirer:
        async def acquire(
            self, plan: RequestPlan, new_identifier: IdentifierFactory
        ) -> Acquisition:
            identifier = new_identifier()
            return Acquisition(
                record={
                    "effective_url": plan.url,
                    "hops": [
                        {
                            "response": {
                                "status_code": 200,
                                "headers": [
                                    {"name_latin1": "Content-Type", "value_latin1": "image/png"}
                                ],
                                "body": {"state": "available", "artifact_id": identifier},
                            }
                        }
                    ],
                },
                bodies=(AcquiredBody(identifier, "image/png", representation, _png()),),
            )

    def validate(body: bytes, headers: JsonValue, representation: JsonValue) -> VerifiedImage:
        validating.set()
        return verify_image(body, headers, representation)

    monkeypatch.setattr(image_workers, "verify_image", validate)
    async with Database.managed(
        tmp_path / "evidence.sqlite", initialize=True, resource_capacities={"cpu": 1}
    ) as database:
        manager = _manager()

        async def collect() -> None:
            outcomes.append(
                await image_workers._collect(
                    CollectImagePayload(
                        reference_record_identifier="reference",
                        reference=reference,
                        request_plan=RequestPlan(
                            url=reference.original_url,
                            compression=("identity",),
                            follow_redirects=False,
                        ),
                    ),
                    AttemptContext(
                        work_item_identifier="image",
                        lease_token="lease",
                        worker_identifier="worker",
                        attempt=1,
                        operation_identifier="operation",
                    ),
                    image_workers.ImageWorkerDependencies(
                        database=database,
                        acquirer=Acquirer(),
                        image_files=ImageFileStore(tmp_path),
                        new_identifier=lambda: str(uuid4()),
                    ),
                )
            )
            finished.set()

        async with anyio.create_task_group() as tasks:
            async with manager.admit(ResourceRequest(name="cpu")):
                _ = tasks.start_soon(collect)
                await anyio.sleep(0.1)
                assert not validating.is_set()
            with anyio.fail_after(2):
                await finished.wait()
            assert validating.is_set()
            assert isinstance(outcomes[0], CompletedWork)
            assert outcomes[0].result["state"] == "saved"


async def test_analysis_adapter_launches_only_after_admission_and_reaps_on_cancel(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "analysis.pid"
    code = "import os,sys,time; from pathlib import Path; Path(sys.argv[1]).write_text(str(os.getpid())); time.sleep(30)"
    scope = anyio.CancelScope()
    finished = anyio.Event()
    async with Database.managed(
        tmp_path / "evidence.sqlite", initialize=True, resource_capacities={"analysis": 1}
    ):
        manager = _manager()

        async def analyze() -> None:
            with scope:
                _ = await ClaudeCli()._capture(
                    (sys.executable, "-c", code, str(marker)), tmp_path, 30
                )
            finished.set()

        async with anyio.create_task_group() as tasks:
            async with manager.admit(ResourceRequest(name="analysis")):
                _ = tasks.start_soon(analyze)
                await anyio.sleep(0.1)
                assert not marker.exists()
            with anyio.fail_after(3):
                while not marker.exists():
                    await anyio.sleep(0.01)
            child_pid = int(marker.read_text())
            scope.cancel()
            with anyio.fail_after(3):
                await finished.wait()
            with pytest.raises(ProcessLookupError):
                os.kill(child_pid, 0)
            with anyio.fail_after(2):
                async with manager.admit(ResourceRequest(name="analysis")):
                    pass


async def test_supervisor_retains_guard_after_its_carl_parent_exits(tmp_path: Path) -> None:
    guard = tmp_path / "supervisor.lock"
    marker = tmp_path / "child.started"
    child = "from pathlib import Path; import sys,time; Path(sys.argv[1]).write_text('ready'); time.sleep(30)"
    owner = (
        "import fcntl,os,subprocess,sys; "
        "fd=os.open(sys.argv[1],os.O_CREAT|os.O_RDWR,0o600); fcntl.flock(fd,fcntl.LOCK_EX); "
        "p=subprocess.Popen((sys.executable,'-m','carl.io.analysis_supervisor',str(fd),'30',"
        "sys.executable,'-c',sys.argv[3],sys.argv[2]),pass_fds=(fd,),start_new_session=True,"
        "stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); "
        "print(p.pid,flush=True); os._exit(0)"
    )
    process = await anyio.run_process(
        (sys.executable, "-c", owner, str(guard), str(marker), child), check=True
    )
    supervisor_pid = int(process.stdout)
    descriptor = os.open(guard, os.O_RDWR)
    try:
        with anyio.fail_after(3):
            while not marker.exists():
                await anyio.sleep(0.01)
        with pytest.raises(BlockingIOError):
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.killpg(supervisor_pid, signal.SIGTERM)
        with anyio.fail_after(3):
            while True:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    await anyio.sleep(0.01)
    finally:
        with suppress(ProcessLookupError):
            os.killpg(supervisor_pid, signal.SIGTERM)
        os.close(descriptor)
