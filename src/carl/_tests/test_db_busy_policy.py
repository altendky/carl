"""Cooperative SQLite busy waits retain finite, connection-local deadlines."""

# Policy boundaries and lifetime checks intentionally inspect the private adapter.
# pyright: reportPrivateUsage=false

import gc
import weakref
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

import anyio
import pytest

import carl.io.db as db

pytestmark = pytest.mark.anyio
type BusyHandler = Callable[[int], Awaitable[bool]]


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@dataclass
class _Clock:
    now: float = 0
    sleeps: list[float] = field(default_factory=list)

    def current_time(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        assert seconds > 0, "Busy retries must yield with a positive delay"
        self.sleeps.append(seconds)
        self.now += seconds
        await anyio.lowlevel.checkpoint()


async def test_each_busy_operation_gets_a_fresh_bounded_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    # Replace only db's module reference, not AnyIO's real clock or scheduler.
    monkeypatch.setattr(db, "anyio", clock)
    handler = db._cooperative_busy_handler(12)
    assert await handler(0)
    assert await handler(1)
    assert not await handler(2)
    assert clock.sleeps == pytest.approx([0.005, 0.005, 0.002])
    assert clock.now == pytest.approx(0.012)
    assert not await handler(3)
    assert len(clock.sleeps) == 3

    clock.now = 100
    assert await handler(0)
    assert await handler(1)
    assert not await handler(2)
    assert clock.sleeps[3:] == pytest.approx([0.005, 0.005, 0.002])
    assert clock.now == pytest.approx(100.012)


async def test_elapsed_time_not_callback_count_bounds_waiting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    monkeypatch.setattr(db, "anyio", clock)
    handler = db._cooperative_busy_handler(100)
    assert await handler(0)
    clock.now = 10
    assert not await handler(1)
    assert clock.sleeps == [0.005]


async def test_busy_deadlines_are_independent_per_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    monkeypatch.setattr(db, "anyio", clock)
    first, second = db._cooperative_busy_handler(12), db._cooperative_busy_handler(12)
    assert first is not second
    assert await first(0)
    clock.now = 0.010
    assert await second(0)
    assert not await first(1)
    assert await second(1)
    assert not await second(2)
    assert clock.now == pytest.approx(0.022)


@pytest.mark.parametrize("milliseconds", (0, -1, -(2**31)))
async def test_nonpositive_timeouts_never_sleep_or_spin(
    monkeypatch: pytest.MonkeyPatch, milliseconds: int
) -> None:
    clock = _Clock(now=100)
    monkeypatch.setattr(db, "anyio", clock)
    handler = db._cooperative_busy_handler(milliseconds)
    assert not await handler(0)
    assert not await handler(1)
    assert clock.sleeps == []


async def test_policy_sleep_preserves_cancellation() -> None:
    handler = db._cooperative_busy_handler(30_000)
    with anyio.CancelScope() as scope:
        scope.cancel()
        _ = await handler(0)
        pytest.fail("Busy wait swallowed cancellation")
    assert scope.cancelled_caught


async def test_every_connection_owns_a_collectible_policy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    policies: list[weakref.ReferenceType[BusyHandler]] = []
    factory = db._cooperative_busy_handler

    def observed_factory(milliseconds: int) -> BusyHandler:
        handler = factory(milliseconds)
        policies.append(weakref.ref(handler))
        return handler

    monkeypatch.setattr(db, "_cooperative_busy_handler", observed_factory)
    async with db.DatabaseConnections.managed(
        tmp_path / "policies.sqlite3", reader_count=2, busy_timeout_ms=100
    ) as wrapper:
        assert len(policies) == 3
        assert all(policy() is not None for policy in policies)
        assert len({id(policy()) for policy in policies}) == 3
        for connection in (wrapper._writer, *wrapper._readers):
            # SQLite exposes only its native timeout, superseded by the custom
            # bounded callback; foreign-key and reader safety are unaffected.
            assert await db._rows(connection, "PRAGMA busy_timeout") == [(0,)]
            assert await db._rows(connection, "PRAGMA foreign_keys") == [(1,)]
        assert await db._rows(wrapper._writer, "PRAGMA query_only") == [(0,)]
        for connection in wrapper._readers:
            assert await db._rows(connection, "PRAGMA query_only") == [(1,)]
    assert wrapper._fully_closed
    _ = gc.collect()
    assert all(policy() is None for policy in policies)


@pytest.mark.parametrize(
    ("milliseconds", "error"),
    (
        (1.5, TypeError),
        ("10", TypeError),
        (None, TypeError),
        (2**31, OverflowError),
        (-(2**31) - 1, OverflowError),
    ),
)
async def test_native_timeout_argument_validation_precedes_handler_installation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    milliseconds: object,
    error: type[Exception],
) -> None:
    def unexpected_factory(_milliseconds: int) -> BusyHandler:
        raise AssertionError("Invalid native timeout reached handler installation")

    path = tmp_path / "invalid.sqlite3"
    with monkeypatch.context() as patch:
        patch.setattr(db, "_cooperative_busy_handler", unexpected_factory)
        with pytest.raises(error):
            async with db.DatabaseConnections.managed(
                path, reader_count=1, busy_timeout_ms=cast(int, milliseconds)
            ):
                pytest.fail("Invalid native timeout was accepted")
    # Constructor failure must still dispose its writer/streams and allow reuse.
    async with (
        db.DatabaseConnections.managed(path, reader_count=1) as wrapper,
        wrapper.writer() as connection,
    ):
        _ = await connection.execute("CREATE TABLE recovered(value INTEGER)")


@pytest.mark.parametrize("milliseconds", (-(2**31), 2**31 - 1, True))
async def test_native_valid_timeout_boundaries_remain_accepted(
    tmp_path: Path, milliseconds: int
) -> None:
    async with (
        db.DatabaseConnections.managed(
            tmp_path / "limits.sqlite3", reader_count=1, busy_timeout_ms=milliseconds
        ) as wrapper,
        wrapper.writer() as connection,
    ):
        _ = await connection.execute("CREATE TABLE counter(value INTEGER)")


async def test_reader_policy_setup_failure_closes_previously_configured_connections(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    policies: list[weakref.ReferenceType[BusyHandler]] = []
    factory = db._cooperative_busy_handler
    calls = 0

    def failing_factory(milliseconds: int) -> BusyHandler:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("injected reader policy setup failure")
        handler = factory(milliseconds)
        policies.append(weakref.ref(handler))
        return handler

    path = tmp_path / "setup.sqlite3"
    with monkeypatch.context() as patch:
        patch.setattr(db, "_cooperative_busy_handler", failing_factory)
        with pytest.raises(RuntimeError, match="injected reader policy setup failure"):
            async with db.DatabaseConnections.managed(path, reader_count=2):
                pytest.fail("Reader policy failure was ignored")
    assert calls == 2
    _ = gc.collect()
    assert len(policies) == 1 and policies[0]() is None
    async with (
        db.DatabaseConnections.managed(path, reader_count=1) as wrapper,
        wrapper.writer() as connection,
    ):
        _ = await connection.execute("CREATE TABLE recovered(value INTEGER)")
