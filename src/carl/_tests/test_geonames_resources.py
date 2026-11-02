"""Offline locality lookups share CPU capacity through physical thread completion."""

# Lookup spies wrap the real immutable SQLite implementation.
# pyright: reportPrivateUsage=false

import hashlib
import sqlite3
import threading
from contextlib import closing
from pathlib import Path

import anyio
import pytest

from carl.core.resources import ResourceRequest
from carl.core.travel import TravelCoordinate
from carl.io.geonames import LocalGeonamesGeocoder
from carl.io.resource_permits import current_resource_manager
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param


def _dataset(root: Path) -> Path:
    path = root / "places.sqlite3"
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.executescript("""
            CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT);
            INSERT INTO metadata VALUES ('schema_version','2'),('country_code','US'),('dataset_identifier','fixture');
            CREATE TABLE state_names(name TEXT PRIMARY KEY,code TEXT);
            INSERT INTO state_names VALUES ('pa','PA'),('pennsylvania','PA');
            CREATE TABLE places(identifier INTEGER PRIMARY KEY,state_code TEXT,latitude REAL,longitude REAL);
            INSERT INTO places VALUES (1,'PA',39.8,-77.7);
            CREATE TABLE names(name TEXT,place_identifier INTEGER,is_canonical INTEGER);
            INSERT INTO names VALUES ('greencastle',1,1);
        """)
    return path


async def _thread_started(event: threading.Event) -> None:
    with anyio.fail_after(3):
        while not event.is_set():
            await anyio.sleep(0.01)


async def test_lookup_waits_for_cpu_then_uses_thread_and_keeps_dataset_immutable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _dataset(tmp_path)
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    geocoder = LocalGeonamesGeocoder(path)
    original = LocalGeonamesGeocoder._geocode
    entered = threading.Event()
    finished = anyio.Event()
    loop_thread = threading.get_ident()
    results: list[TravelCoordinate] = []

    def lookup(self: LocalGeonamesGeocoder, label: str, country: str | None) -> TravelCoordinate:
        assert threading.get_ident() != loop_thread
        entered.set()
        return original(self, label, country)

    monkeypatch.setattr(LocalGeonamesGeocoder, "_geocode", lookup)

    async def query() -> None:
        results.append(await geocoder.geocode("Greencastle PA", "US"))
        finished.set()

    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ):
        manager = current_resource_manager()
        assert manager is not None
        manager.poll_seconds = 0.01
        async with anyio.create_task_group() as tasks:
            async with manager.admit(ResourceRequest(name="cpu")):
                tasks.start_soon(query)
                await anyio.sleep(0.05)
                assert not entered.is_set()
            with anyio.fail_after(3):
                await finished.wait()
    assert results == [TravelCoordinate(latitude=39.8, longitude=-77.7)]
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


async def test_cancelled_lookup_retains_cpu_until_sqlite_thread_finishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    geocoder = LocalGeonamesGeocoder(_dataset(tmp_path))
    original = LocalGeonamesGeocoder._geocode
    started = threading.Event()
    release = threading.Event()
    second_started = threading.Event()
    first_finished = anyio.Event()
    second_finished = anyio.Event()
    cancel = anyio.CancelScope()

    def lookup(self: LocalGeonamesGeocoder, label: str, country: str | None) -> TravelCoordinate:
        if label == "Greencastle PA":
            started.set()
            assert release.wait(5), "Test did not release its lookup thread"
        else:
            second_started.set()
        return original(self, label, country)

    monkeypatch.setattr(LocalGeonamesGeocoder, "_geocode", lookup)

    async def first() -> None:
        with cancel:
            _ = await geocoder.geocode("Greencastle PA", "US")
        first_finished.set()

    async def second() -> None:
        _ = await geocoder.geocode("Greencastle, PA", "US")
        second_finished.set()

    async with Database.managed(
        tmp_path / "carl.sqlite3", initialize=True, resource_capacities={"cpu": 1}
    ):
        manager = current_resource_manager()
        assert manager is not None
        manager.poll_seconds = 0.01
        async with anyio.create_task_group() as tasks:
            try:
                tasks.start_soon(first)
                await _thread_started(started)
                cancel.cancel()
                tasks.start_soon(second)
                await anyio.sleep(0.05)
                assert not second_started.is_set()
                assert not first_finished.is_set()
            finally:
                release.set()
            with anyio.fail_after(3):
                await first_finished.wait()
                await second_finished.wait()
