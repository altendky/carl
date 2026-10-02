"""Offline runtime cutover tests, including work carrying an older Proton route."""

# Reuse the existing offline worker fixtures rather than duplicate their setup.
# pyright: reportPrivateUsage=false

import tomllib
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import time_ns
from typing import cast

import httpx
import pytest

import carl.ebay_item_workers as ebay_workers
import carl.facebook_routed_workers as facebook_workers
from carl._tests import test_decodo_facebook_sessions as decodo_fixtures
from carl._tests import test_ebay_item_workers as ebay_fixtures
from carl._tests import test_facebook_search_workers as search_fixtures
from carl._tests.test_configuration import _configuration_document, _directories
from carl._tests.test_facebook_images import URL, _reference
from carl.core.configuration import CarlConfiguration
from carl.core.ebay_items import COLLECT_EBAY_IMAGE_WORK_KIND, collect_ebay_image_work
from carl.core.facebook_images import COLLECT_IMAGE_WORK_KIND, CollectImagePayload
from carl.core.facebook_search import SearchTraversalPolicy
from carl.core.facebook_work import (
    COLLECT_ITEM_WORK_KIND,
    COLLECT_SEARCH_WORK_KIND,
    CollectItemPayload,
    CollectSearchPayload,
    FacebookSearchRequest,
    SearchDistanceUnit,
    SearchFacebookLocation,
    SearchRadius,
    collect_search_work,
)
from carl.core.http import RequestPlan
from carl.core.models import JsonValue
from carl.core.work import WorkState
from carl.core.worker import AttemptContext, CompletedWork
from carl.io.configuration import LoadedCarlConfiguration
from carl.io.decodo import DecodoSessionManager
from carl.io.httpx import HttpAcquirer
from carl.io.sqlite import Database

PROTON = ("proton", "personal", "carl")
DATACENTER = ("decodo", "personal", "datacenter")
MOBILE = ("decodo", "personal", "carl")


def _loaded(root: Path, *, override: bool) -> LoadedCarlConfiguration:
    document = tomllib.loads(_configuration_document(root / "wireproxy").decode())
    routes = document["routes"]
    next(route for route in routes if route["provider"] == "proton")["network_path"] = list(PROTON)
    next(route for route in routes if route["provider"] == "decodo")["product"] = "mobile_proxy"
    routes.append(
        {
            "provider": "decodo",
            "network_path": list(DATACENTER),
            "account_identifier": "personal",
            "proxy_username": "example",
            "credential_id": "datacenter",
            "product": "datacenter_proxy",
            "endpoint": {"host": "dc.decodo.com", "port": 10001},
        }
    )
    if override:
        document["route_overrides"] = [
            {"requested_network_path": list(PROTON), "network_path": list(DATACENTER)}
        ]
    return LoadedCarlConfiguration(
        configuration=CarlConfiguration.model_validate(document),
        document_sha256="a" * 64,
        source_path=root / "config.toml",
    )


@dataclass
class _Session:
    identifier: str
    acquirer: HttpAcquirer

    def completed_observation(self) -> dict[str, JsonValue]:
        return {"network_session_identifier": self.identifier, "state": "closed"}


def _patch_routes(
    monkeypatch: pytest.MonkeyPatch,
    module: object,
    loaded: LoadedCarlConfiguration,
    expected: tuple[str, ...],
    acquirer: HttpAcquirer,
) -> list[tuple[str, ...]]:
    opened: list[tuple[str, ...]] = []

    def settings(_loaded: object, _directories: object, routing: tuple[str, ...]) -> object:
        assert routing == expected
        opened.append(routing)
        return (routing, object()) if routing[0] == "decodo" else routing

    def factory(*, settings: object, **_kwargs: object) -> Callable[[str], object]:
        assert settings == expected

        @asynccontextmanager
        async def session(identifier: str) -> AsyncGenerator[_Session]:
            yield _Session(identifier, acquirer)

        return session

    def unexpected(*_args: object, **_kwargs: object) -> None:
        pytest.fail("The replaced provider must not be invoked")

    def load(_path: Path) -> LoadedCarlConfiguration:
        return loaded

    monkeypatch.setattr(module, "load_configuration", load)
    monkeypatch.setattr(
        module, "decodo_settings", settings if expected[0] == "decodo" else unexpected
    )
    monkeypatch.setattr(
        module, "proton_settings", settings if expected[0] == "proton" else unexpected
    )
    for source in ("Proton", "Decodo"):
        for purpose in ("Image", "Search"):
            name = f"{source}Facebook{purpose}SessionFactory"
            if hasattr(module, name):
                monkeypatch.setattr(module, name, factory)
    return opened


@pytest.mark.anyio
@pytest.mark.parametrize("override", (False, True))
@pytest.mark.parametrize("purpose", ("search", "image", "item"))
async def test_facebook_runtime_resolves_legacy_routes_but_preserves_mobile_items(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, purpose: str, override: bool
) -> None:
    expected = MOBILE if purpose == "item" else DATACENTER if override else PROTON
    images = ebay_fixtures._Acquirer({})
    opened = _patch_routes(
        monkeypatch, facebook_workers, _loaded(tmp_path, override=override), expected, images
    )
    executed: list[CollectSearchPayload | CollectImagePayload | CollectItemPayload] = []

    async def execute(
        _registry: object, _capability: object, payload: object, _context: object
    ) -> CompletedWork:
        assert isinstance(payload, (CollectSearchPayload, CollectImagePayload, CollectItemPayload))
        executed.append(payload)
        actual = (
            payload.routing
            if isinstance(payload, CollectSearchPayload)
            else payload.request_plan.routing
        )
        assert actual == expected
        return CompletedWork(result={"state": "test_completed"})

    monkeypatch.setattr(facebook_workers, "_execute", execute)
    monkeypatch.setattr(facebook_workers, "brave_navigation_headers", lambda: ())
    identifiers = ebay_fixtures._identifiers()
    if purpose == "search":
        payload = CollectSearchPayload(
            request=FacebookSearchRequest(
                query="telescope",
                location=SearchFacebookLocation(identifier="456"),
                radius=SearchRadius(value=60, unit=SearchDistanceUnit.MILES),
            ),
            traversal=SearchTraversalPolicy(maximum_pages=2, maximum_results=10),
            routing=PROTON,
        )
        kind = COLLECT_SEARCH_WORK_KIND
    elif purpose == "image":
        payload = CollectImagePayload(
            reference_record_identifier="reference",
            reference=_reference(),
            request_plan=RequestPlan(url=URL, follow_redirects=False, routing=PROTON),
        )
        kind = COLLECT_IMAGE_WORK_KIND
    else:
        payload = CollectItemPayload(
            listing_id="123",
            request_plan=RequestPlan(
                url="https://www.facebook.com/marketplace/item/123/", routing=MOBILE
            ),
        )
        kind = COLLECT_ITEM_WORK_KIND
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = facebook_workers.build_routed_facebook_worker_registry(
            database=database, directories=_directories(tmp_path), new_identifier=identifiers
        )
        handler = next(handler for handler in registry.handlers if handler.capability.kind == kind)
        await database.begin_operation(
            operation_id="route-test-operation",
            component=handler.component,
            provenance=ebay_fixtures._provenance(),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        outcome = await handler.execute(
            payload.model_dump(mode="json"),
            AttemptContext(
                work_item_identifier="route-test-work",
                lease_token="route-test-lease",
                worker_identifier="route-test-worker",
                attempt=1,
                operation_identifier="route-test-operation",
            ),
        )
        assert isinstance(outcome, CompletedWork)
        assert len(executed) == 1
        assert opened == [expected]
        # A route override is an execution decision, not a mutation of retained work.
        original = (
            payload.routing
            if isinstance(payload, CollectSearchPayload)
            else payload.request_plan.routing
        )
        assert original == (MOBILE if purpose == "item" else PROTON)
        if purpose == "image":
            snapshot = await database.activity_snapshot(
                captured_at_utc_ns=time_ns(), recent_window_ns=60_000_000_000, maximum_rows=10
            )
            assert snapshot.network.recent_completed == 1
            assert {path.path for path in snapshot.network_paths} == {expected}


@pytest.mark.anyio
@pytest.mark.parametrize("override", (False, True))
async def test_ebay_image_runtime_and_retained_activity_use_effective_route(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, override: bool
) -> None:
    expected = DATACENTER if override else PROTON
    images = ebay_fixtures._Acquirer(
        {ebay_fixtures.IMAGE_URL: ebay_fixtures._Response(ebay_fixtures._png(), "image/png")}
    )
    opened = _patch_routes(
        monkeypatch, ebay_workers, _loaded(tmp_path, override=override), expected, images
    )
    identifiers = ebay_fixtures._identifiers()
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = ebay_fixtures._registry(database, tmp_path, identifiers)
        await ebay_fixtures._retain_records(database, (ebay_fixtures._reference(),), identifiers)
        _ = await ebay_fixtures._enqueue(
            database,
            collect_ebay_image_work(
                identifier=identifiers(), payload=ebay_fixtures._image_payload()
            ),
            identifiers,
        )
        work = await ebay_fixtures._run_next(
            database, registry, identifiers, COLLECT_EBAY_IMAGE_WORK_KIND
        )
        assert work["state"] == WorkState.COMPLETED.value
        assert opened == [expected]
        assert len(images.plans) == 1
        assert images.plans[0].routing == expected
        acquisitions = await database.records_by_kind(("carl", "http", "acquisition"))
        assert len(acquisitions) == 1
        acquisition = cast(dict[str, JsonValue], acquisitions[0][1])
        routing = cast(dict[str, JsonValue], acquisition["routing"])
        assert routing["configured"] == list(expected)
        snapshot = await database.activity_snapshot(
            captured_at_utc_ns=time_ns(), recent_window_ns=60_000_000_000, maximum_rows=10
        )
        assert snapshot.network.recent_completed == 1
        assert {path.path for path in snapshot.network_paths} == {expected}


@pytest.mark.anyio
async def test_routed_facebook_search_retains_datacenter_route_for_bootstrap_and_pagination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    loaded = _loaded(tmp_path, override=True)
    requests: list[httpx.Request] = []
    clients: list[httpx.AsyncClient] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/marketplace/":
            body = search_fixtures._session_bootstrap()
            content_type = "text/html; charset=utf-8"
        elif request.url.path == "/ajax/route-definition/":
            body = search_fixtures._route_definition()
            content_type = "application/json"
        else:
            body = search_fixtures._pagination()
            content_type = "application/json"
        return httpx.Response(
            200, headers={"Content-Type": content_type}, stream=search_fixtures._Stream(body)
        )

    def client_factory(*, proxy: httpx.Proxy, verify: bool) -> httpx.AsyncClient:
        assert str(proxy.url) == "http://dc.decodo.com:10001"
        assert verify is True
        client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        clients.append(client)
        return client

    def load(_path: Path) -> LoadedCarlConfiguration:
        return loaded

    def settings(_loaded: object, _directories: object, path: tuple[str, ...]) -> object:
        assert path == DATACENTER
        return decodo_fixtures._settings(), decodo_fixtures._credentials()

    def unexpected(*_args: object, **_kwargs: object) -> None:
        pytest.fail("A cutover search must not open Proton")

    manager = DecodoSessionManager(client_factory=client_factory)
    monkeypatch.setattr(facebook_workers, "load_configuration", load)
    monkeypatch.setattr(facebook_workers, "decodo_settings", settings)
    monkeypatch.setattr(facebook_workers, "proton_settings", unexpected)
    monkeypatch.setattr(facebook_workers, "DecodoSessionManager", lambda: manager)
    monkeypatch.setattr(facebook_workers, "brave_navigation_headers", lambda: ())
    identifiers = ebay_fixtures._identifiers()
    payload = CollectSearchPayload(
        request=FacebookSearchRequest(
            query="telescope",
            location=SearchFacebookLocation(identifier="456"),
            radius=SearchRadius(value=60, unit=SearchDistanceUnit.MILES),
        ),
        traversal=SearchTraversalPolicy(maximum_pages=3, maximum_results=3),
        routing=PROTON,
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = facebook_workers.build_routed_facebook_worker_registry(
            database=database, directories=_directories(tmp_path), new_identifier=identifiers
        )
        _ = await ebay_fixtures._enqueue(
            database,
            collect_search_work(identifier=identifiers(), payload=payload, not_before_utc_ns=0),
            identifiers,
        )
        work = await ebay_fixtures._run_next(
            database, registry, identifiers, COLLECT_SEARCH_WORK_KIND
        )
        assert work["state"] == WorkState.COMPLETED.value
        assert len(clients) == 1
        assert clients[0].is_closed
        assert [(request.method, request.url.path) for request in requests] == [
            ("GET", "/marketplace/"),
            ("POST", "/ajax/route-definition/"),
            ("POST", "/api/graphql/"),
        ]
        acquisitions = await database.records_by_kind(("carl", "http", "acquisition"))
        assert len(acquisitions) == 3
        for _, acquisition in acquisitions:
            value = cast(dict[str, JsonValue], acquisition)
            routing = cast(dict[str, JsonValue], value["routing"])
            assert routing["configured"] == list(DATACENTER)
        snapshot = await database.activity_snapshot(
            captured_at_utc_ns=time_ns(), recent_window_ns=60_000_000_000, maximum_rows=10
        )
        assert snapshot.network.recent_completed >= 3
        assert {path.path for path in snapshot.network_paths} == {DATACENTER}
