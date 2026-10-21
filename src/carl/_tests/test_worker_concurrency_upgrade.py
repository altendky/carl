"""Upgrading durable limits retires older caps before queued work can run."""

import json
from pathlib import Path
from time import time_ns
from typing import Any

import anyio
import apsw
import pytest

from carl._tests.test_facebook_work import _payload
from carl.core.ebay_items import (
    COLLECT_EBAY_DESCRIPTION_WORK_KIND,
    COLLECT_EBAY_IMAGE_WORK_KIND,
    ebay_item_work_constraints,
)
from carl.core.facebook_images import image_network_constraints, image_session_work_constraint
from carl.core.facebook_work import (
    COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    COLLECT_SEARCH_WORK_KIND,
    collect_search_work,
    facebook_effective_search_work_constraint,
    facebook_network_path_constraint,
    facebook_search_work_constraint,
    legacy_facebook_network_path_constraint_identifier,
)
from carl.core.marketplace_images import marketplace_image_network_constraint
from carl.core.work import SchedulingScope, SchedulingScopeKind, WorkCapability, WorkRequester
from carl.io.sqlite import Database
from carl.work_runtime import prepare_worker_constraints


@pytest.mark.anyio
async def test_upgrade_retires_route_budgets_even_without_queued_searches(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    routes = (("decodo", "personal", "datacenter"), ("proton", "personal", "removed_alias"))
    old_constraints = tuple(
        facebook_network_path_constraint(route).model_copy(
            update={
                "identifier": legacy_facebook_network_path_constraint_identifier(route),
                "scope": SchedulingScope(kind=SchedulingScopeKind.NETWORK_PATH, identity=route),
            }
        )
        for route in routes
    )
    async with Database.managed(path, initialize=True) as database:
        for constraint in old_constraints:
            await database.register_constraint(constraint, registered_at_utc_ns=1)
        assert await database.facebook_search_work_routes() == ()
        await prepare_worker_constraints(database, tmp_path)
        await prepare_worker_constraints(database, tmp_path)
        with apsw.Connection(str(path), flags=apsw.SQLITE_OPEN_READONLY) as connection:
            retired = {
                tuple(json.loads(identity))
                for (identity,) in connection.execute(
                    "SELECT identifier_parts_json FROM scheduling_constraint_retirements"
                )
            }
            assert retired == {constraint.identifier for constraint in old_constraints}
            for route in routes:
                replacement = facebook_network_path_constraint(route)
                row = connection.execute(
                    "SELECT definition_json FROM scheduling_constraints WHERE identifier_parts_json=?",
                    (json.dumps(replacement.identifier, separators=(",", ":")),),
                ).fetchone()
                assert row is not None
                assert json.loads(row[0]) == replacement.model_dump(mode="json")


@pytest.mark.anyio
async def test_upgrade_retires_old_caps_and_is_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        old_work = image_session_work_constraint().model_copy(
            update={
                "identifier": ("carl", "facebook", "image", "work_concurrency", "v2"),
                "maximum_active": 10,
            }
        )
        old_network = image_network_constraints(("proton", "personal", "carl"))[1].model_copy(
            update={
                "identifier": (
                    "carl",
                    "facebook",
                    "image",
                    "network_activity_concurrency",
                    "all_cdns",
                    "v3",
                ),
                "maximum_active": 10,
            }
        )
        old_ebay = tuple(
            constraint.model_copy(
                update={
                    "identifier": (*constraint.scope.identity, "concurrency"),
                    "maximum_active": (
                        5 if constraint.scope.identity == COLLECT_EBAY_IMAGE_WORK_KIND else 1
                    ),
                }
            )
            for constraint in ebay_item_work_constraints()
            if constraint.scope.identity
            in (COLLECT_EBAY_DESCRIPTION_WORK_KIND, COLLECT_EBAY_IMAGE_WORK_KIND)
        )
        old_constraints = (old_work, old_network, *old_ebay)
        for constraint in old_constraints:
            await database.register_constraint(constraint, registered_at_utc_ns=time_ns())

        await prepare_worker_constraints(database, tmp_path)
        await prepare_worker_constraints(database, tmp_path)

        with apsw.Connection(str(path), flags=apsw.SQLITE_OPEN_READONLY) as connection:
            active = {
                tuple(json.loads(identity)): json.loads(definition)
                for identity, definition in connection.execute(
                    """
                    SELECT c.identifier_parts_json,c.definition_json
                    FROM scheduling_constraints AS c
                    WHERE NOT EXISTS (
                        SELECT 1 FROM scheduling_constraint_retirements AS r
                        WHERE r.identifier_parts_json=c.identifier_parts_json
                    )
                    """
                )
            }
            assert all(constraint.identifier not in active for constraint in old_constraints)
            for constraint in (
                image_session_work_constraint(),
                marketplace_image_network_constraint(),
                *ebay_item_work_constraints(),
            ):
                assert active[constraint.identifier] == constraint.model_dump(mode="json")
            assert connection.execute(
                "SELECT count(*) FROM scheduling_constraint_retirements"
            ).fetchone() == (4,)
            assert connection.execute(
                "SELECT count(*) FROM operations WHERE json_extract(result_json,'$.state')='prepared'"
            ).fetchone() == (2,)


@pytest.mark.anyio
async def test_search_upgrade_replaces_decodo_v2_and_v1_caps_before_queued_work_claims(
    tmp_path: Path,
) -> None:
    path = tmp_path / "carl.sqlite3"
    requested_proton = ("proton", "personal", "legacy")
    decodo = ("decodo", "personal", "datacenter")
    proton_constraint = facebook_effective_search_work_constraint(requested_proton).model_copy(
        update={
            "identifier": (
                "carl",
                "facebook",
                "search",
                "effective_route_concurrency",
                "v1",
                *requested_proton,
            )
        }
    )
    old_effective_decodo = proton_constraint.model_copy(
        update={
            "identifier": (
                "carl",
                "facebook",
                "search",
                "effective_route_concurrency",
                "v1",
                *decodo,
            ),
            "scope": proton_constraint.scope.model_copy(
                update={"identity": (proton_constraint.scope.identity[0], *decodo)}
            ),
        }
    )
    old_constraints = (
        facebook_search_work_constraint(requested_proton).model_copy(
            update={
                "identifier": (
                    "carl",
                    "facebook",
                    "search",
                    "route_concurrency",
                    "v1",
                    *requested_proton,
                )
            }
        ),
        facebook_search_work_constraint(decodo).model_copy(
            update={
                "identifier": ("carl", "facebook", "search", "route_concurrency", "v1", *decodo)
            }
        ),
        old_effective_decodo,
        proton_constraint,
        *(
            constraint.model_copy(
                update={
                    "identifier": (*constraint.identifier[:4], "v2", *decodo),
                    "maximum_active": 1,
                }
            )
            for constraint in (
                facebook_search_work_constraint(decodo),
                facebook_effective_search_work_constraint(decodo),
            )
        ),
    )
    async with Database.managed(path, initialize=True) as database:
        for constraint in old_constraints:
            await database.register_constraint(constraint, registered_at_utc_ns=1)
        for constraint in (
            facebook_search_work_constraint(requested_proton),
            facebook_effective_search_work_constraint(requested_proton),
        ):
            await database.register_constraint(constraint, registered_at_utc_ns=1)
        # The previous policy already retired v1. The upgrade must use fresh IDs.
        await prepare_worker_constraints(database, tmp_path)
        for index, routing in enumerate((requested_proton, requested_proton, decodo, decodo)):
            payload = _payload().model_copy(
                update={
                    "routing": routing,
                    "request": _payload().request.model_copy(update={"query": f"search-{index}"}),
                }
            )
            work = collect_search_work(
                identifier=f"search-{index}", payload=payload, not_before_utc_ns=0
            )
            # Pending retries can retain a formerly admitted effective Decodo scope.
            work = work.model_copy(update={"scopes": (*work.scopes, old_effective_decodo.scope)})
            _ = await database.enqueue_work(
                work,
                WorkRequester(
                    request_identifier=f"request-{index}",
                    kind=("test", "legacy_search"),
                    identifier=f"source-{index}",
                    context={},
                ),
                event_identifier=f"enqueue-{index}",
                enqueued_at_utc_ns=index + 1,
            )
        # Reproduce work queued by the older runtime, which did not register v3.
        async with database._connections.writer() as connection:
            await connection.execute(
                "DELETE FROM scheduling_constraints WHERE json_extract(identifier_parts_json,'$[2]')='search' AND json_extract(identifier_parts_json,'$[4]')='v3'"
            )
        await prepare_worker_constraints(database, tmp_path)
        await prepare_worker_constraints(database, tmp_path)
        capability = WorkCapability(
            kind=COLLECT_SEARCH_WORK_KIND,
            payload_schema_version=COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
        )
        for index in range(4):
            claimed = await database.claim_work(
                supported_capabilities=(capability,),
                worker_identifier=f"worker-{index}",
                lease_token=f"lease-{index}",
                lease_duration_ns=100,
                utc_now_ns=lambda: 10,
                event_identifier=f"claim-{index}",
                eligible_identifiers=(f"search-{index}",),
            )
            assert claimed.lease is not None
            assert claimed.lease.work_item_identifier == f"search-{index}"
            if index < 2:
                blocked = await database.claim_work(
                    supported_capabilities=(capability,),
                    worker_identifier=f"blocked-{index}",
                    lease_token=f"blocked-{index}",
                    lease_duration_ns=100,
                    utc_now_ns=lambda: 10,
                    event_identifier=f"blocked-{index}",
                    eligible_identifiers=(f"search-{1 - index}",),
                )
                assert blocked.lease is None
            await database.release_lease(
                work_item_identifier=claimed.lease.work_item_identifier,
                lease_token=claimed.lease.token,
                worker_identifier=claimed.lease.worker_identifier,
                utc_now_ns=lambda: 10,
                eligible_at_utc_ns=1000,
                reason={"kind": "test"},
                event_identifier=f"release-{index}",
            )
        with apsw.Connection(str(path), flags=apsw.SQLITE_OPEN_READONLY) as connection:
            retired = {
                tuple(json.loads(identity))
                for (identity,) in connection.execute(
                    "SELECT identifier_parts_json FROM scheduling_constraint_retirements"
                )
            }
            assert retired == {constraint.identifier for constraint in old_constraints}
            assert (
                facebook_effective_search_work_constraint(requested_proton).identifier
                not in retired
            )
            for route in (requested_proton, decodo):
                for replacement in (
                    facebook_search_work_constraint(route),
                    facebook_effective_search_work_constraint(route),
                ):
                    row = connection.execute(
                        "SELECT definition_json FROM scheduling_constraints WHERE identifier_parts_json=?",
                        (json.dumps(replacement.identifier, separators=(",", ":")),),
                    ).fetchone()
                    assert row is not None and json.loads(row[0]) == replacement.model_dump(
                        mode="json"
                    )
            assert connection.execute(
                """
                SELECT count(*) FROM scheduling_constraint_retirements AS r
                JOIN operations AS o ON o.id=r.operation_id
                WHERE json_extract(o.result_json,'$.state')='prepared'
                """
            ).fetchone() == (6,)
            assert connection.execute(
                "SELECT count(*) FROM operations WHERE json_extract(result_json,'$.state')='prepared'"
            ).fetchone() == (3,)


@pytest.mark.anyio
async def test_policy_audit_finishes_when_shutdown_arrives(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_complete = Database.complete_operation

    async def cancel_at_completion(database: Database, **kwargs: Any) -> None:
        shutdown.cancel()
        await anyio.lowlevel.checkpoint()
        await original_complete(database, **kwargs)

    monkeypatch.setattr(Database, "complete_operation", cancel_at_completion)
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True) as database:
        with anyio.CancelScope() as shutdown:
            await prepare_worker_constraints(database, tmp_path)
        with apsw.Connection(str(path), flags=apsw.SQLITE_OPEN_READONLY) as connection:
            assert connection.execute(
                "SELECT count(*) FROM operations WHERE json_extract(result_json,'$.state')='prepared'"
            ).fetchone() == (1,)
