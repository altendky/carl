"""Image downloads share one durable concurrency budget across marketplaces."""

from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import anyio
import pytest

from carl.core.components import Component, ComponentId
from carl.core.ebay_items import (
    COLLECT_EBAY_DESCRIPTION_WORK_KIND,
    COLLECT_EBAY_IMAGE_WORK_KIND,
    ebay_item_work_constraints,
    legacy_ebay_item_work_constraint_identifiers,
)
from carl.core.facebook_images import (
    image_network_activity,
    image_network_constraints,
    legacy_image_network_constraint_identifiers,
)
from carl.core.marketplace_images import (
    MARKETPLACE_IMAGE_SCOPE,
    marketplace_image_network_constraint,
)
from carl.core.models import CodeProvenance
from carl.core.work import (
    ConcurrencyConstraint,
    NetworkActivityAdmission,
    NetworkActivityAdmissionResult,
    NetworkActivityDefinition,
    NetworkActivityState,
)
from carl.io.network_activity import NetworkActivityScheduler, network_activity_definition
from carl.io.sqlite import Database


@pytest.mark.anyio
async def test_waiting_image_rechecks_capacity_before_permit_expiry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class AdmissionDatabase:
        calls = 0

        async def create_network_activity(self, *_args: object, **_kwargs: object) -> None:
            pass

        async def try_admit_network_activity(
            self, **_kwargs: object
        ) -> NetworkActivityAdmissionResult:
            self.calls += 1
            if self.calls == 1:
                return NetworkActivityAdmissionResult(
                    admission=None, next_eligible_at_utc_ns=600_000_000_000
                )
            return NetworkActivityAdmissionResult(
                admission=NetworkActivityAdmission(
                    activity_identifier="image",
                    token="token",
                    admitted_at_utc_ns=0,
                    permit_expires_at_utc_ns=600_000_000_000,
                ),
                next_eligible_at_utc_ns=None,
            )

        async def finish_network_activity(self, **_kwargs: object) -> None:
            pass

    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(anyio, "sleep", sleep)
    database = AdmissionDatabase()
    scheduler = NetworkActivityScheduler(
        database=cast(Database, cast(object, database)),
        new_identifier=lambda: "identifier",
        utc_now_ns=lambda: 0,
        sample_uniform_holdoff_ns=lambda minimum, maximum: minimum,
        permit_duration_ns=600_000_000_000,
    )
    async with scheduler.admit(_activity("image", "ebay")):
        pass
    assert delays == [1.0]
    assert database.calls == 2


def _activity(identifier: str, source: str) -> NetworkActivityDefinition:
    if source == "facebook":
        return image_network_activity(
            identifier=identifier,
            operation_identifier="operation",
            network_session_identifier=identifier + "-session",
            attempt=1,
            routing=("proton", "personal", "carl"),
            url="https://scontent.example.fbcdn.net/image.jpg",
        )
    activity = network_activity_definition(
        identifier=identifier,
        kind=("carl", "ebay", "network_activity", "gallery_image"),
        operation_identifier="operation",
        network_session_identifier=identifier + "-session",
        network_path=("decodo", "datacenter", "carl"),
    )
    return activity.model_copy(update={"scopes": (*activity.scopes, MARKETPLACE_IMAGE_SCOPE)})


@pytest.mark.anyio
@pytest.mark.parametrize("facebook_count", (0, 13, 25))
async def test_shared_image_budget_admits_25_then_releases_a_slot(
    tmp_path: Path, facebook_count: int
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.begin_operation(
            operation_id="operation",
            component=Component(ComponentId(("test", "images")), 1, lambda: None),
            provenance=CodeProvenance(
                repository_url=None,
                commit_hash=None,
                worktree_state="dirty",
                package_version="test",
                python_implementation="test",
                python_version="test",
                dependencies=(),
                lockfile_sha256=None,
            ),
            invocation={},
            configuration={},
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        await database.register_constraint(
            marketplace_image_network_constraint(), registered_at_utc_ns=1
        )
        sources = ("facebook",) * facebook_count + ("ebay",) * (25 - facebook_count)
        for index, source in enumerate(sources):
            identifier = f"image-{index}"
            _ = await database.create_network_activity(
                _activity(identifier, source),
                created_at_utc_ns=10,
                event_identifier=identifier + "-created",
                sample_uniform_holdoff_ns=lambda minimum, maximum: minimum,
            )
            admitted = await database.try_admit_network_activity(
                network_activity_identifier=identifier,
                admission_token=identifier + "-token",
                permit_duration_ns=100,
                now_utc_ns=10,
                event_identifier=identifier + "-admitted",
            )
            assert admitted.admission is not None

        # Both marketplaces must wait, even with distinct sessions and routes.
        for source in ("facebook", "ebay"):
            identifier = source + "-waiting"
            _ = await database.create_network_activity(
                _activity(identifier, source),
                created_at_utc_ns=10,
                event_identifier=identifier + "-created",
                sample_uniform_holdoff_ns=lambda minimum, maximum: minimum,
            )
            waiting = await database.try_admit_network_activity(
                network_activity_identifier=identifier,
                admission_token=identifier + "-token",
                permit_duration_ns=100,
                now_utc_ns=10,
                event_identifier=identifier + "-blocked",
            )
            assert waiting.admission is None
            assert waiting.next_eligible_at_utc_ns == 110

        # Page and description requests do not consume image slots.
        description = network_activity_definition(
            identifier="description",
            kind=("carl", "ebay", "network_activity", "description"),
            operation_identifier="operation",
            network_session_identifier="description-session",
            network_path=("decodo", "datacenter", "carl"),
        )
        _ = await database.create_network_activity(
            description,
            created_at_utc_ns=10,
            event_identifier="description-created",
            sample_uniform_holdoff_ns=lambda minimum, maximum: minimum,
        )
        description_admission = await database.try_admit_network_activity(
            network_activity_identifier="description",
            admission_token="description-token",
            permit_duration_ns=100,
            now_utc_ns=10,
            event_identifier="description-admitted",
        )
        assert description_admission.admission is not None

        await database.finish_network_activity(
            network_activity_identifier="image-0",
            admission_token="image-0-token",
            state=NetworkActivityState.COMPLETED,
            ended_at_utc_ns=11,
            result={},
            event_identifier="image-0-completed",
        )
        released = await database.try_admit_network_activity(
            network_activity_identifier="ebay-waiting",
            admission_token="ebay-waiting-token",
            permit_duration_ns=100,
            now_utc_ns=11,
            event_identifier="ebay-waiting-admitted",
        )
        assert released.admission is not None
        still_waiting = await database.try_admit_network_activity(
            network_activity_identifier="facebook-waiting",
            admission_token="facebook-waiting-token",
            permit_duration_ns=100,
            now_utc_ns=11,
            event_identifier="facebook-still-waiting",
        )
        assert still_waiting.admission is None


def test_image_and_description_limits_have_new_immutable_identities() -> None:
    route = ("proton", "personal", "carl")
    facebook_constraints = image_network_constraints(route)
    facebook_legacy = legacy_image_network_constraint_identifiers(route)
    assert ("carl", "facebook", "image", "work_concurrency", "v2") in facebook_legacy
    assert (
        "carl",
        "facebook",
        "image",
        "network_activity_concurrency",
        "all_cdns",
        "v3",
    ) in facebook_legacy
    assert all(constraint.identifier not in facebook_legacy for constraint in facebook_constraints)
    assert {
        constraint.maximum_active
        for constraint in facebook_constraints
        if isinstance(constraint, ConcurrencyConstraint)
    } == {25}

    ebay_constraints = ebay_item_work_constraints()
    ebay_legacy = legacy_ebay_item_work_constraint_identifiers()
    assert ebay_legacy == (
        (*COLLECT_EBAY_DESCRIPTION_WORK_KIND, "concurrency"),
        (*COLLECT_EBAY_IMAGE_WORK_KIND, "concurrency"),
    )
    assert all(constraint.identifier not in ebay_legacy for constraint in ebay_constraints)
    assert {
        constraint.scope.identity: constraint.maximum_active for constraint in ebay_constraints
    }[COLLECT_EBAY_DESCRIPTION_WORK_KIND] == 10
