"""Explicit analysis IDs bypass workspace scan bounds and account for omissions."""

# pyright: reportPrivateUsage=false

from pathlib import Path
from time import time_ns
from uuid import uuid4

import pytest

from carl._tests.test_marketplace_listing import _publish
from carl._tests.test_mixed_workspace import _complete
from carl._tests.test_selection_analysis_freshness import (
    _completed_refresh,
    _facebook_run,
    _track,
    _workspace,
)
from carl.core.analysis_batch import SelectionAnalysesRequest
from carl.core.models import RecordDraft
from carl.core.review_workspace import ListingIdsSelection, ReviewWorkspace
from carl.core.work import (
    SchedulingScope,
    SchedulingScopeKind,
    WorkDefinition,
    WorkRequester,
)
from carl.io.sqlite import Database
from carl.review import ReviewApplication


async def _evidence(database: Database, identifier: str, records: tuple[RecordDraft, ...]) -> None:
    kind = ("test", "analysis_selection_evidence")
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
    await _complete(database, identifier, records=records, result={})


async def _application(
    database: Database,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    members: tuple[str, ...],
    observations: tuple[str, ...],
    *,
    sold: str | None = None,
) -> ReviewApplication:
    await _publish(database, (_facebook_run("facebook-run", members),))
    await _completed_refresh(database, "refresh-facebook", "facebook-run", "facebook-run")
    await _evidence(
        database,
        "acquire",
        tuple(
            RecordDraft(
                identifier=f"acquisition-{identifier}",
                kind=("carl", "http", "acquisition"),
                schema_version=1,
                value={},
            )
            for identifier in observations
        ),
    )
    await _evidence(
        database,
        "extract",
        tuple(
            RecordDraft(
                identifier=f"observation-{identifier}",
                kind=("carl", "facebook", "listing_observation"),
                schema_version=1,
                value={
                    "listing_id": identifier,
                    "acquisition_record_id": f"acquisition-{identifier}",
                    "response_classification": {"kind": "full_listing"},
                    "images": [],
                    "fields": {
                        "title": {
                            "state": "present",
                            "evidence": [{"state": "present", "normalized": "Telescope"}],
                        },
                        "availability_sold": {
                            "state": "present",
                            "evidence": [{"state": "present", "normalized": identifier == sold}],
                        },
                        "availability_live": {
                            "state": "present",
                            "evidence": [{"state": "present", "normalized": identifier != sold}],
                        },
                    },
                },
            )
            for identifier in observations
        ),
    )
    workspace = _workspace((_track("facebook", "facebook-run"),))

    async def get_workspace(_app: ReviewApplication, _identifier: str) -> ReviewWorkspace:
        return workspace

    async def active_guide(_app: ReviewApplication, _identifier: str) -> None:
        return None

    async def forbidden_scan(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("Explicit selections must not scan workspace candidates")

    monkeypatch.setattr(ReviewApplication, "get_review_workspace", get_workspace)
    monkeypatch.setattr(ReviewApplication, "_active_product_guide", active_guide)
    monkeypatch.setattr(ReviewApplication, "_workspace_analysis_candidates", forbidden_scan)
    return ReviewApplication(database, tmp_path)


@pytest.mark.anyio
async def test_twenty_two_explicit_ids_beyond_2500_are_all_selected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    members = tuple(str(1000000000000000 + index) for index in range(2600))
    selected = members[-22:]
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        app = await _application(database, tmp_path, monkeypatch, members, selected)
        request = SelectionAnalysesRequest(
            workspace_record_identifier="workspace",
            selection=ListingIdsSelection(listing_identifiers=selected),
            maximum_candidate_listings_examined=1,
        )
        preview = await app.preview_selection_analyses(request)
        requested = await app.request_selection_analyses(request)
        assert preview.selected_observations == requested.selected_observation_count == 22
        assert preview.candidate_listings_examined == requested.candidate_listings_examined == 22
        assert not preview.candidate_examination_limit_reached
        assert not requested.candidate_examination_limit_reached
        assert preview.skipped == requested.skipped == ()
        payload = (await database.work(requested.work_identifier))["payload"]
        assert set(payload["listing_observation_record_identifiers"]) == {
            f"observation-{identifier}" for identifier in selected
        }
        assert set(payload["listing_identifiers_by_observation"].values()) == set(selected)


@pytest.mark.anyio
async def test_every_unselected_id_has_a_planning_skip_reason(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identifiers = ("123", "456", "789", "999")
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        app = await _application(
            database, tmp_path, monkeypatch, identifiers[:3], identifiers[:2], sold="456"
        )
        # Unknown status is explicitly allowed so a missing item page reaches
        # analysis eligibility instead of being excluded by status selection.
        request = SelectionAnalysesRequest.model_validate(
            {
                "workspace_record_identifier": "workspace",
                "selection": {"kind": "listing_ids", "listing_identifiers": identifiers},
                "statuses": ["available", "unknown"],
            }
        )
        preview = await app.preview_selection_analyses(request)
        requested = await app.request_selection_analyses(request)
        assert requested.selected_observation_count == 1
        assert requested.skipped == preview.skipped
        assert {entry.listing_identifier: entry.kind for entry in requested.skipped} == {
            "456": "status_excluded",
            "789": "no_usable_observation",
            "999": "outside_workspace",
        }
        payload = (await database.work(requested.work_identifier))["payload"]
        assert payload["selection_skipped"] == [
            entry.model_dump(mode="json") for entry in requested.skipped
        ]


@pytest.mark.anyio
async def test_maximum_items_reports_exact_ids_not_queued(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identifiers = ("123", "456", "789")
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        app = await _application(database, tmp_path, monkeypatch, identifiers, identifiers)
        requested = await app.request_selection_analyses(
            SelectionAnalysesRequest(
                workspace_record_identifier="workspace",
                selection=ListingIdsSelection(listing_identifiers=identifiers),
                maximum_items=1,
            )
        )
        assert requested.selected_observation_count == 1
        assert len(requested.skipped) == 2
        assert {entry.kind for entry in requested.skipped} == {"maximum_items"}
        payload = (await database.work(requested.work_identifier))["payload"]
        selected = set(payload["listing_identifiers_by_observation"].values())
        assert selected.isdisjoint(entry.listing_identifier for entry in requested.skipped)
        assert selected | {entry.listing_identifier for entry in requested.skipped} == set(
            identifiers
        )
