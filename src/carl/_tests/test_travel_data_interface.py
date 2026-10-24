"""Public setup interfaces are explicit, replayable and correctly constrained."""

import json
from pathlib import Path

import pytest

from carl import travel_cli
from carl.cli import app as cli_app
from carl.core.review_errors import ReviewInputError
from carl.core.travel_data import (
    TRAVEL_DATA_SETUP_WORK_KIND,
    TravelDataFailure,
    TravelDataSetupRequest,
    TravelDataSetupResult,
    TravelDataStatus,
    TravelProviderStatus,
)
from carl.core.work import WorkCapability, WorkState
from carl.io.sqlite import Database
from carl.mcp_server import tool_definitions
from carl.review import ReviewApplication


class Manager:
    def __init__(self) -> None:
        self.status_calls: list[bool] = []
        self.setup_calls: list[TravelDataSetupRequest] = []
        self.fail = False

    async def status(self, *, verify: bool = False) -> TravelDataStatus:
        self.status_calls.append(verify)
        return TravelDataStatus(routing_provider=None, geocoding_provider=None, providers=())

    async def setup(self, request: TravelDataSetupRequest) -> TravelDataSetupResult:
        self.setup_calls.append(request)
        if self.fail:
            raise TravelDataFailure("prerequisite_missing", "Install the local prerequisite.")
        return TravelDataSetupResult(
            provider=request.provider,
            dataset_identifier="fixture-v1",
            manifest_path="/private/data/fixture-v1/manifest.json",
            activated=request.activate,
            message="Verified",
        )


def _successful_cli(arguments: list[str]) -> None:
    with pytest.raises(SystemExit) as exited:
        cli_app(arguments)
    assert exited.value.code == 0


def test_cli_status_and_help_never_schedule_setup(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    manager = Manager()
    monkeypatch.setattr(travel_cli, "TravelDataManager", lambda _: manager)
    _successful_cli(["travel", "status"])
    assert json.loads(capsys.readouterr().out)["providers"] == []
    _successful_cli(["travel", "verify"])
    assert json.loads(capsys.readouterr().out)["providers"] == []
    assert manager.status_calls == [False, True]
    assert not manager.setup_calls
    with pytest.raises(SystemExit) as exited:
        cli_app(["travel", "--help"])
    assert exited.value.code == 0
    help_text = capsys.readouterr().out
    assert all(
        command in help_text for command in ("setup", "update", "activate", "datasets", "verify")
    )
    assert not manager.setup_calls


def test_cli_setup_and_update_explicit_options_reach_manager(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    manager = Manager()
    monkeypatch.setattr(travel_cli, "TravelDataManager", lambda _: manager)
    _successful_cli(
        ["travel", "setup", "osrm", "--no-activate", "--radius-miles", "180", "--threads", "2"]
    )
    output = capsys.readouterr()
    assert json.loads(output.out)["activated"] is False
    assert "23 GiB" in output.err
    request = manager.setup_calls[0]
    assert request.provider == "osrm"
    assert request.radius_miles == 180
    assert request.threads == 2
    assert request.activate is False
    _successful_cli(["travel", "update", "geonames"])
    result = json.loads(capsys.readouterr().out)
    assert result["provider"] == "geonames"
    assert manager.setup_calls[1].activate
    assert manager.setup_calls[1].request_identifier != request.request_identifier


def test_cli_failed_setup_reports_action_and_exits_nonzero(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    manager = Manager()
    manager.fail = True
    monkeypatch.setattr(travel_cli, "TravelDataManager", lambda _: manager)
    with pytest.raises(SystemExit) as exited:
        cli_app(["travel", "setup", "geonames"])
    assert exited.value.code == 1
    output = json.loads(capsys.readouterr().out)
    assert output == {
        "state": "failed",
        "reason": "prerequisite_missing",
        "message": "Install the local prerequisite.",
    }


def test_cli_verify_exits_nonzero_for_missing_selected_data(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    class MissingManager(Manager):
        async def status(self, *, verify: bool = False) -> TravelDataStatus:
            return TravelDataStatus(
                routing_provider="osrm",
                geocoding_provider="disabled",
                providers=(
                    TravelProviderStatus(
                        provider="osrm", state="missing", message="Run carl travel setup osrm"
                    ),
                ),
            )

    monkeypatch.setattr(travel_cli, "TravelDataManager", lambda _: MissingManager())
    with pytest.raises(SystemExit) as raised:
        travel_cli.verify()
    assert raised.value.code == 1
    assert json.loads(capsys.readouterr().out)["providers"][0]["state"] == "missing"


@pytest.mark.parametrize(
    ("routing_provider", "geocoding_provider", "configuration_message"),
    [
        (None, None, "Carl configuration is missing or invalid."),
        ("osrm", "geonames", "Set travel.origin coordinates or origin_label in private config."),
    ],
)
def test_cli_verify_reports_configuration_problems_but_status_remains_inspectable(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    routing_provider: str | None,
    geocoding_provider: str | None,
    configuration_message: str,
) -> None:
    class ConfigurationProblemManager(Manager):
        async def status(self, *, verify: bool = False) -> TravelDataStatus:
            return TravelDataStatus(
                routing_provider=routing_provider,
                geocoding_provider=geocoding_provider,
                configuration_message=configuration_message,
                providers=(),
            )

    monkeypatch.setattr(travel_cli, "TravelDataManager", lambda _: ConfigurationProblemManager())
    _successful_cli(["travel", "status"])
    assert json.loads(capsys.readouterr().out)["configuration_message"] == configuration_message
    with pytest.raises(SystemExit) as raised:
        cli_app(["travel", "verify"])
    assert raised.value.code == 1
    assert json.loads(capsys.readouterr().out)["configuration_message"] == configuration_message


@pytest.mark.anyio
async def test_setup_requests_replay_exactly_without_inline_build_and_serialize_providers(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        application = ReviewApplication(database=database, repository_root=tmp_path)
        request = TravelDataSetupRequest(request_identifier="first", provider="osrm")
        first = await application.request_travel_data_setup(request)
        assert first.created
        assert first.state is WorkState.PENDING
        assert first.worker_required
        assert "worker" in first.message
        replay = await application.request_travel_data_setup(request)
        assert not replay.created
        assert replay.work_identifier == first.work_identifier
        with pytest.raises(ReviewInputError, match="different inputs"):
            await application.request_travel_data_setup(
                request.model_copy(update={"provider": "geonames"})
            )
        second = await application.request_travel_data_setup(
            request.model_copy(update={"request_identifier": "second", "provider": "geonames"})
        )
        assert second.created
        capabilities = (WorkCapability(kind=TRAVEL_DATA_SETUP_WORK_KIND, payload_schema_version=1),)
        now = application.utc_now_ns()
        first_claim = await database.claim_work(
            supported_capabilities=capabilities,
            worker_identifier="one",
            lease_token="one",
            lease_duration_ns=10**12,
            utc_now_ns=lambda: now,
            event_identifier="claim-one",
        )
        assert first_claim.lease is not None
        second_claim = await database.claim_work(
            supported_capabilities=capabilities,
            worker_identifier="two",
            lease_token="two",
            lease_duration_ns=10**12,
            utc_now_ns=lambda: now,
            event_identifier="claim-two",
        )
        assert second_claim.lease is None
        # Neither request nor registration executes the dataset manager.
        assert not list(tmp_path.glob("**/manifest.json"))


@pytest.mark.anyio
async def test_travel_mcp_registry_annotations_distinguish_reads_and_explicit_mutations(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        application = ReviewApplication(database=database, repository_root=tmp_path)
        definitions = {tool.name: tool for tool in tool_definitions(application)}
        for name in ("get_travel_data_status", "list_travel_datasets"):
            assert definitions[name].annotations.read_only_hint
        for name in ("request_travel_data_setup", "activate_travel_dataset"):
            annotations = definitions[name].annotations
            assert not annotations.read_only_hint
            assert annotations.idempotent_hint
            assert annotations.open_world_hint == (name == "request_travel_data_setup")
        assert "worker" in definitions["request_travel_data_setup"].description
