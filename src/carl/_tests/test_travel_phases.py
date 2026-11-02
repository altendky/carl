"""Offline observations exercise every serializable travel planning boundary."""

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pytest
from pydantic import TypeAdapter, ValidationError

from carl.core.configuration import GeonamesConfiguration, OsrmConfiguration, TravelConfiguration
from carl.core.models import JsonValue
from carl.core.travel import TravelCoordinate, TravelDestination, TravelEstimate
from carl.core.travel_phases import (
    CacheObserved,
    CacheRead,
    CacheWrite,
    Complete,
    Geocode,
    GeocodeObserved,
    GeocodeResult,
    Matrix,
    MatrixObserved,
    TravelAction,
    TravelEvent,
    TravelMode,
    TravelSnapshot,
    TravelState,
    TravelStep,
    advance_travel,
    geocode_cache_key,
    parse_hosted_geocode,
    parse_hosted_matrix,
    resolved_origin_identity,
    route_cache_key,
    start_travel,
)

NOW = 1_700_000_000_000_000_000
ORIGIN = TravelCoordinate(latitude=10, longitude=20)
DESTINATION = TravelCoordinate(latitude=11, longitude=21)
_EVENT = TypeAdapter(TravelEvent)
_ACTION = TypeAdapter(TravelAction)


def _snapshot(**changes: object) -> TravelSnapshot:
    configuration = TravelConfiguration(origin=ORIGIN, country_code="US")
    return TravelSnapshot(configuration=configuration.model_copy(update=changes))


def _destination(
    coordinate: TravelCoordinate | None = DESTINATION, label: str = "Town"
) -> TravelDestination:
    return TravelDestination(
        coordinate=coordinate,
        label=label,
        precision="source_approximate" if coordinate is not None else "unavailable",
    )


def _hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


@dataclass
class _Driver:
    cache: dict[str, JsonValue] = field(default_factory=dict)
    geocode: Callable[[Geocode], GeocodeResult] = lambda _: GeocodeResult(coordinate=DESTINATION)
    matrix: Callable[[Matrix], MatrixObserved] = lambda action: MatrixObserved(
        action_id=action.action_id, metrics=((1000.0, 100.0),) * len(action.groups)
    )
    actions: list[TravelAction] = field(default_factory=list)
    writes: list[CacheWrite] = field(default_factory=list)
    states: list[TravelState] = field(default_factory=list)

    def run(self, state: TravelState) -> Complete:
        step = advance_travel(state, now_utc_ns=NOW)
        for _ in range(1000):
            # Restarts can happen at every external boundary, including terminal.
            restored = TravelStep.model_validate_json(step.model_dump_json())
            assert restored == step
            assert _ACTION.validate_json(_ACTION.dump_json(step.action)) == step.action
            assert advance_travel(restored.state, now_utc_ns=NOW).action == step.action
            self.states.append(restored.state)
            self.actions.append(step.action)
            for write in step.cache_writes:
                self.cache[write.key] = write.value
                self.writes.append(write)
            action = step.action
            if isinstance(action, Complete):
                return action
            if isinstance(action, CacheRead):
                event = CacheObserved(
                    action_id=action.action_id, key=action.key, value=self.cache.get(action.key)
                )
            elif isinstance(action, Geocode):
                observed = self.geocode(action)
                event = GeocodeObserved(
                    action_id=action.action_id,
                    coordinate=observed.coordinate,
                    reason=observed.reason,
                )
            else:
                event = self.matrix(action)
            event = _EVENT.validate_json(_EVENT.dump_json(event))
            step = advance_travel(restored.state, event, now_utc_ns=NOW)
        raise AssertionError("Travel engine did not finish bounded offline work")


def test_all_boundaries_resume_and_batch_50_ordered_deduplicated_destinations() -> None:
    destinations = tuple(
        _destination(TravelCoordinate(latitude=11, longitude=index / 10), str(index))
        for index in range(51)
    )
    destinations = (*destinations, destinations[0].model_copy(update={"label": "Duplicate"}))
    driver = _Driver()
    result = driver.run(start_travel(_snapshot(), destinations))
    matrices = [action for action in driver.actions if isinstance(action, Matrix)]
    assert [len(action.groups) for action in matrices] == [50, 1]
    assert matrices[0].groups[0].indices == (0, 51)
    assert len(result.estimates) == 52
    assert [estimate.destination_label for estimate in result.estimates] == [
        item.label for item in destinations
    ]
    assert all(estimate.status == "ready" for estimate in result.estimates)
    ids = [action.action_id for action in driver.actions if not isinstance(action, Complete)]
    assert ids == list(range(1, len(ids) + 1))
    assert len(driver.writes) == 51
    assert all(write.expires_at_utc_ns == NOW + 604800 * 1_000_000_000 for write in driver.writes)


def test_pacing_replay_does_not_spend_another_geocode_or_action_identifier() -> None:
    step = advance_travel(
        start_travel(_snapshot(maximum_geocodes_per_batch=1), (_destination(None),)), now_utc_ns=NOW
    )
    assert isinstance(step.action, CacheRead)
    step = advance_travel(
        step.state,
        CacheObserved(action_id=step.action.action_id, key=step.action.key),
        now_utc_ns=NOW,
    )
    assert isinstance(step.action, Geocode) and step.state.geocodes_used == 1
    for _ in range(5):
        restored = TravelState.model_validate_json(step.state.model_dump_json())
        assert advance_travel(restored, now_utc_ns=NOW + 1_000_000_000) == step


def test_wrong_duplicate_and_cross_kind_events_are_rejected_without_mutating_state() -> None:
    first = advance_travel(start_travel(_snapshot(), (_destination(),)), now_utc_ns=NOW)
    assert isinstance(first.action, CacheRead)
    event = CacheObserved(action_id=first.action.action_id, key=first.action.key)
    for wrong in (
        event.model_copy(update={"action_id": 9}),
        event.model_copy(update={"key": "wrong"}),
        GeocodeObserved(action_id=first.action.action_id, coordinate=DESTINATION),
    ):
        with pytest.raises(ValueError, match="does not match"):
            advance_travel(first.state, wrong, now_utc_ns=NOW)
    matrix = advance_travel(first.state, event, now_utc_ns=NOW)
    assert isinstance(matrix.action, Matrix)
    with pytest.raises(ValueError, match="does not match"):
        advance_travel(matrix.state, event, now_utc_ns=NOW)
    done = advance_travel(
        matrix.state,
        MatrixObserved(action_id=matrix.action.action_id, metrics=((1.0, 2.0),)),
        now_utc_ns=NOW,
    )
    with pytest.raises(ValueError, match="does not match"):
        advance_travel(done.state, event, now_utc_ns=NOW)
    assert first.state.destination_cursor == 0


@pytest.mark.parametrize(
    "snapshot",
    [TravelSnapshot(), TravelSnapshot(unavailable_reason="routing_configuration_unavailable")],
)
def test_empty_estimate_returns_before_configuration_and_origin_work(
    snapshot: TravelSnapshot,
) -> None:
    result = _Driver().run(start_travel(snapshot, ()))
    assert result == Complete(origin_identity=None, estimates=())


@pytest.mark.parametrize("reason", ["routing_not_configured", "routing_configuration_unavailable"])
def test_unconfigured_origin_identity_is_the_existing_literal(reason: str) -> None:
    snapshot = TravelSnapshot(unavailable_reason=reason)
    driver = _Driver()
    result = driver.run(start_travel(snapshot, (), mode="origin_identity"))
    assert result.origin_identity == reason
    assert len(driver.actions) == 1


def test_exact_legacy_origin_and_cache_identity_hashes() -> None:
    snapshot = _snapshot(origin_label="My origin")
    configuration = snapshot.configuration
    assert configuration is not None
    assert resolved_origin_identity(snapshot, ORIGIN) == _hash(
        (
            "ors-driving-car-v1",
            "My origin",
            "US",
            ORIGIN.model_dump(),
            604800,
            "openrouteservice",
            "openrouteservice",
            None,
            None,
        )
    )
    assert geocode_cache_key("  EXAMPLE   town ", "US", "openrouteservice", None) == _hash(
        ("geocode-v1", "example town", "US")
    )
    assert route_cache_key(snapshot, ORIGIN, DESTINATION) == _hash(
        (
            "ors-driving-car-v1",
            ORIGIN.model_dump(),
            DESTINATION.model_dump(),
            604800,
            "openrouteservice",
            None,
        )
    )
    failed = _snapshot(origin=None, origin_label="Somewhere")
    driver = _Driver(geocode=lambda _: GeocodeResult(reason="provider_http_429"))
    result = driver.run(start_travel(failed, (), mode="origin_identity"))
    assert failed.configuration is not None
    assert result.origin_identity == _hash(
        (
            "ors-driving-car-v1",
            (failed.configuration.model_dump(mode="json"), None, None),
            "origin_unavailable",
            "provider_http_429",
        )
    )
    assert driver.writes == []


def test_destination_budget_valid_and_invalid_cached_geocodes_and_origin_exemption() -> None:
    snapshot = _snapshot(origin=None, origin_label="Home", maximum_geocodes_per_batch=1)
    driver = _Driver()
    driver.cache[geocode_cache_key("Cached", "US", "openrouteservice", None)] = (
        DESTINATION.model_dump(mode="json")
    )
    driver.cache[geocode_cache_key("Malformed", "US", "openrouteservice", None)] = {
        "latitude": True,
        "longitude": 0,
    }
    result = driver.run(
        start_travel(
            snapshot,
            (
                _destination(None, "Cached"),
                _destination(None, "Malformed"),
                _destination(None, "Over budget"),
            ),
        )
    )
    geocodes = [action for action in driver.actions if isinstance(action, Geocode)]
    assert [(action.role, action.label) for action in geocodes] == [
        ("origin", "Home"),
        ("destination", "Malformed"),
    ]
    assert [estimate.status for estimate in result.estimates] == ["ready", "ready", "unavailable"]
    assert result.estimates[2].unavailable_reason == "geocode_budget_exhausted"
    assert result.estimates[0].location_precision == "geocoded_locality"
    assert driver.states[-1].geocodes_used == 1


def test_disabled_geocoding_can_use_positive_and_negative_cache_without_http() -> None:
    snapshot = _snapshot(geocoding_provider="disabled", maximum_geocodes_per_batch=0)
    driver = _Driver()
    driver.cache[geocode_cache_key("Cached", "US", "disabled", None)] = DESTINATION.model_dump(
        mode="json"
    )
    driver.cache[geocode_cache_key("Negative", "US", "disabled", None)] = {
        "reason": "ambiguous_location"
    }
    result = driver.run(
        start_travel(
            snapshot,
            (
                _destination(None, "Cached"),
                _destination(None, "Negative"),
                _destination(None, "Missing"),
            ),
        )
    )
    assert not any(isinstance(action, Geocode) for action in driver.actions)
    assert [estimate.unavailable_reason for estimate in result.estimates] == [
        None,
        "ambiguous_location",
        "geocoding_disabled",
    ]


def test_cached_routes_preserve_labels_validate_provider_and_refresh_bypasses_only_route_cache() -> (
    None
):
    snapshot = _snapshot()
    key = route_cache_key(snapshot, ORIGIN, DESTINATION)
    ready = TravelEstimate(status="ready", distance_meters=4.0, duration_seconds=5.0)
    driver = _Driver(cache={key: ready.model_dump(mode="json")})
    result = driver.run(start_travel(snapshot, (_destination(label="Current"),)))
    assert (
        result.estimates[0].distance_meters == 4
        and result.estimates[0].destination_label == "Current"
    )
    assert not any(isinstance(action, Matrix) for action in driver.actions)
    driver = _Driver(
        cache={key: ready.model_copy(update={"provider": "osrm"}).model_dump(mode="json")}
    )
    result = driver.run(start_travel(snapshot, (_destination(),)))
    assert result.estimates[0].distance_meters == 1000
    geo_key = geocode_cache_key("Town", "US", "openrouteservice", None)
    driver = _Driver(
        cache={key: ready.model_dump(mode="json"), geo_key: DESTINATION.model_dump(mode="json")}
    )
    result = driver.run(
        start_travel(
            snapshot,
            (_destination(None),),
            mode="refresh",
            expected_origin_identity=resolved_origin_identity(snapshot, ORIGIN),
        )
    )
    assert result.estimates[0].distance_meters == 1000
    assert not any(isinstance(action, Geocode) for action in driver.actions)
    assert [action.purpose for action in driver.actions if isinstance(action, CacheRead)] == [
        "destination_geocode"
    ]


def test_changed_origin_finishes_before_destination_lookup_or_matrix() -> None:
    driver = _Driver()
    result = driver.run(
        start_travel(
            _snapshot(), (_destination(None),), mode="refresh", expected_origin_identity="old"
        )
    )
    assert len(driver.actions) == 1 and result.estimates[0].unavailable_reason == "origin_changed"


@pytest.mark.parametrize("mode", ["estimate", "refresh"])
def test_conflicts_override_supplied_coordinate_and_missing_labels_skip_all_requests(
    mode: TravelMode,
) -> None:
    driver = _Driver()
    result = driver.run(
        start_travel(
            _snapshot(),
            (
                _destination().model_copy(update={"unavailable_reason": "conflicting_coordinates"}),
                TravelDestination(),
            ),
            mode=mode,
        )
    )
    assert [estimate.unavailable_reason for estimate in result.estimates] == [
        "conflicting_coordinates",
        "location_unavailable",
    ]
    assert len(driver.actions) == 1


@pytest.mark.parametrize(
    "reason",
    [
        "provider_http_429",
        "provider_network_failure",
        "provider_invalid_response",
        "provider_cooldown",
    ],
)
def test_provider_failures_are_immediate_unavailable_with_short_cache(reason: str) -> None:
    driver = _Driver(
        matrix=lambda action: MatrixObserved(action_id=action.action_id, reason=reason)
    )
    result = driver.run(start_travel(_snapshot(), (_destination(),)))
    assert result.estimates[0].unavailable_reason == reason
    assert driver.writes[0].expires_at_utc_ns == NOW + 60 * 1_000_000_000


def test_mixed_matrix_cells_and_mismatched_response_lengths() -> None:
    destinations = tuple(_destination(TravelCoordinate(latitude=11, longitude=i)) for i in range(3))
    driver = _Driver(
        matrix=lambda action: MatrixObserved(
            action_id=action.action_id,
            metrics=((1.0, 2.0), "no_route", "provider_invalid_response"),
        )
    )
    result = driver.run(start_travel(_snapshot(), destinations))
    assert [value.unavailable_reason for value in result.estimates] == [
        None,
        "no_route",
        "provider_invalid_response",
    ]
    assert [
        int(datetime.fromisoformat(value.expires_at_utc or "").timestamp())
        for value in result.estimates
    ] == [NOW // 1_000_000_000 + 604800, NOW // 1_000_000_000 + 60, NOW // 1_000_000_000 + 60]
    driver = _Driver(
        matrix=lambda action: MatrixObserved(action_id=action.action_id, metrics=((1.0, 2.0),))
    )
    result = driver.run(start_travel(_snapshot(), destinations))
    assert all(
        value.unavailable_reason == "provider_invalid_response" for value in result.estimates
    )


def _feature(longitude: object, latitude: object, confidence: object) -> dict[str, object]:
    return {
        "geometry": {"coordinates": [longitude, latitude]},
        "properties": {"confidence": confidence},
    }


@pytest.mark.parametrize(
    "features,reason",
    [
        ([], "location_not_found"),
        ([_feature(21, 11, True)], "location_not_found"),
        ([_feature(21, 11, float("nan"))], "location_not_found"),
        ([_feature(True, 11, 1)], "location_not_found"),
        ([_feature(21, 11, 0.79)], "ambiguous_location"),
        ([_feature(21, 11, 0.9), _feature(22, 12, 0.88)], "ambiguous_location"),
    ],
)
def test_geocode_classifier_rejects_invalid_and_ambiguous_candidates(
    features: list[object], reason: str
) -> None:
    assert parse_hosted_geocode({"features": features}).reason == reason


def test_geocode_classifier_deduplicates_before_ambiguity_and_cache_expiry() -> None:
    observed = parse_hosted_geocode(
        {"features": [_feature(21, 11, 0.9), _feature(21, 11, 0.89), _feature(22, 12, 0.8)]}
    )
    assert observed.coordinate == DESTINATION
    driver = _Driver(geocode=lambda _: observed)
    driver.run(start_travel(_snapshot(), (_destination(None),)))
    assert driver.writes[0].expires_at_utc_ns == NOW + 30 * 86400 * 1_000_000_000
    driver = _Driver(geocode=lambda _: GeocodeResult(reason="ambiguous_location"))
    result = driver.run(start_travel(_snapshot(), (_destination(None),)))
    assert result.estimates[0].unavailable_reason == "ambiguous_location"
    assert driver.writes[0].expires_at_utc_ns == NOW + 86400 * 1_000_000_000


@pytest.mark.parametrize(
    "response", [None, {}, {"distances": []}, {"distances": [[1]], "durations": [[1, 2]]}]
)
def test_matrix_classifier_requires_exact_row_shape(response: object) -> None:
    assert parse_hosted_matrix(response, 2) == ("provider_invalid_response",) * 2


def test_matrix_classifier_preserves_valid_zero_and_pair_null_and_rejects_boolean_nonfinite_negative() -> (
    None
):
    assert parse_hosted_matrix(
        {
            "distances": [[0, None, None, True, -1, float("inf")]],
            "durations": [[0, None, 1, 1, 1, 1]],
        },
        6,
    ) == (
        (0.0, 0.0),
        "no_route",
        "provider_invalid_response",
        "provider_invalid_response",
        "provider_invalid_response",
        "provider_invalid_response",
    )


def test_local_config_roundtrips_and_geonames_does_not_consume_hosted_allowance() -> None:
    snapshot = TravelSnapshot(
        configuration=TravelConfiguration(
            provider="osrm",
            geocoding_provider="geonames",
            origin_label="Home",
            maximum_geocodes_per_batch=0,
        ),
        osrm=OsrmConfiguration(
            dataset_path=Path("/tmp/fixture.osrm"), dataset_identifier="roads-v1"
        ),
        geonames=GeonamesConfiguration(
            database_path=Path("/tmp/geonames.sqlite3"), dataset_identifier="places-v1"
        ),
    )
    assert TravelSnapshot.model_validate_json(snapshot.model_dump_json()) == snapshot
    driver = _Driver()
    result = driver.run(start_travel(snapshot, (_destination(None),)))
    assert all(
        action.backend == "geonames" for action in driver.actions if isinstance(action, Geocode)
    )
    assert all(action.backend == "osrm" for action in driver.actions if isinstance(action, Matrix))
    assert driver.states[-1].geocodes_used == 0
    assert (
        result.estimates[0].provider == "osrm"
        and result.estimates[0].dataset_identifier == "roads-v1"
    )
    assert geocode_cache_key("Town", None, "geonames", snapshot.geonames) == _hash(
        (
            "geocode-geonames-v1",
            "town",
            None,
            snapshot.geonames.model_dump(mode="json") if snapshot.geonames else None,
        )
    )


def test_irrelevant_osrm_configuration_does_not_change_hosted_selection_or_route_key() -> None:
    snapshot = _snapshot()
    extra = snapshot.model_copy(
        update={
            "osrm": OsrmConfiguration(
                dataset_path=Path("/tmp/fixture.osrm"), dataset_identifier="unused"
            )
        }
    )
    assert route_cache_key(extra, ORIGIN, DESTINATION) == route_cache_key(
        snapshot, ORIGIN, DESTINATION
    )
    driver = _Driver()
    result = driver.run(start_travel(extra, (_destination(),)))
    assert result.estimates[0].dataset_identifier is None
    assert all(
        action.backend == "openrouteservice"
        for action in driver.actions
        if isinstance(action, Matrix)
    )


def test_observations_require_exactly_one_answer() -> None:
    with pytest.raises(ValidationError, match="coordinate or a reason"):
        GeocodeObserved(action_id=1)
    with pytest.raises(ValidationError, match="metrics or a failure"):
        MatrixObserved(action_id=1, metrics=(), reason="provider_cooldown")
