"""Pure, replayable travel planning between cache, geocoding and matrix I/O."""

import hashlib
import json
import math
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import Field, model_validator

from carl.core.configuration import GeonamesConfiguration, OsrmConfiguration, TravelConfiguration
from carl.core.models import JsonValue, StrictModel
from carl.core.travel import TravelCoordinate, TravelDestination, TravelEstimate

_VERSION = "ors-driving-car-v1"
type Metric = tuple[float, float] | str
type TravelMode = Literal["origin_identity", "estimate", "refresh"]


class TravelSnapshot(StrictModel):
    configuration: TravelConfiguration | None = None
    osrm: OsrmConfiguration | None = None
    geonames: GeonamesConfiguration | None = None
    unavailable_reason: str | None = None


class RouteGroup(StrictModel):
    key: str
    coordinate: TravelCoordinate
    indices: tuple[int, ...]


class ResolvedDestination(StrictModel):
    index: int = Field(ge=0)
    destination: TravelDestination


class CacheRead(StrictModel):
    kind: Literal["cache_read"] = "cache_read"
    action_id: int = Field(ge=1)
    key: str
    purpose: Literal["origin_geocode", "destination_geocode", "route"]
    index: int | None = None


class Geocode(StrictModel):
    kind: Literal["geocode"] = "geocode"
    action_id: int = Field(ge=1)
    role: Literal["origin", "destination"]
    index: int | None = None
    backend: Literal["openrouteservice", "geonames"]
    label: str
    country: str | None
    cache_key: str


class Matrix(StrictModel):
    kind: Literal["matrix"] = "matrix"
    action_id: int = Field(ge=1)
    backend: Literal["openrouteservice", "osrm"]
    origin: TravelCoordinate
    groups: tuple[RouteGroup, ...] = Field(min_length=1, max_length=50)


class Complete(StrictModel):
    kind: Literal["complete"] = "complete"
    origin_identity: str | None
    estimates: tuple[TravelEstimate, ...]


type TravelAction = Annotated[CacheRead | Geocode | Matrix | Complete, Field(discriminator="kind")]


class CacheObserved(StrictModel):
    kind: Literal["cache_observed"] = "cache_observed"
    action_id: int = Field(ge=1)
    key: str
    value: JsonValue = None


class GeocodeResult(StrictModel):
    coordinate: TravelCoordinate | None = None
    reason: str | None = None

    @model_validator(mode="after")
    def exactly_one_result(self) -> "GeocodeResult":
        if (self.coordinate is None) == (self.reason is None):
            raise ValueError("Geocoding requires a coordinate or a reason")
        return self


class GeocodeObserved(GeocodeResult):
    kind: Literal["geocode_observed"] = "geocode_observed"
    action_id: int = Field(ge=1)


class MatrixObserved(StrictModel):
    kind: Literal["matrix_observed"] = "matrix_observed"
    action_id: int = Field(ge=1)
    metrics: tuple[Metric, ...] | None = None
    reason: str | None = None

    @model_validator(mode="after")
    def exactly_one_result(self) -> "MatrixObserved":
        if (self.metrics is None) == (self.reason is None):
            raise ValueError("A matrix requires metrics or a failure reason")
        return self


type TravelEvent = Annotated[
    CacheObserved | GeocodeObserved | MatrixObserved, Field(discriminator="kind")
]


class CacheWrite(StrictModel):
    key: str
    value: JsonValue
    expires_at_utc_ns: int


class TravelState(StrictModel):
    schema_version: Literal[1] = 1
    snapshot: TravelSnapshot
    mode: TravelMode
    destinations: tuple[TravelDestination, ...]
    expected_origin_identity: str | None = None
    origin: TravelCoordinate | None = None
    origin_identity: str | None = None
    phase: Literal["origin", "destinations", "route", "matrix", "complete"] = "origin"
    destination_cursor: int = Field(default=0, ge=0)
    geocodes_used: int = Field(default=0, ge=0)
    resolved: tuple[ResolvedDestination, ...] = ()
    results: tuple[TravelEstimate | None, ...]
    pending_routes: tuple[RouteGroup, ...] = ()
    matrix_cursor: int = Field(default=0, ge=0)
    action_sequence: int = Field(default=0, ge=0)
    awaiting: TravelAction | None = None


class TravelStep(StrictModel):
    state: TravelState
    action: TravelAction
    cache_writes: tuple[CacheWrite, ...] = ()


def _identity(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def geocode_cache_key(
    label: str, country: str | None, provider: str, geonames: GeonamesConfiguration | None
) -> str:
    normalized = " ".join(label.split()).casefold()
    if provider != "geonames":
        return _identity(("geocode-v1", normalized, country))
    return _identity(
        (
            "geocode-geonames-v1",
            normalized,
            country,
            geonames.model_dump(mode="json") if geonames else None,
        )
    )


def resolved_origin_identity(snapshot: TravelSnapshot, origin: TravelCoordinate) -> str:
    configuration = snapshot.configuration
    assert configuration is not None
    return _identity(
        (
            _VERSION,
            configuration.origin_label,
            configuration.country_code,
            origin.model_dump(),
            configuration.cache_ttl_seconds,
            configuration.provider,
            configuration.geocoding_provider,
            snapshot.osrm.model_dump(mode="json")
            if configuration.provider == "osrm" and snapshot.osrm is not None
            else None,
            snapshot.geonames.model_dump(mode="json")
            if configuration.geocoding_provider == "geonames" and snapshot.geonames is not None
            else None,
        )
    )


def route_cache_key(
    snapshot: TravelSnapshot, origin: TravelCoordinate, destination: TravelCoordinate
) -> str:
    configuration = snapshot.configuration
    assert configuration is not None
    return _identity(
        (
            _VERSION,
            origin.model_dump(),
            destination.model_dump(),
            configuration.cache_ttl_seconds,
            configuration.provider,
            snapshot.osrm.model_dump(mode="json")
            if configuration.provider == "osrm" and snapshot.osrm is not None
            else None,
        )
    )


def parse_hosted_geocode(response: object) -> GeocodeResult:
    candidates: list[tuple[float, TravelCoordinate]] = []
    features = response.get("features", []) if isinstance(response, dict) else []
    if isinstance(features, list):
        for feature in features:
            try:
                coordinates = feature["geometry"]["coordinates"]
                confidence = feature["properties"]["confidence"]
                if (
                    isinstance(confidence, bool)
                    or not isinstance(confidence, (int, float))
                    or not math.isfinite(confidence)
                ):
                    continue
                candidates.append(
                    (
                        float(confidence),
                        TravelCoordinate(latitude=coordinates[1], longitude=coordinates[0]),
                    )
                )
            except (KeyError, IndexError, TypeError, ValueError, OverflowError):
                continue
    candidates.sort(key=lambda candidate: candidate[0], reverse=True)
    unique: dict[str, tuple[float, TravelCoordinate]] = {}
    for candidate in candidates:
        unique.setdefault(candidate[1].model_dump_json(), candidate)
    candidates = list(unique.values())
    if not candidates:
        return GeocodeResult(reason="location_not_found")
    if candidates[0][0] < 0.8 or (
        len(candidates) > 1 and candidates[0][0] - candidates[1][0] < 0.05
    ):
        return GeocodeResult(reason="ambiguous_location")
    return GeocodeResult(coordinate=candidates[0][1])


def parse_hosted_matrix(response: object, expected_count: int) -> tuple[Metric, ...]:
    if not isinstance(response, dict):
        return ("provider_invalid_response",) * expected_count
    rows = []
    for matrix in (response.get("distances"), response.get("durations")):
        if (
            not isinstance(matrix, list)
            or len(matrix) != 1
            or not isinstance(matrix[0], list)
            or len(matrix[0]) != expected_count
        ):
            return ("provider_invalid_response",) * expected_count
        rows.append(matrix[0])
    result: list[Metric] = []
    for distance, duration in zip(*rows, strict=True):
        try:
            valid = all(
                not isinstance(value, bool)
                and isinstance(value, (int, float))
                and math.isfinite(value)
                and value >= 0
                for value in (distance, duration)
            )
        except OverflowError:
            valid = False
        if valid:
            result.append((float(distance), float(duration)))
        else:
            result.append(
                "no_route" if distance is None and duration is None else "provider_invalid_response"
            )
    return tuple(result)


def start_travel(
    snapshot: TravelSnapshot,
    destinations: Sequence[TravelDestination],
    *,
    mode: TravelMode = "estimate",
    expected_origin_identity: str | None = None,
) -> TravelState:
    return TravelState(
        snapshot=snapshot,
        mode=mode,
        destinations=tuple(destinations),
        expected_origin_identity=expected_origin_identity,
        results=(None,) * len(destinations),
    )


class _Advance:
    def __init__(self, state: TravelState, now: int):
        self.state = state
        self.now = now
        self.writes: list[CacheWrite] = []

    def update(self, **values: object) -> None:
        self.state = self.state.model_copy(update=values)

    def unavailable(self, destination: TravelDestination, reason: str) -> TravelEstimate:
        configuration = self.state.snapshot.configuration
        return TravelEstimate(
            status="unavailable",
            provider=configuration.provider if configuration else "openrouteservice",
            dataset_identifier=self.state.snapshot.osrm.dataset_identifier
            if configuration is not None
            and configuration.provider == "osrm"
            and self.state.snapshot.osrm is not None
            else None,
            destination_label=destination.label,
            location_precision=destination.precision,
            unavailable_reason=reason,
        )

    def result(self, index: int, value: TravelEstimate) -> None:
        results = list(self.state.results)
        results[index] = value
        self.update(results=tuple(results))

    def next_destination(self) -> None:
        self.update(destination_cursor=self.state.destination_cursor + 1, phase="destinations")

    def fail_origin(self, reason: str) -> None:
        snapshot = self.state.snapshot
        configuration = snapshot.configuration
        if self.state.mode == "origin_identity":
            backend = (
                None
                if configuration is None
                else (
                    configuration.model_dump(mode="json"),
                    snapshot.osrm.model_dump(mode="json")
                    if configuration.provider == "osrm" and snapshot.osrm is not None
                    else None,
                    snapshot.geonames.model_dump(mode="json")
                    if configuration.geocoding_provider == "geonames"
                    and snapshot.geonames is not None
                    else None,
                )
            )
            identity = (
                reason
                if reason in ("routing_not_configured", "routing_configuration_unavailable")
                else _identity((_VERSION, backend, "origin_unavailable", reason))
            )
            self.update(origin_identity=identity)
        else:
            self.update(
                results=tuple(
                    self.unavailable(destination, reason) for destination in self.state.destinations
                )
            )
        self.update(phase="complete")

    def origin_ready(self, coordinate: TravelCoordinate) -> None:
        identity = resolved_origin_identity(self.state.snapshot, coordinate)
        self.update(origin=coordinate, origin_identity=identity)
        if (
            self.state.expected_origin_identity is not None
            and identity != self.state.expected_origin_identity
        ):
            self.fail_origin("origin_changed")
        else:
            self.update(
                phase="complete" if self.state.mode == "origin_identity" else "destinations"
            )

    def destination_ready(self, coordinate: TravelCoordinate, *, geocoded: bool) -> None:
        index = self.state.destination_cursor
        destination = self.state.destinations[index]
        if geocoded:
            destination = destination.model_copy(
                update={"coordinate": coordinate, "precision": "geocoded_locality"}
            )
        self.update(
            resolved=(
                *self.state.resolved,
                ResolvedDestination(index=index, destination=destination),
            ),
            phase="route",
        )

    def geocode_result(self, role: str, result: GeocodeResult) -> None:
        if result.coordinate is not None:
            if role == "origin":
                self.origin_ready(result.coordinate)
            else:
                self.destination_ready(result.coordinate, geocoded=True)
        elif role == "origin":
            self.fail_origin(result.reason or "location_not_found")
        else:
            index = self.state.destination_cursor
            self.result(
                index,
                self.unavailable(
                    self.state.destinations[index], result.reason or "location_not_found"
                ),
            )
            self.next_destination()

    def write(self, key: str, value: JsonValue, seconds: int) -> None:
        self.writes.append(
            CacheWrite(key=key, value=value, expires_at_utc_ns=self.now + seconds * 1_000_000_000)
        )

    def action(self, action: CacheRead | Geocode | Matrix) -> TravelStep:
        self.update(action_sequence=action.action_id, awaiting=action)
        return TravelStep(state=self.state, action=action, cache_writes=tuple(self.writes))

    def geocode(self, role: Literal["origin", "destination"], key: str) -> TravelStep | None:
        configuration = self.state.snapshot.configuration
        assert configuration is not None
        backend = configuration.geocoding_provider
        if backend == "disabled":
            self.geocode_result(role, GeocodeResult(reason="geocoding_disabled"))
            return None
        if backend == "geonames" and self.state.snapshot.geonames is None:
            self.geocode_result(role, GeocodeResult(reason="geonames_not_ready"))
            return None
        if role == "destination" and backend == "openrouteservice":
            if self.state.geocodes_used >= configuration.maximum_geocodes_per_batch:
                self.geocode_result(role, GeocodeResult(reason="geocode_budget_exhausted"))
                return None
            self.update(geocodes_used=self.state.geocodes_used + 1)
        index = self.state.destination_cursor if role == "destination" else None
        label = (
            self.state.destinations[index].label
            if index is not None
            else configuration.origin_label
        )
        assert label is not None
        return self.action(
            Geocode(
                action_id=self.state.action_sequence + 1,
                role=role,
                index=index,
                backend=backend,
                label=label,
                country=configuration.country_code,
                cache_key=key,
            )
        )

    def enqueue_route(self, key: str) -> None:
        resolved = self.state.resolved[-1]
        coordinate = resolved.destination.coordinate
        assert coordinate is not None
        groups = list(self.state.pending_routes)
        for index, group in enumerate(groups):
            if group.key == key:
                groups[index] = group.model_copy(
                    update={"indices": (*group.indices, resolved.index)}
                )
                break
        else:
            groups.append(RouteGroup(key=key, coordinate=coordinate, indices=(resolved.index,)))
        self.update(pending_routes=tuple(groups))
        self.next_destination()

    def consume(self, event: TravelEvent) -> TravelStep | None:
        action = self.state.awaiting
        if (
            not isinstance(action, (CacheRead, Geocode, Matrix))
            or event.action_id != action.action_id
        ):
            raise ValueError("Travel event does not match its pending action")
        if isinstance(action, CacheRead):
            if not isinstance(event, CacheObserved) or event.key != action.key:
                raise ValueError("Travel cache event does not match its action")
            self.update(awaiting=None)
            if action.purpose != "route":
                role = "origin" if action.purpose == "origin_geocode" else "destination"
                value = event.value
                if isinstance(value, dict):
                    if isinstance(value.get("reason"), str):
                        self.geocode_result(role, GeocodeResult(reason=value["reason"]))
                        return None
                    try:
                        coordinate = TravelCoordinate.model_validate(value)
                    except ValueError:
                        pass
                    else:
                        self.geocode_result(role, GeocodeResult(coordinate=coordinate))
                        return None
                return self.geocode(role, action.key)
            destination = self.state.resolved[-1].destination
            if isinstance(event.value, dict):
                try:
                    cached = TravelEstimate.model_validate(
                        {
                            **event.value,
                            "destination_label": destination.label,
                            "location_precision": destination.precision,
                        }
                    )
                    configuration = self.state.snapshot.configuration
                    assert configuration is not None
                    if cached.provider != configuration.provider:
                        raise ValueError("Cached route provider does not match")
                except ValueError:
                    pass
                else:
                    self.result(self.state.destination_cursor, cached)
                    self.next_destination()
                    return None
            self.enqueue_route(action.key)
        elif isinstance(action, Geocode):
            if not isinstance(event, GeocodeObserved):
                raise ValueError("Travel geocode event does not match its action")
            self.update(awaiting=None)
            if event.coordinate is not None:
                self.write(action.cache_key, event.coordinate.model_dump(mode="json"), 30 * 86400)
            elif action.backend == "openrouteservice" and event.reason in (
                "location_not_found",
                "ambiguous_location",
            ):
                self.write(action.cache_key, {"reason": event.reason}, 86400)
            self.geocode_result(action.role, event)
        else:
            if not isinstance(event, MatrixObserved):
                raise ValueError("Travel matrix event does not match its action")
            self.update(awaiting=None)
            configuration = self.state.snapshot.configuration
            assert configuration is not None
            metrics = event.metrics
            if metrics is None or len(metrics) != len(action.groups):
                metrics = (event.reason or "provider_invalid_response",) * len(action.groups)
            resolved = {item.index: item.destination for item in self.state.resolved}
            for group, metric in zip(action.groups, metrics, strict=True):
                valid = isinstance(metric, tuple) and all(
                    not isinstance(value, bool)
                    and isinstance(value, (int, float))
                    and math.isfinite(value)
                    and value >= 0
                    for value in metric
                )
                seconds = configuration.cache_ttl_seconds if valid else 60
                value = TravelEstimate(
                    status="ready" if valid else "unavailable",
                    provider=configuration.provider,
                    dataset_identifier=self.state.snapshot.osrm.dataset_identifier
                    if configuration.provider == "osrm" and self.state.snapshot.osrm is not None
                    else None,
                    distance_meters=metric[0] if valid and isinstance(metric, tuple) else None,
                    duration_seconds=metric[1] if valid and isinstance(metric, tuple) else None,
                    estimated_at_utc=datetime.fromtimestamp(
                        self.now / 1_000_000_000, UTC
                    ).isoformat(),
                    expires_at_utc=datetime.fromtimestamp(
                        self.now / 1_000_000_000 + seconds, UTC
                    ).isoformat(),
                    unavailable_reason=None
                    if valid
                    else metric
                    if isinstance(metric, str)
                    else "provider_invalid_response",
                )
                self.write(group.key, value.model_dump(mode="json"), seconds)
                for index in group.indices:
                    destination = resolved[index]
                    self.result(
                        index,
                        value.model_copy(
                            update={
                                "destination_label": destination.label,
                                "location_precision": destination.precision,
                            }
                        ),
                    )
            self.update(matrix_cursor=self.state.matrix_cursor + len(action.groups))
        return None

    def run(self) -> TravelStep:
        while True:
            state = self.state
            configuration = state.snapshot.configuration
            sequence = state.action_sequence + 1
            if state.phase == "complete":
                complete = Complete(
                    origin_identity=state.origin_identity,
                    estimates=tuple(
                        value
                        if value is not None
                        else self.unavailable(state.destinations[index], "location_unavailable")
                        for index, value in enumerate(state.results)
                    ),
                )
                self.update(awaiting=complete)
                return TravelStep(
                    state=self.state, action=complete, cache_writes=tuple(self.writes)
                )
            if state.phase == "origin":
                if state.mode != "origin_identity" and not state.destinations:
                    self.update(phase="complete")
                elif state.snapshot.unavailable_reason is not None or configuration is None:
                    self.fail_origin(state.snapshot.unavailable_reason or "routing_not_configured")
                elif configuration.origin is not None:
                    self.origin_ready(configuration.origin)
                elif configuration.origin_label is None:
                    self.fail_origin("origin_not_configured")
                else:
                    key = geocode_cache_key(
                        configuration.origin_label,
                        configuration.country_code,
                        configuration.geocoding_provider,
                        state.snapshot.geonames,
                    )
                    return self.action(
                        CacheRead(action_id=sequence, key=key, purpose="origin_geocode")
                    )
            elif state.phase == "destinations":
                assert configuration is not None
                if state.destination_cursor >= len(state.destinations):
                    self.update(phase="matrix")
                    continue
                destination = state.destinations[state.destination_cursor]
                if destination.unavailable_reason == "conflicting_coordinates":
                    self.result(
                        state.destination_cursor,
                        self.unavailable(destination, "conflicting_coordinates"),
                    )
                    self.next_destination()
                elif destination.coordinate is not None:
                    self.destination_ready(destination.coordinate, geocoded=False)
                elif not destination.label:
                    self.result(
                        state.destination_cursor,
                        self.unavailable(
                            destination, destination.unavailable_reason or "location_unavailable"
                        ),
                    )
                    self.next_destination()
                else:
                    key = geocode_cache_key(
                        destination.label,
                        configuration.country_code,
                        configuration.geocoding_provider,
                        state.snapshot.geonames,
                    )
                    return self.action(
                        CacheRead(
                            action_id=sequence,
                            key=key,
                            purpose="destination_geocode",
                            index=state.destination_cursor,
                        )
                    )
            elif state.phase == "route":
                assert state.origin is not None
                coordinate = state.resolved[-1].destination.coordinate
                assert coordinate is not None
                key = route_cache_key(state.snapshot, state.origin, coordinate)
                if state.mode == "refresh":
                    self.enqueue_route(key)
                else:
                    return self.action(
                        CacheRead(
                            action_id=sequence,
                            key=key,
                            purpose="route",
                            index=state.destination_cursor,
                        )
                    )
            elif state.phase == "matrix":
                assert configuration is not None and state.origin is not None
                groups = state.pending_routes[state.matrix_cursor : state.matrix_cursor + 50]
                if not groups:
                    self.update(phase="complete")
                else:
                    return self.action(
                        Matrix(
                            action_id=sequence,
                            backend=configuration.provider,
                            origin=state.origin,
                            groups=groups,
                        )
                    )


def advance_travel(
    state: TravelState, event: TravelEvent | None = None, *, now_utc_ns: int
) -> TravelStep:
    """Consume exactly one matching observation and stop at the next I/O boundary.

    Calling without an event on a restored pending state returns the same action;
    pacing waits therefore neither allocate another ID nor spend another geocode.
    """
    if event is None and state.awaiting is not None:
        return TravelStep(state=state, action=state.awaiting)
    engine = _Advance(state, now_utc_ns)
    if event is not None:
        step = engine.consume(event)
        if step is not None:
            return step
    return engine.run()
