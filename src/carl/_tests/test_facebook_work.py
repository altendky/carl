from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from carl.core.facebook_search import (
    OverlappingPricePartitionSearchTraversalStrategy,
    SearchTraversalPolicy,
)
from carl.core.facebook_work import (
    COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    COLLECT_SEARCH_WORK_KIND,
    SEARCH_PAGINATION_NETWORK_ACTIVITY_KIND,
    SEARCH_ROUTE_DEFINITION_NETWORK_ACTIVITY_KIND,
    CollectSearchPayload,
    FacebookSearchRequest,
    SearchDistanceUnit,
    SearchFacebookLocation,
    SearchPriceRange,
    SearchRadius,
    SearchTextLocation,
    collect_search_work,
    facebook_item_network_constraints,
    facebook_network_policy_constraints,
    facebook_search_network_activity,
    facebook_search_network_constraints,
    facebook_search_work_constraint,
    is_transient_search_failure,
    legacy_facebook_network_constraint_identifiers,
)
from carl.core.json import encode_json
from carl.core.work import (
    ConcurrencyConstraint,
    SchedulingScopeKind,
    SchedulingSubjectKind,
    SlidingWindowRateConstraint,
    UniformHoldoffConstraint,
    WorkCapability,
    WorkRequester,
)
from carl.io.sqlite import Database


def _payload() -> CollectSearchPayload:
    return CollectSearchPayload(
        request=FacebookSearchRequest(
            query="telescope",
            location=SearchFacebookLocation(
                identifier="123",
                label="Hagerstown, Maryland",
            ),
            radius=SearchRadius(value=60, unit=SearchDistanceUnit.MILES),
            price=SearchPriceRange(currency="USD", maximum=Decimal("600")),
        ),
        traversal=SearchTraversalPolicy(maximum_pages=3),
        routing=("bright_data", "account", "residential"),
    )


def test_search_request_round_trips_as_typed_json() -> None:
    payload = _payload()

    serialized = payload.as_json()

    assert serialized == {
        "request": {
            "query": "telescope",
            "location": {
                "kind": "facebook_location",
                "identifier": "123",
                "label": "Hagerstown, Maryland",
            },
            "radius": {"value": 60, "unit": "miles"},
            "price": {"currency": "USD", "minimum": None, "maximum": "600"},
            "exact_match": False,
        },
        "traversal": {
            "maximum_pages": 3,
            "maximum_results": None,
            "maximum_elapsed_duration_ns": None,
            "maximum_transferred_bytes": None,
            "maximum_decoded_body_bytes": None,
            "maximum_consecutive_pages_without_new_listings": None,
            "requested_page_size": None,
        },
        "traversal_strategy": {"kind": "cursor"},
        "routing": ["bright_data", "account", "residential"],
        "retry_attempt_offset": 0,
    }
    assert CollectSearchPayload.model_validate_json(payload.model_dump_json()) == payload


def test_legacy_unhandled_protocol_error_is_retryable_transport_failure() -> None:
    assert is_transient_search_failure(
        {
            "kind": "unhandled_handler_error",
            "stage": "handler",
            "type": "ProtocolError",
        }
    )
    assert not is_transient_search_failure(
        {"kind": "unhandled_handler_error", "stage": "handler", "type": "ValueError"}
    )


def test_price_partition_traversal_requires_a_finite_upper_bound() -> None:
    base = _payload()
    request = base.request.model_copy(
        update={"price": SearchPriceRange(currency="USD", minimum=Decimal("10"))}
    )

    with pytest.raises(ValidationError, match="requires a maximum price"):
        _ = CollectSearchPayload(
            request=request,
            traversal=base.traversal,
            traversal_strategy=OverlappingPricePartitionSearchTraversalStrategy(
                width=Decimal("100"),
                overlap=Decimal("5"),
            ),
            routing=base.routing,
        )


def test_search_work_retains_intent_and_network_scopes() -> None:
    payload = _payload()

    work = collect_search_work(
        identifier="search-work",
        payload=payload,
        not_before_utc_ns=123,
    )

    assert work.kind == COLLECT_SEARCH_WORK_KIND
    assert work.payload_schema_version == COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION
    assert CollectSearchPayload.model_validate_json(encode_json(work.payload)) == payload
    assert work.not_before_utc_ns == 123
    assert {(scope.kind, scope.identity) for scope in work.scopes} == {
        (SchedulingScopeKind.OVERALL, ()),
        (SchedulingScopeKind.NETWORK_PATH, payload.routing),
        (
            SchedulingScopeKind.NETWORK_PATH,
            ("search_acquisition", *payload.routing),
        ),
        (SchedulingScopeKind.WORK_KIND, COLLECT_SEARCH_WORK_KIND),
    }


def test_search_network_policy_is_typed_and_applies_to_each_followup_request() -> None:
    routing = ("proton", "personal", "route")

    constraints = facebook_search_network_constraints(routing)
    activity = facebook_search_network_activity(
        identifier="activity",
        kind=SEARCH_PAGINATION_NETWORK_ACTIVITY_KIND,
        operation_identifier="operation",
        network_session_identifier="session",
        ordinal=3,
        attempt=1,
        routing=routing,
    )

    rates = tuple(item for item in constraints if isinstance(item, SlidingWindowRateConstraint))
    holdoffs = tuple(item for item in constraints if isinstance(item, UniformHoldoffConstraint))
    assert all(rate.subject_kind is SchedulingSubjectKind.NETWORK_ACTIVITY for rate in rates)
    assert {(rate.scope.kind, rate.scope.identity) for rate in rates} == {
        (SchedulingScopeKind.NETWORK_PATH, routing),
        (SchedulingScopeKind.REMOTE_ORIGIN, ("https", "www.facebook.com", "443")),
    }
    assert {(rate.maximum_starts, rate.period_ns) for rate in rates} == {
        (180, 60_000_000_000),
        (3, 1_000_000_000),
    }
    assert {constraint.scope.identity for constraint in holdoffs} == {
        SEARCH_ROUTE_DEFINITION_NETWORK_ACTIVITY_KIND,
        SEARCH_PAGINATION_NETWORK_ACTIVITY_KIND,
    }
    assert {(holdoff.minimum_ns, holdoff.maximum_ns) for holdoff in holdoffs} == {(0, 100_000_000)}
    work_limits = tuple(item for item in constraints if isinstance(item, ConcurrencyConstraint))
    assert [(limit.maximum_active, limit.scope.identity) for limit in work_limits] == [
        (1, ("search_acquisition", *routing))
    ]
    item_constraints = facebook_item_network_constraints(routing)
    assert any(
        isinstance(constraint, UniformHoldoffConstraint)
        and (constraint.minimum_ns, constraint.maximum_ns) == (0, 100_000_000)
        for constraint in item_constraints
    )
    policy = facebook_network_policy_constraints(routing)
    assert len({constraint.identifier for constraint in policy}) == len(policy)
    assert {constraint.identifier for constraint in policy}.isdisjoint(
        legacy_facebook_network_constraint_identifiers(routing)
    )
    assert {(scope.kind, scope.identity) for scope in activity.scopes} == {
        (SchedulingScopeKind.OVERALL, ()),
        (SchedulingScopeKind.NETWORK_PATH, routing),
        (
            SchedulingScopeKind.NETWORK_ACTIVITY_KIND,
            SEARCH_PAGINATION_NETWORK_ACTIVITY_KIND,
        ),
        (SchedulingScopeKind.REMOTE_ORIGIN, ("https", "www.facebook.com", "443")),
    }


@pytest.mark.anyio
async def test_search_input_survives_durable_queue_round_trip(tmp_path: Path) -> None:
    payload = _payload()
    work = collect_search_work(
        identifier="search-work",
        payload=payload,
        not_before_utc_ns=0,
    )

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.enqueue_work(
            work,
            WorkRequester(
                request_identifier="search-request",
                kind=("carl", "test", "search_request"),
                identifier="search-input",
                context={},
            ),
            event_identifier="search-enqueued",
            enqueued_at_utc_ns=1,
        )
        claimed = await database.claim_work(
            supported_capabilities=(
                WorkCapability(
                    kind=COLLECT_SEARCH_WORK_KIND,
                    payload_schema_version=COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
                ),
            ),
            worker_identifier="test-worker",
            lease_token="test-lease",
            lease_duration_ns=100,
            utc_now_ns=lambda: 1,
            event_identifier="search-claimed",
        )

    assert claimed.lease is not None
    assert CollectSearchPayload.model_validate_json(encode_json(claimed.lease.payload)) == payload


@pytest.mark.anyio
async def test_search_work_is_serialized_per_route(tmp_path: Path) -> None:
    first_payload = _payload().model_copy(update={"routing": ("proton", "personal", "one")})
    second_payload = first_payload.model_copy(
        update={"request": first_payload.request.model_copy(update={"query": "eyepiece"})}
    )
    other_payload = first_payload.model_copy(
        update={
            "request": first_payload.request.model_copy(update={"query": "binoculars"}),
            "routing": ("proton", "personal", "two"),
        }
    )
    payloads = (first_payload, second_payload, other_payload)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        for routing in {payload.routing for payload in payloads}:
            await database.register_constraint(
                facebook_search_work_constraint(routing), registered_at_utc_ns=1
            )
        for index, payload in enumerate(payloads):
            await database.enqueue_work(
                collect_search_work(
                    identifier=f"search-{index}", payload=payload, not_before_utc_ns=0
                ),
                WorkRequester(
                    request_identifier=f"request-{index}",
                    kind=("carl", "test", "search_request"),
                    identifier=f"source-{index}",
                    context={},
                ),
                event_identifier=f"enqueued-{index}",
                enqueued_at_utc_ns=index + 1,
            )
        capability = WorkCapability(
            kind=COLLECT_SEARCH_WORK_KIND,
            payload_schema_version=COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
        )
        first = await database.claim_work(
            supported_capabilities=(capability,),
            worker_identifier="worker-1",
            lease_token="lease-1",
            lease_duration_ns=100,
            utc_now_ns=lambda: 10,
            event_identifier="claimed-1",
        )
        second = await database.claim_work(
            supported_capabilities=(capability,),
            worker_identifier="worker-2",
            lease_token="lease-2",
            lease_duration_ns=100,
            utc_now_ns=lambda: 10,
            event_identifier="claimed-2",
        )

    assert first.lease is not None
    assert first.lease.work_item_identifier == "search-0"
    assert second.lease is not None
    assert second.lease.work_item_identifier == "search-2"


@pytest.mark.anyio
async def test_search_transport_failure_backs_off_every_pending_search_on_route(
    tmp_path: Path,
) -> None:
    route = ("proton", "personal", "one")
    payloads = (
        _payload().model_copy(update={"routing": route}),
        _payload().model_copy(
            update={
                "request": _payload().request.model_copy(update={"query": "eyepiece"}),
                "routing": route,
            }
        ),
        _payload().model_copy(
            update={
                "request": _payload().request.model_copy(update={"query": "binoculars"}),
                "routing": ("proton", "personal", "two"),
            }
        ),
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        for index, payload in enumerate(payloads):
            await database.enqueue_work(
                collect_search_work(
                    identifier=f"search-{index}", payload=payload, not_before_utc_ns=0
                ),
                WorkRequester(
                    request_identifier=f"request-{index}",
                    kind=("carl", "test", "search_request"),
                    identifier=f"source-{index}",
                    context={},
                ),
                event_identifier=f"enqueued-{index}",
                enqueued_at_utc_ns=index + 1,
            )
        deferred = await database.defer_pending_collect_search_work_for_route(
            routing=route,
            eligible_at_utc_ns=101,
            recorded_at_utc_ns=100,
            event_batch_identifier="route-backoff",
            reason={"kind": "test_transport_failure"},
        )
        work = tuple([await database.work(f"search-{index}") for index in range(3)])

    assert deferred == ("search-0", "search-1")
    assert [item["eligible_at_utc_ns"] for item in work] == [101, 101, 3]


def test_location_resolution_changes_search_work_identity() -> None:
    resolved = _payload()
    unresolved = resolved.model_copy(
        update={
            "request": resolved.request.model_copy(
                update={"location": SearchTextLocation(text="Hagerstown, Maryland")}
            )
        }
    )

    resolved_work = collect_search_work(
        identifier="resolved",
        payload=resolved,
        not_before_utc_ns=0,
    )
    unresolved_work = collect_search_work(
        identifier="unresolved",
        payload=unresolved,
        not_before_utc_ns=0,
    )

    assert resolved_work.deduplication_identity != unresolved_work.deduplication_identity


def test_search_work_identity_covers_every_requested_dimension() -> None:
    original = _payload()
    original_identity = collect_search_work(
        identifier="original",
        payload=original,
        not_before_utc_ns=1,
    ).deduplication_identity
    same_request = collect_search_work(
        identifier="another-occurrence",
        payload=original,
        not_before_utc_ns=999,
    )
    changes = (
        original.model_copy(
            update={"request": original.request.model_copy(update={"query": "binoculars"})}
        ),
        original.model_copy(
            update={
                "request": original.request.model_copy(
                    update={
                        "radius": SearchRadius(
                            value=97,
                            unit=SearchDistanceUnit.KILOMETERS,
                        )
                    }
                )
            }
        ),
        original.model_copy(
            update={
                "request": original.request.model_copy(
                    update={
                        "price": SearchPriceRange(
                            currency="USD",
                            maximum=Decimal("500"),
                        )
                    }
                )
            }
        ),
        original.model_copy(
            update={"request": original.request.model_copy(update={"exact_match": True})}
        ),
        original.model_copy(update={"routing": ("bright_data", "account", "isp")}),
        original.model_copy(update={"traversal": SearchTraversalPolicy(maximum_pages=4)}),
    )

    assert same_request.deduplication_identity == original_identity
    assert all(
        collect_search_work(
            identifier=f"changed-{index}",
            payload=changed,
            not_before_utc_ns=0,
        ).deduplication_identity
        != original_identity
        for index, changed in enumerate(changes)
    )


def test_applied_filters_cannot_be_mixed_into_requested_search_input() -> None:
    value = _payload().as_json()
    request = value["request"]
    assert isinstance(request, dict)
    request["applied_filters"] = {"filter_radius_km": 97}

    with pytest.raises(ValidationError):
        CollectSearchPayload.model_validate_json(encode_json(value))


def test_search_input_rejects_invalid_ranges_and_identities() -> None:
    with pytest.raises(ValidationError):
        SearchPriceRange(currency="USD")
    with pytest.raises(ValidationError):
        SearchPriceRange(
            currency="USD",
            minimum=Decimal("10"),
            maximum=Decimal("9.99"),
        )
    with pytest.raises(ValidationError):
        SearchRadius(value=0, unit=SearchDistanceUnit.MILES)
    with pytest.raises(ValidationError):
        SearchRadius(value=True, unit=SearchDistanceUnit.MILES)
    with pytest.raises(ValidationError):
        SearchPriceRange(currency="USD", maximum=Decimal("NaN"))
    with pytest.raises(ValidationError):
        SearchFacebookLocation(identifier="not-numeric")
    with pytest.raises(ValidationError):
        CollectSearchPayload(
            request=_payload().request,
            traversal=_payload().traversal,
            routing=(),
        )


def test_zero_and_one_sided_price_bounds_are_valid() -> None:
    assert SearchPriceRange(currency="USD", minimum=Decimal("0")).minimum == 0
    assert SearchPriceRange(currency="USD", maximum=Decimal("0")).maximum == 0


def test_search_price_range_accepts_advertised_json_decimal_scalars() -> None:
    price = SearchPriceRange.model_validate_json('{"currency":"USD","minimum":"0","maximum":150}')

    assert price.minimum == Decimal("0")
    assert price.maximum == Decimal("150")


@pytest.mark.parametrize("query", ["", " telescope", "telescope "])
def test_search_query_must_be_nonempty_without_surrounding_whitespace(query: str) -> None:
    with pytest.raises(ValidationError):
        FacebookSearchRequest(
            query=query,
            location=SearchTextLocation(text="Hagerstown, Maryland"),
            radius=SearchRadius(value=60, unit=SearchDistanceUnit.MILES),
        )
