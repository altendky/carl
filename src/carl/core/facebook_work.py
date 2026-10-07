"""Pure work payloads and queue definitions for Facebook Marketplace."""

import hashlib
from decimal import Decimal, InvalidOperation
from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator
from pydantic.json_schema import SkipJsonSchema

from carl.core.facebook import FacebookItemResponseKind, listing_id_from_url
from carl.core.facebook_search import (
    CursorSearchTraversalStrategy,
    OverlappingPricePartitionSearchTraversalStrategy,
    SearchTraversalPolicy,
    SearchTraversalStrategy,
)
from carl.core.http import RequestPlan
from carl.core.models import JsonStringEnumeration, JsonValue, StrictModel
from carl.core.network_defaults import (
    DEFAULT_DATACENTER_NETWORK_PATH,
    AcquisitionNetworkPath,
    LegacyProtonRoute,
    populate_legacy_proton_paths,
    validate_legacy_proton_paths,
)
from carl.core.work import (
    ConcurrencyConstraint,
    Constraint,
    NetworkActivityDefinition,
    SchedulingScope,
    SchedulingScopeKind,
    SchedulingSubjectKind,
    SlidingWindowRateConstraint,
    UniformHoldoffConstraint,
    WorkDefinition,
)

COLLECT_ITEM_WORK_KIND = ("carl", "facebook", "work", "collect_item")
EXTRACT_ITEM_WORK_KIND = ("carl", "facebook", "work", "extract_item")
COLLECT_SEARCH_WORK_KIND = ("carl", "facebook", "work", "collect_search")
ITEM_PAGE_NETWORK_ACTIVITY_KIND = (
    "carl",
    "facebook",
    "network_activity",
    "item_page",
)
SEARCH_SESSION_BOOTSTRAP_NETWORK_ACTIVITY_KIND = (
    "carl",
    "facebook",
    "network_activity",
    "search_session_bootstrap",
)
SEARCH_ROUTE_DEFINITION_NETWORK_ACTIVITY_KIND = (
    "carl",
    "facebook",
    "network_activity",
    "search_route_definition",
)
SEARCH_PAGINATION_NETWORK_ACTIVITY_KIND = (
    "carl",
    "facebook",
    "network_activity",
    "search_pagination",
)
COLLECT_ITEM_PAYLOAD_SCHEMA_VERSION = 1
EXTRACT_ITEM_PAYLOAD_SCHEMA_VERSION = 1
LEGACY_COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION = 1
PREVIOUS_COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION = 2
COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION = 3

SEARCH_TRANSPORT_MAXIMUM_ATTEMPTS = 6
RETRYABLE_SEARCH_SESSION_FAILURE_CODES = frozenset(
    {
        "proton_egress_probe_failed",
        "wireproxy_cleanup_failed",
        "wireproxy_device_identity_busy",
        "wireproxy_exited_during_startup",
        "wireproxy_port_allocation_busy",
        "wireproxy_start_failed",
        "wireproxy_startup_timeout",
    }
)
RETRYABLE_UNHANDLED_SEARCH_TRANSPORT_ERROR_TYPES = frozenset(
    {
        "ConnectError",
        "ConnectTimeout",
        "PoolTimeout",
        "ProtocolError",
        "ReadError",
        "ReadTimeout",
        "RemoteProtocolError",
        "WriteError",
        "WriteTimeout",
    }
)

SEARCH_FOLLOWUP_HOLDOFF_MINIMUM_NS = 0
SEARCH_FOLLOWUP_HOLDOFF_MAXIMUM_NS = 100_000_000
ITEM_PAGE_HOLDOFF_MINIMUM_NS = 0
ITEM_PAGE_HOLDOFF_MAXIMUM_NS = 100_000_000
FACEBOOK_NETWORK_RATE_PERIOD_NS = 60_000_000_000
FACEBOOK_NETWORK_RATE_MAXIMUM_STARTS = 180
FACEBOOK_NETWORK_SHORT_RATE_PERIOD_NS = 1_000_000_000
FACEBOOK_NETWORK_SHORT_RATE_MAXIMUM_STARTS = 3
FACEBOOK_NETWORK_PATH_RATE_PERIOD_NS = 60_000_000_000
FACEBOOK_NETWORK_PATH_RATE_MAXIMUM_STARTS = 180
FACEBOOK_SEARCH_ROUTE_MAXIMUM_ACTIVE = 1
FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY = "effective_search_acquisition"


def _search_route_scope(routing: tuple[str, ...]) -> SchedulingScope:
    """Identify search work on a route without constraining other routed work."""

    return SchedulingScope(
        kind=SchedulingScopeKind.NETWORK_PATH,
        identity=("search_acquisition", *routing),
    )


def facebook_search_work_constraint(routing: tuple[str, ...]) -> ConcurrencyConstraint:
    """Historical requested-route limit, retained only to identify old policy."""

    return ConcurrencyConstraint(
        identifier=("carl", "facebook", "search", "route_concurrency", "v1", *routing),
        subject_kind=SchedulingSubjectKind.WORK_ITEM,
        scope=_search_route_scope(routing),
        maximum_active=FACEBOOK_SEARCH_ROUTE_MAXIMUM_ACTIVE,
    )


def facebook_effective_search_scope(routing: tuple[str, ...]) -> SchedulingScope:
    """Record the actual acquisition route without changing the requested payload."""

    return SchedulingScope(
        kind=SchedulingScopeKind.NETWORK_PATH,
        identity=(FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY, *routing),
    )


def facebook_effective_search_work_constraint(
    routing: tuple[str, ...],
) -> ConcurrencyConstraint | None:
    """Serialize shared Proton transports, not independent Decodo sessions."""

    if routing[0] == "decodo":
        return None

    return ConcurrencyConstraint(
        identifier=("carl", "facebook", "search", "effective_route_concurrency", "v1", *routing),
        subject_kind=SchedulingSubjectKind.WORK_ITEM,
        scope=facebook_effective_search_scope(routing),
        maximum_active=FACEBOOK_SEARCH_ROUTE_MAXIMUM_ACTIVE,
    )


def _facebook_remote_origin_network_constraints() -> tuple[SlidingWindowRateConstraint, ...]:
    scope = SchedulingScope(
        kind=SchedulingScopeKind.REMOTE_ORIGIN,
        identity=("https", "www.facebook.com", "443"),
    )
    return (
        SlidingWindowRateConstraint(
            identifier=("carl", "facebook", "network_activity_rate", "remote_origin", "v2"),
            subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
            scope=scope,
            maximum_starts=FACEBOOK_NETWORK_RATE_MAXIMUM_STARTS,
            period_ns=FACEBOOK_NETWORK_RATE_PERIOD_NS,
        ),
        SlidingWindowRateConstraint(
            identifier=(
                "carl",
                "facebook",
                "network_activity_rate",
                "remote_origin",
                "second",
                "v2",
            ),
            subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
            scope=scope,
            maximum_starts=FACEBOOK_NETWORK_SHORT_RATE_MAXIMUM_STARTS,
            period_ns=FACEBOOK_NETWORK_SHORT_RATE_PERIOD_NS,
        ),
    )


def facebook_network_path_constraint(routing: tuple[str, ...]) -> SlidingWindowRateConstraint:
    """A route-wide budget shared by Marketplace pages and CDN image traffic."""

    return SlidingWindowRateConstraint(
        identifier=("carl", "facebook", "network_path", "network_activity_rate", "v2", *routing),
        subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
        scope=SchedulingScope(kind=SchedulingScopeKind.NETWORK_PATH, identity=routing),
        maximum_starts=FACEBOOK_NETWORK_PATH_RATE_MAXIMUM_STARTS,
        period_ns=FACEBOOK_NETWORK_PATH_RATE_PERIOD_NS,
    )


def facebook_item_network_constraints(routing: tuple[str, ...]) -> tuple[Constraint, ...]:
    return (
        *_facebook_remote_origin_network_constraints(),
        facebook_network_path_constraint(routing),
        UniformHoldoffConstraint(
            identifier=("carl", "facebook", "item", "network_activity_holdoff", "v2"),
            subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
            scope=SchedulingScope(
                kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND,
                identity=ITEM_PAGE_NETWORK_ACTIVITY_KIND,
            ),
            minimum_ns=ITEM_PAGE_HOLDOFF_MINIMUM_NS,
            maximum_ns=ITEM_PAGE_HOLDOFF_MAXIMUM_NS,
        ),
    )


def facebook_item_network_activity(
    *,
    identifier: str,
    operation_identifier: str,
    network_session_identifier: str,
    attempt: int,
    routing: tuple[str, ...],
) -> NetworkActivityDefinition:
    return NetworkActivityDefinition(
        identifier=identifier,
        kind=ITEM_PAGE_NETWORK_ACTIVITY_KIND,
        operation_identifier=operation_identifier,
        network_session_identifier=network_session_identifier,
        ordinal=1,
        attempt=attempt,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(kind=SchedulingScopeKind.NETWORK_PATH, identity=routing),
            SchedulingScope(
                kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND,
                identity=ITEM_PAGE_NETWORK_ACTIVITY_KIND,
            ),
            SchedulingScope(
                kind=SchedulingScopeKind.REMOTE_ORIGIN,
                identity=("https", "www.facebook.com", "443"),
            ),
        ),
    )


def facebook_search_network_constraints(routing: tuple[str, ...]) -> tuple[Constraint, ...]:
    """Return the current request-pacing policy for one network path."""

    constraints: list[Constraint] = [
        *_facebook_remote_origin_network_constraints(),
        facebook_network_path_constraint(routing),
    ]
    for activity_kind in (
        SEARCH_ROUTE_DEFINITION_NETWORK_ACTIVITY_KIND,
        SEARCH_PAGINATION_NETWORK_ACTIVITY_KIND,
    ):
        constraints.append(
            UniformHoldoffConstraint(
                identifier=(
                    "carl",
                    "facebook",
                    "search",
                    "network_activity_holdoff",
                    *activity_kind,
                    "v2",
                ),
                subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
                scope=SchedulingScope(
                    kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND,
                    identity=activity_kind,
                ),
                minimum_ns=SEARCH_FOLLOWUP_HOLDOFF_MINIMUM_NS,
                maximum_ns=SEARCH_FOLLOWUP_HOLDOFF_MAXIMUM_NS,
            )
        )
    return tuple(constraints)


def facebook_network_policy_constraints(routing: tuple[str, ...]) -> tuple[Constraint, ...]:
    """Register one coherent page policy before either search or item traffic."""

    by_identifier = {
        constraint.identifier: constraint
        for constraint in (
            *facebook_search_network_constraints(routing),
            *facebook_item_network_constraints(routing),
        )
    }
    return tuple(by_identifier.values())


def legacy_facebook_network_constraint_identifiers(
    routing: tuple[str, ...],
) -> tuple[tuple[str, ...], ...]:
    """Initial rules that must no longer throttle future Marketplace pages."""

    return (
        ("carl", "facebook", "network_activity_rate", "remote_origin"),
        ("carl", "facebook", "search", "network_activity_rate", *routing),
        ("carl", "facebook", "item", "network_activity_rate", *routing),
        ("carl", "facebook", "item", "network_activity_holdoff"),
        *(
            ("carl", "facebook", "search", "network_activity_holdoff", *kind)
            for kind in (
                SEARCH_ROUTE_DEFINITION_NETWORK_ACTIVITY_KIND,
                SEARCH_PAGINATION_NETWORK_ACTIVITY_KIND,
            )
        ),
    )


def facebook_search_network_activity(
    *,
    identifier: str,
    kind: tuple[str, ...],
    operation_identifier: str,
    network_session_identifier: str,
    ordinal: int,
    attempt: int,
    routing: tuple[str, ...],
) -> NetworkActivityDefinition:
    if kind not in {
        SEARCH_SESSION_BOOTSTRAP_NETWORK_ACTIVITY_KIND,
        SEARCH_ROUTE_DEFINITION_NETWORK_ACTIVITY_KIND,
        SEARCH_PAGINATION_NETWORK_ACTIVITY_KIND,
    }:
        raise ValueError("Unknown Facebook search network activity kind")
    return NetworkActivityDefinition(
        identifier=identifier,
        kind=kind,
        operation_identifier=operation_identifier,
        network_session_identifier=network_session_identifier,
        ordinal=ordinal,
        attempt=attempt,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(kind=SchedulingScopeKind.NETWORK_PATH, identity=routing),
            SchedulingScope(kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND, identity=kind),
            SchedulingScope(
                kind=SchedulingScopeKind.REMOTE_ORIGIN,
                identity=("https", "www.facebook.com", "443"),
            ),
        ),
    )


class SearchLocationKind(JsonStringEnumeration):
    TEXT = "text"
    FACEBOOK_LOCATION = "facebook_location"


class SearchDistanceUnit(JsonStringEnumeration):
    KILOMETERS = "kilometers"
    MILES = "miles"


class SearchTextLocation(StrictModel):
    kind: Literal[SearchLocationKind.TEXT] = SearchLocationKind.TEXT
    text: str = Field(min_length=1)

    @field_validator("text")
    @classmethod
    def validate_text(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("Location text must not have surrounding whitespace")
        return value


class SearchFacebookLocation(StrictModel):
    kind: Literal[SearchLocationKind.FACEBOOK_LOCATION] = SearchLocationKind.FACEBOOK_LOCATION
    identifier: str = Field(pattern=r"^[0-9]+$")
    label: str | None = Field(default=None, min_length=1)

    @field_validator("label")
    @classmethod
    def validate_label(cls, value: str | None) -> str | None:
        if value is not None and value != value.strip():
            raise ValueError("Location label must not have surrounding whitespace")
        return value


type SearchLocation = Annotated[
    SearchTextLocation | SearchFacebookLocation,
    Field(discriminator="kind"),
]


class SearchRadius(StrictModel):
    value: int = Field(gt=0)
    unit: SearchDistanceUnit


class SearchPriceRange(StrictModel):
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    minimum: Decimal | None = Field(default=None, ge=0)
    maximum: Decimal | None = Field(default=None, ge=0)

    @field_validator("minimum", "maximum", mode="before")
    @classmethod
    def parse_json_price(cls, value: object) -> object:
        """Convert the JSON scalar forms advertised by the generated schema."""

        if value is None or isinstance(value, (Decimal, bool)):
            return value
        if isinstance(value, str) and (not value or value != value.strip()):
            raise ValueError("Search price must be a nonempty trimmed decimal")
        if isinstance(value, (str, int, float)):
            try:
                parsed = Decimal(str(value))
            except InvalidOperation as error:
                raise ValueError("Search price must be a decimal value") from error
            if not parsed.is_finite():
                raise ValueError("Search price must be finite")
            return parsed
        return value

    @model_validator(mode="after")
    def validate_bounds(self) -> "SearchPriceRange":
        if self.minimum is None and self.maximum is None:
            raise ValueError("A price range requires at least one bound")
        if self.minimum is not None and self.maximum is not None and self.minimum > self.maximum:
            raise ValueError("Minimum price cannot exceed maximum price")
        return self


class FacebookSearchRequest(StrictModel):
    query: str = Field(min_length=1)
    location: SearchLocation
    radius: SearchRadius
    price: SearchPriceRange | None = None
    exact_match: bool = False

    @field_validator("query")
    @classmethod
    def validate_query(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("Search query must not have surrounding whitespace")
        return value


class CollectSearchPayload(StrictModel):
    request: FacebookSearchRequest
    traversal: SearchTraversalPolicy
    traversal_strategy: SearchTraversalStrategy = Field(
        default_factory=CursorSearchTraversalStrategy
    )
    routing: tuple[str, ...]
    retry_attempt_offset: int = Field(default=0, ge=0)

    @field_validator("routing")
    @classmethod
    def validate_routing(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or any(not part for part in value):
            raise ValueError("Routing identity parts must be nonempty")
        return value

    @model_validator(mode="after")
    def validate_traversal_strategy(self) -> "CollectSearchPayload":
        if isinstance(
            self.traversal_strategy,
            OverlappingPricePartitionSearchTraversalStrategy,
        ) and (self.request.price is None or self.request.price.maximum is None):
            raise ValueError("Overlapping price partition traversal requires a maximum price")
        return self

    def as_json(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json")


class CreateSearchRequest(StrictModel):
    """Queue one new bounded Marketplace search."""

    request: FacebookSearchRequest
    traversal: SearchTraversalPolicy
    traversal_strategy: SearchTraversalStrategy = Field(
        default_factory=CursorSearchTraversalStrategy
    )
    network_path: AcquisitionNetworkPath = Field(
        default=DEFAULT_DATACENTER_NETWORK_PATH,
        description="Explicit acquisition route; defaults directly to Decodo datacenter.",
    )
    proton_route: SkipJsonSchema[LegacyProtonRoute | None] = Field(default=None, exclude=True)

    @model_validator(mode="before")
    @classmethod
    def populate_legacy_route(cls, value: object) -> object:
        return populate_legacy_proton_paths(
            value, legacy_field="proton_route", network_fields=("network_path",)
        )

    @model_validator(mode="after")
    def validate_legacy_route(self) -> "CreateSearchRequest":
        validate_legacy_proton_paths(self.proton_route, self.network_path)
        return self

    @property
    def requested_network_path(self) -> tuple[str, ...]:
        return self.network_path


class CreateSearchResult(StrictModel):
    work_identifier: str
    created: bool
    state: str


def is_transient_search_failure(error: JsonValue) -> bool:
    """Return whether an operator may safely retry a terminal search failure."""

    if not isinstance(error, dict):
        return False
    if error.get("kind") == "search_acquisition_failure":
        return error.get("stopping_condition") == "transport_failure"
    if error.get("kind") == "unhandled_handler_error":
        return error.get("type") in RETRYABLE_UNHANDLED_SEARCH_TRANSPORT_ERROR_TYPES
    return (
        error.get("kind") == "network_session_failure"
        and error.get("code") in RETRYABLE_SEARCH_SESSION_FAILURE_CODES
    )


def retryable_search_failure(error: JsonValue, *, attempt: int) -> bool:
    """Return whether a terminal search failure can resume under the current policy."""

    return attempt < SEARCH_TRANSPORT_MAXIMUM_ATTEMPTS and is_transient_search_failure(error)


class CollectItemPayload(StrictModel):
    listing_id: str = Field(pattern=r"^[0-9]+$")
    request_plan: RequestPlan

    @model_validator(mode="after")
    def validate_listing_url(self) -> "CollectItemPayload":
        if self.request_plan.method != "GET":
            raise ValueError("Facebook item collection requires GET")
        if listing_id_from_url(self.request_plan.url) != self.listing_id:
            raise ValueError("Request URL and listing identifier disagree")
        return self

    def as_json(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json", by_alias=True)


class ExtractItemPayload(StrictModel):
    acquisition_record_identifier: str = Field(min_length=1)

    def as_json(self) -> dict[str, JsonValue]:
        return self.model_dump(mode="json")


class SearchRunListingCandidates(StrictModel):
    search_run_record_identifier: str = Field(min_length=1)
    listing_identifiers: tuple[str, ...]

    @field_validator("listing_identifiers")
    @classmethod
    def validate_listing_identifiers(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not identifier.isdecimal() for identifier in value):
            raise ValueError("Listing identifiers must be decimal strings")
        if len(set(value)) != len(value):
            raise ValueError("A search run's listing identifiers must be unique")
        return value


class SuccessfulItemPageResult(StrictModel):
    listing_id: str = Field(pattern=r"^[0-9]+$")
    acquisition_record_identifier: str = Field(min_length=1)
    observation_record_identifier: str = Field(min_length=1)
    response_classification: FacebookItemResponseKind
    acquisition_completion_sequence: int = Field(ge=1)
    extraction_completion_sequence: int = Field(ge=1)

    @model_validator(mode="after")
    def validate_response_classification(self) -> "SuccessfulItemPageResult":
        if self.response_classification not in {
            FacebookItemResponseKind.FULL_LISTING,
            FacebookItemResponseKind.LISTING_UNAVAILABLE,
        }:
            raise ValueError("A successful item-page result must be semantically usable")
        return self


class ItemPageFollowupDecision(StrictModel):
    listing_id: str = Field(pattern=r"^[0-9]+$")
    search_run_record_identifiers: tuple[str, ...]
    latest_successful_result: SuccessfulItemPageResult | None


class ItemPageFollowupPlan(StrictModel):
    search_run_record_identifiers: tuple[str, ...]
    listing_references: int = Field(ge=0)
    unique_listings: int = Field(ge=0)
    decisions: tuple[ItemPageFollowupDecision, ...]


def plan_item_page_followups(
    search_runs: tuple[SearchRunListingCandidates, ...],
    successful_results: tuple[SuccessfulItemPageResult, ...],
    *,
    maximum_items: int | None = None,
) -> ItemPageFollowupPlan:
    """Select one latest usable item result, or one collection, per exact listing ID."""

    if not search_runs:
        raise ValueError("At least one search run is required")
    if maximum_items is not None and maximum_items < 1:
        raise ValueError("The maximum item count must be positive")

    search_run_identifiers = tuple(
        search_run.search_run_record_identifier for search_run in search_runs
    )
    if len(set(search_run_identifiers)) != len(search_run_identifiers):
        raise ValueError("Search-run record identifiers must be unique")

    requesters_by_listing: dict[str, list[str]] = {}
    listing_references = 0
    for search_run in search_runs:
        for listing_identifier in search_run.listing_identifiers:
            listing_references += 1
            requesters_by_listing.setdefault(listing_identifier, []).append(
                search_run.search_run_record_identifier
            )

    selected_listing_identifiers = tuple(requesters_by_listing)[:maximum_items]
    latest_by_listing: dict[str, SuccessfulItemPageResult] = {}
    for result in successful_results:
        if result.listing_id not in requesters_by_listing:
            raise ValueError("A successful result does not belong to an input listing")
        current = latest_by_listing.get(result.listing_id)
        if current is None or (
            result.acquisition_completion_sequence,
            result.extraction_completion_sequence,
        ) > (
            current.acquisition_completion_sequence,
            current.extraction_completion_sequence,
        ):
            latest_by_listing[result.listing_id] = result

    return ItemPageFollowupPlan(
        search_run_record_identifiers=search_run_identifiers,
        listing_references=listing_references,
        unique_listings=len(requesters_by_listing),
        decisions=tuple(
            ItemPageFollowupDecision(
                listing_id=listing_identifier,
                search_run_record_identifiers=tuple(requesters_by_listing[listing_identifier]),
                latest_successful_result=latest_by_listing.get(listing_identifier),
            )
            for listing_identifier in selected_listing_identifiers
        ),
    )


def _request_plan_digest(request_plan: RequestPlan) -> str:
    encoded = request_plan.model_dump_json(by_alias=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _search_payload_digest(payload: CollectSearchPayload) -> str:
    return hashlib.sha256(payload.model_dump_json().encode("utf-8")).hexdigest()


def collect_search_work(
    *, identifier: str, payload: CollectSearchPayload, not_before_utc_ns: int
) -> WorkDefinition:
    return WorkDefinition(
        identifier=identifier,
        kind=COLLECT_SEARCH_WORK_KIND,
        payload_schema_version=COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
        payload=payload.as_json(),
        deduplication_identity=(
            "facebook_marketplace",
            "search",
            _search_payload_digest(payload),
        ),
        not_before_utc_ns=not_before_utc_ns,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(kind=SchedulingScopeKind.NETWORK_PATH, identity=payload.routing),
            _search_route_scope(payload.routing),
            SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=COLLECT_SEARCH_WORK_KIND),
        ),
    )


def collect_item_work(
    *, identifier: str, payload: CollectItemPayload, not_before_utc_ns: int
) -> WorkDefinition:
    return WorkDefinition(
        identifier=identifier,
        kind=COLLECT_ITEM_WORK_KIND,
        payload_schema_version=COLLECT_ITEM_PAYLOAD_SCHEMA_VERSION,
        payload=payload.as_json(),
        deduplication_identity=(
            "facebook_marketplace",
            "item_page",
            payload.listing_id,
            _request_plan_digest(payload.request_plan),
        ),
        not_before_utc_ns=not_before_utc_ns,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(
                kind=SchedulingScopeKind.NETWORK_PATH,
                identity=payload.request_plan.routing,
            ),
            SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=COLLECT_ITEM_WORK_KIND),
        ),
    )


def extract_item_work(
    *,
    identifier: str,
    payload: ExtractItemPayload,
    extractor_identifier: tuple[str, ...],
    extractor_schema_version: int,
    not_before_utc_ns: int,
) -> WorkDefinition:
    if not extractor_identifier or any(not part for part in extractor_identifier):
        raise ValueError("Extractor identifier parts must be nonempty")
    if extractor_schema_version < 1:
        raise ValueError("Extractor schema versions start at one")
    return WorkDefinition(
        identifier=identifier,
        kind=EXTRACT_ITEM_WORK_KIND,
        payload_schema_version=EXTRACT_ITEM_PAYLOAD_SCHEMA_VERSION,
        payload=payload.as_json(),
        deduplication_identity=(
            "facebook_marketplace",
            "item_extraction",
            payload.acquisition_record_identifier,
            *extractor_identifier,
            str(extractor_schema_version),
        ),
        not_before_utc_ns=not_before_utc_ns,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=EXTRACT_ITEM_WORK_KIND),
        ),
    )
