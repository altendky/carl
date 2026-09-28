"""Command-line interface for the initial Carl vertical slice."""

import sys
from contextlib import suppress
from datetime import UTC, datetime
from decimal import Decimal
from getpass import getpass
from pathlib import Path
from secrets import randbelow
from time import perf_counter_ns, time_ns
from uuid import uuid4

import anyio
import apsw
from cyclopts import App
from pydantic import ConfigDict, TypeAdapter, ValidationError
from rich.console import Console
from rich.live import Live

from carl.app import extract_acquisition
from carl.core.activity import ActivitySnapshot
from carl.core.components import Component, ComponentId
from carl.core.composed_projection import (
    AnalysisPresenceFilter,
    ComposedListingFilters,
    GetComposedListingRequest,
    ListComposedSearchRequest,
    ListingStatus,
)
from carl.core.facebook import listing_id_from_url
from carl.core.facebook_images import (
    COLLECT_IMAGE_WORK_KIND,
    COLLECT_IMAGE_WORK_SCHEMA_VERSION,
    EXTRACT_IMAGE_WORK_KIND,
    EXTRACT_IMAGE_WORK_SCHEMA_VERSION,
    IMAGE_RATE_MAXIMUM_STARTS,
    CollectImagePayload,
    ExtractImagePayload,
    ImageReuseMatchKind,
    RetryImageFailuresRequest,
    SavedImageCandidate,
    collect_image_work,
    extract_image_work,
    gallery_references,
    image_network_constraints,
    image_reuse_record,
    legacy_image_network_constraint_identifiers,
    plan_image_followups,
)
from carl.core.facebook_search import (
    CursorSearchTraversalStrategy,
    OverlappingPricePartitionSearchTraversalStrategy,
    SearchPricePartitionOrder,
    SearchTraversalPolicy,
)
from carl.core.facebook_work import (
    COLLECT_ITEM_PAYLOAD_SCHEMA_VERSION,
    COLLECT_ITEM_WORK_KIND,
    EXTRACT_ITEM_PAYLOAD_SCHEMA_VERSION,
    EXTRACT_ITEM_WORK_KIND,
    CollectItemPayload,
    CreateSearchRequest,
    FacebookSearchRequest,
    SearchDistanceUnit,
    SearchFacebookLocation,
    SearchPriceRange,
    SearchRadius,
    SearchRunListingCandidates,
    collect_item_work,
    facebook_network_policy_constraints,
    legacy_facebook_network_constraint_identifiers,
    plan_item_page_followups,
)
from carl.core.http import RequestPlan
from carl.core.item_analysis import (
    ANALYSIS_RECIPE_VERSION,
    ANALYZE_ITEM_WORK_KIND,
    ANALYZE_ITEM_WORK_SCHEMA_VERSION,
    AnalyzeItemPayload,
    ClaudeEffort,
    ProductGuideDefinition,
    ProductGuideKind,
    analyze_item_work,
    listing_analysis_evidence_set,
    product_guide_definition,
    saved_gallery_for_analysis,
)
from carl.core.json import encode_json
from carl.core.models import Header, JsonValue, NamedOutput, RecordDraft
from carl.core.routing import BatchItemNetworkProvider, ItemNetworkProvider, NetworkProvider
from carl.core.work import WorkCapability, WorkRequester, WorkState
from carl.core.worker import WorkerSettings
from carl.facebook_analysis_workers import (
    PLAN_FACEBOOK_LISTING_ANALYSIS_EVIDENCE,
    REGISTER_PRODUCT_GUIDE,
    AnalysisWorkerDependencies,
    build_analysis_component_registry,
    build_analysis_worker_registry,
)
from carl.facebook_image_workers import (
    EXTRACT_FACEBOOK_GALLERY_REFERENCES,
    PLAN_FACEBOOK_IMAGE_FOLLOWUPS,
    REUSE_FACEBOOK_GALLERY_IMAGE,
    ImageWorkerDependencies,
    build_image_component_registry,
    build_image_worker_registry,
)
from carl.facebook_routed_workers import build_routed_facebook_worker_registry
from carl.facebook_workers import (
    PLAN_FACEBOOK_ITEM_PAGE_FOLLOWUPS,
    FacebookWorkerDependencies,
    build_component_registry,
    build_facebook_worker_registry,
)
from carl.io.activity import activity_dashboard
from carl.io.browser_identity import brave_navigation_headers
from carl.io.claude import ClaudeCli
from carl.io.configuration import (
    ConfigurationFailure,
    decodo_settings,
    import_mullvad_configuration_archive,
    import_proton_configuration,
    load_configuration,
    mullvad_settings,
    proton_settings,
)
from carl.io.configuration import (
    configure_decodo_credential as store_decodo_credential,
)
from carl.io.decodo import DecodoSessionManager, ManagedDecodoHttpAcquirer
from carl.io.facebook_items import (
    DecodoFacebookItemSessionFactory,
    FacebookItemSessionFactory,
    MullvadFacebookItemSessionFactory,
)
from carl.io.httpx import AcquisitionFailure, DirectHttpxAcquirer
from carl.io.image_files import ImageFileStore
from carl.io.image_migration import (
    MIGRATE_EXTERNAL_IMAGE_FILES,
    build_image_migration_component_registry,
    externalize_saved_images,
)
from carl.io.mullvad import ManagedMullvadHttpAcquirer, MullvadWireproxyManager
from carl.io.network_activity import NetworkActivityScheduler
from carl.io.paths import user_directories
from carl.io.processes import database_process_activity
from carl.io.proton import ManagedProtonHttpAcquirer, ProtonWireproxyManager
from carl.io.provenance import collect_code_provenance_async, process_invocation
from carl.io.sqlite import Database
from carl.io.worker import WorkerRuntimeServices, execute_lease
from carl.mcp_server import serve_stdio
from carl.review import ReviewApplication
from carl.work_runtime import managed_worker_pool, work_forever

app = App(name="carl", help="Preserve web evidence and derived information")
_HEADER_PAIRS = TypeAdapter(list[tuple[str, str]], config=ConfigDict(strict=True))
_DEFAULT_PROTON_ROUTE = "carl"


def _identifier() -> str:
    return str(uuid4())


def _saved_image_candidates(
    results: tuple[tuple[str, dict[str, JsonValue]], ...],
) -> tuple[SavedImageCandidate, ...]:
    return tuple(
        SavedImageCandidate.model_validate(
            {
                "image_result_record_identifier": identifier,
                "source_photo_id": value.get("source_photo_id"),
                "original_url": value.get("original_url"),
                "width": value.get("width"),
                "height": value.get("height"),
            }
        )
        for identifier, value in results
    )


def _sample_uniform_holdoff_ns(minimum_ns: int, maximum_ns: int) -> int:
    return minimum_ns + randbelow(maximum_ns - minimum_ns + 1)


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _utc_text(utc_ns: int) -> str:
    return datetime.fromtimestamp(utc_ns / 1_000_000_000, tz=UTC).isoformat()


def _default_database() -> Path:
    return user_directories().database_file


_DEFAULT_DATABASE = _default_database()


@app.command
def get_composed_listing(
    listing_identifier: str,
    *,
    search_run_record_identifier: str | None = None,
    product_guide_record_identifier: str | None = None,
    maximum_ancestry_runs: int = 100,
    maximum_gallery_images: int = 20,
    maximum_analyses: int = 10,
    maximum_observations: int = 100,
    database: Path = _DEFAULT_DATABASE,
) -> None:
    """Compose one listing from retained evidence without network access."""

    async def perform() -> dict[str, JsonValue]:
        async with Database.managed(database) as evidence_database:
            application = ReviewApplication(
                database=evidence_database,
                repository_root=_repository_root(),
            )
            result = await application.get_composed_listing(
                GetComposedListingRequest(
                    listing_identifier=listing_identifier,
                    search_run_record_identifier=search_run_record_identifier,
                    product_guide_record_identifier=product_guide_record_identifier,
                    maximum_ancestry_runs=maximum_ancestry_runs,
                    maximum_gallery_images=maximum_gallery_images,
                    maximum_analyses=maximum_analyses,
                    maximum_observations=maximum_observations,
                )
            )
            return result.model_dump(mode="json")

    _print_json(anyio.run(perform, backend="trio"))


@app.command
def list_composed_search(
    search_run_record_identifier: str,
    *,
    status: tuple[ListingStatus, ...] = (ListingStatus.AVAILABLE,),
    product_guide_record_identifier: str | None = None,
    analysis: AnalysisPresenceFilter = AnalysisPresenceFilter.ANY,
    maximum_ancestry_runs: int = 100,
    maximum_gallery_images_per_listing: int = 0,
    maximum_analyses_per_listing: int = 1,
    maximum_observations_per_listing: int = 100,
    maximum_candidate_listings_examined: int = 2500,
    page_size: int = 25,
    cursor: str | None = None,
    database: Path = _DEFAULT_DATABASE,
) -> None:
    """List an on-demand search projection, available-only by default."""

    async def perform() -> dict[str, JsonValue]:
        async with Database.managed(database) as evidence_database:
            application = ReviewApplication(
                database=evidence_database,
                repository_root=_repository_root(),
            )
            result = await application.list_composed_search(
                ListComposedSearchRequest(
                    search_run_record_identifier=search_run_record_identifier,
                    filters=ComposedListingFilters(
                        statuses=status,
                        product_guide_record_identifier=product_guide_record_identifier,
                        analysis=analysis,
                    ),
                    maximum_ancestry_runs=maximum_ancestry_runs,
                    maximum_gallery_images_per_listing=maximum_gallery_images_per_listing,
                    maximum_analyses_per_listing=maximum_analyses_per_listing,
                    maximum_observations_per_listing=maximum_observations_per_listing,
                    maximum_candidate_listings_examined=maximum_candidate_listings_examined,
                    page_size=page_size,
                    cursor=cursor,
                )
            )
            return result.model_dump(mode="json")

    _print_json(anyio.run(perform, backend="trio"))


@app.command
def retry_image_failures(
    source_identifier: str,
    maximum_items: int = 1000,
    database: Path = _DEFAULT_DATABASE,
) -> None:
    """Requeue terminal image work for one exact search run or refresh."""

    async def perform() -> dict[str, JsonValue]:
        async with Database.managed(database) as evidence_database:
            application = ReviewApplication(
                database=evidence_database,
                repository_root=_repository_root(),
            )
            result = await application.retry_image_failures(
                RetryImageFailuresRequest(
                    source_identifier=source_identifier,
                    maximum_items=maximum_items,
                )
            )
            return result.model_dump(mode="json")

    _print_json(anyio.run(perform, backend="trio"))


async def _ensure_product_guide(
    evidence_database: Database, definition: ProductGuideDefinition
) -> str:
    """Reuse an exact registered guide or retain its identity and text once."""

    started_utc_ns = time_ns()
    started_monotonic_ns = perf_counter_ns()
    provenance = await collect_code_provenance_async(_repository_root())
    ended_utc_ns = time_ns()
    return await evidence_database.ensure_product_guide_registration(
        definition=definition,
        component=build_analysis_component_registry().require(REGISTER_PRODUCT_GUIDE),
        operation_identifier=_identifier(),
        record_identifier=_identifier(),
        text_identifier=_identifier(),
        provenance=provenance,
        invocation=process_invocation(),
        started_at_utc=_utc_text(started_utc_ns),
        ended_at_utc=_utc_text(ended_utc_ns),
        duration_ns=max(0, perf_counter_ns() - started_monotonic_ns),
    )


def _headers(path: Path | None, browser_like: bool) -> tuple[Header, ...]:
    result = list(brave_navigation_headers() if browser_like else ())
    if path is not None:
        pairs = _HEADER_PAIRS.validate_json(path.read_bytes())
        result.extend(
            Header(name=name.encode("latin-1"), value=value.encode("latin-1"))
            for name, value in pairs
        )
    return tuple(result)


def _print_json(value: object) -> None:
    print(encode_json(value, indent=2))


def _print_work_result(result: dict[str, JsonValue]) -> None:
    _print_json(result)
    if result.get("state") != WorkState.COMPLETED.value:
        raise SystemExit(1)


async def _activate_facebook_page_network_policy(
    evidence_database: Database, network_path: tuple[str, ...]
) -> None:
    """Retain a versioned, provenance-linked page policy before network work."""

    replacements = facebook_network_policy_constraints(network_path)
    retired_identifiers = legacy_facebook_network_constraint_identifiers(network_path)
    component = Component(
        ComponentId(("carl", "facebook", "network_policy", "activate")),
        1,
        facebook_network_policy_constraints,
    )
    operation_identifier = _identifier()
    policy_record_identifier = _identifier()
    started_utc_ns = time_ns()
    started_monotonic_ns = perf_counter_ns()
    await evidence_database.begin_operation(
        operation_id=operation_identifier,
        component=component,
        provenance=await collect_code_provenance_async(_repository_root()),
        invocation=process_invocation(),
        configuration={"network_path": list(network_path)},
        started_at_utc=_utc_text(started_utc_ns),
        inputs=(),
    )
    try:
        with anyio.CancelScope(shield=True):
            await evidence_database.supersede_constraints(
                retired_identifiers=retired_identifiers,
                replacements=replacements,
                operation_identifier=operation_identifier,
                at_utc_ns=time_ns(),
                reason="Marketplace page request pace raised for a bounded trial",
            )
            await evidence_database.complete_operation(
                operation_id=operation_identifier,
                records=(
                    RecordDraft(
                        identifier=policy_record_identifier,
                        kind=("carl", "facebook", "network_policy_activation"),
                        schema_version=1,
                        value={
                            "network_path": list(network_path),
                            "replacement_constraints": [
                                constraint.model_dump(mode="json") for constraint in replacements
                            ],
                            "retired_constraint_identifiers": [
                                list(identifier) for identifier in retired_identifiers
                            ],
                            "operation_identifier": operation_identifier,
                        },
                    ),
                ),
                artifacts=(),
                outputs=(
                    NamedOutput(
                        name=("network_policy_activation",),
                        object_identifier=policy_record_identifier,
                    ),
                ),
                result={"state": "active", "network_path": list(network_path)},
                ended_at_utc=_utc_text(time_ns()),
                duration_ns=max(0, perf_counter_ns() - started_monotonic_ns),
            )
    except BaseException as error:
        with anyio.CancelScope(shield=True):
            await evidence_database.fail_operation(
                operation_id=operation_identifier,
                error={"kind": "network_policy_activation_failure", "type": type(error).__name__},
                result={"state": "failed"},
                ended_at_utc=_utc_text(time_ns()),
                duration_ns=max(0, perf_counter_ns() - started_monotonic_ns),
            )
        raise


@app.command
def locations() -> None:
    """Show Carl's per-user configuration, data, cache, state, and runtime locations."""

    directories = user_directories()
    _print_json(
        {
            "configuration_file": str(directories.configuration_file),
            "database_file": str(directories.database_file),
            "image_directory": str(directories.image_directory),
            "data_directory": str(directories.data),
            "cache_directory": str(directories.cache),
            "state_directory": str(directories.state),
            "runtime_directory": str(directories.runtime),
        }
    )


@app.command
def monitor(
    *,
    database: Path = _DEFAULT_DATABASE,
    refresh_interval_seconds: float = 5,
    recent_window_minutes: float = 5,
    maximum_rows: int = 10,
    once: bool = False,
    work: bool = False,
) -> None:
    """Watch durable activity, optionally processing queued work in the same process."""

    if refresh_interval_seconds <= 0 or recent_window_minutes <= 0 or maximum_rows < 1:
        raise ValueError("Monitor intervals and row count must be positive")
    recent_window_ns = int(recent_window_minutes * 60 * 1_000_000_000)
    console = Console()

    async def snapshot(evidence_database: Database) -> ActivitySnapshot:
        activity = await evidence_database.activity_snapshot(
            captured_at_utc_ns=time_ns(),
            recent_window_ns=recent_window_ns,
            maximum_rows=maximum_rows,
        )
        processes = await anyio.to_thread.run_sync(
            database_process_activity,
            evidence_database.path,
            abandon_on_cancel=True,
        )
        return activity.model_copy(update={"database_processes": processes})

    async def perform() -> None:
        async with Database.managed(database) as evidence_database:

            async def display() -> None:
                initial = await snapshot(evidence_database)
                if once:
                    console.print(activity_dashboard(initial, database))
                    return
                with Live(
                    activity_dashboard(initial, database),
                    console=console,
                    auto_refresh=False,
                    screen=False,
                ) as live:
                    while True:
                        await anyio.sleep(refresh_interval_seconds)
                        live.update(
                            activity_dashboard(await snapshot(evidence_database), database),
                            refresh=True,
                        )

            if work:
                async with managed_worker_pool(evidence_database, _repository_root()):
                    await display()
            else:
                await display()

    with suppress(KeyboardInterrupt):
        anyio.run(perform, backend="trio")


@app.command
def import_proton_config(source: Path, configuration_id: str) -> None:
    """Validate and privately import one exported Proton WireGuard configuration."""

    imported = import_proton_configuration(
        source,
        directories=user_directories(),
        configuration_id=configuration_id,
    )
    _print_json({**imported.as_json(), "state": "imported"})


@app.command
def import_mullvad_config(source: Path, configuration_id: str) -> None:
    """Validate and privately import one Mullvad WireGuard configuration archive."""

    imported = import_mullvad_configuration_archive(
        source,
        directories=user_directories(),
        configuration_id=configuration_id,
    )
    _print_json({**imported.as_json(), "state": "imported"})


@app.command
def configure_decodo_credential(credential_id: str) -> None:
    """Prompt for and privately store one Decodo proxy password."""

    proxy_password = getpass("Decodo proxy password: ")
    configured = store_decodo_credential(
        proxy_password,
        directories=user_directories(),
        credential_id=credential_id,
    )
    _print_json({**configured.as_json(), "state": "configured"})


@app.command
def init(*, database: Path = _DEFAULT_DATABASE) -> None:
    """Initialize the SQLite evidence store."""

    async def perform() -> None:
        async with Database.managed(database, initialize=True):
            pass

    anyio.run(perform, backend="trio")
    _print_json({"database": str(database), "state": "initialized"})


@app.command
def migrate_image_files(*, database: Path = _DEFAULT_DATABASE) -> None:
    """Move validated image bytes from SQLite into content-addressed files."""

    async def perform() -> dict[str, JsonValue]:
        operation_identifier = _identifier()
        record_identifier = _identifier()
        started_at_utc = _utc_text(time_ns())
        started_monotonic_ns = perf_counter_ns()
        component = build_image_migration_component_registry().require(MIGRATE_EXTERNAL_IMAGE_FILES)
        async with Database.managed(database) as evidence_database:
            await evidence_database.begin_operation(
                operation_id=operation_identifier,
                component=component,
                provenance=await collect_code_provenance_async(_repository_root()),
                invocation=process_invocation(),
                configuration={
                    "database": str(database),
                    "external_data_directory": str(database.parent),
                },
                started_at_utc=started_at_utc,
            )
            try:
                result = await externalize_saved_images(
                    database=evidence_database,
                    image_files=ImageFileStore(database.parent),
                    migration_operation_identifier=operation_identifier,
                    progress=lambda completed, total: (
                        print(
                            f"Processed {completed}/{total} saved image results",
                            file=sys.stderr,
                            flush=True,
                        )
                        if completed % 100 == 0 or completed == total
                        else None
                    ),
                )
                await evidence_database.complete_operation(
                    operation_id=operation_identifier,
                    records=(
                        RecordDraft(
                            identifier=record_identifier,
                            kind=("carl", "storage", "image_file_migration"),
                            schema_version=1,
                            value={
                                **result,
                                "operation_identifier": operation_identifier,
                                "external_data_directory": str(database.parent),
                            },
                        ),
                    ),
                    artifacts=(),
                    outputs=(
                        NamedOutput(name=("migration",), object_identifier=record_identifier),
                    ),
                    result=result,
                    ended_at_utc=_utc_text(time_ns()),
                    duration_ns=max(0, perf_counter_ns() - started_monotonic_ns),
                )
            except BaseException as error:
                with anyio.CancelScope(shield=True):
                    await evidence_database.fail_operation(
                        operation_id=operation_identifier,
                        error={
                            "kind": "image_file_migration_failure",
                            "type": type(error).__name__,
                        },
                        result={"state": "failed"},
                        ended_at_utc=_utc_text(time_ns()),
                        duration_ns=max(0, perf_counter_ns() - started_monotonic_ns),
                    )
                raise
        return {"state": "completed", "migration_record_identifier": record_identifier, **result}

    _print_work_result(anyio.run(perform, backend="trio"))


@app.command
def collect(
    url: str,
    *,
    database: Path = _DEFAULT_DATABASE,
    network_provider: ItemNetworkProvider = ItemNetworkProvider.DECODO,
    network_route: str = "carl",
    browser_like: bool = True,
    headers_json: Path | None = None,
) -> None:
    """Collect and extract one Facebook item page through an explicit configured route."""

    async def perform() -> dict[str, JsonValue]:
        listing_id = listing_id_from_url(url)
        if not network_route or network_route != network_route.strip():
            raise ValueError("The network route identifier must be nonempty and trimmed")
        network_path = (network_provider.value, "personal", network_route)
        directories = user_directories()
        loaded = load_configuration(directories.configuration_file)
        if network_provider is ItemNetworkProvider.PROTON:
            route_settings = proton_settings(loaded, directories, network_path)
            acquirer = ManagedProtonHttpAcquirer(
                manager=ProtonWireproxyManager(),
                settings=route_settings,
            )
        elif network_provider is ItemNetworkProvider.MULLVAD:
            mullvad_route_settings = mullvad_settings(loaded, directories, network_path)
            acquirer = ManagedMullvadHttpAcquirer(
                manager=MullvadWireproxyManager(),
                settings=mullvad_route_settings,
            )
        else:
            decodo_route_settings, credential_source = decodo_settings(
                loaded, directories, network_path
            )
            acquirer = ManagedDecodoHttpAcquirer(
                manager=DecodoSessionManager(),
                settings=decodo_route_settings,
                credential_source=credential_source,
            )
        request_headers = await anyio.to_thread.run_sync(
            _headers,
            headers_json,
            browser_like,
            abandon_on_cancel=True,
        )
        payload = CollectItemPayload(
            listing_id=listing_id,
            request_plan=RequestPlan(
                url=url,
                headers=request_headers,
                routing=network_path,
            ),
        )
        collection_identifier = _identifier()
        settings = WorkerSettings(
            worker_count=1,
            lease_duration_ns=600_000_000_000,
            renewal_interval_ns=30_000_000_000,
            idle_poll_interval_ns=100_000_000,
        )
        async with Database.managed(database, initialize=True) as evidence_database:
            await _activate_facebook_page_network_policy(evidence_database, network_path)
            network_activity_scheduler = NetworkActivityScheduler(
                database=evidence_database,
                new_identifier=_identifier,
                utc_now_ns=time_ns,
                sample_uniform_holdoff_ns=_sample_uniform_holdoff_ns,
                permit_duration_ns=settings.lease_duration_ns,
            )
            registry = build_facebook_worker_registry(
                FacebookWorkerDependencies(
                    database=evidence_database,
                    acquirer=acquirer,
                    new_identifier=_identifier,
                    network_activity_scheduler=network_activity_scheduler,
                )
            )
            enqueued = await evidence_database.enqueue_work(
                collect_item_work(
                    identifier=collection_identifier,
                    payload=payload,
                    not_before_utc_ns=0,
                ),
                WorkRequester(
                    request_identifier=_identifier(),
                    kind=("carl", "cli", "item_request"),
                    identifier=_identifier(),
                    context={"invocation": process_invocation()},
                ),
                event_identifier=_identifier(),
                enqueued_at_utc_ns=time_ns(),
            )
            collection_identifier = enqueued.work_item_identifier
            services = WorkerRuntimeServices(
                new_identifier=_identifier,
                utc_now_ns=time_ns,
                monotonic_ns=perf_counter_ns,
                code_provenance=lambda: collect_code_provenance_async(_repository_root()),
                invocation=process_invocation,
            )
            capabilities = (
                WorkCapability(
                    kind=COLLECT_ITEM_WORK_KIND,
                    payload_schema_version=COLLECT_ITEM_PAYLOAD_SCHEMA_VERSION,
                ),
                WorkCapability(
                    kind=EXTRACT_ITEM_WORK_KIND,
                    payload_schema_version=EXTRACT_ITEM_PAYLOAD_SCHEMA_VERSION,
                ),
            )

            async def run_until_terminal(identifier: str) -> dict[str, JsonValue]:
                while await evidence_database.work_state(identifier) in {
                    WorkState.PENDING,
                    WorkState.LEASED,
                }:
                    claim = await evidence_database.claim_work(
                        supported_capabilities=capabilities,
                        worker_identifier=_identifier(),
                        lease_token=_identifier(),
                        lease_duration_ns=settings.lease_duration_ns,
                        utc_now_ns=time_ns,
                        event_identifier=_identifier(),
                    )
                    if claim.lease is not None:
                        await execute_lease(
                            database=evidence_database,
                            registry=registry,
                            settings=settings,
                            services=services,
                            lease=claim.lease,
                        )
                        continue
                    delay_ns = settings.idle_poll_interval_ns
                    if claim.next_eligible_at_utc_ns is not None:
                        delay_ns = min(
                            delay_ns,
                            max(0, claim.next_eligible_at_utc_ns - time_ns()),
                        )
                    await anyio.sleep(delay_ns / 1_000_000_000)
                return await evidence_database.work(identifier)

            collection = await run_until_terminal(collection_identifier)
            extraction: dict[str, JsonValue] | None = None
            collection_result = collection.get("result")
            if isinstance(collection_result, dict):
                extraction_identifier = collection_result.get("extraction_work_identifier")
                if isinstance(extraction_identifier, str):
                    extraction = await run_until_terminal(extraction_identifier)
            completed = collection.get("state") == WorkState.COMPLETED.value and (
                extraction is None or extraction.get("state") == WorkState.COMPLETED.value
            )
            return {
                "state": WorkState.COMPLETED.value if completed else "terminal_failure",
                "listing_id": listing_id,
                "collection": collection,
                "extraction": extraction,
            }

    _print_work_result(anyio.run(perform, backend="trio"))


@app.command
def collect_search_items(
    *search_run_record_identifiers: str,
    network_provider: BatchItemNetworkProvider = BatchItemNetworkProvider.DECODO,
    network_route: str = "carl",
    maximum_items: int | None = None,
    database: Path = _DEFAULT_DATABASE,
) -> None:
    """Reuse or collect items discovered by one or more retained search runs."""

    async def perform() -> dict[str, JsonValue]:
        if not search_run_record_identifiers:
            raise ValueError("Specify at least one Facebook search-run record")
        if not network_route or network_route != network_route.strip():
            raise ValueError("The network route identifier must be nonempty and trimmed")
        if maximum_items is not None and maximum_items < 1:
            raise ValueError("The maximum item count must be positive")
        network_path = (network_provider.value, "personal", network_route)
        settings = WorkerSettings(
            worker_count=1,
            lease_duration_ns=600_000_000_000,
            renewal_interval_ns=30_000_000_000,
            idle_poll_interval_ns=100_000_000,
        )
        async with Database.managed(database) as evidence_database:
            search_runs: list[SearchRunListingCandidates] = []
            for search_run_record_identifier in search_run_record_identifiers:
                kind, _, search_run = await evidence_database.get_record(
                    search_run_record_identifier
                )
                if kind != ("carl", "facebook", "search_run") or not isinstance(search_run, dict):
                    raise ValueError("An input is not a Facebook search-run record")
                traversal = search_run.get("traversal")
                listing_identifiers = (
                    traversal.get("unique_listing_identifiers")
                    if isinstance(traversal, dict)
                    else None
                )
                if not isinstance(listing_identifiers, list) or not all(
                    isinstance(identifier, str) and identifier.isdecimal()
                    for identifier in listing_identifiers
                ):
                    raise ValueError("A search-run record has no valid unique listing identifiers")
                search_runs.append(
                    SearchRunListingCandidates(
                        search_run_record_identifier=search_run_record_identifier,
                        listing_identifiers=tuple(listing_identifiers),
                    )
                )
            all_listing_identifiers = tuple(
                dict.fromkeys(
                    listing_identifier
                    for search_run in search_runs
                    for listing_identifier in search_run.listing_identifiers
                )
            )
            successful_results = await evidence_database.successful_facebook_item_page_results(
                all_listing_identifiers
            )
            planner_component = build_component_registry().require(
                PLAN_FACEBOOK_ITEM_PAGE_FOLLOWUPS
            )
            planner_operation_identifier = _identifier()
            followup_plan_record_identifier = _identifier()
            planner_started_utc_ns = time_ns()
            planner_started_monotonic_ns = perf_counter_ns()
            await evidence_database.begin_operation(
                operation_id=planner_operation_identifier,
                component=planner_component,
                provenance=await collect_code_provenance_async(_repository_root()),
                invocation=process_invocation(),
                configuration={
                    "search_run_record_identifiers": list(search_run_record_identifiers),
                    "maximum_items": maximum_items,
                },
                started_at_utc=_utc_text(planner_started_utc_ns),
                inputs=(
                    *(
                        (("search_run", str(index)), identifier)
                        for index, identifier in enumerate(search_run_record_identifiers)
                    ),
                    *(
                        (
                            ("successful_item_page_result", str(index)),
                            result.observation_record_identifier,
                        )
                        for index, result in enumerate(successful_results)
                    ),
                ),
            )
            try:
                plan = plan_item_page_followups(
                    tuple(search_runs),
                    successful_results,
                    maximum_items=maximum_items,
                )
                with anyio.CancelScope(shield=True):
                    await evidence_database.complete_operation(
                        operation_id=planner_operation_identifier,
                        records=(
                            RecordDraft(
                                identifier=followup_plan_record_identifier,
                                kind=("carl", "facebook", "item_page_followup_plan"),
                                schema_version=1,
                                value={
                                    **plan.model_dump(mode="json"),
                                    "operation_identifier": planner_operation_identifier,
                                    "planner": {
                                        "component_parts": list(planner_component.identifier.parts),
                                        "output_schema_version": (
                                            planner_component.output_schema_version
                                        ),
                                    },
                                },
                            ),
                        ),
                        artifacts=(),
                        outputs=(
                            NamedOutput(
                                name=("item_page_followup_plan",),
                                object_identifier=followup_plan_record_identifier,
                            ),
                        ),
                        result={
                            "followup_plan_record_identifier": (followup_plan_record_identifier),
                            "listing_references": plan.listing_references,
                            "unique_listings": plan.unique_listings,
                            "selected_items": len(plan.decisions),
                        },
                        ended_at_utc=_utc_text(time_ns()),
                        duration_ns=max(0, perf_counter_ns() - planner_started_monotonic_ns),
                    )
            except BaseException as error:
                with anyio.CancelScope(shield=True):
                    await evidence_database.fail_operation(
                        operation_id=planner_operation_identifier,
                        error={
                            "kind": "item_page_followup_planning_failure",
                            "type": type(error).__name__,
                        },
                        result={"state": "failed"},
                        ended_at_utc=_utc_text(time_ns()),
                        duration_ns=max(0, perf_counter_ns() - planner_started_monotonic_ns),
                    )
                raise
            collection_decisions = tuple(
                decision for decision in plan.decisions if decision.latest_successful_result is None
            )
            if not collection_decisions:
                reused_classifications: dict[str, int] = {}
                for decision in plan.decisions:
                    selected = decision.latest_successful_result
                    if selected is None:
                        raise AssertionError("A reused decision has no successful result")
                    name = selected.response_classification.value
                    reused_classifications[name] = reused_classifications.get(name, 0) + 1
                return {
                    "state": WorkState.COMPLETED.value,
                    "followup_plan_record_identifier": followup_plan_record_identifier,
                    "search_run_record_identifiers": list(search_run_record_identifiers),
                    "network_path": list(network_path),
                    "network_session_identifier": None,
                    "listing_references": plan.listing_references,
                    "unique_listings": plan.unique_listings,
                    "selected_items": len(plan.decisions),
                    "reused_successful_results": len(plan.decisions),
                    "new_collection_requested": 0,
                    "new_collection_completed": 0,
                    "new_collection_failed": 0,
                    "new_extractions": 0,
                    "reused_classifications": reused_classifications,
                    "new_classifications": {},
                }
            directories = user_directories()
            loaded = load_configuration(directories.configuration_file)
            factory: FacebookItemSessionFactory
            if network_provider is BatchItemNetworkProvider.DECODO:
                route_settings, credential_source = decodo_settings(
                    loaded, directories, network_path
                )
                factory = DecodoFacebookItemSessionFactory(
                    manager=DecodoSessionManager(),
                    settings=route_settings,
                    credential_source=credential_source,
                )
            else:
                mullvad_route_settings = mullvad_settings(loaded, directories, network_path)
                factory = MullvadFacebookItemSessionFactory(
                    manager=MullvadWireproxyManager(),
                    settings=mullvad_route_settings,
                )
            request_headers = await anyio.to_thread.run_sync(
                brave_navigation_headers,
                abandon_on_cancel=True,
            )
            await _activate_facebook_page_network_policy(evidence_database, network_path)
            network_activity_scheduler = NetworkActivityScheduler(
                database=evidence_database,
                new_identifier=_identifier,
                utc_now_ns=time_ns,
                sample_uniform_holdoff_ns=_sample_uniform_holdoff_ns,
                permit_duration_ns=settings.lease_duration_ns,
            )
            network_session_identifier = _identifier()
            async with factory(network_session_identifier) as session:
                registry = build_facebook_worker_registry(
                    FacebookWorkerDependencies(
                        database=evidence_database,
                        acquirer=session.acquirer,
                        new_identifier=_identifier,
                        network_activity_scheduler=network_activity_scheduler,
                        network_session_identifier=network_session_identifier,
                    )
                )
                collection_identifiers: list[str] = []
                for decision in collection_decisions:
                    payload = CollectItemPayload(
                        listing_id=decision.listing_id,
                        request_plan=RequestPlan(
                            url=(
                                f"https://www.facebook.com/marketplace/item/{decision.listing_id}/"
                            ),
                            headers=request_headers,
                            routing=network_path,
                        ),
                    )
                    definition = collect_item_work(
                        identifier=_identifier(),
                        payload=payload,
                        not_before_utc_ns=0,
                    )
                    collection_identifier: str | None = None
                    for search_run_record_identifier in decision.search_run_record_identifiers:
                        enqueued = await evidence_database.enqueue_work(
                            definition,
                            WorkRequester(
                                request_identifier=_identifier(),
                                kind=("carl", "facebook", "search_run"),
                                identifier=search_run_record_identifier,
                                context={
                                    "listing_identifier": decision.listing_id,
                                    "selection": "no_successful_item_page_result",
                                    "followup_plan_record_identifier": (
                                        followup_plan_record_identifier
                                    ),
                                    "invocation": process_invocation(),
                                },
                            ),
                            event_identifier=_identifier(),
                            enqueued_at_utc_ns=time_ns(),
                        )
                        if collection_identifier is None:
                            collection_identifier = enqueued.work_item_identifier
                        elif collection_identifier != enqueued.work_item_identifier:
                            raise RuntimeError("Item-page requesters did not share one work item")
                    plan_request = await evidence_database.enqueue_work(
                        definition,
                        WorkRequester(
                            request_identifier=_identifier(),
                            kind=("carl", "facebook", "item_page_followup_plan"),
                            identifier=followup_plan_record_identifier,
                            context={
                                "listing_identifier": decision.listing_id,
                                "search_run_record_identifiers": list(
                                    decision.search_run_record_identifiers
                                ),
                            },
                        ),
                        event_identifier=_identifier(),
                        enqueued_at_utc_ns=time_ns(),
                    )
                    if (
                        collection_identifier is not None
                        and collection_identifier != plan_request.work_item_identifier
                    ):
                        raise RuntimeError("Follow-up plan did not share the item work")
                    collection_identifier = plan_request.work_item_identifier
                    if collection_identifier is None:
                        raise AssertionError("A listing decision has no search-run requester")
                    collection_identifiers.append(collection_identifier)

                services = WorkerRuntimeServices(
                    new_identifier=_identifier,
                    utc_now_ns=time_ns,
                    monotonic_ns=perf_counter_ns,
                    code_provenance=lambda: collect_code_provenance_async(_repository_root()),
                    invocation=process_invocation,
                )

                async def run_until_terminal(
                    identifiers: list[str], capability: WorkCapability
                ) -> list[dict[str, JsonValue]]:
                    remaining = {
                        identifier
                        for identifier in identifiers
                        if await evidence_database.work_state(identifier)
                        in {WorkState.PENDING, WorkState.LEASED}
                    }
                    while remaining:
                        claim = await evidence_database.claim_work(
                            supported_capabilities=(capability,),
                            worker_identifier=_identifier(),
                            lease_token=_identifier(),
                            lease_duration_ns=settings.lease_duration_ns,
                            utc_now_ns=time_ns,
                            event_identifier=_identifier(),
                            eligible_identifiers=tuple(remaining),
                        )
                        if claim.lease is not None:
                            await execute_lease(
                                database=evidence_database,
                                registry=registry,
                                settings=settings,
                                services=services,
                                lease=claim.lease,
                            )
                            if (
                                claim.lease.work_item_identifier in remaining
                                and await evidence_database.work_state(
                                    claim.lease.work_item_identifier
                                )
                                not in {WorkState.PENDING, WorkState.LEASED}
                            ):
                                remaining.remove(claim.lease.work_item_identifier)
                            continue
                        delay_ns = settings.idle_poll_interval_ns
                        if claim.next_eligible_at_utc_ns is not None:
                            delay_ns = min(
                                delay_ns,
                                max(0, claim.next_eligible_at_utc_ns - time_ns()),
                            )
                        await anyio.sleep(delay_ns / 1_000_000_000)
                    return [await evidence_database.work(identifier) for identifier in identifiers]

                collections = await run_until_terminal(
                    collection_identifiers,
                    WorkCapability(
                        kind=COLLECT_ITEM_WORK_KIND,
                        payload_schema_version=COLLECT_ITEM_PAYLOAD_SCHEMA_VERSION,
                    ),
                )
                extraction_identifiers = [
                    extraction_identifier
                    for collection in collections
                    if isinstance(collection.get("result"), dict)
                    and isinstance(
                        extraction_identifier := collection["result"].get(
                            "extraction_work_identifier"
                        ),
                        str,
                    )
                ]
                extractions = await run_until_terminal(
                    extraction_identifiers,
                    WorkCapability(
                        kind=EXTRACT_ITEM_WORK_KIND,
                        payload_schema_version=EXTRACT_ITEM_PAYLOAD_SCHEMA_VERSION,
                    ),
                )
            new_classifications: dict[str, int] = {}
            for extraction in extractions:
                result = extraction.get("result")
                classification = (
                    result.get("response_classification") if isinstance(result, dict) else None
                )
                name = classification if isinstance(classification, str) else "extraction_failure"
                new_classifications[name] = new_classifications.get(name, 0) + 1
            reused_classifications: dict[str, int] = {}
            for decision in plan.decisions:
                selected = decision.latest_successful_result
                if selected is None:
                    continue
                name = selected.response_classification.value
                reused_classifications[name] = reused_classifications.get(name, 0) + 1
            return {
                "state": WorkState.COMPLETED.value,
                "followup_plan_record_identifier": followup_plan_record_identifier,
                "search_run_record_identifiers": list(search_run_record_identifiers),
                "network_path": list(network_path),
                "network_session_identifier": network_session_identifier,
                "listing_references": plan.listing_references,
                "unique_listings": plan.unique_listings,
                "selected_items": len(plan.decisions),
                "reused_successful_results": len(plan.decisions) - len(collection_decisions),
                "new_collection_requested": len(collection_decisions),
                "new_collection_completed": sum(
                    collection["state"] == WorkState.COMPLETED.value for collection in collections
                ),
                "new_collection_failed": sum(
                    collection["state"] == WorkState.TERMINAL_FAILURE.value
                    for collection in collections
                ),
                "new_extractions": len(extractions),
                "reused_classifications": reused_classifications,
                "new_classifications": new_classifications,
            }

    _print_work_result(anyio.run(perform, backend="trio"))


@app.command
def collect_images(
    *search_run_record_identifiers: str,
    maximum_images: int = 10,
    proton_route: str = _DEFAULT_PROTON_ROUTE,
    database: Path = _DEFAULT_DATABASE,
) -> None:
    """Collect a bounded set of missing gallery renditions from retained search runs."""

    async def perform() -> dict[str, JsonValue]:
        if not search_run_record_identifiers:
            raise ValueError("Specify at least one Facebook search-run record")
        if maximum_images < 1:
            raise ValueError("The maximum image count must be positive")
        if not proton_route or proton_route != proton_route.strip():
            raise ValueError("The Proton route identifier must be nonempty and trimmed")
        network_path = (NetworkProvider.PROTON.value, "personal", proton_route)
        settings = WorkerSettings(
            worker_count=8,
            lease_duration_ns=600_000_000_000,
            renewal_interval_ns=30_000_000_000,
            idle_poll_interval_ns=100_000_000,
        )
        async with Database.managed(database) as evidence_database:
            listing_ids: list[str] = []
            for search_run_identifier in search_run_record_identifiers:
                kind, _, run = await evidence_database.get_record(search_run_identifier)
                if kind != ("carl", "facebook", "search_run") or not isinstance(run, dict):
                    raise ValueError("An input is not a Facebook search-run record")
                traversal = run.get("traversal")
                identifiers = (
                    traversal.get("unique_listing_identifiers")
                    if isinstance(traversal, dict)
                    else None
                )
                if not isinstance(identifiers, list) or not all(
                    isinstance(identifier, str) and identifier.isdecimal()
                    for identifier in identifiers
                ):
                    raise ValueError("A search run has no valid listing IDs")
                listing_ids.extend(identifiers)
            unique_listing_ids = tuple(dict.fromkeys(listing_ids))
            successful = await evidence_database.successful_facebook_item_page_results(
                unique_listing_ids
            )
            latest_by_listing = {}
            for result in successful:
                latest_by_listing[result.listing_id] = result
            references = []
            for result in latest_by_listing.values():
                kind, _, observation = await evidence_database.get_record(
                    result.observation_record_identifier
                )
                if kind != ("carl", "facebook", "listing_observation"):
                    raise ValueError("Item-page result has an invalid observation")
                references.extend(
                    gallery_references(
                        observation_identifier=result.observation_record_identifier,
                        observation=observation,
                    )
                )
            unique_renditions: set[tuple[str | None, str]] = set()
            for reference in references:
                unique_renditions.add((reference.photo_id, reference.original_url))
            saved_before_resumption = await evidence_database.saved_facebook_image_renditions()
            pending_extractions = await evidence_database.pending_facebook_image_extractions()
            resumed_keys: set[tuple[str | None, str]] = set()
            resumed_identifiers: list[str] = []
            for reference in references:
                key = (reference.photo_id, reference.original_url)
                if (
                    key in saved_before_resumption
                    or key in resumed_keys
                    or key not in pending_extractions
                    or len(resumed_keys) >= maximum_images
                ):
                    continue
                resumed_keys.add(key)
                resumed_identifiers.extend(pending_extractions[key])
            resumed_results: list[dict[str, JsonValue]] = []
            if resumed_identifiers:
                offline_registry = build_image_worker_registry(
                    ImageWorkerDependencies(
                        database=evidence_database,
                        acquirer=DirectHttpxAcquirer(),
                        image_files=ImageFileStore(evidence_database.path.parent),
                        new_identifier=_identifier,
                    )
                )
                offline_services = WorkerRuntimeServices(
                    new_identifier=_identifier,
                    utc_now_ns=time_ns,
                    monotonic_ns=perf_counter_ns,
                    code_provenance=lambda: collect_code_provenance_async(_repository_root()),
                    invocation=process_invocation,
                )
                remaining_resumptions = set(resumed_identifiers)
                while remaining_resumptions:
                    claim = await evidence_database.claim_work(
                        supported_capabilities=(
                            WorkCapability(
                                kind=EXTRACT_IMAGE_WORK_KIND,
                                payload_schema_version=EXTRACT_IMAGE_WORK_SCHEMA_VERSION,
                            ),
                        ),
                        worker_identifier=_identifier(),
                        lease_token=_identifier(),
                        lease_duration_ns=settings.lease_duration_ns,
                        utc_now_ns=time_ns,
                        event_identifier=_identifier(),
                        eligible_identifiers=tuple(remaining_resumptions),
                    )
                    if claim.lease is not None:
                        await execute_lease(
                            database=evidence_database,
                            registry=offline_registry,
                            settings=settings,
                            services=offline_services,
                            lease=claim.lease,
                        )
                    else:
                        await anyio.sleep(settings.idle_poll_interval_ns / 1_000_000_000)
                    remaining_resumptions = {
                        identifier
                        for identifier in remaining_resumptions
                        if await evidence_database.work_state(identifier)
                        in {WorkState.PENDING, WorkState.LEASED}
                    }
                resumed_results = [
                    await evidence_database.work(identifier) for identifier in resumed_identifiers
                ]
            resumed_saved = sum(
                result["state"] == WorkState.COMPLETED.value for result in resumed_results
            )
            resumed_failed = len(resumed_results) - resumed_saved
            saved_results = await evidence_database.saved_facebook_image_results()
            saved_candidates = _saved_image_candidates(saved_results)
            remaining_image_limit = maximum_images - len(resumed_keys)
            followup_plan = plan_image_followups(
                tuple(references),
                saved_candidates,
                remaining_image_limit,
                frozenset(resumed_keys),
            )
            groups = followup_plan.download_groups
            reuse_decisions = followup_plan.reuse_decisions
            existing_references = await evidence_database.facebook_gallery_reference_identifiers()
            reference_identifiers_by_value = dict(existing_references)
            new_references = {}
            for reference in references:
                if reference not in reference_identifiers_by_value:
                    identifier = _identifier()
                    reference_identifiers_by_value[reference] = identifier
                    new_references[reference] = identifier
            reference_identifiers = tuple(
                reference_identifiers_by_value[reference] for reference in references
            )
            selected = tuple(
                tuple((reference_identifiers[index], references[index]) for index in group)
                for group in groups
            )
            source_asset_reuses = tuple(
                decision
                for decision in reuse_decisions
                if decision.match_kind is ImageReuseMatchKind.SOURCE_PHOTO_ADEQUATE_DIMENSIONS
            )
            exact_reuses = len(reuse_decisions) - len(source_asset_reuses)

            plan_identifier = _identifier()
            operation_identifier = _identifier()
            started_utc_ns = time_ns()
            started_monotonic_ns = perf_counter_ns()
            await evidence_database.begin_operation(
                operation_id=operation_identifier,
                component=build_image_component_registry().require(PLAN_FACEBOOK_IMAGE_FOLLOWUPS),
                provenance=await collect_code_provenance_async(_repository_root()),
                invocation=process_invocation(),
                configuration={
                    "search_run_record_identifiers": list(search_run_record_identifiers),
                    "maximum_images": maximum_images,
                    "network_path": list(network_path),
                },
                started_at_utc=_utc_text(started_utc_ns),
                inputs=tuple(
                    (("listing_observation", str(index)), result.observation_record_identifier)
                    for index, result in enumerate(latest_by_listing.values())
                ),
            )
            image_records = tuple(
                RecordDraft(
                    identifier=reference_identifier,
                    kind=("carl", "facebook", "gallery_image_reference"),
                    schema_version=1,
                    value={
                        **reference.model_dump(mode="json"),
                        "reference_extractor": {
                            "component_parts": list(EXTRACT_FACEBOOK_GALLERY_REFERENCES.parts),
                            "output_schema_version": 1,
                        },
                        "operation_identifier": operation_identifier,
                    },
                )
                for reference, reference_identifier in new_references.items()
            )
            await evidence_database.complete_operation(
                operation_id=operation_identifier,
                records=(
                    RecordDraft(
                        identifier=plan_identifier,
                        kind=("carl", "facebook", "image_followup_plan"),
                        schema_version=1,
                        value={
                            "search_run_record_identifiers": list(search_run_record_identifiers),
                            "listing_references": len(listing_ids),
                            "unique_listings": len(unique_listing_ids),
                            "gallery_references": len(references),
                            "unique_renditions": len(unique_renditions),
                            "already_saved_exact_renditions": exact_reuses,
                            "reused_source_photo_images": len(source_asset_reuses),
                            "resumed_extractions": len(resumed_results),
                            "resumed_images_saved": resumed_saved,
                            "resumed_images_failed": resumed_failed,
                            "selected_new_renditions": len(selected),
                            "nominal_network_minutes_at_rate_cap": (
                                len(selected) / IMAGE_RATE_MAXIMUM_STARTS
                            ),
                            "reference_record_identifiers": list(reference_identifiers),
                            "selected_reference_record_identifiers": [
                                identifier for entries in selected for identifier, _ in entries
                            ],
                            "operation_identifier": operation_identifier,
                        },
                    ),
                    *image_records,
                ),
                artifacts=(),
                outputs=(
                    NamedOutput(name=("image_followup_plan",), object_identifier=plan_identifier),
                    *(
                        NamedOutput(
                            name=("gallery_image_reference", str(index)),
                            object_identifier=record.identifier,
                        )
                        for index, record in enumerate(image_records)
                    ),
                ),
                result={
                    "selected_new_renditions": len(selected),
                    "already_saved_exact_renditions": exact_reuses,
                    "reused_source_photo_images": len(source_asset_reuses),
                },
                ended_at_utc=_utc_text(time_ns()),
                duration_ns=max(0, perf_counter_ns() - started_monotonic_ns),
            )

            existing_reuses = {
                (
                    resolution.gallery_image_reference_record_identifier,
                    resolution.source_image_result_record_identifier,
                    resolution.match_kind,
                )
                for resolution in await evidence_database.facebook_image_reuse_resolutions()
            }
            new_reuses = tuple(
                (
                    reference_identifiers[decision.reference_index],
                    decision.candidate.image_result_record_identifier,
                    decision.match_kind,
                )
                for decision in source_asset_reuses
                if (
                    reference_identifiers[decision.reference_index],
                    decision.candidate.image_result_record_identifier,
                    decision.match_kind,
                )
                not in existing_reuses
            )
            if new_reuses:
                reuse_operation_identifier = _identifier()
                reuse_started_utc_ns = time_ns()
                reuse_started_monotonic_ns = perf_counter_ns()
                reuse_record_identifiers = tuple(_identifier() for _ in new_reuses)
                reuse_component = build_image_component_registry().require(
                    REUSE_FACEBOOK_GALLERY_IMAGE
                )
                reuse_provenance = await collect_code_provenance_async(_repository_root())
                with anyio.CancelScope(shield=True):
                    await evidence_database.begin_operation(
                        operation_id=reuse_operation_identifier,
                        component=reuse_component,
                        provenance=reuse_provenance,
                        invocation=process_invocation(),
                        configuration={
                            "selection_policy": {
                                "source_identity": "facebook_photo_id",
                                "minimum_dimensions": (
                                    "saved_width_and_height_at_least_declared_dimensions"
                                ),
                                "missing_source_or_declared_dimensions": (
                                    "require_exact_signed_url"
                                ),
                            }
                        },
                        started_at_utc=_utc_text(reuse_started_utc_ns),
                        inputs=tuple(
                            input_value
                            for index, (reference_identifier, result_identifier, _) in enumerate(
                                new_reuses
                            )
                            for input_value in (
                                (
                                    ("gallery_image_reference", f"{index:08d}"),
                                    reference_identifier,
                                ),
                                (
                                    ("source_image_result", f"{index:08d}"),
                                    result_identifier,
                                ),
                            )
                        ),
                    )
                    await evidence_database.complete_operation(
                        operation_id=reuse_operation_identifier,
                        records=tuple(
                            RecordDraft(
                                identifier=record_identifier,
                                kind=("carl", "facebook", "image_reuse"),
                                schema_version=1,
                                value=image_reuse_record(match_kind).model_dump(mode="json"),
                            )
                            for record_identifier, (_, _, match_kind) in zip(
                                reuse_record_identifiers, new_reuses, strict=True
                            )
                        ),
                        artifacts=(),
                        outputs=tuple(
                            NamedOutput(
                                name=("image_reuse", f"{index:08d}"),
                                object_identifier=record_identifier,
                            )
                            for index, record_identifier in enumerate(reuse_record_identifiers)
                        ),
                        result={"recorded_source_asset_reuses": len(new_reuses)},
                        ended_at_utc=_utc_text(time_ns()),
                        duration_ns=max(0, perf_counter_ns() - reuse_started_monotonic_ns),
                    )
            if not selected:
                return {
                    "state": "completed" if resumed_failed == 0 else "terminal_failure",
                    "image_followup_plan_record_identifier": plan_identifier,
                    "selected_new_renditions": 0,
                    "already_saved_exact_renditions": exact_reuses,
                    "reused_source_photo_images": len(source_asset_reuses),
                    "recorded_source_asset_reuses": len(new_reuses),
                    "resumed_extractions": len(resumed_results),
                    "resumed_images_saved": resumed_saved,
                    "resumed_images_failed": resumed_failed,
                }

            await evidence_database.supersede_constraints(
                retired_identifiers=legacy_image_network_constraint_identifiers(network_path),
                replacements=image_network_constraints(network_path),
                operation_identifier=operation_identifier,
                at_utc_ns=time_ns(),
                reason="Serialize exclusive Proton image sessions and retain bounded network pacing",
            )
            registry = build_routed_facebook_worker_registry(
                database=evidence_database,
                directories=user_directories(),
                new_identifier=_identifier,
            )
            collection_identifiers: list[str] = []
            for entries in selected:
                reference_identifier, reference = entries[0]
                payload = CollectImagePayload(
                    reference_record_identifier=reference_identifier,
                    reference=reference,
                    request_plan=RequestPlan(
                        url=reference.original_url,
                        headers=(
                            Header(
                                name=b"Accept",
                                value=b"image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                            ),
                        ),
                        follow_redirects=False,
                        max_redirects=0,
                        routing=network_path,
                        compression=("identity",),
                    ),
                )
                definition = collect_image_work(
                    identifier=_identifier(), payload=payload, not_before_utc_ns=0
                )
                collection_identifier = None
                for requester_identifier, image_reference in entries:
                    enqueued = await evidence_database.enqueue_work(
                        definition,
                        WorkRequester(
                            request_identifier=_identifier(),
                            kind=("carl", "facebook", "gallery_image_reference"),
                            identifier=requester_identifier,
                            context={
                                "listing_id": image_reference.listing_id,
                                "image_followup_plan_record_identifier": plan_identifier,
                            },
                        ),
                        event_identifier=_identifier(),
                        enqueued_at_utc_ns=time_ns(),
                    )
                    collection_identifier = enqueued.work_item_identifier
                if collection_identifier is None:
                    raise AssertionError("Selected image has no reference")
                collection_identifiers.append(collection_identifier)
            services = WorkerRuntimeServices(
                new_identifier=_identifier,
                utc_now_ns=time_ns,
                monotonic_ns=perf_counter_ns,
                code_provenance=lambda: collect_code_provenance_async(_repository_root()),
                invocation=process_invocation,
            )

            async def run_until_terminal(
                identifiers: list[str], capability: WorkCapability
            ) -> list[dict[str, JsonValue]]:
                remaining = {
                    identifier
                    for identifier in identifiers
                    if await evidence_database.work_state(identifier)
                    in {WorkState.PENDING, WorkState.LEASED}
                }
                finished = 0
                failed = 0

                async def observe_finished_elsewhere() -> None:
                    nonlocal finished, failed
                    for identifier in tuple(remaining):
                        state = await evidence_database.work_state(identifier)
                        if state in {WorkState.PENDING, WorkState.LEASED}:
                            continue
                        if identifier not in remaining:
                            continue
                        remaining.remove(identifier)
                        finished += 1
                        if state is not WorkState.COMPLETED:
                            failed += 1

                async def worker() -> None:
                    nonlocal finished, failed
                    while remaining:
                        await observe_finished_elsewhere()
                        if not remaining:
                            return
                        claim = await evidence_database.claim_work(
                            supported_capabilities=(capability,),
                            worker_identifier=_identifier(),
                            lease_token=_identifier(),
                            lease_duration_ns=settings.lease_duration_ns,
                            utc_now_ns=time_ns,
                            event_identifier=_identifier(),
                            eligible_identifiers=tuple(remaining),
                        )
                        if claim.lease is None:
                            await anyio.sleep(settings.idle_poll_interval_ns / 1_000_000_000)
                            continue
                        await execute_lease(
                            database=evidence_database,
                            registry=registry,
                            settings=settings,
                            services=services,
                            lease=claim.lease,
                        )
                        state = await evidence_database.work_state(claim.lease.work_item_identifier)
                        if (
                            state not in {WorkState.PENDING, WorkState.LEASED}
                            and claim.lease.work_item_identifier in remaining
                        ):
                            remaining.remove(claim.lease.work_item_identifier)
                            finished += 1
                            if state is not WorkState.COMPLETED:
                                failed += 1
                            if finished % 100 == 0:
                                print(
                                    f"{capability.kind[-1]}: {finished}/{len(identifiers)} "
                                    f"finished, {failed} failed",
                                    file=sys.stderr,
                                    flush=True,
                                )

                async with anyio.create_task_group() as group:
                    for _ in range(min(settings.worker_count, len(remaining))):
                        group.start_soon(worker)
                return [await evidence_database.work(identifier) for identifier in identifiers]

            collections = await run_until_terminal(
                collection_identifiers,
                WorkCapability(
                    kind=COLLECT_IMAGE_WORK_KIND,
                    payload_schema_version=COLLECT_IMAGE_WORK_SCHEMA_VERSION,
                ),
            )
            saved_count = sum(
                collection.get("state") == WorkState.COMPLETED.value
                and isinstance(collection.get("result"), dict)
                and collection["result"].get("state") == "saved"
                for collection in collections
            )
            failed_count = len(selected) - saved_count
            return {
                "state": "completed" if failed_count + resumed_failed == 0 else "terminal_failure",
                "image_followup_plan_record_identifier": plan_identifier,
                "gallery_references": len(references),
                "unique_renditions": len(unique_renditions),
                "already_saved_exact_renditions": exact_reuses,
                "reused_source_photo_images": len(source_asset_reuses),
                "recorded_source_asset_reuses": len(new_reuses),
                "new_collections": len(collections),
                "new_images_saved": saved_count,
                "new_images_failed": failed_count,
                "resumed_extractions": len(resumed_results),
                "resumed_images_saved": resumed_saved,
                "resumed_images_failed": resumed_failed,
            }

    _print_work_result(anyio.run(perform, backend="trio"))


@app.command
def analyze_items(
    *listing_identifiers: str,
    product_guide: ProductGuideKind,
    maximum_items: int = 1,
    worker_count: int = 1,
    model: str = "claude-sonnet-5",
    effort: ClaudeEffort = ClaudeEffort.MEDIUM,
    timeout_seconds: int = 150,
    claude_executable: str = "claude",
    database: Path = _DEFAULT_DATABASE,
) -> None:
    """Identify and research a bounded set of fully saved Marketplace listings with Claude."""

    async def perform() -> dict[str, JsonValue]:
        if maximum_items < 1 or worker_count < 1 or timeout_seconds < 1:
            raise ValueError("Maximum items, worker count, and timeout must be positive")
        if not claude_executable:
            raise ValueError("Claude executable must be nonempty")
        if not model or model != model.strip():
            raise ValueError("Claude model must be nonempty and trimmed")
        if any(not identifier.isdecimal() for identifier in listing_identifiers):
            raise ValueError("Listing IDs must be decimal strings")
        claude = ClaudeCli(claude_executable)
        settings = WorkerSettings(
            worker_count=worker_count,
            lease_duration_ns=900_000_000_000,
            renewal_interval_ns=30_000_000_000,
            idle_poll_interval_ns=100_000_000,
        )
        async with Database.managed(database) as evidence_database:
            guide_record_identifier = await _ensure_product_guide(
                evidence_database, product_guide_definition(product_guide)
            )
            outstanding = await evidence_database.outstanding_facebook_item_analysis_work(
                listing_identifiers=(
                    tuple(dict.fromkeys(listing_identifiers)) if listing_identifiers else None
                ),
                maximum_items=maximum_items,
                recipe_version=ANALYSIS_RECIPE_VERSION,
                product_guide_record_identifier=guide_record_identifier,
            )
            remaining_slots = maximum_items - len(outstanding)
            outstanding_inputs: set[tuple[str, str]] = set()
            for work_identifier in outstanding:
                existing_work = await evidence_database.work(work_identifier)
                existing_payload = existing_work.get("payload")
                typed_payload = AnalyzeItemPayload.model_validate_json(
                    encode_json(existing_payload)
                )
                outstanding_inputs.add(
                    (
                        typed_payload.evidence_set_record_identifier,
                        typed_payload.product_guide_record_identifier,
                    )
                )
            identifiers = (
                ()
                if remaining_slots == 0
                else (
                    tuple(dict.fromkeys(listing_identifiers))
                    if listing_identifiers
                    else await evidence_database.full_facebook_listing_identifiers()
                )
            )
            results = await evidence_database.successful_facebook_item_page_results(identifiers)
            latest = {
                result.listing_id: result
                for result in results
                if result.response_classification.value == "full_listing"
            }
            saved = (
                await evidence_database.saved_facebook_image_results()
                if remaining_slots > 0
                else ()
            )
            saved_by_rendition: dict[tuple[str | None, str], tuple[str, dict[str, JsonValue]]] = {}
            for record_identifier, value in saved:
                photo_id = value.get("source_photo_id")
                original_url = value.get("original_url")
                if photo_id is not None and not isinstance(photo_id, str):
                    continue
                if isinstance(original_url, str):
                    saved_by_rendition[(photo_id, original_url)] = (record_identifier, value)
            reference_identifiers = (
                await evidence_database.facebook_gallery_reference_identifiers()
                if remaining_slots > 0
                else {}
            )
            saved_by_reference = (
                await evidence_database.resolved_facebook_image_results_by_reference()
                if remaining_slots > 0
                else {}
            )
            evidence_sets = (
                await evidence_database.facebook_listing_analysis_evidence_sets()
                if remaining_slots > 0
                else {}
            )
            analyzed: frozenset[AnalyzeItemPayload] = (
                await evidence_database.completed_facebook_item_analysis_payloads()
                if remaining_slots > 0
                else frozenset()
            )
            selected: list[AnalyzeItemPayload] = []
            skipped_missing_images = 0
            skipped_existing = 0
            skipped_unavailable = 0
            for listing_id in identifiers:
                result = latest.get(listing_id)
                if result is None:
                    skipped_unavailable += 1
                    continue
                kind, _, observation = await evidence_database.get_record(
                    result.observation_record_identifier
                )
                if kind != ("carl", "facebook", "listing_observation"):
                    raise ValueError("Item result has no valid observation")
                references = gallery_references(
                    observation_identifier=result.observation_record_identifier,
                    observation=observation,
                )
                if not references:
                    skipped_missing_images += 1
                    continue
                images = saved_gallery_for_analysis(
                    tuple(references),
                    tuple(reference_identifiers.items()),
                    saved_by_rendition,
                    saved_by_reference,
                )
                if images is None:
                    skipped_missing_images += 1
                    continue
                evidence = listing_analysis_evidence_set(
                    listing_observation_record_identifier=result.observation_record_identifier,
                    gallery_images=images,
                )
                evidence_set_identifier = evidence_sets.get(evidence)
                if evidence_set_identifier is None:
                    evidence_set_identifier = _identifier()
                    operation_identifier = _identifier()
                    started_utc_ns = time_ns()
                    started_monotonic_ns = perf_counter_ns()
                    await evidence_database.begin_operation(
                        operation_id=operation_identifier,
                        component=build_analysis_component_registry().require(
                            PLAN_FACEBOOK_LISTING_ANALYSIS_EVIDENCE
                        ),
                        provenance=await collect_code_provenance_async(_repository_root()),
                        invocation=process_invocation(),
                        configuration={
                            "selection_policy": {
                                "listing_observation": "latest_successful_full_listing",
                                "gallery_images": "all_saved_or_source_asset_reused_images",
                            }
                        },
                        started_at_utc=_utc_text(started_utc_ns),
                        inputs=(
                            (
                                ("listing_observation",),
                                result.observation_record_identifier,
                            ),
                            *(
                                input_value
                                for index, image in enumerate(images)
                                for input_value in (
                                    (
                                        ("gallery_image_reference", f"{index:08d}"),
                                        (image.gallery_image_reference_record_identifier),
                                    ),
                                    (
                                        ("image_result", f"{index:08d}"),
                                        image.image_result_record_identifier,
                                    ),
                                )
                            ),
                        ),
                    )
                    await evidence_database.complete_operation(
                        operation_id=operation_identifier,
                        records=(
                            RecordDraft(
                                identifier=evidence_set_identifier,
                                kind=("carl", "facebook", "listing_analysis_evidence"),
                                schema_version=1,
                                value={},
                            ),
                        ),
                        artifacts=(),
                        outputs=(
                            NamedOutput(
                                name=("listing_analysis_evidence",),
                                object_identifier=evidence_set_identifier,
                            ),
                        ),
                        result={"state": "completed"},
                        ended_at_utc=_utc_text(time_ns()),
                        duration_ns=max(0, perf_counter_ns() - started_monotonic_ns),
                    )
                    evidence_sets[evidence] = evidence_set_identifier
                payload = AnalyzeItemPayload(
                    evidence_set_record_identifier=evidence_set_identifier,
                    product_guide_record_identifier=guide_record_identifier,
                    model=model,
                    effort=effort,
                    timeout_seconds=timeout_seconds,
                    maximum_turns=max(8, len(images) + 5),
                )
                if (evidence_set_identifier, guide_record_identifier) in outstanding_inputs:
                    continue
                if payload in analyzed:
                    skipped_existing += 1
                    continue
                selected.append(payload)
                if len(selected) >= remaining_slots:
                    break

            work_identifiers = list(outstanding)
            for payload in selected:
                enqueued = await evidence_database.enqueue_work(
                    analyze_item_work(identifier=_identifier(), payload=payload),
                    WorkRequester(
                        request_identifier=_identifier(),
                        kind=("carl", "cli", "analyze_items_request"),
                        identifier=payload.evidence_set_record_identifier,
                        context={"invocation": process_invocation()},
                    ),
                    event_identifier=_identifier(),
                    enqueued_at_utc_ns=time_ns(),
                )
                work_identifiers.append(enqueued.work_item_identifier)
            if not work_identifiers:
                return {
                    "state": "completed",
                    "product_guide_record_identifier": guide_record_identifier,
                    "selected": 0,
                    "resumed": 0,
                    "skipped_existing": skipped_existing,
                    "skipped_missing_images": skipped_missing_images,
                    "skipped_unavailable": skipped_unavailable,
                }
            registry = build_analysis_worker_registry(
                AnalysisWorkerDependencies(
                    database=evidence_database,
                    claude=claude,
                    new_identifier=_identifier,
                )
            )
            services = WorkerRuntimeServices(
                new_identifier=_identifier,
                utc_now_ns=time_ns,
                monotonic_ns=perf_counter_ns,
                code_provenance=lambda: collect_code_provenance_async(_repository_root()),
                invocation=process_invocation,
            )
            remaining = set(work_identifiers)

            async def worker() -> None:
                worker_identifier = _identifier()
                while remaining:
                    claim = await evidence_database.claim_work(
                        supported_capabilities=(
                            WorkCapability(
                                kind=ANALYZE_ITEM_WORK_KIND,
                                payload_schema_version=ANALYZE_ITEM_WORK_SCHEMA_VERSION,
                            ),
                        ),
                        worker_identifier=worker_identifier,
                        lease_token=_identifier(),
                        lease_duration_ns=settings.lease_duration_ns,
                        utc_now_ns=time_ns,
                        event_identifier=_identifier(),
                        eligible_identifiers=tuple(remaining),
                    )
                    if claim.lease is not None:
                        await execute_lease(
                            database=evidence_database,
                            registry=registry,
                            settings=settings,
                            services=services,
                            lease=claim.lease,
                        )
                        state = await evidence_database.work_state(claim.lease.work_item_identifier)
                        if state not in {WorkState.PENDING, WorkState.LEASED}:
                            remaining.discard(claim.lease.work_item_identifier)
                        continue
                    remaining.intersection_update(
                        {
                            identifier
                            for identifier in tuple(remaining)
                            if await evidence_database.work_state(identifier)
                            in {WorkState.PENDING, WorkState.LEASED}
                        }
                    )
                    if remaining:
                        await anyio.sleep(settings.idle_poll_interval_ns / 1_000_000_000)

            async with anyio.create_task_group() as task_group:
                for _ in range(min(settings.worker_count, len(remaining))):
                    task_group.start_soon(worker)
            completed = [
                await evidence_database.work(identifier) for identifier in work_identifiers
            ]
            failures = sum(work["state"] != WorkState.COMPLETED.value for work in completed)
            return {
                "state": "completed" if failures == 0 else "terminal_failure",
                "product_guide_record_identifier": guide_record_identifier,
                "selected": len(work_identifiers),
                "resumed": len(outstanding),
                "completed": len(work_identifiers) - failures,
                "failed": failures,
                "skipped_existing": skipped_existing,
                "skipped_missing_images": skipped_missing_images,
                "skipped_unavailable": skipped_unavailable,
                "results": [work["result"] for work in completed],
            }

    _print_work_result(anyio.run(perform, backend="trio"))


@app.command
def search(
    query: str,
    *,
    facebook_location: str,
    radius: int,
    maximum_results: int | None = None,
    maximum_pages: int | None = None,
    proton_route: str = _DEFAULT_PROTON_ROUTE,
    radius_unit: SearchDistanceUnit = SearchDistanceUnit.MILES,
    location_label: str | None = None,
    minimum_price: Decimal | None = None,
    maximum_price: Decimal | None = None,
    currency: str = "USD",
    exact_match: bool = False,
    maximum_elapsed_seconds: float | None = None,
    maximum_transferred_bytes: int | None = None,
    maximum_decoded_body_bytes: int | None = None,
    maximum_consecutive_pages_without_new_listings: int | None = None,
    requested_page_size: int | None = None,
    price_partition_width: Decimal | None = None,
    price_partition_overlap: Decimal | None = None,
    price_partition_order: SearchPricePartitionOrder = SearchPricePartitionOrder.BALANCED,
    database: Path = _DEFAULT_DATABASE,
) -> None:
    """Queue one bounded Facebook Marketplace search for an explicit worker pool."""

    async def perform() -> dict[str, JsonValue]:
        if maximum_results is None and maximum_pages is None:
            raise ValueError("Specify --maximum-results, --maximum-pages, or both")
        if not proton_route or proton_route != proton_route.strip():
            raise ValueError("The Proton route identifier must be nonempty and trimmed")
        price = (
            None
            if minimum_price is None and maximum_price is None
            else SearchPriceRange(
                currency=currency,
                minimum=minimum_price,
                maximum=maximum_price,
            )
        )
        if price_partition_width is None:
            if price_partition_overlap is not None:
                raise ValueError("--price-partition-overlap requires --price-partition-width")
            traversal_strategy = CursorSearchTraversalStrategy()
        else:
            traversal_strategy = OverlappingPricePartitionSearchTraversalStrategy(
                width=price_partition_width,
                overlap=(
                    Decimal(0) if price_partition_overlap is None else price_partition_overlap
                ),
                order=price_partition_order,
            )
        request = CreateSearchRequest(
            request=FacebookSearchRequest(
                query=query,
                location=SearchFacebookLocation(
                    identifier=facebook_location,
                    label=location_label,
                ),
                radius=SearchRadius(value=radius, unit=radius_unit),
                price=price,
                exact_match=exact_match,
            ),
            traversal=SearchTraversalPolicy(
                maximum_pages=maximum_pages,
                maximum_results=maximum_results,
                maximum_elapsed_duration_ns=(
                    None
                    if maximum_elapsed_seconds is None
                    else int(maximum_elapsed_seconds * 1_000_000_000)
                ),
                maximum_transferred_bytes=maximum_transferred_bytes,
                maximum_decoded_body_bytes=maximum_decoded_body_bytes,
                maximum_consecutive_pages_without_new_listings=(
                    maximum_consecutive_pages_without_new_listings
                ),
                requested_page_size=requested_page_size,
            ),
            traversal_strategy=traversal_strategy,
            proton_route=proton_route,
        )
        async with Database.managed(database, initialize=True) as evidence_database:
            application = ReviewApplication(
                database=evidence_database,
                repository_root=_repository_root(),
            )
            result = await application.create_search(request)
            return result.model_dump(mode="json")

    _print_json(anyio.run(perform, backend="trio"))


@app.command
def extract(acquisition_record_id: str, *, database: Path = _DEFAULT_DATABASE) -> None:
    """Rerun extraction from a saved acquisition."""

    async def perform() -> dict[str, object]:
        async with Database.managed(database) as evidence_database:
            return await extract_acquisition(evidence_database, acquisition_record_id)

    _print_json(anyio.run(perform, backend="trio"))


@app.command
def extract_image(acquisition_record_id: str, *, database: Path = _DEFAULT_DATABASE) -> None:
    """Validate a saved image response again without making an HTTP request."""

    async def perform() -> dict[str, JsonValue]:
        settings = WorkerSettings(
            worker_count=1,
            lease_duration_ns=600_000_000_000,
            renewal_interval_ns=30_000_000_000,
            idle_poll_interval_ns=100_000_000,
        )
        async with Database.managed(database) as evidence_database:
            kind, _, acquisition = await evidence_database.get_record(acquisition_record_id)
            if kind != ("carl", "http", "acquisition") or not isinstance(acquisition, dict):
                raise ValueError("Input is not an HTTP acquisition record")
            reference_identifier = acquisition.get("image_reference_record_identifier")
            if not isinstance(reference_identifier, str):
                raise ValueError("Acquisition has no gallery image reference")
            kind, _, _ = await evidence_database.get_record(reference_identifier)
            if kind != ("carl", "facebook", "gallery_image_reference"):
                raise ValueError("Acquisition has no valid gallery image reference")
            payload = ExtractImagePayload(
                acquisition_record_identifier=acquisition_record_id,
                reference_record_identifier=reference_identifier,
            )
            enqueued = await evidence_database.enqueue_work(
                extract_image_work(identifier=_identifier(), payload=payload, not_before_utc_ns=0),
                WorkRequester(
                    request_identifier=_identifier(),
                    kind=("carl", "cli", "offline_image_extraction"),
                    identifier=acquisition_record_id,
                    context={"invocation": process_invocation()},
                ),
                event_identifier=_identifier(),
                enqueued_at_utc_ns=time_ns(),
            )
            registry = build_image_worker_registry(
                ImageWorkerDependencies(
                    database=evidence_database,
                    acquirer=DirectHttpxAcquirer(),
                    image_files=ImageFileStore(evidence_database.path.parent),
                    new_identifier=_identifier,
                )
            )
            services = WorkerRuntimeServices(
                new_identifier=_identifier,
                utc_now_ns=time_ns,
                monotonic_ns=perf_counter_ns,
                code_provenance=lambda: collect_code_provenance_async(_repository_root()),
                invocation=process_invocation,
            )
            while await evidence_database.work_state(enqueued.work_item_identifier) in {
                WorkState.PENDING,
                WorkState.LEASED,
            }:
                claim = await evidence_database.claim_work(
                    supported_capabilities=(
                        WorkCapability(
                            kind=EXTRACT_IMAGE_WORK_KIND,
                            payload_schema_version=EXTRACT_IMAGE_WORK_SCHEMA_VERSION,
                        ),
                    ),
                    worker_identifier=_identifier(),
                    lease_token=_identifier(),
                    lease_duration_ns=settings.lease_duration_ns,
                    utc_now_ns=time_ns,
                    event_identifier=_identifier(),
                )
                if claim.lease is not None:
                    await execute_lease(
                        database=evidence_database,
                        registry=registry,
                        settings=settings,
                        services=services,
                        lease=claim.lease,
                    )
                else:
                    await anyio.sleep(settings.idle_poll_interval_ns / 1_000_000_000)
            return await evidence_database.work(enqueued.work_item_identifier)

    _print_work_result(anyio.run(perform, backend="trio"))


@app.command
def operation(operation_id: str, *, database: Path = _DEFAULT_DATABASE) -> None:
    """Show an operation and its provenance."""

    async def perform() -> dict[str, object]:
        async with Database.managed(database) as evidence_database:
            return await evidence_database.operation(operation_id)

    _print_json(anyio.run(perform, backend="trio"))


@app.command
def mcp(*, database: Path = _DEFAULT_DATABASE) -> None:
    """Serve Carl's queueing and review tools over MCP stdio."""

    anyio.run(serve_stdio, database, _repository_root(), backend="trio")


@app.command
def work(*, database: Path = _DEFAULT_DATABASE) -> None:
    """Process Carl's durable work queue until interrupted."""

    with suppress(KeyboardInterrupt):
        anyio.run(work_forever, database, _repository_root(), backend="trio")


def main() -> None:
    try:
        app()
    except (
        AcquisitionFailure,
        ConfigurationFailure,
        KeyError,
        OSError,
        RuntimeError,
        apsw.Error,
        UnicodeError,
        ValidationError,
        ValueError,
    ) as error:
        print(f"{type(error).__name__}: {error}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
