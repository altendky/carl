"""Route-aware handlers for ordinary Facebook collection work."""

from __future__ import annotations

from collections.abc import Callable
from secrets import randbelow
from time import perf_counter_ns, time_ns

import anyio

from carl.core.facebook_images import (
    COLLECT_IMAGE_WORK_KIND,
    COLLECT_IMAGE_WORK_SCHEMA_VERSION,
    EXTRACT_IMAGE_WORK_KIND,
    EXTRACT_IMAGE_WORK_SCHEMA_VERSION,
    LEGACY_COLLECT_IMAGE_WORK_SCHEMA_VERSION,
    CollectImagePayload,
    ExtractImagePayload,
    image_network_constraints,
    legacy_image_network_constraint_identifiers,
)
from carl.core.facebook_work import (
    COLLECT_ITEM_PAYLOAD_SCHEMA_VERSION,
    COLLECT_ITEM_WORK_KIND,
    COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    COLLECT_SEARCH_WORK_KIND,
    EXTRACT_ITEM_PAYLOAD_SCHEMA_VERSION,
    EXTRACT_ITEM_WORK_KIND,
    LEGACY_COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    PREVIOUS_COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    CollectItemPayload,
    CollectSearchPayload,
    ExtractItemPayload,
    facebook_network_policy_constraints,
    legacy_facebook_network_constraint_identifiers,
)
from carl.core.work import WorkCapability
from carl.core.worker import AttemptContext, TerminalFailureWork, WorkOutcome
from carl.facebook_image_workers import (
    COLLECT_FACEBOOK_IMAGE,
    EXTRACT_FACEBOOK_IMAGE,
    ImageWorkerDependencies,
    build_image_component_registry,
    build_image_worker_registry,
    image_session_failure_work,
)
from carl.facebook_search_workers import FacebookSearchWorkerDependencies
from carl.facebook_workers import (
    COLLECT_FACEBOOK_ITEM,
    COLLECT_FACEBOOK_SEARCH,
    EXTRACT_FACEBOOK,
    FacebookWorkerDependencies,
    build_component_registry,
    build_facebook_worker_registry,
)
from carl.io.browser_identity import brave_navigation_headers
from carl.io.configuration import (
    ConfigurationFailure,
    decodo_settings,
    load_configuration,
    proton_settings,
)
from carl.io.decodo import DecodoSessionManager, ManagedDecodoHttpAcquirer
from carl.io.facebook_images import FacebookImageSessionFailure, ProtonFacebookImageSessionFactory
from carl.io.facebook_search import ProtonFacebookSearchSessionFactory
from carl.io.httpx import DirectHttpxAcquirer, RouteConfigurationFailure
from carl.io.image_files import ImageFileStore
from carl.io.network_activity import NetworkActivityScheduler
from carl.io.paths import CarlDirectories
from carl.io.proton import ProtonSessionManager, ProtonWireproxyManager
from carl.io.sqlite import Database
from carl.io.worker import TypedWorkHandler, WorkHandlerRegistry

_NETWORK_PERMIT_DURATION_NS = 600_000_000_000


def _sample_uniform_holdoff_ns(minimum_ns: int, maximum_ns: int) -> int:
    return minimum_ns + randbelow(maximum_ns - minimum_ns + 1)


def _scheduler(database: Database, new_identifier: Callable[[], str]) -> NetworkActivityScheduler:
    return NetworkActivityScheduler(
        database=database,
        new_identifier=new_identifier,
        utc_now_ns=time_ns,
        sample_uniform_holdoff_ns=_sample_uniform_holdoff_ns,
        permit_duration_ns=_NETWORK_PERMIT_DURATION_NS,
    )


async def _execute(
    registry: WorkHandlerRegistry,
    capability: WorkCapability,
    payload: (
        CollectSearchPayload
        | CollectItemPayload
        | ExtractItemPayload
        | CollectImagePayload
        | ExtractImagePayload
    ),
    context: AttemptContext,
) -> WorkOutcome:
    for handler in registry.handlers:
        if handler.capability == capability:
            return await handler.execute(payload.model_dump(mode="json"), context)
    raise KeyError((capability.kind, capability.payload_schema_version))


def build_routed_facebook_worker_registry(
    *,
    database: Database,
    directories: CarlDirectories,
    new_identifier: Callable[[], str],
    proton_manager: ProtonSessionManager | None = None,
) -> WorkHandlerRegistry:
    """Build independently claimable handlers that resolve routing per work payload."""

    resolved_proton_manager = proton_manager or ProtonWireproxyManager()

    item_capability = WorkCapability(
        kind=COLLECT_ITEM_WORK_KIND,
        payload_schema_version=COLLECT_ITEM_PAYLOAD_SCHEMA_VERSION,
    )
    item_extraction_capability = WorkCapability(
        kind=EXTRACT_ITEM_WORK_KIND,
        payload_schema_version=EXTRACT_ITEM_PAYLOAD_SCHEMA_VERSION,
    )
    search_capability = WorkCapability(
        kind=COLLECT_SEARCH_WORK_KIND,
        payload_schema_version=COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    )
    legacy_search_capability = WorkCapability(
        kind=COLLECT_SEARCH_WORK_KIND,
        payload_schema_version=LEGACY_COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    )
    previous_search_capability = WorkCapability(
        kind=COLLECT_SEARCH_WORK_KIND,
        payload_schema_version=PREVIOUS_COLLECT_SEARCH_PAYLOAD_SCHEMA_VERSION,
    )
    image_capability = WorkCapability(
        kind=COLLECT_IMAGE_WORK_KIND,
        payload_schema_version=COLLECT_IMAGE_WORK_SCHEMA_VERSION,
    )
    legacy_image_capability = WorkCapability(
        kind=COLLECT_IMAGE_WORK_KIND,
        payload_schema_version=LEGACY_COLLECT_IMAGE_WORK_SCHEMA_VERSION,
    )
    image_extraction_capability = WorkCapability(
        kind=EXTRACT_IMAGE_WORK_KIND,
        payload_schema_version=EXTRACT_IMAGE_WORK_SCHEMA_VERSION,
    )

    direct_facebook_registry = build_facebook_worker_registry(
        FacebookWorkerDependencies(
            database=database,
            acquirer=DirectHttpxAcquirer(),
            new_identifier=new_identifier,
        )
    )
    direct_image_registry = build_image_worker_registry(
        ImageWorkerDependencies(
            database=database,
            acquirer=DirectHttpxAcquirer(),
            image_files=ImageFileStore(database.path.parent),
            new_identifier=new_identifier,
        )
    )

    async def collect_search(payload: CollectSearchPayload, context: AttemptContext) -> WorkOutcome:
        try:
            await database.supersede_constraints(
                retired_identifiers=legacy_facebook_network_constraint_identifiers(payload.routing),
                replacements=facebook_network_policy_constraints(payload.routing),
                operation_identifier=context.operation_identifier,
                at_utc_ns=time_ns(),
                reason="Apply the current Marketplace page-request policy",
            )
            loaded = load_configuration(directories.configuration_file)
            route_settings = proton_settings(loaded, directories, payload.routing)
            headers = await anyio.to_thread.run_sync(
                brave_navigation_headers, abandon_on_cancel=True
            )
            registry = build_facebook_worker_registry(
                FacebookWorkerDependencies(
                    database=database,
                    acquirer=DirectHttpxAcquirer(),
                    new_identifier=new_identifier,
                ),
                FacebookSearchWorkerDependencies(
                    database=database,
                    session_factory=ProtonFacebookSearchSessionFactory(
                        manager=resolved_proton_manager, settings=route_settings
                    ),
                    navigation_headers=headers,
                    new_identifier=new_identifier,
                    utc_now_ns=time_ns,
                    monotonic_ns=perf_counter_ns,
                    network_activity_scheduler=_scheduler(database, new_identifier),
                ),
            )
            return await _execute(registry, search_capability, payload, context)
        except RouteConfigurationFailure as error:
            return TerminalFailureWork(
                error={
                    "kind": "route_configuration_failure",
                    "code": error.code,
                    "decision": "terminal",
                },
                result={
                    "state": "search_session_failed",
                    "session_failure": {
                        "kind": "route_configuration_failure",
                        "code": error.code,
                    },
                },
            )
        except ConfigurationFailure as error:
            return TerminalFailureWork(
                error={
                    "kind": "configuration_failure",
                    "code": error.code,
                    "decision": "terminal",
                },
                result={
                    "state": "search_session_failed",
                    "session_failure": {
                        "kind": "configuration_failure",
                        "code": error.code,
                    },
                },
            )

    async def collect_item(payload: CollectItemPayload, context: AttemptContext) -> WorkOutcome:
        loaded = load_configuration(directories.configuration_file)
        route_settings, credential_source = decodo_settings(
            loaded, directories, payload.request_plan.routing
        )
        registry = build_facebook_worker_registry(
            FacebookWorkerDependencies(
                database=database,
                acquirer=ManagedDecodoHttpAcquirer(
                    manager=DecodoSessionManager(),
                    settings=route_settings,
                    credential_source=credential_source,
                ),
                new_identifier=new_identifier,
                network_activity_scheduler=_scheduler(database, new_identifier),
            )
        )
        return await _execute(registry, item_capability, payload, context)

    async def collect_image(payload: CollectImagePayload, context: AttemptContext) -> WorkOutcome:
        try:
            await database.supersede_constraints(
                retired_identifiers=legacy_image_network_constraint_identifiers(
                    payload.request_plan.routing
                ),
                replacements=image_network_constraints(payload.request_plan.routing),
                operation_identifier=context.operation_identifier,
                at_utc_ns=time_ns(),
                reason="Share Proton transport across bounded concurrent image requests",
            )
            loaded = load_configuration(directories.configuration_file)
            route_settings = proton_settings(loaded, directories, payload.request_plan.routing)
            session_identifier = new_identifier()
            factory = ProtonFacebookImageSessionFactory(
                manager=resolved_proton_manager, settings=route_settings
            )
            async with factory(session_identifier) as session:
                registry = build_image_worker_registry(
                    ImageWorkerDependencies(
                        database=database,
                        acquirer=session.acquirer,
                        image_files=ImageFileStore(database.path.parent),
                        new_identifier=new_identifier,
                        network_activity_scheduler=_scheduler(database, new_identifier),
                        network_session_identifier=session_identifier,
                    )
                )
                return await _execute(registry, image_capability, payload, context)
        except FacebookImageSessionFailure as error:
            return image_session_failure_work(
                code=error.code,
                diagnostic=error.diagnostic,
                exit_code=error.exit_code,
                context=context,
            )
        except RouteConfigurationFailure as error:
            return TerminalFailureWork(
                error={
                    "kind": "route_configuration_failure",
                    "code": error.code,
                    "decision": "terminal",
                },
                result={
                    "state": "image_session_failed",
                    "session_failure": {
                        "kind": "route_configuration_failure",
                        "code": error.code,
                    },
                },
            )
        except ConfigurationFailure as error:
            return TerminalFailureWork(
                error={
                    "kind": "configuration_failure",
                    "code": error.code,
                    "decision": "terminal",
                },
                result={
                    "state": "image_session_failed",
                    "session_failure": {
                        "kind": "configuration_failure",
                        "code": error.code,
                    },
                },
            )

    facebook_components = build_component_registry()
    image_components = build_image_component_registry()

    return WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=legacy_search_capability,
                component=facebook_components.require(COLLECT_FACEBOOK_SEARCH),
                payload_type=CollectSearchPayload,
                handler=collect_search,
            ),
            TypedWorkHandler(
                capability=previous_search_capability,
                component=facebook_components.require(COLLECT_FACEBOOK_SEARCH),
                payload_type=CollectSearchPayload,
                handler=collect_search,
            ),
            TypedWorkHandler(
                capability=search_capability,
                component=facebook_components.require(COLLECT_FACEBOOK_SEARCH),
                payload_type=CollectSearchPayload,
                handler=collect_search,
            ),
            TypedWorkHandler(
                capability=item_capability,
                component=facebook_components.require(COLLECT_FACEBOOK_ITEM),
                payload_type=CollectItemPayload,
                handler=collect_item,
            ),
            TypedWorkHandler(
                capability=item_extraction_capability,
                component=facebook_components.require(EXTRACT_FACEBOOK),
                payload_type=ExtractItemPayload,
                handler=lambda payload, context: _execute(
                    direct_facebook_registry, item_extraction_capability, payload, context
                ),
            ),
            TypedWorkHandler(
                capability=legacy_image_capability,
                component=image_components.require(COLLECT_FACEBOOK_IMAGE),
                payload_type=CollectImagePayload,
                handler=collect_image,
            ),
            TypedWorkHandler(
                capability=image_capability,
                component=image_components.require(COLLECT_FACEBOOK_IMAGE),
                payload_type=CollectImagePayload,
                handler=collect_image,
            ),
            TypedWorkHandler(
                capability=image_extraction_capability,
                component=image_components.require(EXTRACT_FACEBOOK_IMAGE),
                payload_type=ExtractImagePayload,
                handler=lambda payload, context: _execute(
                    direct_image_registry, image_extraction_capability, payload, context
                ),
            ),
        )
    )
