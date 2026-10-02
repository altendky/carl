"""Durable worker adapter for configured bounded eBay searches."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from time import time_ns
from typing import cast

from carl.core.ebay import (
    COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION,
    COLLECT_EBAY_SEARCH_WORK_KIND,
    EBAY_SEARCH_FAILURE_KINDS,
    LEGACY_COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION,
    CollectEbaySearchPayload,
)
from carl.core.ebay_search_support import (
    UnsupportedEbaySearchMode,
    require_supported_ebay_search_acquisition,
)
from carl.core.models import JsonValue, NamedInput
from carl.core.work import WorkCapability
from carl.core.worker import (
    AttemptContext,
    CompletedWork,
    RetryWork,
    TerminalFailureWork,
    WorkOutcome,
)
from carl.ebay import (
    RUN_EBAY_SEARCH_WORK,
    build_ebay_component_registry,
    collect_configured_ebay_search,
)
from carl.io.configuration import ConfigurationFailure
from carl.io.httpx import AcquisitionFailure, RouteConfigurationFailure
from carl.io.paths import CarlDirectories
from carl.io.sqlite import Database
from carl.io.worker import TypedWorkHandler, WorkHandlerRegistry


@dataclass(frozen=True, slots=True)
class EbaySearchWorkerDependencies:
    database: Database
    directories: CarlDirectories
    new_identifier: Callable[[], str]
    collector: Callable[..., Awaitable[dict[str, JsonValue]]] = collect_configured_ebay_search
    utc_now_ns: Callable[[], int] = time_ns


async def _collect(
    payload: CollectEbaySearchPayload,
    context: AttemptContext,
    dependencies: EbaySearchWorkerDependencies,
) -> WorkOutcome:
    try:
        require_supported_ebay_search_acquisition(payload.request)
    except UnsupportedEbaySearchMode as error:
        return TerminalFailureWork(
            error={
                "kind": "unsupported_ebay_search_mode",
                "listing_state": payload.request.listing_state,
                "message": str(error),
            },
            result={"state": "unsupported"},
        )
    policy_attempt = context.retry_attempt(payload.retry_attempt_offset)
    if policy_attempt < 1:
        return TerminalFailureWork(
            error={
                "kind": "invalid_retry_attempt_offset",
                "attempt": context.attempt,
                "retry_attempt_offset": payload.retry_attempt_offset,
            },
            result={"state": "configuration_failed"},
        )
    try:
        result = await dependencies.collector(
            dependencies.database,
            directories=dependencies.directories,
            request=payload.request,
            new_identifier=dependencies.new_identifier,
        )
    except ConfigurationFailure as error:
        return TerminalFailureWork(
            error={"kind": "configuration_failure", "code": error.code},
            result={"state": "configuration_failed"},
        )
    except RouteConfigurationFailure as error:
        return TerminalFailureWork(
            error={"kind": "route_configuration_failure", "code": error.code},
            result={"state": "configuration_failed"},
        )
    except AcquisitionFailure as error:
        if error.result.get("stopping_condition") == "transport_failure":
            reason: dict[str, JsonValue] = {
                "kind": "ebay_search_acquisition_failure",
                "attempt": context.attempt,
                "policy_attempt": policy_attempt,
                "retry_attempt_offset": payload.retry_attempt_offset,
                "maximum_attempts": 3,
            }
            if policy_attempt < 3:
                return RetryWork(
                    delay_ns=2 ** (policy_attempt - 1) * 1_000_000_000,
                    reason=reason,
                    result={"state": "retryable_failure", "acquisition": error.result},
                )
            return TerminalFailureWork(
                error=reason,
                result={"state": "acquisition_failed", "acquisition": error.result},
            )
        return TerminalFailureWork(
            error={"kind": "ebay_search_acquisition_failure"},
            result={"state": "acquisition_failed", "acquisition": error.result},
        )

    # The collector's child operations already own these records. Link their
    # evidence into this work operation without publishing it a second time.
    inputs: list[NamedInput] = []
    for name, key in (
        ("acquisition", "acquisition_record_identifiers"),
        ("extraction", "extraction_record_identifiers"),
    ):
        identifiers = result.get(key)
        if isinstance(identifiers, list):
            identifier_values = cast(list[JsonValue], identifiers)
            typed_identifiers = tuple(
                identifier for identifier in identifier_values if isinstance(identifier, str)
            )
            inputs.extend(
                NamedInput(
                    name=("search", "page", str(index), name),
                    object_identifier=identifier,
                )
                for index, identifier in enumerate(typed_identifiers, start=1)
            )
    search_run_identifier = result.get("search_run_record_identifier")
    if isinstance(search_run_identifier, str):
        inputs.append(
            NamedInput(
                name=("search", "run"),
                object_identifier=search_run_identifier,
            )
        )
    classification = result.get("response_classification")
    kind = (
        cast(dict[str, JsonValue], classification).get("kind")
        if isinstance(classification, dict)
        else None
    )
    if (
        (isinstance(kind, str) and kind in EBAY_SEARCH_FAILURE_KINDS)
        or result.get("state") == "response_failed"
        or result.get("stopping_reason") == "invalid_next_page"
    ):
        reason = {
            "kind": "ebay_search_response_failure",
            "response_classification": classification,
            "stopping_reason": result.get("stopping_reason"),
            "attempt": context.attempt,
            "policy_attempt": policy_attempt,
            "retry_attempt_offset": payload.retry_attempt_offset,
            "maximum_attempts": 3,
        }
        evidence = (
            cast(dict[str, JsonValue], classification).get("evidence")
            if isinstance(classification, dict)
            else None
        )
        blocked = kind == "challenge" or (
            isinstance(evidence, list)
            and any(marker in evidence for marker in ("http_status_403", "http_status_429"))
        )
        delay_ns = (120 if blocked else 1) * 2 ** (policy_attempt - 1) * 1_000_000_000
        if blocked:
            reason["cooldown"] = {
                "stack_identifier": payload.request.stack_identifier,
                "until_utc_ns": dependencies.utc_now_ns() + delay_ns,
            }
        if policy_attempt < 3:
            return RetryWork(
                inputs=tuple(inputs),
                delay_ns=delay_ns,
                reason=reason,
                result={**result, "state": "retryable_failure"},
            )
        return TerminalFailureWork(
            inputs=tuple(inputs),
            error=reason,
            result={**result, "state": "terminal_failure"},
        )
    return CompletedWork(inputs=tuple(inputs), result=result)


def build_ebay_worker_registry(dependencies: EbaySearchWorkerDependencies) -> WorkHandlerRegistry:
    component = build_ebay_component_registry().require(RUN_EBAY_SEARCH_WORK)
    return WorkHandlerRegistry(
        handlers=tuple(
            TypedWorkHandler(
                capability=WorkCapability(
                    kind=COLLECT_EBAY_SEARCH_WORK_KIND,
                    payload_schema_version=schema_version,
                ),
                component=component,
                payload_type=CollectEbaySearchPayload,
                handler=lambda payload, context: _collect(payload, context, dependencies),
            )
            for schema_version in (
                LEGACY_COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION,
                COLLECT_EBAY_SEARCH_PAYLOAD_SCHEMA_VERSION,
            )
        )
    )
