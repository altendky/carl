"""Durable identification of exact saved eBay item and gallery evidence."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, cast
from uuid import NAMESPACE_URL, uuid5

import anyio
import apsw

from carl.core.components import Component, ComponentId, Registry
from carl.core.ebay_analysis import (
    ANALYZE_EBAY_ITEM_WORK_KIND,
    EBAY_ANALYSIS_RECIPE_VERSION,
    EbayAnalysisEvidenceSelection,
    EbayAnalyzeItemPayload,
    analyze_ebay_item_work,
)
from carl.core.item_analysis import (
    ANALYSIS_TIMEOUT_MAXIMUM_ATTEMPTS,
    ANALYSIS_TIMEOUT_RETRY_BASE_DELAY_NS,
    AnalysisImageSelection,
    AnalysisLimitEnforcement,
    AnalysisLimitStatus,
    ProductGuideRecord,
    UnavailableAnalysisImageSelection,
    analysis_limit_observations,
    identification_prompt,
)
from carl.core.json import encode_json
from carl.core.models import (
    BytesDraft,
    CodeProvenance,
    JsonValue,
    NamedInput,
    NamedOutput,
    RecordDraft,
)
from carl.core.review import ProductGuideDetails, RequestAnalysisRequest, RequestAnalysisResult
from carl.core.review_errors import IncompleteGalleryError, ReviewInputError
from carl.core.work import WorkCapability, WorkRequester
from carl.core.worker import (
    AttemptContext,
    CompletedWork,
    RetryWork,
    TerminalFailureWork,
    WorkOutcome,
)
from carl.facebook_analysis_workers import (
    AUTHOR_PRODUCT_GUIDE,
    REGISTER_PRODUCT_GUIDE,
    AnalysisWorkerDependencies,
)
from carl.io.claude import temporary_analysis_directory
from carl.io.provenance import process_invocation
from carl.io.sqlite import Database
from carl.io.worker import TypedWorkHandler, WorkHandlerRegistry

ANALYZE_EBAY_ITEM = ComponentId(("carl", "ebay", "analyze", "listing_identification"))
PLAN_EBAY_LISTING_ANALYSIS_EVIDENCE = ComponentId(
    ("carl", "ebay", "plan", "listing_analysis_evidence")
)
_EXTENSIONS = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
}


class EbayAnalysisApplication(Protocol):
    @property
    def database(self) -> Database: ...

    @property
    def new_identifier(self) -> Callable[[], str]: ...

    @property
    def utc_now_ns(self) -> Callable[[], int]: ...

    @property
    def monotonic_ns(self) -> Callable[[], int]: ...

    async def _active_product_guide(self, record_identifier: str) -> ProductGuideDetails: ...

    async def _code_provenance(self) -> CodeProvenance: ...


def _anchor() -> None:
    """Identity anchor for the eBay analysis recipe."""


def build_ebay_analysis_component_registry() -> Registry:
    return Registry(
        (
            Component(ANALYZE_EBAY_ITEM, 1, _anchor),
            Component(PLAN_EBAY_LISTING_ANALYSIS_EVIDENCE, 1, _anchor),
        )
    )


def _gallery_urls(observation: dict[str, JsonValue]) -> tuple[str, ...]:
    urls = observation.get("gallery_urls")
    if not isinstance(urls, list) or not all(isinstance(url, str) for url in urls):
        raise ValueError("eBay observation gallery URLs are malformed")
    return tuple(cast(list[str], urls))


async def select_ebay_analysis_evidence(
    database: Database, observation_identifier: str, *, allow_incomplete_gallery: bool = False
) -> EbayAnalysisEvidenceSelection:
    """Select only exact observation-owned references, results and seller description."""

    kind, _, observation = await database.get_record(observation_identifier)
    if kind != ("carl", "ebay", "listing_observation") or not isinstance(observation, dict):
        raise ReviewInputError("Analysis input is not an eBay listing observation")
    observation = cast(dict[str, JsonValue], observation)
    if observation.get("classification") != "detail":
        raise ReviewInputError("Analysis requires a full eBay item-detail observation")
    urls = _gallery_urls(observation)
    references: dict[int, tuple[str, dict[str, JsonValue]]] = {}
    for identifier, value in await database.records_by_kind(
        ("carl", "ebay", "gallery_image_reference")
    ):
        if (
            not isinstance(value, dict)
            or value.get("observation_record_identifier") != observation_identifier
        ):
            continue
        value = cast(dict[str, JsonValue], value)
        order = value.get("gallery_order")
        if (
            not isinstance(order, int)
            or isinstance(order, bool)
            or not 0 <= order < len(urls)
            or value.get("url") != urls[order]
            or value.get("item_identifier") != observation.get("item_identifier")
        ):
            raise ReviewInputError("eBay gallery reference does not match its item observation")
        if order in references:
            raise ReviewInputError("eBay observation has duplicate gallery references")
        references[order] = (identifier, value)
    results: dict[str, tuple[str, dict[str, JsonValue]]] = {}
    by_identifier = {identifier: value for identifier, value in references.values()}
    for identifier, value in await database.records_by_kind(("carl", "ebay", "image_result")):
        if not isinstance(value, dict) or value.get("state") != "saved":
            continue
        value = cast(dict[str, JsonValue], value)
        reference_identifier = value.get("reference_record_identifier")
        reference = (
            by_identifier.get(reference_identifier)
            if isinstance(reference_identifier, str)
            else None
        )
        if (
            isinstance(reference_identifier, str)
            and reference is not None
            and value.get("observation_record_identifier") == observation_identifier
            and value.get("item_identifier") == observation.get("item_identifier")
            and value.get("url") == reference.get("url")
            and isinstance(value.get("image_artifact_identifier"), str)
        ):
            results[reference_identifier] = (identifier, value)
    included: list[AnalysisImageSelection] = []
    unavailable: list[UnavailableAnalysisImageSelection] = []
    inputs = [NamedInput(name=("listing_observation",), object_identifier=observation_identifier)]
    for order, (identifier, _) in sorted(references.items()):
        index = f"{order:08d}"
        inputs.append(
            NamedInput(name=("gallery_image_reference", index), object_identifier=identifier)
        )
        saved = results.get(identifier)
        if saved is None:
            unavailable.append(
                UnavailableAnalysisImageSelection(
                    gallery_image_reference_record_identifier=identifier,
                    gallery_order=order,
                    reason="no_saved_usable_image",
                )
            )
        else:
            included.append(
                AnalysisImageSelection(
                    gallery_image_reference_record_identifier=identifier,
                    image_result_record_identifier=saved[0],
                )
            )
            inputs.append(NamedInput(name=("image_result", index), object_identifier=saved[0]))
    unretained = tuple(order for order in range(len(urls)) if order not in references)
    absence = "listing_observation_has_no_gallery_references" if not urls else None
    unavailable_orders = tuple(
        sorted((*unretained, *(image.gallery_order for image in unavailable)))
    )
    if (unavailable_orders or absence) and not allow_incomplete_gallery:
        raise IncompleteGalleryError(unavailable_orders, absence)
    description_url = observation.get("description_url")
    description_state = "inline" if observation.get("description") else "not_available"
    if isinstance(description_url, str):
        description_state = "not_available"
        description_result: tuple[str, dict[str, JsonValue]] | None = None
        for identifier, value in await database.records_by_kind(
            ("carl", "ebay", "description_result")
        ):
            if (
                isinstance(value, dict)
                and value.get("observation_record_identifier") == observation_identifier
                and value.get("item_identifier") == observation.get("item_identifier")
                and value.get("url") == description_url
                and (
                    description_result is None
                    or value.get("state") == "saved"
                    or description_result[1].get("state") != "saved"
                )
            ):
                description_result = (identifier, value)
        if description_result is not None:
            inputs.append(
                NamedInput(name=("description_result",), object_identifier=description_result[0])
            )
            description_state = (
                "saved" if description_result[1].get("state") == "saved" else "failed"
            )
        else:
            work_identifiers = observation.get("description_work_identifiers", [])
            if isinstance(work_identifiers, list):
                for identifier in work_identifiers:
                    if isinstance(identifier, str) and (await database.work(identifier)).get(
                        "state"
                    ) in {"pending", "leased"}:
                        raise ReviewInputError(
                            "Wait for the eBay seller-description download before requesting analysis"
                        )
    return EbayAnalysisEvidenceSelection(
        value={
            "unavailable_gallery_images": [image.model_dump(mode="json") for image in unavailable],
            "unretained_gallery_images": [
                {
                    "gallery_order": order,
                    "url": urls[order],
                    "reason": "gallery_reference_not_retained",
                }
                for order in unretained
            ],
            "gallery_absence_reason": absence,
            "description_state": description_state,
            "description_url": description_url,
        },
        inputs=tuple(inputs),
        included=tuple(included),
        unavailable=tuple(unavailable),
        unretained_gallery_orders=unretained,
        gallery_absence_reason=absence,
    )


async def request_ebay_listing_analysis(
    application: EbayAnalysisApplication,
    request: RequestAnalysisRequest,
    *,
    requester_kind: tuple[str, ...] = ("carl", "mcp", "request_listing_analysis"),
    requester_identifier: str | None = None,
    requester_context: JsonValue | None = None,
) -> RequestAnalysisResult:
    """Publish a stable selection and enqueue the same durable analysis workflow."""

    _ = await application._active_product_guide(request.product_guide_record_identifier)
    selection = await select_ebay_analysis_evidence(
        application.database,
        request.listing_observation_record_identifier,
        allow_incomplete_gallery=request.allow_incomplete_gallery,
    )
    # A deterministic selection identifier prevents concurrent requests from creating
    # different AI subjects from identical immutable evidence edges.
    evidence_identifier = str(
        uuid5(NAMESPACE_URL, "carl:ebay:analysis-evidence:" + selection.evidence_identity)
    )
    try:
        _ = await application.database.get_record(evidence_identifier)
    except KeyError:
        started_utc_ns = application.utc_now_ns()
        started_monotonic_ns = application.monotonic_ns()
        provenance = await application._code_provenance()
        try:
            await application.database.publish_records_operation(
                component=build_ebay_analysis_component_registry().require(
                    PLAN_EBAY_LISTING_ANALYSIS_EVIDENCE
                ),
                operation_identifier=application.new_identifier(),
                records=(
                    RecordDraft(
                        identifier=evidence_identifier,
                        kind=("carl", "ebay", "listing_analysis_evidence"),
                        schema_version=1,
                        value=selection.value,
                    ),
                ),
                inputs=selection.inputs,
                outputs=(
                    NamedOutput(
                        name=("listing_analysis_evidence",), object_identifier=evidence_identifier
                    ),
                ),
                provenance=provenance,
                invocation=process_invocation(),
                started_at_utc=datetime.fromtimestamp(
                    started_utc_ns / 1_000_000_000, tz=UTC
                ).isoformat(),
                ended_at_utc=datetime.fromtimestamp(
                    application.utc_now_ns() / 1_000_000_000, tz=UTC
                ).isoformat(),
                duration_ns=max(0, application.monotonic_ns() - started_monotonic_ns),
                result={"state": "completed"},
            )
        except apsw.ConstraintError:
            # Publication is transactional. Only an identical selection published by
            # a competing request is a valid resolution of this constraint failure.
            kind, version, value = await application.database.get_record(evidence_identifier)
            _, inputs, _ = await application.database.object_operation_relations(
                evidence_identifier
            )
            if (
                kind != ("carl", "ebay", "listing_analysis_evidence")
                or version != 1
                or value != selection.value
                or set(inputs) != set(selection.inputs)
            ):
                raise
    payload = EbayAnalyzeItemPayload(
        evidence_set_record_identifier=evidence_identifier,
        product_guide_record_identifier=request.product_guide_record_identifier,
        maximum_turns=max(8, len(selection.included) + 5),
    )
    work_requester = WorkRequester(
        request_identifier=application.new_identifier(),
        kind=requester_kind,
        identifier=requester_identifier or request.listing_observation_record_identifier,
        context=requester_context
        if requester_context is not None
        else {"request": request.model_dump(mode="json")},
    )
    existing_work_identifier = await application.database.ebay_item_analysis_work_identifier(
        payload
    )
    if existing_work_identifier is not None:
        await application.database.attach_work_request(
            existing_work_identifier,
            work_requester,
            event_identifier=application.new_identifier(),
            requested_at_utc_ns=application.utc_now_ns(),
        )
        return RequestAnalysisResult(
            work_identifier=existing_work_identifier,
            created=False,
            evidence_set_record_identifier=evidence_identifier,
            included_gallery_count=len(selection.included),
            unavailable_gallery_orders=selection.unavailable_gallery_orders,
            gallery_absence_reason=selection.gallery_absence_reason,
        )
    enqueued = await application.database.enqueue_work(
        analyze_ebay_item_work(identifier=application.new_identifier(), payload=payload),
        work_requester,
        event_identifier=application.new_identifier(),
        enqueued_at_utc_ns=application.utc_now_ns(),
    )
    return RequestAnalysisResult(
        work_identifier=enqueued.work_item_identifier,
        created=enqueued.created,
        evidence_set_record_identifier=evidence_identifier,
        included_gallery_count=len(selection.included),
        unavailable_gallery_orders=selection.unavailable_gallery_orders,
        gallery_absence_reason=selection.gallery_absence_reason,
    )


async def _guide_prompt(database: Database, identifier: str) -> str:
    kind, version, value = await database.get_record(identifier)
    if kind != ("carl", "analysis", "product_guide") or version not in {1, 2}:
        raise ValueError("Input is not a supported product guide record")
    _ = ProductGuideRecord.model_validate_json(encode_json(value))
    operation_identifier, _, outputs = await database.object_operation_relations(identifier)
    operation = await database.operation(operation_identifier)
    if (
        operation.get("component_parts")
        not in (list(REGISTER_PRODUCT_GUIDE.parts), list(AUTHOR_PRODUCT_GUIDE.parts))
        or operation.get("output_schema_version") != 1
        or operation.get("state") != "completed"
    ):
        raise ValueError("Product guide was not produced by a registered guide component")
    artifacts = [edge.object_identifier for edge in outputs if edge.name == ("guide_text",)]
    if len(artifacts) != 1:
        raise ValueError("Product guide has no unique exact-text artifact")
    metadata, content = await database.get_artifact(artifacts[0])
    if metadata.get("media_type") != "text/plain; charset=utf-8" or metadata.get(
        "representation"
    ) != {"exact_product_guide": True}:
        raise ValueError("Product guide text artifact has unsupported metadata")
    prompt = identification_prompt(content.decode("utf-8")).replace(
        "single saved Facebook Marketplace listing", "single saved eBay listing", 1
    )
    return (
        prompt + "\nIf listing.json.description_state is failed or not_available, explicitly state "
        "that the seller description is unavailable; any inline summary may be incomplete.\n"
    )


async def _analysis_input(
    database: Database, evidence_identifier: str
) -> tuple[dict[str, JsonValue], tuple[tuple[str, bytes], ...]]:
    """Validate retained selection edges again before exposing anything to Claude."""

    kind, version, value = await database.get_record(evidence_identifier)
    if (
        kind != ("carl", "ebay", "listing_analysis_evidence")
        or version != 1
        or not isinstance(value, dict)
    ):
        raise ValueError("Input is not supported eBay listing-analysis evidence")
    value = cast(dict[str, JsonValue], value)
    if set(value) != {
        "unavailable_gallery_images",
        "unretained_gallery_images",
        "gallery_absence_reason",
        "description_state",
        "description_url",
    }:
        raise ValueError("eBay analysis evidence metadata is malformed")
    _, edges, _ = await database.object_operation_relations(evidence_identifier)
    if len({edge.name for edge in edges}) != len(edges) or any(
        edge.name not in {("listing_observation",), ("description_result",)}
        and not (
            len(edge.name) == 2 and edge.name[0] in {"gallery_image_reference", "image_result"}
        )
        for edge in edges
    ):
        raise ValueError("eBay analysis evidence has duplicate or unknown input edges")
    observations = [
        edge.object_identifier for edge in edges if edge.name == ("listing_observation",)
    ]
    if len(observations) != 1:
        raise ValueError("eBay analysis evidence has no unique observation")
    observation_identifier = observations[0]
    kind, _, observation = await database.get_record(observation_identifier)
    if (
        kind != ("carl", "ebay", "listing_observation")
        or not isinstance(observation, dict)
        or observation.get("classification") != "detail"
    ):
        raise ValueError("eBay analysis evidence does not reference a full detail observation")
    observation = cast(dict[str, JsonValue], observation)
    urls = _gallery_urls(observation)
    references = {
        edge.name[1]: edge.object_identifier
        for edge in edges
        if len(edge.name) == 2 and edge.name[0] == "gallery_image_reference"
    }
    results = {
        edge.name[1]: edge.object_identifier
        for edge in edges
        if len(edge.name) == 2 and edge.name[0] == "image_result"
    }
    if not results.keys() <= references.keys():
        raise ValueError("eBay analysis evidence image edges are malformed")
    unavailable_raw = value["unavailable_gallery_images"]
    unretained = value["unretained_gallery_images"]
    if not isinstance(unavailable_raw, list) or not isinstance(unretained, list):
        raise ValueError("eBay unavailable-gallery metadata is malformed")
    unavailable = tuple(
        UnavailableAnalysisImageSelection.model_validate(image) for image in unavailable_raw
    )
    unavailable_by_reference = {
        image.gallery_image_reference_record_identifier: image for image in unavailable
    }
    if set(unavailable_by_reference) != {
        references[index] for index in references.keys() - results.keys()
    }:
        raise ValueError("eBay analysis evidence has incomplete gallery metadata")
    represented_orders: list[int] = []
    files: list[tuple[str, bytes]] = []
    for index, identifier in sorted(references.items()):
        kind, _, reference = await database.get_record(identifier)
        if kind != ("carl", "ebay", "gallery_image_reference") or not isinstance(reference, dict):
            raise ValueError("eBay analysis contains an invalid gallery reference")
        reference = cast(dict[str, JsonValue], reference)
        order = reference.get("gallery_order")
        if (
            not isinstance(order, int)
            or isinstance(order, bool)
            or not 0 <= order < len(urls)
            or index != f"{order:08d}"
            or reference.get("observation_record_identifier") != observation_identifier
            or reference.get("item_identifier") != observation.get("item_identifier")
            or reference.get("url") != urls[order]
        ):
            raise ValueError("eBay gallery reference belongs to another observation or URL")
        represented_orders.append(order)
        if index not in results:
            if unavailable_by_reference[identifier].gallery_order != order:
                raise ValueError("Unavailable eBay image does not match its gallery reference")
            continue
        kind, _, result = await database.get_record(results[index])
        if (
            kind != ("carl", "ebay", "image_result")
            or not isinstance(result, dict)
            or result.get("state") != "saved"
            or result.get("reference_record_identifier") != identifier
            or result.get("observation_record_identifier") != observation_identifier
            or result.get("item_identifier") != observation.get("item_identifier")
            or result.get("url") != urls[order]
        ):
            raise ValueError("eBay image result does not satisfy its gallery reference")
        artifact = result.get("image_artifact_identifier")
        if not isinstance(artifact, str):
            raise ValueError("eBay image result has no saved artifact")
        metadata, content = await database.get_artifact(artifact)
        media_type = metadata.get("media_type")
        if (
            not isinstance(media_type, str)
            or media_type not in _EXTENSIONS
            or hashlib.sha256(content).hexdigest() != metadata.get("sha256")
        ):
            raise ValueError("Saved eBay image artifact has invalid media type or digest")
        files.append((f"images/{order:03d}{_EXTENSIONS[media_type]}", content))
    for missing in unretained:
        if not isinstance(missing, dict) or set(missing) != {"gallery_order", "url", "reason"}:
            raise ValueError("Unretained eBay image metadata is malformed")
        order = missing.get("gallery_order")
        if (
            not isinstance(order, int)
            or isinstance(order, bool)
            or not 0 <= order < len(urls)
            or missing.get("url") != urls[order]
            or missing.get("reason") != "gallery_reference_not_retained"
        ):
            raise ValueError("Unretained eBay image does not match its observation")
        represented_orders.append(order)
    if sorted(represented_orders) != list(range(len(urls))):
        raise ValueError("eBay gallery evidence omits or duplicates an expected view")
    absence = "listing_observation_has_no_gallery_references" if not urls else None
    if value["gallery_absence_reason"] != absence or value["description_url"] != observation.get(
        "description_url"
    ):
        raise ValueError("eBay evidence does not match its observation metadata")
    description = observation.get("description")
    description_edges = [
        edge.object_identifier for edge in edges if edge.name == ("description_result",)
    ]
    description_state = value["description_state"]
    if len(description_edges) > 1:
        raise ValueError("eBay evidence has multiple seller descriptions")
    if description_edges:
        kind, _, result = await database.get_record(description_edges[0])
        if (
            kind != ("carl", "ebay", "description_result")
            or not isinstance(result, dict)
            or result.get("observation_record_identifier") != observation_identifier
            or result.get("item_identifier") != observation.get("item_identifier")
            or result.get("url") != observation.get("description_url")
        ):
            raise ValueError("eBay seller description belongs to another observation or URL")
        expected_state = "saved" if result.get("state") == "saved" else "failed"
        if description_state != expected_state:
            raise ValueError("eBay seller-description state does not match its selected result")
        if expected_state == "saved":
            description = result.get("description")
            if not isinstance(description, str) or not description.strip():
                raise ValueError("Saved eBay seller description has no usable text")
    elif description_state not in {"inline", "not_available"}:
        raise ValueError("eBay seller-description selection has no matching input edge")
    return {
        "marketplace": "ebay",
        "listing_id": observation.get("item_identifier"),
        "fields": {
            "title": observation.get("title"),
            "description": description,
            "price": observation.get("displayed_price"),
            "currency": observation.get("currency"),
            "condition": observation.get("condition"),
        },
        "description_state": description_state,
        "gallery_images": [{"filename": filename} for filename, _ in files],
        "unavailable_gallery_images": [*unavailable_raw, *unretained],
        "gallery_absence_reason": absence,
    }, tuple(files)


async def _write(path: Path, value: bytes) -> None:
    _ = await anyio.to_thread.run_sync(path.write_bytes, value)


async def _analyze(
    payload: EbayAnalyzeItemPayload,
    context: AttemptContext,
    dependencies: AnalysisWorkerDependencies,
) -> WorkOutcome:
    inputs = (
        NamedInput(
            name=("listing_analysis_evidence",),
            object_identifier=payload.evidence_set_record_identifier,
        ),
        NamedInput(
            name=("product_guide",), object_identifier=payload.product_guide_record_identifier
        ),
    )
    try:
        if payload.recipe_version != EBAY_ANALYSIS_RECIPE_VERSION:
            raise ValueError("Queued eBay analysis recipe differs from registered recipe")
        prompt = await _guide_prompt(dependencies.database, payload.product_guide_record_identifier)
        brief, images = await _analysis_input(
            dependencies.database, payload.evidence_set_record_identifier
        )
        brief_bytes = encode_json(brief, indent=2).encode()
        async with temporary_analysis_directory(
            dependencies.database.path.parent.resolve() / ".analysis"
        ) as directory:
            await anyio.to_thread.run_sync(lambda: (directory / "images").mkdir(mode=0o700))
            await _write(directory / "listing.json", brief_bytes)
            for filename, content in images:
                await _write(directory / filename, content)
            run = await dependencies.claude.run(
                directory=directory,
                prompt=prompt,
                model=payload.model,
                effort=payload.effort,
                timeout_seconds=payload.timeout_seconds,
                maximum_turns=payload.maximum_turns,
            )
    except (KeyError, UnicodeDecodeError, ValueError, OSError) as error:
        return TerminalFailureWork(
            inputs=inputs,
            error={
                "kind": "analysis_input_failure",
                "type": type(error).__name__,
                "message": str(error),
            },
            result={"state": "failed"},
        )
    identifiers = [dependencies.new_identifier() for _ in range(5)]
    brief_identifier, prompt_identifier, stdout_identifier, stderr_identifier, result_identifier = (
        identifiers
    )
    artifacts = (
        BytesDraft(
            identifier=brief_identifier,
            kind=("carl", "ebay", "analysis_input"),
            media_type="application/json",
            representation={"filename": "listing.json", "exact_agent_input": True},
            content=brief_bytes,
        ),
        BytesDraft(
            identifier=prompt_identifier,
            kind=("carl", "ebay", "analysis_prompt"),
            media_type="text/plain; charset=utf-8",
            representation={"exact_agent_prompt": True},
            content=prompt.encode(),
        ),
        BytesDraft(
            identifier=stdout_identifier,
            kind=("carl", "claude", "stdout"),
            media_type="application/x-ndjson",
            representation={"exact_process_stdout": True},
            content=run.stdout,
        ),
        BytesDraft(
            identifier=stderr_identifier,
            kind=("carl", "claude", "stderr"),
            media_type="text/plain; charset=utf-8",
            representation={"exact_process_stderr": True},
            content=run.stderr,
        ),
    )
    failure = run.failure_kind
    state = "completed" if failure is None else "failed"
    raw_turns = run.output_metadata.get("num_turns") if run.output_metadata else None
    limits = analysis_limit_observations(
        payload=payload,
        duration_ns=run.duration_ns,
        observed_turns=raw_turns if isinstance(raw_turns, int) else None,
        observed_web_tool_calls=sum(name in {"WebSearch", "WebFetch"} for name in run.tool_calls),
        report_text=run.text,
        failure_kind=failure,
    )
    record = RecordDraft(
        identifier=result_identifier,
        kind=("carl", "ebay", "item_analysis"),
        schema_version=5,
        value={
            "state": state,
            "analysis_text": run.text,
            "limit_observations": [limit.model_dump(mode="json") for limit in limits],
            "warnings": [
                limit.kind.value
                for limit in limits
                if limit.enforcement is AnalysisLimitEnforcement.ADVISORY
                and limit.status is AnalysisLimitStatus.EXCEEDED
            ],
            "claude": {
                "argv": list(run.argv),
                "version": run.version,
                "version_source": "stream_json_system_init",
                "tool_calls": list(run.tool_calls),
                "external_research_provenance": "agent_reported_urls",
                "exit_code": run.exit_code,
                "started_at_utc_ns": run.started_at_utc_ns,
                "ended_at_utc_ns": run.ended_at_utc_ns,
                "duration_ns": run.duration_ns,
                "output_metadata": run.output_metadata,
                "failure_kind": failure,
            },
        },
    )
    outputs = tuple(
        NamedOutput(name=name, object_identifier=identifier)
        for name, identifier in zip(
            (
                ("listing_input",),
                ("prompt",),
                ("claude", "stdout"),
                ("claude", "stderr"),
                ("item_analysis",),
            ),
            identifiers,
            strict=True,
        )
    )
    result: dict[str, JsonValue] = {
        "state": state,
        "analysis_record_identifier": result_identifier,
        "failure_kind": failure,
    }
    if failure == "claude_timeout" and context.attempt < ANALYSIS_TIMEOUT_MAXIMUM_ATTEMPTS:
        return RetryWork(
            inputs=inputs,
            records=(record,),
            artifacts=artifacts,
            outputs=outputs,
            delay_ns=ANALYSIS_TIMEOUT_RETRY_BASE_DELAY_NS * 2 ** (context.attempt - 1),
            reason={
                "kind": failure,
                "decision": "retry",
                "attempt": context.attempt,
                "maximum_attempts": ANALYSIS_TIMEOUT_MAXIMUM_ATTEMPTS,
            },
            result=result,
        )
    if failure is not None:
        return TerminalFailureWork(
            inputs=inputs,
            records=(record,),
            artifacts=artifacts,
            outputs=outputs,
            error={
                "kind": failure,
                "decision": "retry_exhausted" if failure == "claude_timeout" else "terminal",
                "attempt": context.attempt,
                "maximum_attempts": ANALYSIS_TIMEOUT_MAXIMUM_ATTEMPTS
                if failure == "claude_timeout"
                else 1,
            },
            result=result,
        )
    return CompletedWork(
        inputs=inputs, records=(record,), artifacts=artifacts, outputs=outputs, result=result
    )


def build_ebay_analysis_worker_registry(
    dependencies: AnalysisWorkerDependencies,
) -> WorkHandlerRegistry:
    return WorkHandlerRegistry(
        handlers=(
            TypedWorkHandler(
                capability=WorkCapability(
                    kind=ANALYZE_EBAY_ITEM_WORK_KIND, payload_schema_version=1
                ),
                component=build_ebay_analysis_component_registry().require(ANALYZE_EBAY_ITEM),
                payload_type=EbayAnalyzeItemPayload,
                handler=lambda payload, context: _analyze(payload, context, dependencies),
            ),
        )
    )
