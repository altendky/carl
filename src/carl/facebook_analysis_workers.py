"""Durable, provenance-linked identification of saved Marketplace listings."""

import hashlib
import os
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import anyio

from carl.core.components import Component, ComponentId, Registry
from carl.core.item_analysis import (
    ANALYSIS_RECIPE_VERSION,
    ANALYSIS_TIMEOUT_MAXIMUM_ATTEMPTS,
    ANALYSIS_TIMEOUT_RETRY_BASE_DELAY_NS,
    ANALYZE_ITEM_WORK_KIND,
    ANALYZE_ITEM_WORK_SCHEMA_VERSION,
    LEGACY_ANALYZE_ITEM_WORK_SCHEMA_VERSION,
    AnalysisLimitEnforcement,
    AnalysisLimitStatus,
    AnalyzeItemPayload,
    ProductGuideRecord,
    UnavailableAnalysisImageSelection,
    analysis_limit_observations,
    identification_prompt,
    listing_analysis_evidence_set,
    listing_brief,
    product_guide_definition,
)
from carl.core.json import encode_json
from carl.core.models import BytesDraft, JsonValue, NamedInput, NamedOutput, RecordDraft
from carl.core.review import authored_product_guide
from carl.core.work import WorkCapability
from carl.core.worker import (
    AttemptContext,
    CompletedWork,
    RetryWork,
    TerminalFailureWork,
    WorkOutcome,
)
from carl.io.claude import ClaudeCli, temporary_analysis_directory
from carl.io.sqlite import Database
from carl.io.worker import TypedWorkHandler, WorkHandlerRegistry

ANALYZE_FACEBOOK_ITEM = ComponentId(("carl", "facebook", "analyze", "listing_identification"))
PLAN_FACEBOOK_LISTING_ANALYSIS_EVIDENCE = ComponentId(
    ("carl", "facebook", "plan", "listing_analysis_evidence")
)
REGISTER_PRODUCT_GUIDE = ComponentId(("carl", "analysis", "register", "product_guide"))
AUTHOR_PRODUCT_GUIDE = ComponentId(("carl", "analysis", "author", "product_guide"))

_IMAGE_EXTENSIONS = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
}


def build_analysis_component_registry() -> Registry:
    return Registry(
        (
            Component(ANALYZE_FACEBOOK_ITEM, 12, listing_brief),
            Component(
                PLAN_FACEBOOK_LISTING_ANALYSIS_EVIDENCE,
                1,
                listing_analysis_evidence_set,
            ),
            Component(REGISTER_PRODUCT_GUIDE, 1, product_guide_definition),
            Component(AUTHOR_PRODUCT_GUIDE, 1, authored_product_guide),
        )
    )


@dataclass(frozen=True, slots=True)
class AnalysisWorkerDependencies:
    database: Database
    claude: ClaudeCli
    new_identifier: Callable[[], str]


@dataclass(frozen=True, slots=True)
class _ResolvedImage:
    artifact_identifier: str
    sha256: str
    mime_type: str
    gallery_order: int


def _image_result_satisfies_reference(
    *,
    reference_identifier: str,
    reference: dict[str, JsonValue],
    image_result_identifier: str,
    image_result: dict[str, JsonValue],
    reused_results_by_reference: dict[str, str],
) -> bool:
    if image_result.get("state") != "saved":
        return False
    exact_rendition = image_result.get("source_photo_id") == reference.get(
        "photo_id"
    ) and image_result.get("original_url") == reference.get("original_url")
    recorded_reuse = (
        reused_results_by_reference.get(reference_identifier) == image_result_identifier
    )
    return exact_rendition or recorded_reuse


async def _write_bytes(path: Path, value: bytes) -> None:
    await anyio.to_thread.run_sync(path.write_bytes, value)


async def _link_file(path: Path, target: Path) -> None:
    await anyio.to_thread.run_sync(lambda: os.link(target, path, follow_symlinks=False))


async def _analyze(
    payload: AnalyzeItemPayload,
    context: AttemptContext,
    dependencies: AnalysisWorkerDependencies,
) -> WorkOutcome:
    inputs = [
        NamedInput(
            name=("listing_analysis_evidence",),
            object_identifier=payload.evidence_set_record_identifier,
        ),
        NamedInput(
            name=("product_guide",),
            object_identifier=payload.product_guide_record_identifier,
        ),
    ]
    try:
        guide_kind, guide_schema_version, guide_value = await dependencies.database.get_record(
            payload.product_guide_record_identifier
        )
        if guide_kind != ("carl", "analysis", "product_guide") or guide_schema_version not in {
            1,
            2,
        }:
            raise ValueError("Input is not a supported product guide record")
        _ = ProductGuideRecord.model_validate_json(encode_json(guide_value))
        (
            guide_operation_identifier,
            _,
            guide_outputs,
        ) = await dependencies.database.object_operation_relations(
            payload.product_guide_record_identifier
        )
        guide_operation = await dependencies.database.operation(guide_operation_identifier)
        if guide_operation.get("component_parts") not in (
            list(REGISTER_PRODUCT_GUIDE.parts),
            list(AUTHOR_PRODUCT_GUIDE.parts),
        ):
            raise ValueError("Product guide was not produced by a registered guide component")
        if (
            guide_operation.get("output_schema_version") != 1
            or guide_operation.get("state") != "completed"
        ):
            raise ValueError("Product guide was not produced by the registered guide component")
        guide_text_identifiers = [
            output.object_identifier for output in guide_outputs if output.name == ("guide_text",)
        ]
        if len(guide_text_identifiers) != 1:
            raise ValueError("Product guide has no unique exact-text artifact")
        guide_metadata, guide_bytes = await dependencies.database.get_artifact(
            guide_text_identifiers[0]
        )
        if guide_metadata.get("media_type") != "text/plain; charset=utf-8" or guide_metadata.get(
            "representation"
        ) != {"exact_product_guide": True}:
            raise ValueError("Product guide text artifact has unsupported metadata")
        guide_text = guide_bytes.decode("utf-8")
        prompt = identification_prompt(guide_text)
        evidence_kind, _, evidence_value = await dependencies.database.get_record(
            payload.evidence_set_record_identifier
        )
        if evidence_kind != ("carl", "facebook", "listing_analysis_evidence"):
            raise ValueError("Input is not listing-analysis evidence")
        if not isinstance(evidence_value, dict):
            raise ValueError("Listing-analysis evidence record is malformed")
        raw_unavailable = evidence_value.get("unavailable_gallery_images", [])
        gallery_absence_reason = evidence_value.get("gallery_absence_reason")
        if (
            set(evidence_value) - {"unavailable_gallery_images", "gallery_absence_reason"}
            or not isinstance(raw_unavailable, list)
            or (gallery_absence_reason is not None and not isinstance(gallery_absence_reason, str))
        ):
            raise ValueError("Listing-analysis evidence selection metadata is malformed")
        unavailable_images = tuple(
            UnavailableAnalysisImageSelection.model_validate(item) for item in raw_unavailable
        )
        _, evidence_inputs, _ = await dependencies.database.object_operation_relations(
            payload.evidence_set_record_identifier
        )
        observation_identifiers = [
            input_value.object_identifier
            for input_value in evidence_inputs
            if input_value.name == ("listing_observation",)
        ]
        references = {
            input_value.name[1]: input_value.object_identifier
            for input_value in evidence_inputs
            if len(input_value.name) == 2 and input_value.name[0] == "gallery_image_reference"
        }
        image_results = {
            input_value.name[1]: input_value.object_identifier
            for input_value in evidence_inputs
            if len(input_value.name) == 2 and input_value.name[0] == "image_result"
        }
        if len(observation_identifiers) != 1 or not image_results.keys() <= references.keys():
            raise ValueError("Listing-analysis evidence has malformed input edges")
        unavailable_reference_identifiers = {
            image.gallery_image_reference_record_identifier for image in unavailable_images
        }
        if {
            references[index] for index in references.keys() - image_results.keys()
        } != unavailable_reference_identifiers:
            raise ValueError("Listing-analysis evidence has incomplete gallery selection metadata")
        observation_identifier = observation_identifiers[0]
        kind, _, observation = await dependencies.database.get_record(observation_identifier)
        if kind != ("carl", "facebook", "listing_observation"):
            raise ValueError("Evidence does not reference a listing observation")
        if payload.recipe_version != ANALYSIS_RECIPE_VERSION:
            raise ValueError("Queued analysis recipe differs from registered recipe")
        reused_results_by_reference = {
            reuse.gallery_image_reference_record_identifier: (
                reuse.source_image_result_record_identifier
            )
            for reuse in await dependencies.database.facebook_image_reuse_resolutions(
                tuple(references.values())
            )
        }
        resolved_images: list[_ResolvedImage] = []
        unavailable_by_reference = {
            image.gallery_image_reference_record_identifier: image for image in unavailable_images
        }
        for index in sorted(references):
            reference_kind, _, reference = await dependencies.database.get_record(references[index])
            if reference_kind != ("carl", "facebook", "gallery_image_reference"):
                raise ValueError("Evidence contains an invalid gallery reference")
            if not isinstance(reference, dict):
                raise ValueError("Gallery evidence is malformed")
            if reference.get("listing_observation_record_identifier") != observation_identifier:
                raise ValueError("Gallery reference belongs to another observation")
            if index not in image_results:
                unavailable = unavailable_by_reference.get(references[index])
                if unavailable is None or unavailable.gallery_order != reference.get(
                    "gallery_order"
                ):
                    raise ValueError("Unavailable gallery evidence does not match its reference")
                continue
            result_kind, _, image_result = await dependencies.database.get_record(
                image_results[index]
            )
            if result_kind != ("carl", "facebook", "image_result"):
                raise ValueError("Evidence contains an invalid image result")
            if not isinstance(image_result, dict):
                raise ValueError("Image evidence is malformed")
            if not _image_result_satisfies_reference(
                reference_identifier=references[index],
                reference=reference,
                image_result_identifier=image_results[index],
                image_result=image_result,
                reused_results_by_reference=reused_results_by_reference,
            ):
                raise ValueError("Image result does not satisfy its gallery reference")
            artifact_identifier = image_result.get("image_artifact_identifier")
            gallery_order = reference.get("gallery_order")
            if not isinstance(artifact_identifier, str) or not isinstance(gallery_order, int):
                raise ValueError("Image evidence has no saved artifact or gallery order")
            external = await dependencies.database.get_external_artifact_path(artifact_identifier)
            if external is not None:
                metadata, _ = external
            else:
                metadata, _ = await dependencies.database.get_artifact(artifact_identifier)
            sha256 = metadata.get("sha256")
            mime_type = metadata.get("media_type")
            if not isinstance(sha256, str) or mime_type not in _IMAGE_EXTENSIONS:
                raise ValueError("Saved image artifact has unsupported metadata")
            resolved_images.append(
                _ResolvedImage(
                    artifact_identifier=artifact_identifier,
                    sha256=sha256,
                    mime_type=mime_type,
                    gallery_order=gallery_order,
                )
            )
        resolved_images.sort(key=lambda image: image.gallery_order)
        orders = [image.gallery_order for image in resolved_images]
        if len(set(orders)) != len(orders):
            raise ValueError("Evidence images are not in unique gallery order")
        filenames = tuple(
            f"images/{image.gallery_order:03d}{_IMAGE_EXTENSIONS[image.mime_type]}"
            for image in resolved_images
        )
        brief = listing_brief(
            observation=observation,
            image_filenames=filenames,
            unavailable_gallery_images=tuple(
                sorted(unavailable_images, key=lambda image: image.gallery_order)
            ),
            gallery_absence_reason=gallery_absence_reason,
        )
        brief_bytes = encode_json(brief, indent=2).encode("utf-8")
        prompt_bytes = prompt.encode("utf-8")
        analysis_root = dependencies.database.path.parent.resolve() / ".analysis"
        async with temporary_analysis_directory(analysis_root) as directory:
            await anyio.to_thread.run_sync(lambda: (directory / "images").mkdir(mode=0o700))
            await _write_bytes(directory / "listing.json", brief_bytes)
            for image, filename in zip(resolved_images, filenames, strict=True):
                external = await dependencies.database.get_external_artifact_path(
                    image.artifact_identifier
                )
                destination = directory / filename
                if external is not None:
                    metadata, source = external
                    if (
                        metadata["sha256"] != image.sha256
                        or metadata["media_type"] != image.mime_type
                    ):
                        raise ValueError("Saved image no longer matches queued analysis input")
                    await _link_file(destination, source)
                else:
                    metadata, content = await dependencies.database.get_artifact(
                        image.artifact_identifier
                    )
                    if (
                        metadata["sha256"] != image.sha256
                        or metadata["media_type"] != image.mime_type
                        or hashlib.sha256(content).hexdigest() != image.sha256
                    ):
                        raise ValueError("Saved image no longer matches queued analysis input")
                    await _write_bytes(destination, content)
            run = await dependencies.claude.run(
                directory=directory,
                prompt=prompt,
                model=payload.model,
                effort=payload.effort,
                timeout_seconds=payload.timeout_seconds,
                maximum_turns=payload.maximum_turns,
            )
    except (KeyError, UnicodeDecodeError, ValueError) as error:
        return TerminalFailureWork(
            inputs=tuple(inputs),
            error={
                "kind": "analysis_input_failure",
                "type": type(error).__name__,
                "message": str(error),
            },
            result={"state": "failed"},
        )

    brief_identifier = dependencies.new_identifier()
    prompt_identifier = dependencies.new_identifier()
    stdout_identifier = dependencies.new_identifier()
    stderr_identifier = dependencies.new_identifier()
    result_identifier = dependencies.new_identifier()
    artifacts = (
        BytesDraft(
            identifier=brief_identifier,
            kind=("carl", "facebook", "analysis_input"),
            media_type="application/json",
            representation={"filename": "listing.json", "exact_agent_input": True},
            content=brief_bytes,
        ),
        BytesDraft(
            identifier=prompt_identifier,
            kind=("carl", "facebook", "analysis_prompt"),
            media_type="text/plain; charset=utf-8",
            representation={"exact_agent_prompt": True},
            content=prompt_bytes,
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
    failure_kind = run.failure_kind
    state = "completed" if failure_kind is None else "failed"
    reported_turns = (
        run.output_metadata.get("num_turns") if run.output_metadata is not None else None
    )
    observed_turns = reported_turns if isinstance(reported_turns, int) else None
    observed_web_tool_calls = sum(name in {"WebSearch", "WebFetch"} for name in run.tool_calls)
    limits = analysis_limit_observations(
        payload=payload,
        duration_ns=run.duration_ns,
        observed_turns=observed_turns,
        observed_web_tool_calls=observed_web_tool_calls,
        report_text=run.text,
        failure_kind=failure_kind,
    )
    warnings = [
        limit.kind.value
        for limit in limits
        if limit.enforcement is AnalysisLimitEnforcement.ADVISORY
        and limit.status is AnalysisLimitStatus.EXCEEDED
    ]
    record = RecordDraft(
        identifier=result_identifier,
        kind=("carl", "facebook", "item_analysis"),
        schema_version=5,
        value={
            "state": state,
            "analysis_text": run.text,
            "limit_observations": [limit.model_dump(mode="json") for limit in limits],
            "warnings": warnings,
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
                "failure_kind": failure_kind,
            },
        },
    )
    outputs = (
        NamedOutput(name=("listing_input",), object_identifier=brief_identifier),
        NamedOutput(name=("prompt",), object_identifier=prompt_identifier),
        NamedOutput(name=("claude", "stdout"), object_identifier=stdout_identifier),
        NamedOutput(name=("claude", "stderr"), object_identifier=stderr_identifier),
        NamedOutput(name=("item_analysis",), object_identifier=result_identifier),
    )
    result: dict[str, JsonValue] = {
        "state": state,
        "analysis_record_identifier": result_identifier,
        "failure_kind": failure_kind,
    }
    if failure_kind is not None:
        if failure_kind == "claude_timeout" and context.attempt < ANALYSIS_TIMEOUT_MAXIMUM_ATTEMPTS:
            return RetryWork(
                inputs=tuple(inputs),
                records=(record,),
                artifacts=artifacts,
                outputs=outputs,
                delay_ns=ANALYSIS_TIMEOUT_RETRY_BASE_DELAY_NS * 2 ** (context.attempt - 1),
                reason={
                    "kind": failure_kind,
                    "decision": "retry",
                    "attempt": context.attempt,
                    "maximum_attempts": ANALYSIS_TIMEOUT_MAXIMUM_ATTEMPTS,
                },
                result=result,
            )
        return TerminalFailureWork(
            inputs=tuple(inputs),
            records=(record,),
            artifacts=artifacts,
            outputs=outputs,
            error={
                "kind": failure_kind,
                "decision": ("retry_exhausted" if failure_kind == "claude_timeout" else "terminal"),
                "attempt": context.attempt,
                "maximum_attempts": (
                    ANALYSIS_TIMEOUT_MAXIMUM_ATTEMPTS if failure_kind == "claude_timeout" else 1
                ),
            },
            result=result,
        )
    return CompletedWork(
        inputs=tuple(inputs),
        records=(record,),
        artifacts=artifacts,
        outputs=outputs,
        result=result,
    )


def build_analysis_worker_registry(dependencies: AnalysisWorkerDependencies) -> WorkHandlerRegistry:
    component = build_analysis_component_registry().require(ANALYZE_FACEBOOK_ITEM)
    return WorkHandlerRegistry(
        handlers=tuple(
            TypedWorkHandler(
                capability=WorkCapability(
                    kind=ANALYZE_ITEM_WORK_KIND,
                    payload_schema_version=schema_version,
                ),
                component=component,
                payload_type=AnalyzeItemPayload,
                handler=lambda payload, context: _analyze(payload, context, dependencies),
            )
            for schema_version in (
                LEGACY_ANALYZE_ITEM_WORK_SCHEMA_VERSION,
                ANALYZE_ITEM_WORK_SCHEMA_VERSION,
            )
        )
    )
