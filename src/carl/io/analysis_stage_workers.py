"""Opt-in durable CPU/child-process/CPU analysis orchestration."""

import hashlib
import os
from collections.abc import Awaitable, Callable
from pathlib import Path
from time import time_ns
from typing import Literal, Protocol

import anyio
from pydantic import Field

from carl.core.analysis_stages import (
    MAX_ANALYSIS_INPUT_BYTES,
    MAX_ANALYSIS_OUTPUT_BYTES,
    MAX_ANALYSIS_STDERR_BYTES,
    MAX_ANALYSIS_STDOUT_BYTES,
    PreparedAnalysis,
)
from carl.core.components import Component
from carl.core.item_analysis import AnalyzeItemPayload
from carl.core.json import encode_json
from carl.core.models import JsonValue, NamedInput, StrictModel
from carl.core.resources import ResourceRequest
from carl.core.stages import (
    StageAttemptContext,
    StageDefinition,
    StageOutcome,
    StageWaitKind,
    WaitWork,
)
from carl.core.work import WorkCapability, WorkDefinition
from carl.core.worker import AttemptContext, TerminalFailureWork, WorkOutcome
from carl.io.analysis_checkpoints import (
    checkpoint_analysis_inputs,
    checkpoint_claude_capture,
    read_analysis_input_manifest,
    restore_analysis_inputs,
    restore_claude_capture,
)
from carl.io.claude import ClaudeCli, ClaudeRun, parse_claude_capture, temporary_analysis_directory
from carl.io.resource_permits import run_cpu
from carl.io.sqlite import Database
from carl.io.stage_workers import StageHandlerRegistry, TypedStageHandler, WorkStageAdapter


class AnalysisStageDependencies(Protocol):
    @property
    def database(self) -> Database: ...

    @property
    def claude(self) -> ClaudeCli: ...

    @property
    def new_identifier(self) -> Callable[[], str]: ...


class AnalysisWorkReference(StrictModel):
    work_kind: tuple[str, ...] = Field(min_length=1)
    payload_schema_version: int = Field(ge=1)


class AnalysisCaptureReference(AnalysisWorkReference):
    input_checkpoint_identifier: str = Field(min_length=1)


class AnalysisPublishReference(AnalysisCaptureReference):
    capture_checkpoint_identifier: str = Field(min_length=1)


async def materialize_analysis_inputs(
    database: Database, prepared: PreparedAnalysis, directory: Path
) -> None:
    """Rebuild a private child directory from immutable, verified input artifacts."""

    await anyio.to_thread.run_sync(lambda: (directory / "images").mkdir(mode=0o700))
    await anyio.to_thread.run_sync((directory / "listing.json").write_bytes, prepared.listing_input)
    for image in prepared.images:
        external = await database.get_external_artifact_path(
            image.artifact_identifier, maximum_bytes=image.byte_length
        )
        destination = directory / image.filename
        if external is not None:
            metadata, source = external
            if (
                metadata["sha256"] != image.sha256
                or metadata["media_type"] != image.media_type
                or metadata["size"] != image.byte_length
            ):
                raise ValueError("Saved image no longer matches queued analysis input")
            await anyio.to_thread.run_sync(
                lambda source=source, destination=destination: os.link(
                    source, destination, follow_symlinks=False
                )
            )
        else:
            metadata, content = await database.get_artifact(
                image.artifact_identifier, maximum_bytes=MAX_ANALYSIS_INPUT_BYTES
            )
            if (
                metadata["sha256"] != image.sha256
                or metadata["media_type"] != image.media_type
                or len(content) != image.byte_length
                or hashlib.sha256(content).hexdigest() != image.sha256
            ):
                raise ValueError("Saved image no longer matches queued analysis input")
            await anyio.to_thread.run_sync(destination.write_bytes, content)


def _inputs(payload: AnalyzeItemPayload) -> tuple[NamedInput, ...]:
    return (
        NamedInput(
            name=("listing_analysis_evidence",),
            object_identifier=payload.evidence_set_record_identifier,
        ),
        NamedInput(
            name=("product_guide",), object_identifier=payload.product_guide_record_identifier
        ),
    )


def _input_failure(payload: AnalyzeItemPayload, error: Exception) -> TerminalFailureWork:
    return TerminalFailureWork(
        inputs=_inputs(payload),
        error={
            "kind": "analysis_input_failure",
            "type": type(error).__name__,
            "message": str(error),
        },
        result={"state": "failed"},
    )


def _validate_capture_limits(claude: ClaudeCli) -> None:
    if (
        claude.maximum_stdout_bytes > MAX_ANALYSIS_STDOUT_BYTES
        or claude.maximum_stderr_bytes > MAX_ANALYSIS_STDERR_BYTES
        or claude.maximum_output_bytes > MAX_ANALYSIS_OUTPUT_BYTES
    ):
        raise ValueError("Configured Claude output limits exceed durable checkpoint bounds")


def build_analysis_stage_registry[PayloadT: AnalyzeItemPayload](
    *,
    dependencies: AnalysisStageDependencies,
    marketplace: Literal["facebook", "ebay"],
    component: Component,
    capabilities: tuple[WorkCapability, ...],
    payload_type: type[PayloadT],
    prepare: Callable[[PayloadT, Database], Awaitable[PreparedAnalysis]],
    outcome_builder: Callable[
        [PayloadT, AttemptContext, ClaudeRun, Callable[[], str]], WorkOutcome
    ],
    input_error_types: tuple[type[Exception], ...] = (KeyError, UnicodeDecodeError, ValueError),
    utc_now_ns: Callable[[], int] = time_ns,
) -> StageHandlerRegistry:
    """Build handlers without starting workers or changing the active runtime."""

    kinds = {
        phase: ("carl", marketplace, "stage", "analysis", phase)
        for phase in ("prepare", "capture", "publish")
    }

    def initial(work: WorkDefinition, identifier: str, operation: str) -> StageDefinition:
        return StageDefinition(
            identifier=identifier,
            work_item_identifier=work.identifier,
            operation_identifier=operation,
            kind=kinds["prepare"],
            payload=AnalysisWorkReference(
                work_kind=work.kind, payload_schema_version=work.payload_schema_version
            ).model_dump(mode="json"),
            resources=(ResourceRequest(name="cpu"),),
            not_before_utc_ns=work.not_before_utc_ns,
        )

    async def load(
        reference: AnalysisWorkReference, context: StageAttemptContext
    ) -> tuple[PayloadT, dict[str, JsonValue]]:
        capability = WorkCapability(
            kind=reference.work_kind, payload_schema_version=reference.payload_schema_version
        )
        if capability not in capabilities:
            raise ValueError("Not a registered analysis work capability")
        work = await dependencies.database.work_definition(context.work_item_identifier)
        if (
            work.kind != reference.work_kind
            or work.payload_schema_version != reference.payload_schema_version
        ):
            raise ValueError("Analysis continuation differs from its immutable work definition")
        payload = payload_type.model_validate_json(encode_json(work.payload))
        return payload, {
            "work_kind": list(work.kind),
            "payload_schema_version": work.payload_schema_version,
            "payload": payload.model_dump(mode="json"),
        }

    def continuation(
        context: StageAttemptContext, phase: str, payload: StrictModel
    ) -> StageDefinition:
        return StageDefinition(
            identifier=dependencies.new_identifier(),
            work_item_identifier=context.work_item_identifier,
            operation_identifier=context.operation_identifier,
            kind=kinds[phase],
            payload=payload.model_dump(mode="json"),
            resources=(ResourceRequest(name="analysis" if phase == "capture" else "cpu"),),
        )

    async def prepare_stage(
        reference: AnalysisWorkReference, context: StageAttemptContext
    ) -> StageOutcome:
        try:
            payload, configuration = await load(reference, context)
        except (KeyError, UnicodeDecodeError, ValueError) as error:
            return TerminalFailureWork(
                error={"kind": "analysis_input_failure", "type": type(error).__name__},
                result={"state": "failed"},
            )
        try:
            _validate_capture_limits(dependencies.claude)
            prepared = await prepare(payload, dependencies.database)
            checkpoint = await checkpoint_analysis_inputs(
                dependencies.database,
                prepared,
                configuration=configuration,
                marketplace=marketplace,
                new_identifier=dependencies.new_identifier,
            )
        except input_error_types as error:
            # A missing queued input must not turn terminal publication into a
            # foreign-key failure and leave the stage endlessly pending.
            retained_inputs: list[NamedInput] = []
            for item in _inputs(payload):
                try:
                    _ = await dependencies.database.get_record(item.object_identifier)
                except KeyError:
                    continue
                retained_inputs.append(item)
            return _input_failure(payload, error).model_copy(
                update={"inputs": tuple(retained_inputs)}
            )
        return WaitWork(
            inputs=prepared.inputs,
            records=checkpoint.records,
            artifacts=checkpoint.artifacts,
            outputs=checkpoint.outputs,
            continuation=continuation(
                context,
                "capture",
                AnalysisCaptureReference(
                    work_kind=reference.work_kind,
                    payload_schema_version=reference.payload_schema_version,
                    input_checkpoint_identifier=checkpoint.record_identifier,
                ),
            ),
            wait_kind=StageWaitKind.RESOURCE,
            result={"state": "analysis_prepared"},
        )

    async def verify_input(
        reference: AnalysisCaptureReference,
        context: StageAttemptContext,
        configuration: dict[str, JsonValue],
    ) -> None:
        manifest = await read_analysis_input_manifest(
            dependencies.database, reference.input_checkpoint_identifier
        )
        operation, _, _ = await dependencies.database.object_operation_relations(
            reference.input_checkpoint_identifier
        )
        if (
            operation != context.operation_identifier
            or manifest.marketplace != marketplace
            or manifest.configuration != configuration
        ):
            raise ValueError(
                "Analysis checkpoint belongs to another logical attempt or configuration"
            )

    async def capture_stage(
        reference: AnalysisCaptureReference, context: StageAttemptContext
    ) -> StageOutcome:
        try:
            payload, configuration = await load(reference, context)
        except (KeyError, UnicodeDecodeError, ValueError) as error:
            return TerminalFailureWork(
                error={"kind": "analysis_input_failure", "type": type(error).__name__},
                result={"state": "failed"},
            )
        try:
            _validate_capture_limits(dependencies.claude)
            await verify_input(reference, context, configuration)
            prepared = await restore_analysis_inputs(
                dependencies.database, reference.input_checkpoint_identifier
            )
            capture = None
            cleanup_error_type = None
            try:
                async with temporary_analysis_directory(
                    dependencies.database.path.parent.resolve() / ".analysis"
                ) as directory:
                    await materialize_analysis_inputs(dependencies.database, prepared, directory)
                    charge = await dependencies.database.charge_stage_invocation(
                        context,
                        request_identity=(marketplace, "analysis", "claude"),
                        maximum_attempts=1,
                        now_utc_ns=utc_now_ns(),
                    )
                    if charge is None or not charge.newly_charged:
                        return TerminalFailureWork(
                            error={
                                "kind": "claude_uncertain_previous_dispatch",
                                "decision": "terminal",
                                "attempt": context.attempt,
                                "maximum_attempts": 1,
                            },
                            result={
                                "state": "failed",
                                "failure_kind": "claude_uncertain_previous_dispatch",
                            },
                        )
                    capture = await dependencies.claude.capture(
                        directory=directory,
                        prompt=prepared.prompt.decode("utf-8"),
                        model=payload.model,
                        effort=payload.effort,
                        timeout_seconds=payload.timeout_seconds,
                        maximum_turns=payload.maximum_turns,
                    )
            except Exception as error:
                # A returned capture proves the child has been reaped. A later
                # workspace cleanup failure must not discard that paid result.
                if capture is None:
                    raise
                cleanup_error_type = type(error).__name__[:128]
            checkpoint = await checkpoint_claude_capture(
                dependencies.database,
                capture,
                input_checkpoint_identifier=reference.input_checkpoint_identifier,
                new_identifier=dependencies.new_identifier,
            )
        except input_error_types as error:
            return _input_failure(payload, error).model_copy(update={"inputs": ()})
        return WaitWork(
            records=checkpoint.records,
            artifacts=checkpoint.artifacts,
            outputs=checkpoint.outputs,
            continuation=continuation(
                context,
                "publish",
                AnalysisPublishReference(
                    work_kind=reference.work_kind,
                    payload_schema_version=reference.payload_schema_version,
                    input_checkpoint_identifier=reference.input_checkpoint_identifier,
                    capture_checkpoint_identifier=checkpoint.record_identifier,
                ),
            ),
            wait_kind=StageWaitKind.RESOURCE,
            result={
                "state": "analysis_captured",
                **({"cleanup_failure_type": cleanup_error_type} if cleanup_error_type else {}),
            },
        )

    async def publish_stage(
        reference: AnalysisPublishReference, context: StageAttemptContext
    ) -> StageOutcome:
        try:
            payload, configuration = await load(reference, context)
        except (KeyError, UnicodeDecodeError, ValueError) as error:
            return TerminalFailureWork(
                error={"kind": "analysis_input_failure", "type": type(error).__name__},
                result={"state": "failed"},
            )
        try:
            await verify_input(reference, context, configuration)
            operation, _, _ = await dependencies.database.object_operation_relations(
                reference.capture_checkpoint_identifier
            )
            if operation != context.operation_identifier:
                raise ValueError("Analysis capture belongs to another logical attempt")
            capture = await restore_claude_capture(
                dependencies.database,
                reference.capture_checkpoint_identifier,
                input_checkpoint_identifier=reference.input_checkpoint_identifier,
            )
            run = await run_cpu(parse_claude_capture, capture)
        except input_error_types as error:
            return _input_failure(payload, error).model_copy(update={"inputs": ()})
        outcome = outcome_builder(payload, context, run, dependencies.new_identifier)
        return outcome

    return StageHandlerRegistry(
        adapters=tuple(
            WorkStageAdapter(capability=capability, component=component, initial_stage=initial)
            for capability in capabilities
        ),
        handlers=(
            TypedStageHandler(
                kind=kinds["prepare"],
                schema_version=1,
                payload_type=AnalysisWorkReference,
                handler=prepare_stage,
            ),
            TypedStageHandler(
                kind=kinds["capture"],
                schema_version=1,
                payload_type=AnalysisCaptureReference,
                handler=capture_stage,
            ),
            TypedStageHandler(
                kind=kinds["publish"],
                schema_version=1,
                payload_type=AnalysisPublishReference,
                handler=publish_stage,
            ),
        ),
    )
