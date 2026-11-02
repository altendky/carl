"""Facebook's explicit analysis stages preserve validation and legacy capabilities."""

# Reuse bounded offline integration fixtures without marketplace acquisition.
# pyright: reportPrivateUsage=false

import hashlib
from pathlib import Path
from uuid import uuid4

import pytest

from carl._tests.test_analysis_stages import _cli, _initial
from carl._tests.test_facebook_stateless_stages import _register, _run, _store
from carl._tests.test_item_analysis import (
    _fake_claude,
    _observation,
    _payload,
    _provenance,
    _reference,
)
from carl.core.components import Component, ComponentId
from carl.core.item_analysis import (
    ANALYZE_ITEM_WORK_KIND,
    TELESCOPE_PRODUCT_GUIDE,
    AnalyzeItemPayload,
    analyze_item_work,
)
from carl.core.models import ArtifactDraft, BytesDraft, ExternalFileDraft, NamedOutput, RecordDraft
from carl.core.stages import WaitWork
from carl.core.work import WorkCapability
from carl.core.worker import CompletedWork, TerminalFailureWork
from carl.facebook_analysis_stages import build_facebook_analysis_stage_registry
from carl.facebook_analysis_workers import (
    REGISTER_PRODUCT_GUIDE,
    AnalysisWorkerDependencies,
    build_analysis_component_registry,
)
from carl.io.claude import ClaudeCli
from carl.io.sqlite import Database
from carl.io.stages import stage_manager

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.mark.parametrize("version", (3, 4))
async def test_invalid_guide_fails_before_analysis_admission_or_child(
    tmp_path: Path, version: int
) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = build_facebook_analysis_stage_registry(
            AnalysisWorkerDependencies(database, _cli(tmp_path), lambda: str(uuid4()))
        )
        adapter = registry.require_adapter(
            WorkCapability(kind=ANALYZE_ITEM_WORK_KIND, payload_schema_version=version)
        )
        assert adapter.component.output_schema_version == 12
        initial, adapter = await _initial(database, registry, version=version)
        assert tuple(resource.name for resource in initial.resources) == ("cpu",)
        failed = await _run(registry, stage_manager(database), initial, adapter)
        assert isinstance(failed, TerminalFailureWork)
        assert failed.error["kind"] == "analysis_input_failure"
        assert failed.error["message"] == "Input is not a supported product guide record"
        assert failed.artifacts == failed.records == failed.outputs == ()
        assert tuple(item.object_identifier for item in failed.inputs) == ("evidence", "guide")
        assert not (tmp_path / "calls").exists()


async def _evidence(database: Database, root: Path, *, external: bool) -> None:
    image_bytes = b"image bytes"
    if external:
        image = root / "image.bin"
        image.write_bytes(image_bytes)
        image.chmod(0o600)
        artifact: ArtifactDraft = ExternalFileDraft(
            identifier="image-file",
            kind=("carl", "facebook", "image_file"),
            media_type="image/png",
            representation={},
            sha256=hashlib.sha256(image_bytes).hexdigest(),
            size=len(image_bytes),
            locator=image.relative_to(root).as_posix(),
        )
    else:
        artifact = BytesDraft(
            identifier="image-file",
            kind=("carl", "facebook", "image_file"),
            media_type="image/png",
            representation={},
            content=image_bytes,
        )
    await _store(
        database,
        (
            RecordDraft(
                identifier="observation",
                kind=("carl", "facebook", "listing_observation"),
                schema_version=1,
                value=_observation(),
            ),
            RecordDraft(
                identifier="gallery-reference",
                kind=("carl", "facebook", "gallery_image_reference"),
                schema_version=1,
                value=_reference().model_dump(mode="json"),
            ),
            RecordDraft(
                identifier="image-result",
                kind=("carl", "facebook", "image_result"),
                schema_version=1,
                value={
                    "state": "saved",
                    "source_photo_id": "photo-1",
                    "original_url": _reference().original_url,
                    "image_artifact_identifier": "image-file",
                },
            ),
        ),
        (artifact,),
    )
    await database.begin_operation(
        operation_id="evidence-operation",
        component=Component(ComponentId(("test", "evidence")), 1, lambda: None),
        provenance=_provenance(),
        invocation={},
        configuration={},
        started_at_utc="2026-11-02T00:00:00+00:00",
        inputs=(
            (("listing_observation",), "observation"),
            (("gallery_image_reference", "00000000"), "gallery-reference"),
            (("image_result", "00000000"), "image-result"),
        ),
    )
    await database.complete_operation(
        operation_id="evidence-operation",
        records=(
            RecordDraft(
                identifier="evidence-set",
                kind=("carl", "facebook", "listing_analysis_evidence"),
                schema_version=1,
                value={},
            ),
        ),
        artifacts=(),
        outputs=(NamedOutput(name=("evidence_set",), object_identifier="evidence-set"),),
        result={},
        ended_at_utc="2026-11-02T00:00:01+00:00",
        duration_ns=1,
    )
    await database.begin_operation(
        operation_id="guide-operation",
        component=build_analysis_component_registry().require(REGISTER_PRODUCT_GUIDE),
        provenance=_provenance(),
        invocation={},
        configuration={},
        started_at_utc="2026-11-02T00:00:00+00:00",
    )
    await database.complete_operation(
        operation_id="guide-operation",
        records=(
            RecordDraft(
                identifier="product-guide",
                kind=("carl", "analysis", "product_guide"),
                schema_version=1,
                value={"identity": ["carl", "product_guide", "telescope"], "version": 1},
            ),
        ),
        artifacts=(
            BytesDraft(
                identifier="guide-text",
                kind=("carl", "analysis", "product_guide_text"),
                media_type="text/plain; charset=utf-8",
                representation={"exact_product_guide": True},
                content=TELESCOPE_PRODUCT_GUIDE.encode(),
            ),
        ),
        outputs=(
            NamedOutput(name=("product_guide",), object_identifier="product-guide"),
            NamedOutput(name=("guide_text",), object_identifier="guide-text"),
        ),
        result={},
        ended_at_utc="2026-11-02T00:00:01+00:00",
        duration_ns=1,
    )


@pytest.mark.parametrize("external", (False, True))
async def test_real_facebook_evidence_reaches_child_as_exact_hardlinked_images(
    tmp_path: Path, external: bool
) -> None:
    executable = tmp_path / "fake-claude"
    _fake_claude(executable, expect_hard_link=True)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _evidence(database, tmp_path, external=external)
        registry = build_facebook_analysis_stage_registry(
            AnalysisWorkerDependencies(database, ClaudeCli(str(executable)), lambda: str(uuid4()))
        )
        first, adapter = await _register(
            database, registry, analyze_item_work(identifier="analysis-work", payload=_payload())
        )
        prepared = await _run(registry, stage_manager(database), first, adapter)
        assert isinstance(prepared, WaitWork)
        captured = await _run(registry, stage_manager(database), prepared.continuation, adapter)
        assert isinstance(captured, WaitWork)
        completed = await _run(registry, stage_manager(database), captured.continuation, adapter)
        assert isinstance(completed, CompletedWork)
        assert (
            completed.records[0].value["analysis_text"]
            == "Likely a telescope with tripod. Confidence: medium."
        )
        assert completed.records[0].value["claude"]["failure_kind"] is None


async def test_missing_guide_is_terminal_without_publishing_dangling_input(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        registry = build_facebook_analysis_stage_registry(
            AnalysisWorkerDependencies(database, _cli(tmp_path), lambda: str(uuid4()))
        )
        await _store(
            database,
            (
                RecordDraft(
                    identifier="evidence", kind=("test", "evidence"), schema_version=1, value={}
                ),
            ),
        )
        first, adapter = await _register(
            database,
            registry,
            analyze_item_work(
                identifier="analysis-work",
                payload=AnalyzeItemPayload(
                    evidence_set_record_identifier="evidence",
                    product_guide_record_identifier="missing",
                ),
            ),
        )
        failed = await _run(registry, stage_manager(database), first, adapter)
        assert isinstance(failed, TerminalFailureWork)
        assert failed.error["kind"] == "analysis_input_failure"
        assert tuple(item.object_identifier for item in failed.inputs) == ("evidence",)
        assert not (tmp_path / "calls").exists()
