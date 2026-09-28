"""Offline listing analysis selection, process handling, and provenance."""

import hashlib
import json
import sqlite3
from contextlib import closing
from itertools import count
from pathlib import Path
from time import perf_counter_ns, time_ns

import anyio
import httpx
import pytest

from carl._tests.test_facebook import HTML
from carl.cli import _ensure_product_guide, analyze_items
from carl.core.components import Component, ComponentId
from carl.core.facebook_images import GalleryImageReference, gallery_references
from carl.core.facebook_work import CollectItemPayload, collect_item_work
from carl.core.http import RequestPlan
from carl.core.item_analysis import (
    ANALYSIS_RECIPE_VERSION,
    IDENTIFICATION_PROMPT,
    TELESCOPE_PRODUCT_GUIDE,
    AnalysisImageSelection,
    AnalysisLimitKind,
    AnalysisLimitStatus,
    AnalyzeItemPayload,
    ClaudeEffort,
    ListingAnalysisEvidenceSet,
    ProductGuideKind,
    analysis_limit_observations,
    analyze_item_work,
    identification_prompt,
    listing_brief,
    product_guide_definition,
    saved_gallery_for_analysis,
)
from carl.core.models import (
    BytesDraft,
    CodeProvenance,
    ExternalFileDraft,
    NamedInput,
    NamedOutput,
    RecordDraft,
)
from carl.core.work import WorkRequester, WorkState
from carl.core.worker import WorkerSettings
from carl.facebook_analysis_workers import (
    REGISTER_PRODUCT_GUIDE,
    AnalysisWorkerDependencies,
    _image_result_satisfies_reference,
    build_analysis_component_registry,
    build_analysis_worker_registry,
)
from carl.facebook_workers import FacebookWorkerDependencies, build_facebook_worker_registry
from carl.io.claude import ClaudeCli, _parse_stream_output
from carl.io.httpx import DirectHttpxAcquirer
from carl.io.sqlite import Database
from carl.io.worker import WorkerRuntimeServices, execute_lease
from carl.review import ReviewApplication


def _provenance() -> CodeProvenance:
    return CodeProvenance(
        repository_url=None,
        commit_hash=None,
        worktree_state="dirty",
        package_version="test",
        python_implementation="test",
        python_version="test",
        dependencies=(),
        lockfile_sha256=None,
    )


async def _async_provenance() -> CodeProvenance:
    return _provenance()


def _reference() -> GalleryImageReference:
    return GalleryImageReference(
        listing_id="123",
        listing_observation_record_identifier="observation",
        acquisition_record_identifier="acquisition",
        block_index=0,
        json_path=("listing_photos", 0),
        original_url="https://example.fbcdn.net/photo?token=signed",
        gallery_order=0,
        photo_id="photo-1",
    )


def _payload() -> AnalyzeItemPayload:
    return AnalyzeItemPayload(
        evidence_set_record_identifier="evidence-set",
        product_guide_record_identifier="product-guide",
        claude_version="2.1.278 (Claude Code)",
    )


def test_analysis_payload_defaults_to_explicit_sonnet_model() -> None:
    assert _payload().model == "claude-sonnet-5"
    assert _payload().timeout_seconds == 210


def _evidence_set() -> ListingAnalysisEvidenceSet:
    return ListingAnalysisEvidenceSet(
        listing_observation_record_identifier="observation",
        gallery_images=(
            AnalysisImageSelection(
                gallery_image_reference_record_identifier="gallery-reference",
                image_result_record_identifier="image-result",
            ),
        ),
    )


def test_image_result_requires_exact_rendition_or_recorded_reuse() -> None:
    reference = _reference().model_dump(mode="json")
    result = {
        "state": "saved",
        "source_photo_id": reference["photo_id"],
        "original_url": reference["original_url"],
    }
    assert _image_result_satisfies_reference(
        reference_identifier="gallery-reference",
        reference=reference,
        image_result_identifier="image-result",
        image_result=result,
        reused_results_by_reference={},
    )

    refreshed_reference = {
        **reference,
        "original_url": f"{reference['original_url']}&refreshed=1",
    }
    assert not _image_result_satisfies_reference(
        reference_identifier="gallery-reference",
        reference=refreshed_reference,
        image_result_identifier="image-result",
        image_result=result,
        reused_results_by_reference={},
    )
    assert _image_result_satisfies_reference(
        reference_identifier="gallery-reference",
        reference=refreshed_reference,
        image_result_identifier="image-result",
        image_result=result,
        reused_results_by_reference={"gallery-reference": "image-result"},
    )
    assert not _image_result_satisfies_reference(
        reference_identifier="gallery-reference",
        reference=refreshed_reference,
        image_result_identifier="image-result",
        image_result=result,
        reused_results_by_reference={"gallery-reference": "another-result"},
    )
    assert not _image_result_satisfies_reference(
        reference_identifier="gallery-reference",
        reference=refreshed_reference,
        image_result_identifier="image-result",
        image_result={**result, "state": "failed"},
        reused_results_by_reference={"gallery-reference": "image-result"},
    )


@pytest.mark.anyio
async def test_terminal_analysis_work_does_not_block_explicit_retry(tmp_path: Path) -> None:
    database_path = tmp_path / "carl.sqlite3"
    payload = _payload()
    async with Database.managed(database_path, initialize=True) as database:
        await database.enqueue_work(
            analyze_item_work(identifier="failed-analysis", payload=payload),
            WorkRequester(
                request_identifier="request",
                kind=("test", "request"),
                identifier="subject",
                context={},
            ),
            event_identifier="enqueued",
            enqueued_at_utc_ns=1,
        )
        assert await database.facebook_item_analysis_work_identifier(payload) == ("failed-analysis")
        assert await database.facebook_item_analysis_work_identifier(
            payload.model_copy(update={"claude_version": None})
        ) == ("failed-analysis")

    with closing(sqlite3.connect(database_path)) as connection:
        connection.execute(
            "UPDATE work_items SET state = 'terminal_failure' WHERE id = 'failed-analysis'"
        )
        connection.commit()

    async with Database.managed(database_path) as database:
        assert await database.facebook_item_analysis_work_identifier(payload) is None

    with closing(sqlite3.connect(database_path)) as connection:
        connection.execute(
            """
            UPDATE work_items
            SET state = 'completed', payload_schema_version = 3
            WHERE id = 'failed-analysis'
            """
        )
        connection.commit()

    async with Database.managed(database_path) as database:
        assert await database.facebook_item_analysis_work_identifier(payload) == "failed-analysis"
        assert payload.model_copy(update={"claude_version": None}) in (
            await database.completed_facebook_item_analysis_payloads()
        )

    with closing(sqlite3.connect(database_path)) as connection:
        connection.execute("UPDATE work_items SET state = 'pending' WHERE id = 'failed-analysis'")
        connection.commit()

    async with Database.managed(database_path) as database:
        assert await database.facebook_item_analysis_work_identifier(payload) is None


def _observation() -> dict[str, object]:
    return {
        "listing_id": "123",
        "fields": {
            "title": {
                "state": "present",
                "evidence": [
                    {
                        "state": "present",
                        "normalized": "Celestron telescope",
                        "original": "Celestron telescope",
                        "evidence_kind": "seller_claim",
                    }
                ],
            },
            "description": {
                "state": "present",
                "evidence": [
                    {
                        "state": "present",
                        "normalized": "Includes tripod",
                        "original": "Includes tripod",
                        "evidence_kind": "seller_claim",
                    }
                ],
            },
            "location_text": {"state": "missing", "evidence": []},
            "seller": {"state": "present", "evidence": [{"original": "private seller"}]},
        },
    }


def test_exact_saved_gallery_and_brief() -> None:
    reference = _reference()
    payload = _payload()
    saved = {
        (reference.photo_id, reference.original_url): (
            "image-result",
            {
                "image_artifact_identifier": "image-file",
                "sha256": hashlib.sha256(b"image bytes").hexdigest(),
                "mime_type": "image/png",
            },
        )
    }
    reference_identifiers = ((reference, "gallery-reference"),)
    expected = _evidence_set().gallery_images
    assert saved_gallery_for_analysis((reference,), reference_identifiers, saved) == expected
    assert saved_gallery_for_analysis((reference, reference), reference_identifiers, saved) == (
        expected
    )
    assert saved_gallery_for_analysis((reference,), reference_identifiers, {}) is None
    changed_url = reference.model_copy(update={"original_url": reference.original_url + "&size=2"})
    assert saved_gallery_for_analysis((changed_url,), reference_identifiers, saved) is None
    assert saved_gallery_for_analysis(
        (changed_url,),
        (*reference_identifiers, (changed_url, "refreshed-gallery-reference")),
        saved,
        {
            "refreshed-gallery-reference": (
                "image-result",
                saved[(reference.photo_id, reference.original_url)][1],
            )
        },
    ) == (
        AnalysisImageSelection(
            gallery_image_reference_record_identifier="refreshed-gallery-reference",
            image_result_record_identifier="image-result",
        ),
    )
    assert (
        saved_gallery_for_analysis((reference, changed_url), reference_identifiers, saved) is None
    )
    brief = listing_brief(observation=_observation(), image_filenames=("images/000.png",))
    assert brief["location_precision"] == "source_approximate"
    assert brief["fields"]["title"]["values"][0]["evidence_kind"] == "seller_claim"
    assert brief["fields"]["location_text"]["state"] == "missing"
    assert "seller" not in brief["fields"]
    assert brief["gallery_images"][0]["filename"] == "images/000.png"
    assert brief["gallery_images"][0] == {"filename": "images/000.png"}
    changed = payload.model_copy(update={"model": "another-model"})
    assert analyze_item_work(identifier="work", payload=payload).deduplication_identity != (
        analyze_item_work(identifier="other", payload=changed).deduplication_identity
    )
    assert analyze_item_work(identifier="work", payload=payload).deduplication_identity != (
        analyze_item_work(
            identifier="other",
            payload=payload.model_copy(update={"effort": ClaudeEffort.HIGH}),
        ).deduplication_identity
    )
    assert analyze_item_work(identifier="work", payload=payload).deduplication_identity != (
        analyze_item_work(
            identifier="other", payload=payload.model_copy(update={"timeout_seconds": 151})
        ).deduplication_identity
    )


def test_researched_identification_recipe_and_claude_tools() -> None:
    assert ANALYSIS_RECIPE_VERSION == 11
    assert "Research gate:" in IDENTIFICATION_PROMPT
    assert "manufacturer's standard original contents" in IDENTIFICATION_PROMPT
    assert "Continue while successive" in IDENTIFICATION_PROMPT
    assert "searches are narrowing the identity" in IDENTIFICATION_PROMPT
    assert "write the best partial report supported so far" in IDENTIFICATION_PROMPT
    assert "no more than three web tool calls" in IDENTIFICATION_PROMPT
    assert "two distinct search refinements" in IDENTIFICATION_PROMPT
    assert "no more than 1,200 words" in IDENTIFICATION_PROMPT
    assert "Do not assess market price" in IDENTIFICATION_PROMPT
    assert "unavailable_gallery_images" in IDENTIFICATION_PROMPT
    guide = product_guide_definition(ProductGuideKind.TELESCOPE)
    assert guide.identity == ("carl", "product_guide", "telescope")
    assert guide.version == 1
    assert guide.text == TELESCOPE_PRODUCT_GUIDE
    assert "Do not infer hidden fork arms" in guide.text
    prompt = identification_prompt(guide.text)
    assert IDENTIFICATION_PROMPT.rstrip() in prompt
    assert guide.text in prompt
    argv = ClaudeCli().argv(prompt, model="claude-opus-5", effort=ClaudeEffort.MEDIUM)
    assert argv[argv.index("--tools") + 1] == "Read,WebSearch,WebFetch"
    assert argv[argv.index("--allowedTools") + 1] == "Read,WebSearch,WebFetch"
    assert argv[argv.index("--max-turns") + 1] == "8"
    configured_argv = ClaudeCli().argv(
        prompt,
        model="claude-opus-5",
        effort=ClaudeEffort.MEDIUM,
        maximum_turns=14,
    )
    assert configured_argv[configured_argv.index("--max-turns") + 1] == "14"


def test_limit_observations_and_stream_tool_calls() -> None:
    payload = _payload().model_copy(
        update={
            "target_duration_seconds": 1,
            "timeout_seconds": 2,
            "maximum_turns": 3,
            "maximum_web_tool_calls": 1,
            "maximum_report_words": 2,
        }
    )
    observations = analysis_limit_observations(
        payload=payload,
        duration_ns=1_500_000_000,
        observed_turns=4,
        observed_web_tool_calls=2,
        report_text="one two three",
        failure_kind="claude_maximum_turns_exceeded",
    )
    by_kind = {observation.kind: observation for observation in observations}
    assert by_kind[AnalysisLimitKind.TARGET_DURATION].status is AnalysisLimitStatus.EXCEEDED
    assert by_kind[AnalysisLimitKind.MAXIMUM_DURATION].status is AnalysisLimitStatus.WITHIN
    assert by_kind[AnalysisLimitKind.MAXIMUM_TURNS].status is AnalysisLimitStatus.EXCEEDED
    assert by_kind[AnalysisLimitKind.MAXIMUM_WEB_TOOL_CALLS].status is (
        AnalysisLimitStatus.EXCEEDED
    )
    assert by_kind[AnalysisLimitKind.MAXIMUM_REPORT_WORDS].status is (AnalysisLimitStatus.EXCEEDED)

    output, calls, version = _parse_stream_output(
        b'{"type":"system","subtype":"init",'
        b'"claude_code_version":"2.1.278"}\n'
        b'{"type":"assistant","message":{"content":['
        b'{"type":"tool_use","id":"one","name":"Read"},'
        b'{"type":"tool_use","id":"two","name":"WebSearch"}]}}\n'
        b'{"type":"assistant","message":{"content":['
        b'{"type":"tool_use","id":"two","name":"WebSearch"},'
        b'{"type":"tool_use","id":"three","name":"WebFetch"}]}}\n'
        b'{"type":"result","result":"report","is_error":false,"num_turns":4}\n'
    )
    assert output["result"] == "report"
    assert calls == ("Read", "WebSearch", "WebFetch")
    assert version == "2.1.278"


def _fake_claude(
    path: Path, *, failure: bool = False, sleep: bool = False, expect_hard_link: bool = False
) -> None:
    code = (
        """#!/usr/bin/env python3
import json
import pathlib
import sys
import time
if '--version' in sys.argv:
    print('2.1.278 (Claude Code)')
    raise SystemExit(0)
print(json.dumps({'type': 'system', 'subtype': 'init',
                  'claude_code_version': '2.1.278'}))
if SLEEP:
    print(json.dumps({'type': 'assistant', 'message': {'content': [
        {'type': 'tool_use', 'id': 'fetch', 'name': 'WebFetch'}
    ]}}), flush=True)
    print('started', file=sys.stderr, flush=True)
    time.sleep(10)
if FAILURE:
    print('model failed', file=sys.stderr)
    raise SystemExit(7)
listing = json.loads(pathlib.Path('listing.json').read_text())
assert listing['fields']['title']['values'][0]['normalized'] == 'Celestron telescope'
assert 'Product type: astronomical telescope and its mounting system.' in ' '.join(sys.argv)
for image in listing['gallery_images']:
    assert (pathlib.Path(image['filename']).stat().st_nlink > 1) is EXPECT_HARD_LINK
    assert pathlib.Path(image['filename']).read_bytes() == b'image bytes'
print(json.dumps({'result': 'Likely a telescope with tripod. Confidence: medium.',
                  'session_id': 'test-session', 'is_error': False}))
""".replace("SLEEP", str(sleep))
        .replace("FAILURE", str(failure))
        .replace("EXPECT_HARD_LINK", str(expect_hard_link))
    )
    path.write_text(code)
    path.chmod(0o700)


@pytest.mark.anyio
async def test_product_guide_registration_reuse_and_collision(tmp_path: Path) -> None:
    definition = product_guide_definition(ProductGuideKind.TELESCOPE)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        concurrent_results: list[str] = []

        async def ensure() -> None:
            concurrent_results.append(await _ensure_product_guide(database, definition))

        async with anyio.create_task_group() as group:
            group.start_soon(ensure)
            group.start_soon(ensure)
        assert len(set(concurrent_results)) == 1
        identifier = concurrent_results[0]
        assert await _ensure_product_guide(database, definition) == identifier
        assert await database.product_guide_record_identifiers(
            identity=definition.identity, version=definition.version
        ) == (identifier,)
        kind, schema_version, value = await database.get_record(identifier)
        assert kind == ("carl", "analysis", "product_guide")
        assert schema_version == 1
        assert value == {"identity": list(definition.identity), "version": definition.version}
        _, inputs, outputs = await database.object_operation_relations(identifier)
        assert inputs == ()
        output_by_name = {output.name: output.object_identifier for output in outputs}
        metadata, text = await database.get_artifact(output_by_name[("guide_text",)])
        assert metadata["representation"] == {"exact_product_guide": True}
        assert text.decode("utf-8") == definition.text

        await database.begin_operation(
            operation_id="collision-operation",
            component=build_analysis_component_registry().require(REGISTER_PRODUCT_GUIDE),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc="2026-09-22T00:00:00+00:00",
            inputs=(),
        )
        await database.complete_operation(
            operation_id="collision-operation",
            records=(
                RecordDraft(
                    identifier="collision-guide",
                    kind=("carl", "analysis", "product_guide"),
                    schema_version=1,
                    value={
                        "identity": list(definition.identity),
                        "version": definition.version,
                    },
                ),
            ),
            artifacts=(
                BytesDraft(
                    identifier="collision-text",
                    kind=("carl", "analysis", "product_guide_text"),
                    media_type="text/plain; charset=utf-8",
                    representation={"exact_product_guide": True},
                    content=b"different text",
                ),
            ),
            outputs=(
                NamedOutput(name=("product_guide",), object_identifier="collision-guide"),
                NamedOutput(name=("guide_text",), object_identifier="collision-text"),
            ),
            result={"state": "completed"},
            ended_at_utc="2026-09-22T00:00:01+00:00",
            duration_ns=1,
        )
        with pytest.raises(ValueError, match="changed without"):
            await _ensure_product_guide(database, definition)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("fail", "sleep", "external_image"),
    (
        (False, False, False),
        (False, False, True),
        (True, False, False),
        (True, False, True),
        (False, True, False),
    ),
)
async def test_durable_analysis_with_fake_claude(
    tmp_path: Path, fail: bool, sleep: bool, external_image: bool
) -> None:
    binary = tmp_path / "fake-claude"
    _fake_claude(binary, failure=fail, sleep=sleep, expect_hard_link=external_image)
    identifiers = count()

    def new_identifier() -> str:
        return f"generated-{next(identifiers)}"

    payload = _payload().model_copy(update={"timeout_seconds": 1}) if sleep else _payload()
    if external_image:
        image_path = tmp_path / "images" / "image.png"
        image_path.parent.mkdir()
        image_path.write_bytes(b"image bytes")
        image_artifact = ExternalFileDraft(
            identifier="image-file",
            kind=("carl", "facebook", "image_file"),
            media_type="image/png",
            representation={},
            sha256=hashlib.sha256(b"image bytes").hexdigest(),
            size=len(b"image bytes"),
            locator=image_path.relative_to(tmp_path).as_posix(),
        )
    else:
        image_artifact = BytesDraft(
            identifier="image-file",
            kind=("carl", "facebook", "image_file"),
            media_type="image/png",
            representation={},
            content=b"image bytes",
        )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await database.begin_operation(
            operation_id="seed-operation",
            component=Component(ComponentId(("test", "seed")), 1, lambda: None),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc="2026-09-22T00:00:00+00:00",
            inputs=(),
        )
        await database.complete_operation(
            operation_id="seed-operation",
            records=(
                RecordDraft(
                    identifier="observation",
                    kind=("carl", "facebook", "listing_observation"),
                    schema_version=1,
                    value=_observation(),
                ),
                RecordDraft(
                    identifier="acquisition",
                    kind=("carl", "http", "acquisition"),
                    schema_version=1,
                    value={},
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
                RecordDraft(
                    identifier="gallery-reference",
                    kind=("carl", "facebook", "gallery_image_reference"),
                    schema_version=1,
                    value=_reference().model_dump(mode="json"),
                ),
            ),
            artifacts=(image_artifact,),
            outputs=(
                NamedOutput(name=("observation",), object_identifier="observation"),
                NamedOutput(name=("acquisition",), object_identifier="acquisition"),
                NamedOutput(name=("image_result",), object_identifier="image-result"),
                NamedOutput(name=("image_file",), object_identifier="image-file"),
                NamedOutput(name=("gallery_reference",), object_identifier="gallery-reference"),
            ),
            result={},
            ended_at_utc="2026-09-22T00:00:01+00:00",
            duration_ns=1,
        )
        await database.begin_operation(
            operation_id="evidence-operation",
            component=Component(ComponentId(("test", "evidence")), 1, lambda: None),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc="2026-09-22T00:00:01+00:00",
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
            ended_at_utc="2026-09-22T00:00:02+00:00",
            duration_ns=1,
        )
        await database.begin_operation(
            operation_id="guide-operation",
            component=build_analysis_component_registry().require(REGISTER_PRODUCT_GUIDE),
            provenance=_provenance(),
            invocation={},
            configuration={},
            started_at_utc="2026-09-22T00:00:02+00:00",
            inputs=(),
        )
        await database.complete_operation(
            operation_id="guide-operation",
            records=(
                RecordDraft(
                    identifier="product-guide",
                    kind=("carl", "analysis", "product_guide"),
                    schema_version=1,
                    value={
                        "identity": ["carl", "product_guide", "telescope"],
                        "version": 1,
                    },
                ),
            ),
            artifacts=(
                BytesDraft(
                    identifier="product-guide-text",
                    kind=("carl", "analysis", "product_guide_text"),
                    media_type="text/plain; charset=utf-8",
                    representation={"exact_product_guide": True},
                    content=TELESCOPE_PRODUCT_GUIDE.encode("utf-8"),
                ),
            ),
            outputs=(
                NamedOutput(name=("product_guide",), object_identifier="product-guide"),
                NamedOutput(name=("guide_text",), object_identifier="product-guide-text"),
            ),
            result={"state": "completed"},
            ended_at_utc="2026-09-22T00:00:03+00:00",
            duration_ns=1,
        )
        enqueued = await database.enqueue_work(
            analyze_item_work(identifier="analysis-work", payload=payload),
            WorkRequester(
                request_identifier="request",
                kind=("test", "request"),
                identifier="123",
                context={},
            ),
            event_identifier="enqueued",
            enqueued_at_utc_ns=time_ns(),
        )
        assert await database.outstanding_facebook_item_analysis_work(
            listing_identifiers=("123",),
            maximum_items=1,
            product_guide_record_identifier="product-guide",
        ) == (enqueued.work_item_identifier,)
        assert await database.outstanding_facebook_item_analysis_work(
            listing_identifiers=("123",),
            maximum_items=1,
            recipe_version=payload.recipe_version,
            product_guide_record_identifier="product-guide",
        ) == (enqueued.work_item_identifier,)
        assert (
            await database.outstanding_facebook_item_analysis_work(
                listing_identifiers=("123",),
                maximum_items=1,
                product_guide_record_identifier="other-guide",
            )
            == ()
        )
        assert (
            await database.outstanding_facebook_item_analysis_work(
                listing_identifiers=("123",),
                maximum_items=1,
                recipe_version=payload.recipe_version + 1,
            )
            == ()
        )
        assert (
            await database.outstanding_facebook_item_analysis_work(
                listing_identifiers=("456",), maximum_items=1
            )
            == ()
        )
        claim = await database.claim_work(
            supported_capabilities=build_analysis_worker_registry(
                AnalysisWorkerDependencies(database, ClaudeCli(str(binary)), new_identifier)
            ).capabilities,
            worker_identifier="worker",
            lease_token="token",
            lease_duration_ns=10_000_000_000,
            utc_now_ns=time_ns,
            event_identifier="claimed",
        )
        assert claim.lease is not None
        await execute_lease(
            database=database,
            registry=build_analysis_worker_registry(
                AnalysisWorkerDependencies(database, ClaudeCli(str(binary)), new_identifier)
            ),
            settings=WorkerSettings(
                worker_count=1,
                lease_duration_ns=10_000_000_000,
                renewal_interval_ns=1_000_000_000,
                idle_poll_interval_ns=10_000_000,
            ),
            services=WorkerRuntimeServices(
                new_identifier=new_identifier,
                utc_now_ns=time_ns,
                monotonic_ns=perf_counter_ns,
                code_provenance=_async_provenance,
                invocation=lambda: {},
            ),
            lease=claim.lease,
        )
        work = await database.work(enqueued.work_item_identifier)
        expected_state = (
            WorkState.PENDING.value
            if sleep
            else WorkState.TERMINAL_FAILURE.value
            if fail
            else WorkState.COMPLETED.value
        )
        assert work["state"] == expected_state
        assert isinstance(work["result"], dict)
        result_identifier = work["result"]["analysis_record_identifier"]
        kind, _, result = await database.get_record(result_identifier)
        assert kind == ("carl", "facebook", "item_analysis")
        assert set(result) == {
            "state",
            "analysis_text",
            "limit_observations",
            "warnings",
            "claude",
        }
        assert result["analysis_text"] == (
            None if fail or sleep else "Likely a telescope with tripod. Confidence: medium."
        )
        assert result["claude"]["exit_code"] == (None if sleep else 7 if fail else 0)
        assert result["claude"]["version"] == "2.1.278"
        assert result["claude"]["version_source"] == "stream_json_system_init"
        assert "--restricted" in result["claude"]["argv"]
        assert result["claude"]["tool_calls"] == (["WebFetch"] if sleep else [])
        limits = {limit["kind"]: limit for limit in result["limit_observations"]}
        assert limits["maximum_web_tool_calls"]["observed"] == (1 if sleep else 0)
        assert limits["maximum_report_words"]["status"] == (
            "unavailable" if fail or sleep else "within"
        )
        assert result["warnings"] == []
        canonical_payload = payload.model_copy(update={"claude_version": None})
        assert (
            canonical_payload in (await database.completed_facebook_item_analysis_payloads())
        ) is (not fail and not sleep)
        if not fail and not sleep:
            projection_descriptors = await database.facebook_projection_analysis_descriptors(
                ("123",),
                product_guide_record_identifier="product-guide",
                maximum_per_listing=2,
                as_of_completion_sequence=(await database.current_completion_boundary()),
            )
            assert len(projection_descriptors) == 1
            assert projection_descriptors[0][0] == "123"
            assert projection_descriptors[0][1].analysis_record_identifier == result_identifier
        _, inputs, outputs = await database.object_operation_relations(result_identifier)
        assert inputs == (
            NamedInput(name=("listing_analysis_evidence",), object_identifier="evidence-set"),
            NamedInput(name=("product_guide",), object_identifier="product-guide"),
        )
        output_by_name = {output.name: output.object_identifier for output in outputs}
        _, input_bytes = await database.get_artifact(output_by_name[("listing_input",)])
        agent_input = json.loads(input_bytes)
        assert set(agent_input) == {
            "listing_id",
            "location_precision",
            "fields",
            "gallery_images",
            "unavailable_gallery_images",
            "gallery_absence_reason",
        }
        assert agent_input["unavailable_gallery_images"] == []
        assert agent_input["gallery_absence_reason"] is None
        assert agent_input["gallery_images"] == [{"filename": "images/000.png"}]
        descriptors = await database.facebook_analysis_descriptors(("observation",))
        assert tuple(descriptor.analysis_record_identifier for descriptor in descriptors) == (
            () if fail or sleep else (result_identifier,)
        )
        exact_report = await ReviewApplication(database, tmp_path).get_listing_analysis(
            result_identifier
        )
        assert exact_report.descriptor.state == ("failed" if fail or sleep else "completed")
        _, stderr = await database.get_artifact(output_by_name[("claude", "stderr")])
        assert (b"model failed" in stderr) is fail
        assert (b"started" in stderr) is sleep
        stdout_metadata, _ = await database.get_artifact(output_by_name[("claude", "stdout")])
        assert stdout_metadata["media_type"] == "application/x-ndjson"
        outstanding = await database.outstanding_facebook_item_analysis_work(
            listing_identifiers=None, maximum_items=1
        )
        assert outstanding == ((enqueued.work_item_identifier,) if sleep else ())


@pytest.mark.anyio
async def test_claude_process_failure_and_timeout(tmp_path: Path) -> None:
    failed = tmp_path / "failed-claude"
    _fake_claude(failed, failure=True)
    failure = await ClaudeCli(str(failed)).run(
        directory=tmp_path,
        prompt="test",
        model="sonnet",
        effort=ClaudeEffort.MEDIUM,
        timeout_seconds=2,
    )
    assert failure.failure_kind == "claude_process_failure"
    assert failure.exit_code == 7
    assert b"model failed" in failure.stderr

    maxed = tmp_path / "maxed-claude"
    maxed.write_text(
        """#!/usr/bin/env python3
import json
import sys
if '--version' in sys.argv:
    print('2.1.278 (Claude Code)')
    raise SystemExit(0)
print(json.dumps({'type': 'system', 'subtype': 'init',
                  'claude_code_version': '2.1.278'}))
print(json.dumps({'type': 'result', 'result': '', 'is_error': True,
                  'subtype': 'error_max_turns', 'terminal_reason': 'max_turns',
                  'num_turns': 9}))
raise SystemExit(1)
"""
    )
    maxed.chmod(0o700)
    maximum_turns = await ClaudeCli(str(maxed)).run(
        directory=tmp_path,
        prompt="test",
        model="sonnet",
        effort=ClaudeEffort.MEDIUM,
        timeout_seconds=2,
        maximum_turns=8,
    )
    assert maximum_turns.failure_kind == "claude_maximum_turns_exceeded"
    assert maximum_turns.output_metadata is not None
    assert maximum_turns.output_metadata["num_turns"] == 9

    sleeping = tmp_path / "sleeping-claude"
    _fake_claude(sleeping, sleep=True)
    with anyio.fail_after(3):
        timeout = await ClaudeCli(str(sleeping)).run(
            directory=tmp_path,
            prompt="test",
            model="sonnet",
            effort=ClaudeEffort.MEDIUM,
            timeout_seconds=1,
        )
    assert timeout.failure_kind == "claude_timeout"
    assert b"started" in timeout.stderr
    assert timeout.version == "2.1.278"
    assert timeout.tool_calls == ("WebFetch",)

    started = perf_counter_ns()
    with anyio.move_on_after(0.2) as cancellation:
        await ClaudeCli(str(sleeping)).run(
            directory=tmp_path,
            prompt="test",
            model="sonnet",
            effort=ClaudeEffort.MEDIUM,
            timeout_seconds=20,
        )
    assert cancellation.cancel_called
    assert perf_counter_ns() - started < 3_000_000_000


def test_cli_plans_evidence_and_skips_completed(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = tmp_path / "carl.sqlite3"
    binary = tmp_path / "fake-claude"
    _fake_claude(binary)
    html = (
        HTML.replace("A telescope", "Celestron telescope")
        .replace("https://example.invalid/one.jpg", "https://example.fbcdn.net/one.jpg")
        .replace("https://example.invalid/two.jpg", "https://example.fbcdn.net/two.jpg")
    )

    async def seed() -> None:
        numbers = count()

        def identifier() -> str:
            return f"seed-{next(numbers)}"

        class HtmlStream(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield html.encode()

        transport = httpx.MockTransport(
            lambda request: httpx.Response(
                200, headers={"content-type": "text/html; charset=utf-8"}, stream=HtmlStream()
            )
        )
        services = WorkerRuntimeServices(
            new_identifier=identifier,
            utc_now_ns=time_ns,
            monotonic_ns=perf_counter_ns,
            code_provenance=_async_provenance,
            invocation=lambda: {},
        )
        settings = WorkerSettings(
            worker_count=1,
            lease_duration_ns=10_000_000_000,
            renewal_interval_ns=1_000_000_000,
            idle_poll_interval_ns=10_000_000,
        )
        async with Database.managed(path, initialize=True) as database:
            registry = build_facebook_worker_registry(
                FacebookWorkerDependencies(
                    database=database,
                    acquirer=DirectHttpxAcquirer(transport),
                    new_identifier=identifier,
                )
            )
            await database.enqueue_work(
                collect_item_work(
                    identifier="collect-item",
                    payload=CollectItemPayload(
                        listing_id="123",
                        request_plan=RequestPlan(
                            url="https://www.facebook.com/marketplace/item/123/",
                            routing=("test", "mock"),
                        ),
                    ),
                    not_before_utc_ns=0,
                ),
                WorkRequester(
                    request_identifier="source-request",
                    kind=("test", "request"),
                    identifier="123",
                    context={},
                ),
                event_identifier=identifier(),
                enqueued_at_utc_ns=time_ns(),
            )
            for _ in range(2):
                claim = await database.claim_work(
                    supported_capabilities=registry.capabilities,
                    worker_identifier="source-worker",
                    lease_token=identifier(),
                    lease_duration_ns=settings.lease_duration_ns,
                    utc_now_ns=time_ns,
                    event_identifier=identifier(),
                )
                assert claim.lease is not None
                await execute_lease(
                    database=database,
                    registry=registry,
                    settings=settings,
                    services=services,
                    lease=claim.lease,
                )
            latest = (await database.successful_facebook_item_page_results(("123",)))[-1]
            _, _, observation = await database.get_record(latest.observation_record_identifier)
            references = gallery_references(
                observation_identifier=latest.observation_record_identifier,
                observation=observation,
            )
            assert len(references) == 2
            await database.begin_operation(
                operation_id="seed-images",
                component=Component(ComponentId(("test", "images")), 1, lambda: None),
                provenance=_provenance(),
                invocation={},
                configuration={},
                started_at_utc="2026-09-22T00:00:00+00:00",
                inputs=(),
            )
            image_records = []
            image_artifacts = []
            image_outputs = []
            for index, reference in enumerate(references):
                result_id = f"image-result-{index}"
                artifact_id = f"image-file-{index}"
                reference_id = f"gallery-reference-{index}"
                image_records.extend(
                    (
                        RecordDraft(
                            identifier=reference_id,
                            kind=("carl", "facebook", "gallery_image_reference"),
                            schema_version=1,
                            value=reference.model_dump(mode="json"),
                        ),
                        RecordDraft(
                            identifier=result_id,
                            kind=("carl", "facebook", "image_result"),
                            schema_version=1,
                            value={
                                "state": "saved",
                                "source_photo_id": reference.photo_id,
                                "original_url": reference.original_url,
                                "image_artifact_identifier": artifact_id,
                            },
                        ),
                    )
                )
                image_artifacts.append(
                    BytesDraft(
                        identifier=artifact_id,
                        kind=("carl", "facebook", "image_file"),
                        media_type="image/png",
                        representation={},
                        content=b"image bytes",
                    )
                )
                image_outputs.extend(
                    (
                        NamedOutput(name=("image_result", str(index)), object_identifier=result_id),
                        NamedOutput(name=("image_file", str(index)), object_identifier=artifact_id),
                        NamedOutput(
                            name=("gallery_reference", str(index)),
                            object_identifier=reference_id,
                        ),
                    )
                )
            await database.complete_operation(
                operation_id="seed-images",
                records=tuple(image_records),
                artifacts=tuple(image_artifacts),
                outputs=tuple(image_outputs),
                result={},
                ended_at_utc="2026-09-22T00:00:01+00:00",
                duration_ns=1,
            )

    anyio.run(seed, backend="trio")
    analyze_items(
        "123",
        product_guide=ProductGuideKind.TELESCOPE,
        database=path,
        claude_executable=str(binary),
    )
    first = json.loads(capsys.readouterr().out)
    assert first["completed"] == 1
    assert first["resumed"] == 0
    guide_identifier = first["product_guide_record_identifier"]
    assert isinstance(guide_identifier, str)

    async def verify_evidence_graph() -> None:
        async with Database.managed(path) as database:
            evidence_sets = await database.facebook_listing_analysis_evidence_sets()
            assert len(evidence_sets) == 1
            evidence, identifier = next(iter(evidence_sets.items()))
            kind, schema_version, value = await database.get_record(identifier)
            assert kind == ("carl", "facebook", "listing_analysis_evidence")
            assert schema_version == 1
            assert value == {}
            _, inputs, _ = await database.object_operation_relations(identifier)
            assert inputs[0].name == ("gallery_image_reference", "00000000")
            assert {input_value.object_identifier for input_value in inputs} == {
                evidence.listing_observation_record_identifier,
                *(
                    image.gallery_image_reference_record_identifier
                    for image in evidence.gallery_images
                ),
                *(image.image_result_record_identifier for image in evidence.gallery_images),
            }
            assert await database.product_guide_record_identifiers(
                identity=("carl", "product_guide", "telescope"), version=1
            ) == (guide_identifier,)
            review = ReviewApplication(database, tmp_path)
            dossier = await review.get_listing_dossier("123")
            assert dossier.selected_observation_record_identifier == (
                evidence.listing_observation_record_identifier
            )
            assert tuple(image.gallery_order for image in dossier.gallery) == (0, 1)
            assert tuple(image.image_artifact_identifier for image in dossier.gallery) == (
                "image-file-0",
                "image-file-1",
            )
            image = await review.get_image("image-file-0")
            assert image.content == b"image bytes"

    anyio.run(verify_evidence_graph, backend="trio")
    analyze_items(
        "123",
        product_guide=ProductGuideKind.TELESCOPE,
        database=path,
        claude_executable=str(binary),
    )
    second = json.loads(capsys.readouterr().out)
    assert second["selected"] == 0
    assert second["skipped_existing"] == 1
    assert second["product_guide_record_identifier"] == guide_identifier
