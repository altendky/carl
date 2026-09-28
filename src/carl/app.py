"""Application orchestration around Carl's pure core and I/O adapters."""

from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter_ns
from uuid import uuid4

from carl.core.content_encoding import decoded_stored_body
from carl.core.facebook import classify_item_response, extract_listing, listing_id_from_url
from carl.core.http import RequestPlan, stored_content_encodings, stored_response_charset
from carl.core.models import BytesDraft, Header, JsonValue, NamedOutput, RecordDraft
from carl.facebook_workers import ACQUIRE_HTTP, EXTRACT_FACEBOOK, build_component_registry
from carl.io.httpx import AcquisitionFailure, DirectHttpxAcquirer, HttpAcquirer
from carl.io.provenance import collect_code_provenance_async, process_invocation
from carl.io.sqlite import Database


def _identifier() -> str:
    return str(uuid4())


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


async def collect_listing(
    database: Database,
    *,
    url: str,
    headers: tuple[Header, ...],
    routing: tuple[str, ...] = ("direct",),
    acquirer: HttpAcquirer | None = None,
) -> dict[str, JsonValue]:
    listing_id_from_url(url)
    registry = build_component_registry()
    component = registry.require(ACQUIRE_HTTP)
    plan = RequestPlan(url=url, headers=headers, routing=routing)
    operation_id = _identifier()
    started_at = _utc_now()
    started = perf_counter_ns()
    await database.begin_operation(
        operation_id=operation_id,
        component=component,
        provenance=await collect_code_provenance_async(_repository_root()),
        invocation=process_invocation(),
        configuration=plan.as_json(),
        started_at_utc=started_at,
    )
    try:
        acquisition = await (acquirer or DirectHttpxAcquirer()).acquire(plan, _identifier)
    except AcquisitionFailure as error:
        await database.fail_operation(
            operation_id=operation_id,
            error={"type": type(error).__name__, "message": str(error)},
            result=error.result,
            ended_at_utc=_utc_now(),
            duration_ns=perf_counter_ns() - started,
        )
        raise

    acquisition_record_id = _identifier()
    record_value = dict(acquisition.record)
    record_value.update(
        {
            "operation_id": operation_id,
            "requested_listing_id": listing_id_from_url(url),
            "started_at_utc": started_at,
        }
    )
    records = (
        RecordDraft(
            identifier=acquisition_record_id,
            kind=("carl", "http", "acquisition"),
            schema_version=1,
            value=record_value,
        ),
    )
    artifacts = tuple(
        BytesDraft(
            identifier=body.identifier,
            kind=("carl", "http", "response_body"),
            media_type=body.media_type,
            representation=body.representation,
            content=body.content,
        )
        for body in acquisition.bodies
    )
    outputs = [NamedOutput(name=("acquisition",), object_identifier=acquisition_record_id)]
    outputs.extend(
        NamedOutput(name=("response", str(index), "body"), object_identifier=body.identifier)
        for index, body in enumerate(acquisition.bodies)
    )
    hops = acquisition.record["hops"]
    if not isinstance(hops, list) or not isinstance(hops[-1], dict):
        raise AssertionError("Acquisition must have response metadata")
    response = hops[-1]["response"]
    if not isinstance(response, dict):
        raise AssertionError("Acquisition response must be an object")
    await database.complete_operation(
        operation_id=operation_id,
        records=records,
        artifacts=artifacts,
        outputs=outputs,
        result={
            "acquisition_record_id": acquisition_record_id,
            "http_status": response["status_code"],
        },
        ended_at_utc=_utc_now(),
        duration_ns=perf_counter_ns() - started,
    )
    extraction = await extract_acquisition(database, acquisition_record_id)
    return {
        "acquisition_operation_id": operation_id,
        "acquisition_record_id": acquisition_record_id,
        **extraction,
    }


async def extract_acquisition(
    database: Database, acquisition_record_id: str
) -> dict[str, JsonValue]:
    kind, _, acquisition = await database.get_record(acquisition_record_id)
    if kind != ("carl", "http", "acquisition") or not isinstance(acquisition, dict):
        raise ValueError("Input is not an HTTP acquisition record")
    hops = acquisition.get("hops")
    if not isinstance(hops, list) or not hops or not isinstance(hops[-1], dict):
        raise ValueError("Acquisition has no terminal response")
    response = hops[-1].get("response")
    if not isinstance(response, dict):
        raise ValueError("Acquisition terminal response is malformed")
    body = response.get("body")
    if not isinstance(body, dict) or body.get("state") != "available":
        raise ValueError("Acquisition has no complete terminal response body")
    body_identifier = body.get("artifact_id")
    if not isinstance(body_identifier, str):
        raise ValueError("Acquisition body reference is malformed")
    body_metadata, stored_body = await database.get_artifact(body_identifier)
    headers = response.get("headers")

    registry = build_component_registry()
    component = registry.require(EXTRACT_FACEBOOK)
    operation_id = _identifier()
    started_at = _utc_now()
    started = perf_counter_ns()
    await database.begin_operation(
        operation_id=operation_id,
        component=component,
        provenance=await collect_code_provenance_async(_repository_root()),
        invocation=process_invocation(),
        configuration={
            "acquisition_record_id": acquisition_record_id,
            "target_listing_id": acquisition.get("requested_listing_id"),
        },
        started_at_utc=started_at,
        inputs=(
            (("acquisition",), acquisition_record_id),
            (("terminal_response", "body"), body_identifier),
        ),
    )
    try:
        decoded = decoded_stored_body(
            stored_body, body_metadata.get("representation"), stored_content_encodings(headers)
        )
        charset, charset_source = stored_response_charset(headers)
        try:
            html = decoded.decode(charset)
            decoding_warnings: list[JsonValue] = []
        except (LookupError, UnicodeError):
            html = decoded.decode("utf-8", errors="replace")
            charset = "utf-8"
            charset_source = "fallback_after_decode_failure"
            decoding_warnings = [{"kind": "text_decoding_failure", "fallback": "utf-8-replace"}]
        requested_listing_id = acquisition.get("requested_listing_id")
        if not isinstance(requested_listing_id, str):
            raise ValueError("Acquisition has no requested listing identifier")
        extraction = extract_listing(
            html,
            listing_id=requested_listing_id,
            acquisition_record_id=acquisition_record_id,
        )
        effective_url = acquisition.get("effective_url")
        if not isinstance(effective_url, str):
            raise ValueError("Acquisition has no effective URL")
        classification = classify_item_response(
            html,
            requested_listing_id=requested_listing_id,
            effective_url=effective_url,
        )
    except Exception as error:
        await database.fail_operation(
            operation_id=operation_id,
            error={"type": type(error).__name__, "message": str(error)},
            result={"acquisition_record_id": acquisition_record_id},
            ended_at_utc=_utc_now(),
            duration_ns=perf_counter_ns() - started,
        )
        raise

    decoded_identifier = _identifier()
    block_identifiers = tuple(_identifier() for _ in extraction.blocks)
    observation_identifier = _identifier()
    observation = dict(extraction.observation)
    warnings = observation["warnings"]
    assert isinstance(warnings, list)
    warnings.extend(decoding_warnings)
    observation.update(
        {
            "operation_id": operation_id,
            "extractor": {
                "component_parts": list(component.identifier.parts),
                "output_schema_version": component.output_schema_version,
            },
            "json_blocks": [
                {"block_index": index, "record_id": identifier}
                for index, identifier in enumerate(block_identifiers)
            ],
            "decoded_html_artifact_id": decoded_identifier,
            "response_classification": classification.as_json(),
        }
    )
    block_records = tuple(
        RecordDraft(
            identifier=identifier,
            kind=("carl", "html", "embedded_json_block"),
            schema_version=1,
            value={
                **block,
                "acquisition_record_id": acquisition_record_id,
                "decoded_html_artifact_id": decoded_identifier,
            },
        )
        for identifier, block in zip(block_identifiers, extraction.blocks, strict=True)
    )
    records = (
        *block_records,
        RecordDraft(
            identifier=observation_identifier,
            kind=("carl", "facebook", "listing_observation"),
            schema_version=1,
            value=observation,
        ),
    )
    decoded_artifact = BytesDraft(
        identifier=decoded_identifier,
        kind=("carl", "http", "decoded_text"),
        media_type="text/html; charset=utf-8",
        representation={
            "kind": "content_decoded_utf8_text",
            "derived_from_artifact_id": body_identifier,
            "content_encodings_removed": list(stored_content_encodings(headers)),
            "source_charset": charset,
            "charset_source": charset_source,
            "exact_wire_bytes": False,
        },
        content=html.encode("utf-8"),
    )
    outputs = [NamedOutput(name=("decoded_html",), object_identifier=decoded_identifier)]
    outputs.extend(
        NamedOutput(name=("json_block", str(index)), object_identifier=identifier)
        for index, identifier in enumerate(block_identifiers)
    )
    outputs.append(
        NamedOutput(name=("listing_observation",), object_identifier=observation_identifier)
    )
    await database.complete_operation(
        operation_id=operation_id,
        records=records,
        artifacts=(decoded_artifact,),
        outputs=outputs,
        result={
            "observation_record_id": observation_identifier,
            "state": observation["state"],
            "warning_count": len(warnings),
            "json_block_count": len(block_identifiers),
        },
        ended_at_utc=_utc_now(),
        duration_ns=perf_counter_ns() - started,
    )
    return {
        "extraction_operation_id": operation_id,
        "observation_record_id": observation_identifier,
        "extraction_state": observation["state"],
        "warning_count": len(warnings),
    }
