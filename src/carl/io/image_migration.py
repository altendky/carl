"""Resumable migration of validated image bytes from SQLite to files."""

from collections.abc import Callable

import anyio

from carl.core.components import Component, ComponentId, Registry
from carl.core.facebook_images import verify_image
from carl.core.models import JsonValue
from carl.io.image_files import ImageFileStore
from carl.io.sqlite import Database

MIGRATE_EXTERNAL_IMAGE_FILES = ComponentId(("carl", "storage", "migrate", "external_image_files"))


def build_image_migration_component_registry() -> Registry:
    return Registry((Component(MIGRATE_EXTERNAL_IMAGE_FILES, 1, externalize_saved_images),))


def _terminal_body(acquisition: dict[str, JsonValue]) -> dict[str, JsonValue]:
    hops = acquisition.get("hops")
    if not isinstance(hops, list) or not hops or not isinstance(hops[-1], dict):
        raise ValueError("Image acquisition has no terminal response")
    response = hops[-1].get("response")
    if not isinstance(response, dict):
        raise ValueError("Image acquisition terminal response is malformed")
    body = response.get("body")
    if not isinstance(body, dict) or body.get("state") != "available":
        raise ValueError("Image acquisition has no retained body")
    return body


def _replace_terminal_body_artifact(
    acquisition: dict[str, JsonValue],
    *,
    image_identifier: str,
    legacy_identifier: str,
    migration_operation_identifier: str,
) -> dict[str, JsonValue]:
    updated = dict(acquisition)
    hops = updated.get("hops")
    if not isinstance(hops, list) or not hops or not isinstance(hops[-1], dict):
        raise ValueError("Image acquisition has no terminal response")
    copied_hops = [dict(hop) if isinstance(hop, dict) else hop for hop in hops]
    terminal = copied_hops[-1]
    if not isinstance(terminal, dict):
        raise ValueError("Image acquisition terminal response is malformed")
    response = terminal.get("response")
    if not isinstance(response, dict):
        raise ValueError("Image acquisition terminal response is malformed")
    copied_response = dict(response)
    body = copied_response.get("body")
    if not isinstance(body, dict):
        raise ValueError("Image acquisition body metadata is malformed")
    copied_body = dict(body)
    copied_body.update(
        {
            "artifact_id": image_identifier,
            "retained_as": "validated_image_file",
            "legacy_response_body_artifact_id": legacy_identifier,
            "image_storage_migration_operation_identifier": migration_operation_identifier,
        }
    )
    copied_response["body"] = copied_body
    terminal["response"] = copied_response
    updated["hops"] = copied_hops
    return updated


async def externalize_saved_images(
    *,
    database: Database,
    image_files: ImageFileStore,
    migration_operation_identifier: str,
    progress: Callable[[int, int], None] | None = None,
) -> dict[str, JsonValue]:
    """Externalize every saved image and its legacy response-body artifact."""

    results = await database.saved_facebook_image_results()
    processed_artifacts: set[str] = set()
    externalized_artifacts = 0
    migrated_results = 0
    for result_identifier, result in results:
        image_identifier = result.get("image_artifact_identifier")
        acquisition_identifier = result.get("acquisition_record_identifier")
        expected_sha256 = result.get("sha256")
        expected_media_type = result.get("mime_type")
        if not (
            isinstance(image_identifier, str)
            and isinstance(acquisition_identifier, str)
            and isinstance(expected_sha256, str)
            and isinstance(expected_media_type, str)
        ):
            raise ValueError("Saved image result lacks migration identity")
        image_metadata, image_content = await database.get_artifact(image_identifier)
        verified = await anyio.to_thread.run_sync(
            verify_image,
            image_content,
            [{"name_latin1": "Content-Type", "value_latin1": expected_media_type}],
            {"kind": "content_decoded_http_body", "content_decoded": True},
            abandon_on_cancel=True,
        )
        if verified.sha256 != expected_sha256 or verified.media_type != expected_media_type:
            raise ValueError("Saved image result does not match its retained bytes")
        acquisition_kind, _, acquisition = await database.get_record(acquisition_identifier)
        if acquisition_kind != ("carl", "http", "acquisition") or not isinstance(acquisition, dict):
            raise ValueError("Saved image acquisition is malformed")
        body = _terminal_body(acquisition)
        body_identifier = body.get("artifact_id")
        if not isinstance(body_identifier, str):
            raise ValueError("Saved image acquisition body identifier is malformed")
        body_metadata, body_content = await database.get_artifact(body_identifier)
        if body_content != verified.content:
            raise ValueError("Image body and validated image bytes differ")
        stored = await image_files.publish(
            content=verified.content,
            media_type=verified.media_type,
            sha256=verified.sha256,
        )
        for identifier, metadata in (
            (body_identifier, body_metadata),
            (image_identifier, image_metadata),
        ):
            if identifier in processed_artifacts:
                continue
            if metadata.get("sha256") != stored.sha256 or metadata.get("size") != stored.size:
                raise ValueError("Image artifact metadata differs from external file")
            externalized = await database.externalize_artifact(
                identifier=identifier,
                locator=stored.locator,
                sha256=stored.sha256,
                size=stored.size,
                migration_operation_identifier=migration_operation_identifier,
            )
            processed_artifacts.add(identifier)
            externalized_artifacts += int(externalized)
        updated_result = dict(result)
        existing_locator = updated_result.get("image_file_locator")
        if existing_locator is not None and existing_locator != stored.locator:
            raise ValueError("Saved image result has a different external file locator")
        if existing_locator is None:
            updated_result["image_file_locator"] = stored.locator
            updated_result["image_storage_migration_operation_identifier"] = (
                migration_operation_identifier
            )
            await database.replace_record_value(
                identifier=result_identifier,
                expected_kind=("carl", "facebook", "image_result"),
                value=updated_result,
            )
        if body.get("retained_as") != "validated_image_file":
            updated_acquisition = _replace_terminal_body_artifact(
                acquisition,
                image_identifier=image_identifier,
                legacy_identifier=body_identifier,
                migration_operation_identifier=migration_operation_identifier,
            )
            await database.replace_record_value(
                identifier=acquisition_identifier,
                expected_kind=("carl", "http", "acquisition"),
                value=updated_acquisition,
            )
        migrated_results += 1
        if progress is not None:
            progress(migrated_results, len(results))
    return {
        "saved_image_results": len(results),
        "migrated_image_results": migrated_results,
        "externalized_artifacts": externalized_artifacts,
    }
