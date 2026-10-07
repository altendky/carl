"""Explicit, offline rebuilding of queued work after a network route cutover.

This is a migration helper, not a runtime route alias. Callers retain the old
payload in an audit operation and atomically replace the complete definition.
Completed work and immutable evidence records must not be passed to it.
"""

from collections.abc import Mapping

from carl.core.facebook_images import (
    COLLECT_IMAGE_WORK_KIND,
    CollectImagePayload,
    collect_image_work,
)
from carl.core.facebook_listing import (
    REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND,
    RequestFacebookListingDetailsPayload,
    request_facebook_listing_details_work,
)
from carl.core.facebook_refresh import (
    REFRESH_SEARCH_WORK_KIND,
    RefreshSearchPayload,
    refresh_search_work,
)
from carl.core.facebook_work import (
    COLLECT_ITEM_WORK_KIND,
    COLLECT_SEARCH_WORK_KIND,
    CollectItemPayload,
    CollectSearchPayload,
    collect_item_work,
    collect_search_work,
)
from carl.core.json import encode_json
from carl.core.models import JsonValue
from carl.core.pipeline import (
    LISTING_PIPELINE_WORK_KIND,
    SEARCH_PIPELINE_WORK_KIND,
    PipelineListingPayload,
    RequestSearchPipelinePayload,
    listing_pipeline_work,
    search_pipeline_work,
)
from carl.core.work import WorkDefinition


def _replace_paths(
    value: JsonValue,
    replacements: Mapping[tuple[str, ...], tuple[str, ...]],
) -> JsonValue:
    if isinstance(value, list):
        if all(isinstance(part, str) for part in value):
            path = tuple(str(part) for part in value)
            replacement = replacements.get(path)
            if replacement is not None:
                return list(replacement)
        return [_replace_paths(item, replacements) for item in value]
    if isinstance(value, dict):
        return {key: _replace_paths(item, replacements) for key, item in value.items()}
    return value


def rebuild_work_definition(
    *,
    identifier: str,
    kind: tuple[str, ...],
    payload: JsonValue,
    not_before_utc_ns: int,
    replacements: Mapping[tuple[str, ...], tuple[str, ...]],
) -> WorkDefinition | None:
    """Rebuild affected work, including its route-sensitive deduplication/scopes.

    The caller owns lease fencing, old-payload comparison, audit publication,
    and collision checks. An unknown affected kind fails closed so removing a
    route override cannot silently strand executable work.
    """

    for source, target in replacements.items():
        if not source or not target or any(not part for part in (*source, *target)):
            raise ValueError("Cutover network paths require nonempty parts")
    revised = _replace_paths(payload, replacements)
    if (
        kind in (SEARCH_PIPELINE_WORK_KIND, LISTING_PIPELINE_WORK_KIND)
        and isinstance(revised, dict)
        and isinstance(revised.get("options"), dict)
    ):
        options = revised["options"]
        assert isinstance(options, dict)
        legacy_route = options.get("facebook_image_route")
        if isinstance(legacy_route, str):
            replacement = replacements.get(("proton", "personal", legacy_route))
            if replacement is not None:
                if "facebook_image_network_path" in options:
                    raise ValueError("Pipeline payload contains both legacy and explicit routes")
                options.pop("facebook_image_route")
                options["facebook_image_network_path"] = list(replacement)
    if revised == payload:
        return None
    revised_json = encode_json(revised)
    match kind:
        case value if value == COLLECT_SEARCH_WORK_KIND:
            return collect_search_work(
                identifier=identifier,
                payload=CollectSearchPayload.model_validate_json(revised_json),
                not_before_utc_ns=not_before_utc_ns,
            )
        case value if value == REFRESH_SEARCH_WORK_KIND:
            return refresh_search_work(
                identifier=identifier,
                payload=RefreshSearchPayload.model_validate_json(revised_json),
                not_before_utc_ns=not_before_utc_ns,
            )
        case value if value == REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND:
            return request_facebook_listing_details_work(
                identifier=identifier,
                payload=RequestFacebookListingDetailsPayload.model_validate_json(revised_json),
                not_before_utc_ns=not_before_utc_ns,
            )
        case value if value == COLLECT_ITEM_WORK_KIND:
            return collect_item_work(
                identifier=identifier,
                payload=CollectItemPayload.model_validate_json(revised_json),
                not_before_utc_ns=not_before_utc_ns,
            )
        case value if value == COLLECT_IMAGE_WORK_KIND:
            return collect_image_work(
                identifier=identifier,
                payload=CollectImagePayload.model_validate_json(revised_json),
                not_before_utc_ns=not_before_utc_ns,
            )
        case value if value == SEARCH_PIPELINE_WORK_KIND:
            return search_pipeline_work(
                identifier=identifier,
                payload=RequestSearchPipelinePayload.model_validate_json(revised_json),
                not_before_utc_ns=not_before_utc_ns,
            )
        case value if value == LISTING_PIPELINE_WORK_KIND:
            return listing_pipeline_work(
                identifier=identifier,
                payload=PipelineListingPayload.model_validate_json(revised_json),
                not_before_utc_ns=not_before_utc_ns,
            )
        case _:
            raise ValueError(f"No route-cutover definition builder for affected work kind {kind!r}")
