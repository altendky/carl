"""Read retained eBay details without triggering any collection."""

from carl.core.marketplace_listing import (
    GetMarketplaceListingRequest,
    MarketplaceListingDetails,
    MarketplaceListingImage,
)
from carl.core.marketplace_search import Marketplace
from carl.core.models import JsonValue
from carl.io.sqlite import Database


async def ebay_observations(
    database: Database, item_identifier: str
) -> tuple[tuple[str, dict[str, JsonValue]], ...]:
    return tuple(
        (identifier, value)
        for identifier, value in await database.ebay_item_observations(item_identifier)
        if isinstance(value, dict) and value.get("item_identifier") == item_identifier
    )


async def _work_state(database: Database, identifier: str | None) -> str:
    if identifier is None:
        return "not_requested"
    try:
        return str((await database.work(identifier))["state"])
    except KeyError:
        return "pending"


async def read_ebay_listing(
    database: Database, request: GetMarketplaceListingRequest
) -> MarketplaceListingDetails:
    observations = await ebay_observations(database, request.external_identifier)
    latest_identifier, latest = observations[-1] if observations else (None, {})
    usable = [
        (identifier, value)
        for identifier, value in observations
        if value.get("classification") == "detail"
    ]
    identifier, detail = usable[-1] if usable else (latest_identifier, latest)
    acquisition_identifier = detail.get("acquisition_record_identifier")
    equivalent_observations = {identifier}
    if isinstance(acquisition_identifier, str):
        equivalent_observations.update(
            candidate_identifier
            for candidate_identifier, candidate in usable
            if candidate.get("acquisition_record_identifier") == acquisition_identifier
        )
    reference_records = {
        reference_identifier: value
        for reference_identifier, value in await database.records_by_kind(
            ("carl", "ebay", "gallery_image_reference")
        )
        if isinstance(value, dict)
        and value.get("observation_record_identifier") in equivalent_observations
        and isinstance(value.get("url"), str)
    }
    references = {
        value["url"]: (reference_identifier, value)
        for reference_identifier, value in reference_records.items()
    }
    results: dict[str, tuple[str, dict[str, JsonValue]]] = {}
    saved_by_url: dict[str, tuple[str, dict[str, JsonValue]]] = {}
    for result_identifier, value in await database.records_by_kind(
        ("carl", "ebay", "image_result")
    ):
        if (
            not isinstance(value, dict)
            or value.get("observation_record_identifier") not in equivalent_observations
        ):
            continue
        reference_identifier = value.get("reference_record_identifier")
        if isinstance(reference_identifier, str):
            existing = results.get(reference_identifier)
            if (
                existing is None
                or existing[1].get("state") != "saved"
                or value.get("state") == "saved"
            ):
                results[reference_identifier] = (result_identifier, value)
            if (
                value.get("state") == "saved"
                and isinstance(value.get("url"), str)
                and reference_identifier in reference_records
                and reference_records[reference_identifier].get("url") == value["url"]
            ):
                saved_by_url[value["url"]] = (result_identifier, value)
    urls = detail.get("gallery_urls", [])
    urls = [url for url in urls if isinstance(url, str)] if isinstance(urls, list) else []
    images: list[MarketplaceListingImage] = []
    for index, url in enumerate(urls[: request.maximum_images]):
        reference_identifier, reference = references.get(url, (None, {}))
        result_identifier, result = (
            results.get(reference_identifier, (None, {})) if reference_identifier else (None, {})
        )
        if result.get("state") != "saved" and url in saved_by_url:
            result_identifier, result = saved_by_url[url]
            reference_identifier = result["reference_record_identifier"]
            reference = reference_records[reference_identifier]
        work_identifier = reference.get("work_identifier")
        work_identifier = work_identifier if isinstance(work_identifier, str) else None
        artifact_identifier = result.get("image_artifact_identifier")
        locator = result.get("image_file_locator")
        images.append(
            MarketplaceListingImage(
                gallery_order=index,
                url=url,
                state=str(result.get("state") or await _work_state(database, work_identifier)),
                reference_record_identifier=reference_identifier,
                result_record_identifier=result_identifier,
                artifact_identifier=artifact_identifier
                if isinstance(artifact_identifier, str)
                else None,
                work_identifier=work_identifier,
                width=result.get("width"),
                height=result.get("height"),
                local_path=str(database.path.parent / locator)
                if isinstance(locator, str)
                else None,
            )
        )
    description = detail.get("description")
    description_state = "inline" if description else "not_available"
    description_result_identifier = None
    description_work_identifiers = detail.get("description_work_identifiers", [])
    description_work_identifiers = (
        tuple(value for value in description_work_identifiers if isinstance(value, str))
        if isinstance(description_work_identifiers, list)
        else ()
    )
    if detail.get("description_url"):
        description_state = await _work_state(
            database, description_work_identifiers[0] if description_work_identifiers else None
        )
    for result_identifier, value in await database.records_by_kind(
        ("carl", "ebay", "description_result")
    ):
        if (
            isinstance(value, dict)
            and value.get("observation_record_identifier") in equivalent_observations
            and (
                value.get("observation_record_identifier") == identifier
                or value.get("url") == detail.get("description_url")
            )
        ):
            if value.get("state") == "saved":
                description, description_state = value.get("description"), "saved"
                description_result_identifier = result_identifier
            elif description_state != "saved":
                description_state = "failed"
                description_result_identifier = result_identifier
    return MarketplaceListingDetails(
        marketplace=Marketplace.EBAY,
        external_identifier=request.external_identifier,
        canonical_url=f"https://www.ebay.com/itm/{request.external_identifier}",
        classification=str(latest.get("classification", "not_collected")),
        title=detail.get("title"),
        displayed_price=detail.get("displayed_price"),
        currency=detail.get("currency"),
        condition=detail.get("condition"),
        description=description,
        description_state=description_state,
        description_result_record_identifier=description_result_identifier,
        description_work_identifiers=description_work_identifiers,
        observation_record_identifier=identifier,
        acquisition_record_identifier=detail.get("acquisition_record_identifier"),
        latest_observation_record_identifier=latest_identifier,
        observation_record_identifiers=tuple(identifier for identifier, _ in observations[-100:]),
        observation_history_truncated=len(observations) > 100,
        images=tuple(images),
        referenced_image_count=len(urls),
        images_truncated=len(urls) > len(images),
    )
