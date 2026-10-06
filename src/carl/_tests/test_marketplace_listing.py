from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import ValidationError

from carl._tests.test_ebay import _provenance
from carl.core.components import Component, ComponentId
from carl.core.composed_projection import (
    ComposedField,
    ComposedListingProjection,
    ComposedStatus,
    GetComposedListingRequest,
    ListingStatus,
    ProjectionEvidence,
    ProjectionSourceKind,
    projection_revision,
)
from carl.core.ebay_items import COLLECT_EBAY_ITEM_WORK_KIND, EXTRACT_EBAY_ITEM_WORK_KIND
from carl.core.marketplace_listing import (
    GetMarketplaceListingRequest,
    RequestEbayListingDetailsRequest,
)
from carl.core.marketplace_search import Marketplace
from carl.core.models import BytesDraft, JsonValue, NamedOutput, RecordDraft
from carl.io.sqlite import Database
from carl.review import ReviewApplication, ReviewInputError


def _record(identifier: str, kind: str, **value: object) -> RecordDraft:
    return RecordDraft(
        identifier=identifier, kind=("carl", "ebay", kind), schema_version=1, value=value
    )


async def _publish(
    database: Database, records: tuple[RecordDraft, ...], artifacts: tuple[BytesDraft, ...] = ()
) -> None:
    identifier = str(uuid4())
    await database.begin_operation(
        operation_id=identifier,
        component=Component(ComponentId(("test", "listing")), 1, lambda: None),
        provenance=_provenance(),
        invocation={},
        configuration={},
        started_at_utc="2026-09-29T00:00:00+00:00",
    )
    await database.complete_operation(
        operation_id=identifier,
        records=records,
        artifacts=artifacts,
        outputs=tuple(
            NamedOutput(name=("record", r.identifier), object_identifier=r.identifier)
            for r in records
        )
        + tuple(
            NamedOutput(name=("artifact", a.identifier), object_identifier=a.identifier)
            for a in artifacts
        ),
        result={},
        ended_at_utc="2026-09-29T00:00:01+00:00",
        duration_ns=1,
    )


@pytest.mark.anyio
async def test_ebay_detail_history_partial_images_and_saved_image_access(tmp_path: Path) -> None:
    item = "256123456789"
    urls = ["https://i.ebayimg.com/images/one.jpg", "https://i.ebayimg.com/images/two.jpg"]
    records = (
        _record(
            "detail",
            "listing_observation",
            item_identifier=item,
            classification="detail",
            title="Bench scope",
            displayed_price="$49.95",
            condition="Used",
            description="Inline summary",
            description_url="https://vi.vipr.ebaydesc.com/description",
            acquisition_record_identifier="acquisition",
            request={
                "item_identifier": item,
                "stack_identifier": "ebay_anonymous",
                "maximum_images": 20,
            },
            gallery_urls=urls,
        ),
        _record(
            "reference-1",
            "gallery_image_reference",
            item_identifier=item,
            observation_record_identifier="detail",
            url=urls[0],
            gallery_order=0,
        ),
        _record(
            "reference-2",
            "gallery_image_reference",
            item_identifier=item,
            observation_record_identifier="detail",
            url=urls[1],
            gallery_order=1,
        ),
        _record(
            "saved",
            "image_result",
            item_identifier=item,
            observation_record_identifier="detail",
            reference_record_identifier="reference-1",
            url=urls[0],
            state="saved",
            image_artifact_identifier="image",
            width=1,
            height=1,
        ),
        _record(
            "failed-image",
            "image_result",
            item_identifier=item,
            observation_record_identifier="detail",
            reference_record_identifier="reference-2",
            url=urls[1],
            state="failed",
        ),
        _record(
            "description",
            "description_result",
            item_identifier=item,
            observation_record_identifier="detail",
            state="saved",
            description="Full seller description",
        ),
        _record(
            "failed-description",
            "description_result",
            item_identifier=item,
            observation_record_identifier="detail",
            state="failed",
        ),
        _record(
            "later-challenge",
            "listing_observation",
            item_identifier=item,
            classification="challenge",
        ),
    )
    artifact = BytesDraft(
        identifier="image",
        kind=("carl", "ebay", "image_file"),
        media_type="image/png",
        representation={"kind": "validated_http_image_body"},
        content=b"test-validated-by-worker",
    )
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(database, records, (artifact,))
        app = ReviewApplication(database, tmp_path)
        details = await app.get_listing_details(
            GetMarketplaceListingRequest(marketplace=Marketplace.EBAY, external_identifier=item)
        )
        assert details.classification == "challenge"
        assert details.title == "Bench scope"
        assert details.observation_record_identifier == "detail"
        assert details.latest_observation_record_identifier == "later-challenge"
        assert details.observation_record_identifiers == ("detail", "later-challenge")
        assert details.description == "Full seller description"
        assert details.description_state == "saved"
        assert [image.state for image in details.images] == ["saved", "failed"]
        assert details.images[0].artifact_identifier == "image"
        image = await app.get_image("image")
        assert image.content == artifact.content
        limited = await app.get_listing_details(
            GetMarketplaceListingRequest(
                marketplace=Marketplace.EBAY, external_identifier=item, maximum_images=1
            )
        )
        assert limited.referenced_image_count == 2 and limited.images_truncated


@pytest.mark.anyio
async def test_detail_requests_reuse_retained_pages_and_allow_explicit_refresh(
    tmp_path: Path,
) -> None:
    item = "256123456789"
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(
            database,
            (
                _record(
                    "detail",
                    "listing_observation",
                    item_identifier=item,
                    classification="detail",
                    acquisition_record_identifier="acquisition",
                    request={"stack_identifier": "ebay_anonymous"},
                ),
            ),
        )
        app = ReviewApplication(database, tmp_path)
        reused = await app.request_listing_details(
            RequestEbayListingDetailsRequest(external_identifier=item)
        )
        assert reused.reused_item_page
        assert (await database.work(reused.work_identifier))["kind"] == list(
            EXTRACT_EBAY_ITEM_WORK_KIND
        )
        repeated = await app.request_listing_details(
            RequestEbayListingDetailsRequest(external_identifier=item)
        )
        assert not repeated.created and repeated.work_identifier == reused.work_identifier
        refreshed = await app.request_listing_details(
            RequestEbayListingDetailsRequest(external_identifier=item, refresh=True)
        )
        assert not refreshed.reused_item_page
        assert (await database.work(refreshed.work_identifier))["kind"] == list(
            COLLECT_EBAY_ITEM_WORK_KIND
        )


@pytest.mark.anyio
async def test_empty_details_are_read_only_and_unsaved_images_are_rejected(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = ReviewApplication(database, tmp_path)
        ebay = await app.get_listing_details(
            GetMarketplaceListingRequest(
                marketplace=Marketplace.EBAY, external_identifier="256123456789"
            )
        )
        assert ebay.classification == "not_collected" and ebay.images == ()
        facebook = await app.get_listing_details(
            GetMarketplaceListingRequest(
                marketplace=Marketplace.FACEBOOK, external_identifier="123"
            )
        )
        assert facebook.canonical_url == "https://www.facebook.com/marketplace/item/123/"
        assert facebook.classification == "not_collected"
        with pytest.raises(ReviewInputError, match="not a saved listing image"):
            await app.get_image("unsaved")
        assert await database.records_by_kind(("carl", "ebay", "listing_observation")) == ()


def test_listing_request_identity_and_stack_bounds() -> None:
    assert (
        RequestEbayListingDetailsRequest(
            external_identifier="256123456789", stack_identifier="Mixed-Case"
        )
        .item_request()
        .stack_identifier
        == "Mixed-Case"
    )
    with pytest.raises(ValidationError):
        GetMarketplaceListingRequest(marketplace=Marketplace.EBAY, external_identifier="123")
    with pytest.raises(ValidationError):
        RequestEbayListingDetailsRequest(external_identifier="256123456789", maximum_images=51)


@pytest.mark.anyio
async def test_old_page_reprocessing_does_not_advance_acquisition_recency(tmp_path: Path) -> None:
    item = "256123456789"
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        # Each acquisition precedes its observation; later offline work reuses the old acquisition.
        await _publish(
            database,
            (
                RecordDraft(
                    identifier="old-acquisition",
                    kind=("carl", "http", "acquisition"),
                    schema_version=1,
                    value={},
                ),
            ),
        )
        await _publish(
            database,
            (
                _record(
                    "old-detail",
                    "listing_observation",
                    item_identifier=item,
                    classification="detail",
                    title="Old retained scope",
                    acquisition_record_identifier="old-acquisition",
                ),
            ),
        )
        await _publish(
            database,
            (
                RecordDraft(
                    identifier="new-acquisition",
                    kind=("carl", "http", "acquisition"),
                    schema_version=1,
                    value={},
                ),
            ),
        )
        await _publish(
            database,
            (
                _record(
                    "unavailable",
                    "listing_observation",
                    item_identifier=item,
                    classification="unavailable",
                    acquisition_record_identifier="new-acquisition",
                ),
            ),
        )
        await _publish(
            database,
            (
                _record(
                    "reprocessed",
                    "listing_observation",
                    item_identifier=item,
                    classification="detail",
                    title="Old reprocessed scope",
                    acquisition_record_identifier="old-acquisition",
                ),
            ),
        )
        detail = await ReviewApplication(database, tmp_path).get_listing_details(
            GetMarketplaceListingRequest(marketplace=Marketplace.EBAY, external_identifier=item)
        )
        assert detail.classification == "unavailable"
        assert detail.latest_observation_record_identifier == "unavailable"
        assert detail.observation_record_identifier == "reprocessed"
        assert detail.title == "Old reprocessed scope"


@pytest.mark.anyio
async def test_reprocessing_keeps_prior_saved_outcomes_for_same_acquisition(tmp_path: Path) -> None:
    item = "256123456789"
    url = "https://i.ebayimg.com/images/original.jpg"
    description_url = "https://vi.vipr.ebaydesc.com/description"
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(
            database,
            (
                _record(
                    "first",
                    "listing_observation",
                    item_identifier=item,
                    classification="detail",
                    title="Scope",
                    acquisition_record_identifier="same-page",
                    gallery_urls=[url],
                    description_url=description_url,
                ),
                _record(
                    "reference",
                    "gallery_image_reference",
                    observation_record_identifier="first",
                    url=url,
                ),
                _record(
                    "saved-image",
                    "image_result",
                    observation_record_identifier="first",
                    reference_record_identifier="reference",
                    url=url,
                    state="saved",
                    image_artifact_identifier="validated-image",
                ),
                _record(
                    "saved-description",
                    "description_result",
                    observation_record_identifier="first",
                    url=description_url,
                    state="saved",
                    description="Retained seller text",
                ),
                _record(
                    "reprocessed",
                    "listing_observation",
                    item_identifier=item,
                    classification="detail",
                    title="Scope",
                    acquisition_record_identifier="same-page",
                    gallery_urls=[url],
                    description_url=description_url,
                ),
                _record(
                    "later-description-failure",
                    "description_result",
                    observation_record_identifier="reprocessed",
                    url=description_url,
                    state="failed",
                ),
            ),
        )
        detail = await ReviewApplication(database, tmp_path).get_listing_details(
            GetMarketplaceListingRequest(marketplace=Marketplace.EBAY, external_identifier=item)
        )
        assert detail.observation_record_identifier == "reprocessed"
        assert detail.images[0].state == "saved"
        assert detail.images[0].artifact_identifier == "validated-image"
        assert detail.images[0].reference_record_identifier == "reference"
        assert detail.images[0].result_record_identifier == "saved-image"
        assert detail.description == "Retained seller text"
        assert detail.description_result_record_identifier == "saved-description"
        # A genuinely new acquisition cannot borrow an old gallery relationship.
        await _publish(
            database,
            (
                _record(
                    "new-page",
                    "listing_observation",
                    item_identifier=item,
                    classification="detail",
                    title="New scope",
                    acquisition_record_identifier="other-page",
                    gallery_urls=[url],
                    description_url=description_url,
                ),
            ),
        )
        fresh = await ReviewApplication(database, tmp_path).get_listing_details(
            GetMarketplaceListingRequest(marketplace=Marketplace.EBAY, external_identifier=item)
        )
        assert fresh.images[0].artifact_identifier is None
        assert fresh.description is None


@pytest.mark.anyio
@pytest.mark.parametrize("formatted", [None, "$75.00"])
async def test_facebook_detail_maps_price_and_does_not_invent_item_observation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, formatted: str | None
) -> None:
    evidence = ProjectionEvidence(
        evidence_record_identifier="search-occurrence",
        acquisition_record_identifier="search-acquisition",
        acquisition_completion_sequence=1,
        observation_completion_sequence=1,
        source_kind=ProjectionSourceKind.SEARCH_CARD,
    )
    price: dict[str, JsonValue] = {"amount_decimal": "75.00", "currency": "USD"}
    if formatted is not None:
        price["formatted_amount"] = formatted
    fields: dict[str, JsonValue] = dict(
        status=ComposedStatus(value=ListingStatus.AVAILABLE, raw_flags={}, evidence=evidence),
        title=ComposedField(value="Scope", evidence=evidence),
        price=ComposedField(value=price, evidence=evidence),
        location=None,
        description=None,
        seller=None,
        preview_image=None,
        gallery=None,
        analyses=(),
        analyses_truncated=False,
        search_membership=None,
    )
    projection = ComposedListingProjection(
        listing_identifier="123",
        canonical_source_url="https://www.facebook.com/marketplace/item/123/",
        as_of_completion_sequence=1,
        projection_revision=projection_revision(listing_identifier="123", **fields),
        **fields,
    )

    async def get_composed(
        _application: ReviewApplication, request: GetComposedListingRequest
    ) -> ComposedListingProjection:
        assert request.listing_identifier == "123"
        return projection

    monkeypatch.setattr(ReviewApplication, "get_composed_listing", get_composed)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        detail = await ReviewApplication(database, tmp_path).get_listing_details(
            GetMarketplaceListingRequest(
                marketplace=Marketplace.FACEBOOK, external_identifier="123"
            )
        )
    assert detail.title == "Scope"
    assert detail.displayed_price == (formatted or "75.00")
    assert detail.currency == "USD"
    assert detail.observation_record_identifier is None
    assert detail.acquisition_record_identifier == "search-acquisition"
