"""Source-neutral retained item details and explicit eBay follow-up requests."""

from __future__ import annotations

from pydantic import Field, model_validator
from pydantic.json_schema import SkipJsonSchema

from carl.core.ebay_items import EbayItemRequest
from carl.core.marketplace_search import Marketplace
from carl.core.models import StrictModel
from carl.core.network_defaults import (
    DEFAULT_DATACENTER_NETWORK_PATH,
    AcquisitionNetworkPath,
    LegacyProtonRoute,
    populate_legacy_proton_paths,
    validate_legacy_proton_paths,
)


class GetMarketplaceListingRequest(StrictModel):
    marketplace: Marketplace
    external_identifier: str = Field(pattern=r"^[0-9]+$")
    maximum_images: int = Field(default=20, ge=0, le=50)

    @model_validator(mode="after")
    def validate_ebay_identity(self) -> GetMarketplaceListingRequest:
        if self.marketplace is Marketplace.EBAY:
            _ = EbayItemRequest(item_identifier=self.external_identifier)
        return self


class RequestListingDetailsRequest(StrictModel):
    """Request one source-dispatched item, description, and gallery workflow."""

    marketplace: Marketplace = Marketplace.EBAY
    external_identifier: str = Field(pattern=r"^[0-9]+$")
    stack_identifier: str = Field(
        default="ebay_anonymous", pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$"
    )
    maximum_images: int = Field(default=20, ge=0, le=50)
    refresh: bool = False
    image_network_path: AcquisitionNetworkPath = Field(
        default=DEFAULT_DATACENTER_NETWORK_PATH,
        description="Gallery acquisition route; defaults directly to Decodo datacenter.",
    )
    proton_route: SkipJsonSchema[LegacyProtonRoute | None] = Field(default=None, exclude=True)
    decodo_route: str = Field(default="carl", min_length=1)

    @model_validator(mode="before")
    @classmethod
    def populate_legacy_route(cls, value: object) -> object:
        return populate_legacy_proton_paths(
            value, legacy_field="proton_route", network_fields=("image_network_path",)
        )

    @property
    def requested_image_network_path(self) -> tuple[str, ...]:
        return self.image_network_path

    @model_validator(mode="after")
    def validate_source_options(self) -> RequestListingDetailsRequest:
        validate_legacy_proton_paths(self.proton_route, self.image_network_path)
        if self.marketplace is Marketplace.EBAY:
            _ = self.item_request()
            if self.image_network_path != DEFAULT_DATACENTER_NETWORK_PATH:
                raise ValueError("eBay gallery images use the Decodo datacenter route")
            if self.decodo_route != "carl":
                raise ValueError("eBay item pages use stack_identifier, not decodo_route")
        elif self.stack_identifier != "ebay_anonymous":
            raise ValueError("stack_identifier applies only to eBay; Facebook uses route options")
        return self

    def item_request(self) -> EbayItemRequest:
        return EbayItemRequest(
            item_identifier=self.external_identifier,
            stack_identifier=self.stack_identifier,
            maximum_images=self.maximum_images,
        )


# Retain the Python API import used before source-neutral dispatch was added.
RequestEbayListingDetailsRequest = RequestListingDetailsRequest


class RequestListingDetailsResult(StrictModel):
    marketplace: Marketplace
    external_identifier: str
    work_identifier: str
    created: bool
    state: str
    reused_item_page: bool


class MarketplaceListingImage(StrictModel):
    gallery_order: int = Field(ge=0)
    url: str
    state: str
    reference_record_identifier: str | None = None
    result_record_identifier: str | None = None
    artifact_identifier: str | None = None
    work_identifier: str | None = None
    width: int | None = Field(default=None, gt=0)
    height: int | None = Field(default=None, gt=0)
    local_path: str | None = None


class MarketplaceListingDetails(StrictModel):
    marketplace: Marketplace
    external_identifier: str
    canonical_url: str
    classification: str
    title: str | None = None
    displayed_price: str | None = None
    currency: str | None = None
    condition: str | None = None
    description: str | None = None
    description_state: str = "not_available"
    description_result_record_identifier: str | None = None
    description_work_identifiers: tuple[str, ...] = ()
    observation_record_identifier: str | None = None
    acquisition_record_identifier: str | None = None
    latest_observation_record_identifier: str | None = None
    observation_record_identifiers: tuple[str, ...] = Field(default=(), max_length=100)
    observation_history_truncated: bool = False
    images: tuple[MarketplaceListingImage, ...] = Field(default=(), max_length=50)
    referenced_image_count: int = Field(default=0, ge=0)
    images_truncated: bool = False
