"""Durable source-neutral tool support for selected Facebook item details."""

import hashlib

from pydantic import Field

from carl.core.models import StrictModel
from carl.core.work import SchedulingScope, SchedulingScopeKind, WorkDefinition

REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND = ("carl", "facebook", "work", "listing_details")


class RequestFacebookListingDetailsPayload(StrictModel):
    listing_identifier: str = Field(pattern=r"^[0-9]+$")
    maximum_images: int = Field(default=20, ge=0, le=50)
    item_routing: tuple[str, ...] = ("decodo", "personal", "carl")
    image_routing: tuple[str, ...] = ("proton", "personal", "carl")
    refresh: bool = False


def request_facebook_listing_details_work(
    *, identifier: str, payload: RequestFacebookListingDetailsPayload, not_before_utc_ns: int = 0
) -> WorkDefinition:
    return WorkDefinition(
        identifier=identifier,
        kind=REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND,
        payload_schema_version=1,
        payload=payload.model_dump(mode="json"),
        deduplication_identity=(
            "facebook_marketplace",
            "listing_details",
            hashlib.sha256(payload.model_dump_json().encode()).hexdigest(),
        ),
        not_before_utc_ns=not_before_utc_ns,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(
                kind=SchedulingScopeKind.WORK_KIND,
                identity=REQUEST_FACEBOOK_LISTING_DETAILS_WORK_KIND,
            ),
        ),
    )
