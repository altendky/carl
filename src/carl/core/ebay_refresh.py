"""Durable eBay search, detail, description, and image refresh requests."""

import hashlib

from pydantic import Field

from carl.core.ebay import EbaySearchRequest
from carl.core.models import StrictModel
from carl.core.work import (
    ConcurrencyConstraint,
    SchedulingScope,
    SchedulingScopeKind,
    SchedulingSubjectKind,
    WorkDefinition,
)

REFRESH_EBAY_SEARCH_WORK_KIND = ("carl", "ebay", "work", "refresh_search")


class RefreshEbaySearchPayload(StrictModel):
    base_search_run_record_identifier: str = Field(min_length=1)
    search_work_identifier: str = Field(min_length=1)
    search: EbaySearchRequest
    maximum_items: int | None = Field(default=None, ge=1)
    maximum_images: int | None = Field(default=None, ge=1)


def refresh_ebay_search_work(
    *, identifier: str, payload: RefreshEbaySearchPayload, not_before_utc_ns: int = 0
) -> WorkDefinition:
    return WorkDefinition(
        identifier=identifier,
        kind=REFRESH_EBAY_SEARCH_WORK_KIND,
        payload_schema_version=1,
        payload=payload.model_dump(mode="json"),
        deduplication_identity=(
            "ebay",
            "search_refresh",
            hashlib.sha256(
                payload.model_dump_json(exclude={"search_work_identifier"}).encode()
            ).hexdigest(),
        ),
        not_before_utc_ns=not_before_utc_ns,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(
                kind=SchedulingScopeKind.WORK_KIND, identity=REFRESH_EBAY_SEARCH_WORK_KIND
            ),
        ),
    )


def refresh_ebay_search_work_constraints() -> tuple[ConcurrencyConstraint, ...]:
    return (
        ConcurrencyConstraint(
            identifier=("carl", "ebay", "search_refresh", "work_concurrency", "v1"),
            subject_kind=SchedulingSubjectKind.WORK_ITEM,
            scope=SchedulingScope(
                kind=SchedulingScopeKind.WORK_KIND, identity=REFRESH_EBAY_SEARCH_WORK_KIND
            ),
            maximum_active=1,
        ),
    )
