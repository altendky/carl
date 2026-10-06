"""Versioned analysis work and immutable eBay evidence-selection metadata."""

import hashlib

from pydantic import Field

from carl.core.item_analysis import (
    ANALYZE_ITEM_WORK_KIND,
    AnalysisImageSelection,
    AnalyzeItemPayload,
    UnavailableAnalysisImageSelection,
)
from carl.core.json import encode_json
from carl.core.models import JsonValue, NamedInput, StrictModel
from carl.core.work import SchedulingScope, SchedulingScopeKind, WorkDefinition

ANALYZE_EBAY_ITEM_WORK_KIND = ("carl", "ebay", "work", "analyze_item")
EBAY_ANALYSIS_RECIPE_VERSION = 1


class EbayAnalyzeItemPayload(AnalyzeItemPayload):
    recipe_version: int = Field(default=EBAY_ANALYSIS_RECIPE_VERSION, ge=1)


class EbayAnalysisEvidenceSelection(StrictModel):
    """Exact input edges and non-edge metadata for one immutable analysis subject."""

    value: dict[str, JsonValue]
    inputs: tuple[NamedInput, ...]
    included: tuple[AnalysisImageSelection, ...]
    unavailable: tuple[UnavailableAnalysisImageSelection, ...]
    unretained_gallery_orders: tuple[int, ...] = ()
    gallery_absence_reason: str | None = None

    @property
    def unavailable_gallery_orders(self) -> tuple[int, ...]:
        return tuple(
            sorted(
                (
                    *self.unretained_gallery_orders,
                    *(image.gallery_order for image in self.unavailable),
                )
            )
        )

    @property
    def evidence_identity(self) -> str:
        return hashlib.sha256(
            encode_json(
                {
                    "value": self.value,
                    "inputs": [edge.model_dump(mode="json") for edge in self.inputs],
                }
            ).encode()
        ).hexdigest()


def analyze_ebay_item_work(
    *, identifier: str, payload: AnalyzeItemPayload, not_before_utc_ns: int = 0
) -> WorkDefinition:
    """Use the existing Facebook AI scheduling scope to bound total AI concurrency."""

    return WorkDefinition(
        identifier=identifier,
        kind=ANALYZE_EBAY_ITEM_WORK_KIND,
        payload_schema_version=1,
        payload=payload.model_dump(mode="json", exclude_none=True),
        deduplication_identity=(
            "ebay",
            "identify_listing",
            payload.evidence_set_record_identifier,
            encode_json(payload.model_dump(mode="json", exclude={"claude_version"})),
        ),
        not_before_utc_ns=not_before_utc_ns,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(
                kind=SchedulingScopeKind.WORK_KIND, identity=ANALYZE_EBAY_ITEM_WORK_KIND
            ),
            SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=ANALYZE_ITEM_WORK_KIND),
        ),
    )
