"""Exact shipped static capacity policies superseded by physical resource permits.

Keep these definitions immutable: matching an identifier alone could retire a
custom policy with a different scope, subject, schema, or limit. Facebook's
requested/effective route policies are intentionally outside this inventory.
"""

from carl.core.analysis_batch import REQUEST_MISSING_ANALYSES_WORK_KIND
from carl.core.ebay import COLLECT_EBAY_SEARCH_WORK_KIND
from carl.core.ebay_items import (
    COLLECT_EBAY_DESCRIPTION_WORK_KIND,
    COLLECT_EBAY_IMAGE_WORK_KIND,
    COLLECT_EBAY_ITEM_WORK_KIND,
    EXTRACT_EBAY_ITEM_WORK_KIND,
)
from carl.core.ebay_refresh import REFRESH_EBAY_SEARCH_WORK_KIND
from carl.core.facebook_images import (
    COLLECT_IMAGE_WORK_KIND,
    legacy_image_network_concurrency_constraint,
)
from carl.core.facebook_refresh import REFRESH_SEARCH_WORK_KIND
from carl.core.item_analysis import ANALYZE_ITEM_WORK_KIND
from carl.core.marketplace_images import MARKETPLACE_IMAGE_SCOPE
from carl.core.pipeline import LISTING_PIPELINE_WORK_KIND, SEARCH_PIPELINE_WORK_KIND
from carl.core.travel_data import TRAVEL_DATA_SETUP_WORK_KIND
from carl.core.work import (
    ConcurrencyConstraint,
    SchedulingScope,
    SchedulingScopeKind,
    SchedulingSubjectKind,
)


def _work(
    identifier: tuple[str, ...], kind: tuple[str, ...], maximum: int
) -> ConcurrencyConstraint:
    return ConcurrencyConstraint(
        identifier=identifier,
        subject_kind=SchedulingSubjectKind.WORK_ITEM,
        scope=SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=kind),
        maximum_active=maximum,
    )


def static_capacity_constraints() -> tuple[ConcurrencyConstraint, ...]:
    """Return full known definitions, including superseded shipped versions.

    Versions mentioned only by old retirement lists are deliberately not
    reconstructed from an assumed limit. Unknown definitions remain active.
    """
    image_network = legacy_image_network_concurrency_constraint()
    return (
        _work(
            ("carl", "facebook", "analysis", "work_concurrency", "v1"),
            ANALYZE_ITEM_WORK_KIND,
            10,
        ),
        _work(
            ("carl", "facebook", "search_refresh", "work_concurrency", "v1"),
            REFRESH_SEARCH_WORK_KIND,
            1,
        ),
        _work(
            ("carl", "facebook", "analysis_batch", "work_concurrency", "v2"),
            REQUEST_MISSING_ANALYSES_WORK_KIND,
            10,
        ),
        _work(
            ("carl", "facebook", "image", "work_concurrency", "v2"),
            COLLECT_IMAGE_WORK_KIND,
            10,
        ),
        _work(
            ("carl", "facebook", "image", "work_concurrency", "v3"),
            COLLECT_IMAGE_WORK_KIND,
            25,
        ),
        image_network.model_copy(
            update={
                "identifier": (*image_network.identifier[:-1], "v3"),
                "maximum_active": 10,
            }
        ),
        image_network,
        ConcurrencyConstraint(
            identifier=("carl", "marketplace", "image", "network_activity_concurrency", "v1"),
            subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
            scope=MARKETPLACE_IMAGE_SCOPE,
            maximum_active=25,
        ),
        _work(("carl", "constraint", "ebay", "search", "serial"), COLLECT_EBAY_SEARCH_WORK_KIND, 1),
        _work((*COLLECT_EBAY_ITEM_WORK_KIND, "concurrency"), COLLECT_EBAY_ITEM_WORK_KIND, 1),
        _work(
            (*COLLECT_EBAY_DESCRIPTION_WORK_KIND, "concurrency"),
            COLLECT_EBAY_DESCRIPTION_WORK_KIND,
            1,
        ),
        _work(
            (*COLLECT_EBAY_DESCRIPTION_WORK_KIND, "concurrency", "v2"),
            COLLECT_EBAY_DESCRIPTION_WORK_KIND,
            10,
        ),
        _work((*COLLECT_EBAY_IMAGE_WORK_KIND, "concurrency"), COLLECT_EBAY_IMAGE_WORK_KIND, 5),
        _work(
            (*COLLECT_EBAY_IMAGE_WORK_KIND, "concurrency", "v2"), COLLECT_EBAY_IMAGE_WORK_KIND, 25
        ),
        _work((*EXTRACT_EBAY_ITEM_WORK_KIND, "concurrency"), EXTRACT_EBAY_ITEM_WORK_KIND, 5),
        _work(
            ("carl", "ebay", "search_refresh", "work_concurrency", "v1"),
            REFRESH_EBAY_SEARCH_WORK_KIND,
            1,
        ),
        _work((*SEARCH_PIPELINE_WORK_KIND, "work_concurrency", "v1"), SEARCH_PIPELINE_WORK_KIND, 4),
        _work(
            (*LISTING_PIPELINE_WORK_KIND, "work_concurrency", "v1"), LISTING_PIPELINE_WORK_KIND, 10
        ),
        _work(
            (*TRAVEL_DATA_SETUP_WORK_KIND, "work_concurrency", "v1"), TRAVEL_DATA_SETUP_WORK_KIND, 1
        ),
    )
