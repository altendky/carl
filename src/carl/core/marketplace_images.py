"""Shared concurrency policy for marketplace image HTTP downloads."""

from carl.core.work import (
    ConcurrencyConstraint,
    SchedulingScope,
    SchedulingScopeKind,
    SchedulingSubjectKind,
)

MARKETPLACE_IMAGE_MAXIMUM_ACTIVE = 25
MARKETPLACE_IMAGE_SCOPE = SchedulingScope(
    kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND,
    identity=("carl", "marketplace", "network_activity", "image"),
)


def marketplace_image_network_constraint() -> ConcurrencyConstraint:
    """Count image requests across sources, sessions, and network paths."""

    return ConcurrencyConstraint(
        identifier=("carl", "marketplace", "image", "network_activity_concurrency", "v1"),
        subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
        scope=MARKETPLACE_IMAGE_SCOPE,
        maximum_active=MARKETPLACE_IMAGE_MAXIMUM_ACTIVE,
    )
