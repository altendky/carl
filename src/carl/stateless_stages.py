"""Compose stateless marketplace adapters for an explicitly owned dispatcher."""

from carl.ebay_item_workers import EbayItemWorkerDependencies
from carl.ebay_stateless_stages import build_ebay_stateless_stage_registry
from carl.facebook_stateless_stages import (
    FacebookStatelessStageDependencies,
    build_facebook_stateless_stage_registry,
)
from carl.io.stage_workers import StageHandlerRegistry


def build_stateless_stage_registry(
    *,
    facebook: FacebookStatelessStageDependencies,
    ebay: EbayItemWorkerDependencies,
) -> StageHandlerRegistry:
    """Compose adapters without starting workers or changing the active runtime."""

    if facebook.database is not ebay.database:
        raise ValueError("Stateless adapters must share one evidence database")
    facebook_registry = build_facebook_stateless_stage_registry(facebook)
    ebay_registry = build_ebay_stateless_stage_registry(ebay)
    return StageHandlerRegistry(
        adapters=(*facebook_registry.adapters, *ebay_registry.adapters),
        handlers=(*facebook_registry.handlers, *ebay_registry.handlers),
    )
