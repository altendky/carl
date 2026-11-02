"""Opt-in marketplace session adapters; dispatchers own the bounded actor pool."""

from carl.ebay_session_stages import (
    EbaySessionStageDependencies,
    build_ebay_session_stage_registry,
)
from carl.facebook_session_stages import (
    FacebookSessionStageDependencies,
    build_facebook_session_stage_registry,
)
from carl.io.stage_workers import StageHandlerRegistry


def build_session_stage_registry(
    *, facebook: FacebookSessionStageDependencies, ebay: EbaySessionStageDependencies
) -> StageHandlerRegistry:
    if facebook.database is not ebay.database or facebook.residents is not ebay.residents:
        raise ValueError("Session adapters must share their database and resident pool")
    registries = (
        build_facebook_session_stage_registry(facebook),
        build_ebay_session_stage_registry(ebay),
    )
    return StageHandlerRegistry(
        adapters=tuple(adapter for registry in registries for adapter in registry.adapters),
        handlers=tuple(handler for registry in registries for handler in registry.handlers),
    )
