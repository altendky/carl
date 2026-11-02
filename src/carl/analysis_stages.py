"""Compose opt-in analysis stages without starting a worker runtime."""

from carl.ebay_analysis_stages import build_ebay_analysis_stage_registry
from carl.facebook_analysis_stages import build_facebook_analysis_stage_registry
from carl.facebook_analysis_workers import AnalysisWorkerDependencies
from carl.io.stage_workers import StageHandlerRegistry


def build_analysis_stage_registry(
    *, facebook: AnalysisWorkerDependencies, ebay: AnalysisWorkerDependencies
) -> StageHandlerRegistry:
    if facebook.database is not ebay.database:
        raise ValueError("Analysis adapters must share one evidence database")
    registries = (
        build_facebook_analysis_stage_registry(facebook),
        build_ebay_analysis_stage_registry(ebay),
    )
    return StageHandlerRegistry(
        adapters=tuple(adapter for registry in registries for adapter in registry.adapters),
        handlers=tuple(handler for registry in registries for handler in registry.handlers),
    )
