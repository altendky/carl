"""eBay analysis adapters preserve retained evidence and existing report policy."""

from carl.core.ebay_analysis import ANALYZE_EBAY_ITEM_WORK_KIND, EbayAnalyzeItemPayload
from carl.core.work import WorkCapability
from carl.ebay_analysis_workers import (
    ANALYZE_EBAY_ITEM,
    build_ebay_analysis_component_registry,
    ebay_analysis_outcome,
    prepare_ebay_analysis,
)
from carl.facebook_analysis_workers import AnalysisWorkerDependencies
from carl.io.analysis_stage_workers import build_analysis_stage_registry
from carl.io.stage_workers import StageHandlerRegistry


def build_ebay_analysis_stage_registry(
    dependencies: AnalysisWorkerDependencies,
) -> StageHandlerRegistry:
    return build_analysis_stage_registry(
        dependencies=dependencies,
        marketplace="ebay",
        component=build_ebay_analysis_component_registry().require(ANALYZE_EBAY_ITEM),
        capabilities=(WorkCapability(kind=ANALYZE_EBAY_ITEM_WORK_KIND, payload_schema_version=1),),
        payload_type=EbayAnalyzeItemPayload,
        prepare=prepare_ebay_analysis,
        outcome_builder=ebay_analysis_outcome,
        input_error_types=(KeyError, UnicodeDecodeError, ValueError, OSError),
    )
