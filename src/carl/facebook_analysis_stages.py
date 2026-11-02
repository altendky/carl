"""Facebook analysis stages retain legacy work versions and report semantics."""

from carl.core.item_analysis import (
    ANALYZE_ITEM_WORK_KIND,
    ANALYZE_ITEM_WORK_SCHEMA_VERSION,
    LEGACY_ANALYZE_ITEM_WORK_SCHEMA_VERSION,
    AnalyzeItemPayload,
)
from carl.core.work import WorkCapability
from carl.facebook_analysis_workers import (
    ANALYZE_FACEBOOK_ITEM,
    AnalysisWorkerDependencies,
    build_analysis_component_registry,
    facebook_analysis_outcome,
    prepare_facebook_analysis,
)
from carl.io.analysis_stage_workers import build_analysis_stage_registry
from carl.io.stage_workers import StageHandlerRegistry


def build_facebook_analysis_stage_registry(
    dependencies: AnalysisWorkerDependencies,
) -> StageHandlerRegistry:
    return build_analysis_stage_registry(
        dependencies=dependencies,
        marketplace="facebook",
        component=build_analysis_component_registry().require(ANALYZE_FACEBOOK_ITEM),
        capabilities=tuple(
            WorkCapability(kind=ANALYZE_ITEM_WORK_KIND, payload_schema_version=version)
            for version in (
                LEGACY_ANALYZE_ITEM_WORK_SCHEMA_VERSION,
                ANALYZE_ITEM_WORK_SCHEMA_VERSION,
            )
        ),
        payload_type=AnalyzeItemPayload,
        prepare=prepare_facebook_analysis,
        outcome_builder=facebook_analysis_outcome,
    )
