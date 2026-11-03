"""SQLite provenance graph and inline artifact storage."""

from __future__ import annotations

import hashlib
import os
import stat
from collections.abc import AsyncGenerator, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter_ns, time_ns
from typing import TYPE_CHECKING, Literal, cast
from uuid import uuid4

import anyio
import apsw

from carl.core.acquisition_identity import (
    acquisition_resource_identity,
    acquisition_resource_identity_json,
    ebay_image_identity,
    facebook_image_identity,
)
from carl.core.activity import (
    ActiveNetworkActivity,
    ActivitySnapshot,
    BoundedWorkActivity,
    NetworkActivityCounts,
    NetworkConnectivityActivity,
    NetworkPathActivity,
    WorkActivity,
    WorkKindActivity,
    work_error_kind,
    work_stage,
    work_subject,
)
from carl.core.components import Component
from carl.core.composed_projection import (
    ListingObservationCandidate,
    ProjectionEvidence,
    ProjectionSourceKind,
    SavedImageProjectionCandidate,
    SearchCardCandidate,
    SearchMembershipOccurrenceCandidate,
    SearchRunCandidate,
    StatusObservationCandidate,
    normalize_listing_status,
    search_card_field_value,
    status_candidate_from_search_occurrence,
)
from carl.core.connectivity import network_work_kind
from carl.core.ebay import (
    COLLECT_EBAY_SEARCH_WORK_KIND,
    CollectEbaySearchPayload,
    collect_ebay_search_work,
)
from carl.core.ebay_analysis import EbayAnalyzeItemPayload
from carl.core.facebook import FacebookItemResponseKind
from carl.core.facebook_images import (
    COLLECT_IMAGE_WORK_KIND,
    COLLECT_IMAGE_WORK_SCHEMA_VERSION,
    GalleryImageReference,
    ImageReuseRecord,
    ImageReuseResolution,
)
from carl.core.facebook_refresh import SearchRunOrigin
from carl.core.facebook_search import SearchStoppingReason
from carl.core.facebook_search_retry import FACEBOOK_SEARCH_NETWORK_ACTIVITY_KINDS
from carl.core.facebook_work import (
    COLLECT_SEARCH_WORK_KIND,
    FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY,
    SuccessfulItemPageResult,
    facebook_effective_search_scope,
)
from carl.core.fair_scheduling import (
    FairAdmission,
    FairDispatchCursor,
    FairDispatchPage,
    FairLane,
    SchedulingContext,
)
from carl.core.item_analysis import (
    ANALYZE_ITEM_WORK_SCHEMA_VERSION,
    LEGACY_ANALYZE_ITEM_WORK_SCHEMA_VERSION,
    AnalysisImageSelection,
    AnalyzeItemPayload,
    ListingAnalysisEvidenceSet,
    ProductGuideDefinition,
    ProductGuideRecord,
    UnavailableAnalysisImageSelection,
)
from carl.core.json import decode_json, encode_json
from carl.core.legacy_capacity import FACEBOOK_ROUTE_CAPACITY_FAMILIES
from carl.core.listing_identity import is_listing_identifier
from carl.core.marketplace_search import (
    MARKETPLACE_SEARCH_TARGET_KIND,
    MarketplaceSearchTargetRecord,
)
from carl.core.models import (
    ArtifactDraft,
    BytesDraft,
    CodeProvenance,
    Domain,
    ExternalFileDraft,
    JsonValue,
    NamedInput,
    NamedOutput,
    Namespace,
    RecordDraft,
    StorageBackend,
    StorageSchemaIdentity,
)
from carl.core.residents import (
    ResidentDefinition,
    ResidentLease,
    ResidentState,
)
from carl.core.resources import ResourceLease, ResourceRequest
from carl.core.review import (
    AnalysisDescriptor,
    CandidateSource,
    ProductGuideConflict,
    ProductGuideDetails,
    ProductGuideSummary,
    candidate_availability,
)
from carl.core.review_errors import ReviewInputError
from carl.core.review_workspace import (
    ACQUIRE_REVIEW_BATCH,
    LISTING_REVIEW_KIND,
    MAXIMUM_WORKSET_LISTINGS,
    REVIEW_BATCH_KIND,
    REVIEW_WORKSET_KIND,
    WORKSPACE_SEARCH_TRACK_SPECIFICATION_KIND,
    ListingReviewRecord,
    RecordListingReviewsResult,
    RecordWorkspaceBulkReviewResult,
    ReleaseReviewClaimResult,
    ReviewBatch,
    ReviewBatchAcquisition,
    ReviewClaimLease,
    ReviewWorksetConflict,
)
from carl.core.scheduler_activity import (
    SchedulerActivity,
    SchedulerWaitObservation,
    SchedulerWorkStatus,
)
from carl.core.search_scope import search_scope_sha256
from carl.core.stages import (
    StageAttemptContext,
    StageClaimResult,
    StageDefinition,
    StageLease,
    StageRequestCharge,
    StageWaitKind,
    WaitWork,
    stage_network_activity,
)
from carl.core.work import (
    CONSTRAINT_ADAPTER,
    ClaimResult,
    ConcurrencyConstraint,
    Constraint,
    EnqueueResult,
    HoldoffDecision,
    NetworkActivityAdmission,
    NetworkActivityAdmissionResult,
    NetworkActivityDefinition,
    NetworkActivityEventKind,
    NetworkActivityState,
    SchedulingScope,
    SchedulingScopeKind,
    SchedulingSubjectKind,
    SlidingWindowRateConstraint,
    UniformHoldoffConstraint,
    WorkCancellationRequestedError,
    WorkCapability,
    WorkDefinition,
    WorkEventKind,
    WorkLease,
    WorkRequester,
    WorkState,
)
from carl.core.work_dispatch import (
    DispatchCandidate,
    DispatchCursor,
    DispatchPage,
    ExpiredLegacyWorkCandidate,
    ExpiredLegacyWorkPage,
    ExpiredStageCandidate,
    ExpiredStagePage,
    ExpiryCursor,
    InitialStageRegistration,
    WorkDispatchSnapshot,
    WorkSnapshotStale,
)
from carl.core.work_progress import ProgressDrainResult, ProgressReconcilePage
from carl.core.worker import FollowOnWork
from carl.io import fair_scheduling as fair_sql
from carl.io import scheduler_activity as scheduler_sql
from carl.io import work_progress as progress_sql
from carl.io.db import DEFAULT_BUSY_TIMEOUT_MS, DatabaseConnections
from carl.route_cutover import rebuild_work_definition

if TYPE_CHECKING:
    from apsw import AsyncConnection


@dataclass(frozen=True, slots=True)
class FacebookSearchRunRecord:
    record_identifier: str
    completion_sequence: int
    started_at_utc: str
    ended_at_utc: str | None
    origin: SearchRunOrigin
    refresh_source_run_record_identifier: str | None
    value: JsonValue


@dataclass(frozen=True, slots=True)
class FacebookProjectionMembershipCandidate:
    candidate: SearchMembershipOccurrenceCandidate
    internal_run_identifier: str


DATABASE_SCHEMA = StorageSchemaIdentity(
    namespace=Namespace.CARL,
    domain=Domain.STORAGE,
    backend=StorageBackend.SQLITE,
    version=23,
)

_V22_DATABASE_SCHEMA = DATABASE_SCHEMA.model_copy(update={"version": 22})
_V22_SCHEMA_DEFINITION_SHA256 = "c250b9d74c7772b43569ca7342e462e986690aa290d7f1502e2a2525dd52c767"
_V21_DATABASE_SCHEMA = DATABASE_SCHEMA.model_copy(update={"version": 21})
_V21_SCHEMA_DEFINITION_SHA256 = "1996b96bed45d25fe4dd507eeb5603e87964ecaec5a9a85fed4ffd4ef8cc0a01"
_V20_DATABASE_SCHEMA = DATABASE_SCHEMA.model_copy(update={"version": 20})
_V20_SCHEMA_DEFINITION_SHA256 = "b4336f5f4093b362d02befe4ec345ceb34512091a1c04f4ee0904c22c6513059"
_V19_DATABASE_SCHEMA = DATABASE_SCHEMA.model_copy(update={"version": 19})
_V19_SCHEMA_DEFINITION_SHA256 = "e4cba8442c0eb6405811b2e61bc9133f9c0f83593de86331b7c10167452d8845"
_V18_DATABASE_SCHEMA = DATABASE_SCHEMA.model_copy(update={"version": 18})
_V18_SCHEMA_DEFINITION_SHA256 = "e1451850f5bd245e419c06b205c2d4f7b2193220dbdf54b8ce122271f3bffc45"
_V17_DATABASE_SCHEMA = DATABASE_SCHEMA.model_copy(update={"version": 17})
_V17_SCHEMA_DEFINITION_SHA256 = "237aa1054113cb8c61eb1f2d079e741eca10cbb794fac1caa2e74ca312427acf"
_V16_DATABASE_SCHEMA = DATABASE_SCHEMA.model_copy(update={"version": 16})
_V16_SCHEMA_DEFINITION_SHA256 = "9ca33db9613edfd7156e57cf9ef73c68b41f730101ccfcd65448adad93f3e9b6"
_V15_DATABASE_SCHEMA = DATABASE_SCHEMA.model_copy(update={"version": 15})
_V15_SCHEMA_DEFINITION_SHA256 = "24911645cf1e3c228f6ec427a042f0723b1435f2688420b61a227bb90f29177c"
_V14_DATABASE_SCHEMA = DATABASE_SCHEMA.model_copy(update={"version": 14})
_V14_SCHEMA_DEFINITION_SHA256 = "3897ad46eeef3ab426ab20b9964dd4604f69b31335aefcdf76a3becf04c52079"
_V13_DATABASE_SCHEMA = DATABASE_SCHEMA.model_copy(update={"version": 13})
_V13_SCHEMA_DEFINITION_SHA256 = "eae632323c7be6189ce3116ed09e9be6c679861cb64262ab481247e4e4a0c6bb"
_V12_DATABASE_SCHEMA = DATABASE_SCHEMA.model_copy(update={"version": 12})
_V12_SCHEMA_DEFINITION_SHA256 = "93b88a76a1bbc95d29c0cc72876560e5354da0d44250a483f9f5afe177ad2241"
_V11_DATABASE_SCHEMA = DATABASE_SCHEMA.model_copy(update={"version": 11})
_V11_SCHEMA_DEFINITION_SHA256 = "599317d3a36d321c137e2e86034f10d12c8b6dcbcd772b6b8b275e25d9c8bc51"
_V10_DATABASE_SCHEMA = DATABASE_SCHEMA.model_copy(update={"version": 10})
_V10_SCHEMA_DEFINITION_SHA256 = "b2a07eec693b04bc149448ee2ed9c11083f790d235e2a9a9c7e75818a6c9ceda"
_V9_DATABASE_SCHEMA = DATABASE_SCHEMA.model_copy(update={"version": 9})
_V9_SCHEMA_DEFINITION_SHA256 = "cb2a83d9b6cb83b52aa3c4741bbfdf77168d26be7fd8508cb3626d256e4709db"
_V8_DATABASE_SCHEMA = DATABASE_SCHEMA.model_copy(update={"version": 8})
_V8_SCHEMA_DEFINITION_SHA256 = "be508e7e461bfd1ad9df975ec37e03a9822b5fc41f6d9db517e7ed46e4b0faaf"
_V7_DATABASE_SCHEMA = DATABASE_SCHEMA.model_copy(update={"version": 7})
_V7_SCHEMA_DEFINITION_SHA256 = "98ee0b3ee6f75ad08cc657e740762e0f752165cba67f12dd832c548df33e8dbc"
_V6_DATABASE_SCHEMA = DATABASE_SCHEMA.model_copy(update={"version": 6})
_V6_SCHEMA_DEFINITION_SHA256 = "78a8206e8c39e88566ddb364c3e1db62d0c58359edd9ea6e170dbd3a468cf514"
_V5_DATABASE_SCHEMA = DATABASE_SCHEMA.model_copy(update={"version": 5})
_V5_SCHEMA_DEFINITION_SHA256 = "0b9b916daaa4f41ccdd9242c628e2502bc777b1b5348e048ebe6228cb44fb65c"
_V4_DATABASE_SCHEMA = DATABASE_SCHEMA.model_copy(update={"version": 4})
_V4_SCHEMA_DEFINITION_SHA256 = "22ea940b93d3d57686107e46756656ecb08c139ef95e214366726421ebee7d4c"
_V3_DATABASE_SCHEMA = DATABASE_SCHEMA.model_copy(update={"version": 3})
_V3_SCHEMA_DEFINITION_SHA256 = "a43327bac12773649f7817d6d33c126b473d92664a366fdf4fb82465401d9b29"
_V2_DATABASE_SCHEMA = DATABASE_SCHEMA.model_copy(update={"version": 2})
_V2_SCHEMA_DEFINITION_SHA256 = "bdd3d1b6a8dc041e7bf86e6c5881a06df910c0efe72612bb2135174b97db553b"
_V1_DATABASE_SCHEMA = DATABASE_SCHEMA.model_copy(update={"version": 1})
_V1_SCHEMA_DEFINITION_SHA256 = "b3fa83413034992e01f84abf8ffe4993864cfad23b4792442b38b60a47b8dca6"

_HISTORY_QUERY_INDEXES = """
CREATE INDEX IF NOT EXISTS work_items_completed_acquisition
ON work_items(kind_parts_json, acquisition_resource_identity_json, created_at_utc_ns DESC, id)
WHERE state='completed' AND acquisition_resource_identity_json IS NOT NULL;

CREATE INDEX IF NOT EXISTS work_events_terminal_time
ON work_events(recorded_at_utc_ns, work_item_id, event_kind)
WHERE event_kind IN ('completed', 'terminal_failure', 'cancelled');
"""

_SCHEMA = (
    """
CREATE TABLE IF NOT EXISTS stage_attempts (
    operation_id TEXT PRIMARY KEY,
    work_item_id TEXT NOT NULL,
    attempt INTEGER CHECK (attempt IS NULL OR attempt >= 1),
    state TEXT NOT NULL CHECK (state IN ('pending', 'active', 'completed', 'cancelled')),
    created_at_utc_ns INTEGER NOT NULL CHECK (created_at_utc_ns >= 0),
    UNIQUE (work_item_id, operation_id),
    FOREIGN KEY (work_item_id) REFERENCES work_items(id)
) STRICT;

CREATE UNIQUE INDEX IF NOT EXISTS stage_attempts_active_work
ON stage_attempts(work_item_id) WHERE state IN ('pending', 'active');

CREATE INDEX IF NOT EXISTS stage_attempts_work ON stage_attempts(work_item_id);

CREATE TABLE IF NOT EXISTS work_stages (
    id TEXT PRIMARY KEY,
    work_item_id TEXT NOT NULL,
    operation_id TEXT NOT NULL,
    definition_json TEXT NOT NULL CHECK (json_valid(definition_json)),
    state TEXT NOT NULL CHECK (state IN ('pending', 'running', 'completed', 'cancelled')),
    execution INTEGER NOT NULL DEFAULT 0 CHECK (execution >= 0),
    eligible_at_utc_ns INTEGER NOT NULL CHECK (eligible_at_utc_ns >= 0),
    wait_kind TEXT,
    wait_entered_at_utc_ns INTEGER,
    progress_waiting INTEGER NOT NULL DEFAULT 0 CHECK(progress_waiting IN (0,1)),
    created_at_utc_ns INTEGER NOT NULL CHECK (created_at_utc_ns >= 0),
    token TEXT,
    worker_identifier TEXT,
    coordination_scope TEXT,
    lease_expires_at_utc_ns INTEGER,
    lease_json TEXT CHECK (lease_json IS NULL OR json_valid(lease_json)),
    FOREIGN KEY (work_item_id, operation_id)
        REFERENCES stage_attempts(work_item_id, operation_id),
    CHECK (
        (state = 'running' AND token IS NOT NULL AND worker_identifier IS NOT NULL
         AND coordination_scope IS NOT NULL AND lease_expires_at_utc_ns IS NOT NULL
         AND lease_json IS NOT NULL)
        OR
        (state != 'running' AND token IS NULL AND worker_identifier IS NULL
         AND coordination_scope IS NULL AND lease_expires_at_utc_ns IS NULL
         AND lease_json IS NULL)
    )
) STRICT;

CREATE UNIQUE INDEX IF NOT EXISTS work_stages_active_work
ON work_stages(work_item_id) WHERE state IN ('pending', 'running');

CREATE INDEX IF NOT EXISTS work_stages_ready
ON work_stages(state, eligible_at_utc_ns, created_at_utc_ns, id);

CREATE INDEX IF NOT EXISTS work_stages_expired
ON work_stages(coordination_scope, state, lease_expires_at_utc_ns, id);

CREATE TABLE IF NOT EXISTS stage_execution_tokens (
    token TEXT PRIMARY KEY CHECK (length(token) > 0),
    stage_id TEXT NOT NULL,
    execution INTEGER NOT NULL CHECK (execution >= 1),
    UNIQUE (stage_id, execution),
    FOREIGN KEY (stage_id) REFERENCES work_stages(id)
) STRICT;

CREATE TABLE IF NOT EXISTS stage_dependencies (
    stage_id TEXT NOT NULL,
    dependency_work_id TEXT NOT NULL,
    PRIMARY KEY (stage_id, dependency_work_id),
    FOREIGN KEY (stage_id) REFERENCES work_stages(id),
    FOREIGN KEY (dependency_work_id) REFERENCES work_items(id)
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS stage_inputs (
    stage_id TEXT NOT NULL,
    name_parts_json TEXT NOT NULL CHECK (json_valid(name_parts_json)),
    object_id TEXT NOT NULL,
    PRIMARY KEY (stage_id, name_parts_json, object_id),
    FOREIGN KEY (stage_id) REFERENCES work_stages(id),
    FOREIGN KEY (object_id) REFERENCES objects(id)
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS stage_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    stage_id TEXT NOT NULL,
    event_kind TEXT NOT NULL CHECK (length(event_kind) > 0),
    recorded_at_utc_ns INTEGER NOT NULL CHECK (recorded_at_utc_ns >= 0),
    data_json TEXT NOT NULL CHECK (json_valid(data_json)),
    FOREIGN KEY (stage_id) REFERENCES work_stages(id)
) STRICT;

CREATE INDEX IF NOT EXISTS stage_events_stage ON stage_events(stage_id, sequence);

CREATE TABLE IF NOT EXISTS resource_budgets (
    scope TEXT NOT NULL CHECK (length(scope) > 0),
    name TEXT NOT NULL CHECK (length(name) > 0),
    capacity INTEGER NOT NULL CHECK (capacity > 0),
    PRIMARY KEY (scope, name)
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS resource_permits (
    token TEXT PRIMARY KEY CHECK (length(token) > 0),
    scope TEXT NOT NULL CHECK (length(scope) > 0),
    owner TEXT NOT NULL CHECK (length(owner) > 0),
    state TEXT NOT NULL CHECK (state IN ('held', 'released', 'reclaimed')),
    acquired_at_utc_ns INTEGER NOT NULL CHECK (acquired_at_utc_ns >= 0),
    expires_at_utc_ns INTEGER NOT NULL CHECK (expires_at_utc_ns > acquired_at_utc_ns),
    UNIQUE (token, scope)
) STRICT;

CREATE INDEX IF NOT EXISTS resource_permits_occupied
ON resource_permits(scope, state, expires_at_utc_ns, token);

CREATE TABLE IF NOT EXISTS resource_allocations (
    permit_token TEXT NOT NULL,
    scope TEXT NOT NULL,
    name TEXT NOT NULL,
    units INTEGER NOT NULL CHECK (units > 0),
    PRIMARY KEY (permit_token, name),
    FOREIGN KEY (permit_token, scope) REFERENCES resource_permits(token, scope),
    FOREIGN KEY (scope, name) REFERENCES resource_budgets(scope, name)
) STRICT, WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS resource_allocations_budget
ON resource_allocations(scope, name, permit_token, units);

CREATE TABLE IF NOT EXISTS resident_sessions (
    id TEXT PRIMARY KEY CHECK (length(id) > 0),
    work_item_id TEXT NOT NULL,
    operation_id TEXT NOT NULL CHECK (length(operation_id) > 0),
    definition_json TEXT NOT NULL CHECK (json_valid(definition_json)),
    generation INTEGER NOT NULL CHECK (generation >= 1),
    token TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL CHECK (state IN ('opening','ready','closing','closed','parked','lost','quarantined')),
    active_stage_token TEXT UNIQUE,
    UNIQUE (work_item_id, generation),
    FOREIGN KEY (work_item_id) REFERENCES work_items(id),
    FOREIGN KEY (token) REFERENCES resource_permits(token),
    FOREIGN KEY (active_stage_token) REFERENCES stage_execution_tokens(token)
) STRICT;

CREATE UNIQUE INDEX IF NOT EXISTS resident_sessions_live_work
ON resident_sessions(work_item_id) WHERE state IN ('opening','ready','closing','quarantined');

CREATE TABLE IF NOT EXISTS resident_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    resident_id TEXT NOT NULL,
    event_kind TEXT NOT NULL CHECK (length(event_kind) > 0),
    recorded_at_utc_ns INTEGER NOT NULL CHECK (recorded_at_utc_ns >= 0),
    data_json TEXT NOT NULL CHECK (json_valid(data_json)),
    FOREIGN KEY (resident_id) REFERENCES resident_sessions(id)
) STRICT;

CREATE INDEX IF NOT EXISTS resident_events_resident ON resident_events(resident_id, sequence);

CREATE TABLE IF NOT EXISTS resident_request_charges (
    operation_id TEXT NOT NULL,
    request_identity_json TEXT NOT NULL CHECK (json_valid(request_identity_json)),
    stage_token TEXT NOT NULL,
    stage_id TEXT NOT NULL,
    execution INTEGER NOT NULL CHECK (execution >= 1),
    resident_id TEXT,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 1),
    maximum_attempts INTEGER NOT NULL CHECK (maximum_attempts >= 1 AND ordinal <= maximum_attempts),
    charged_at_utc_ns INTEGER NOT NULL CHECK (charged_at_utc_ns >= 0),
    PRIMARY KEY (operation_id, request_identity_json, stage_token),
    UNIQUE (operation_id, request_identity_json, ordinal),
    FOREIGN KEY (operation_id) REFERENCES operations(id),
    FOREIGN KEY (stage_token) REFERENCES stage_execution_tokens(token),
    FOREIGN KEY (stage_id) REFERENCES work_stages(id),
    FOREIGN KEY (resident_id) REFERENCES resident_sessions(id)
) STRICT, WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS network_connectivity (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    paused INTEGER NOT NULL CHECK (paused IN (0, 1)),
    next_probe_utc_ns INTEGER NOT NULL CHECK (next_probe_utc_ns >= 0),
    probe_token TEXT,
    probe_expires_utc_ns INTEGER,
    checked_at_utc_ns INTEGER,
    result_json TEXT CHECK (result_json IS NULL OR json_valid(result_json))
) STRICT;

CREATE TABLE IF NOT EXISTS schema_metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS code_states (
    id INTEGER PRIMARY KEY,
    commit_hash BLOB CHECK (commit_hash IS NULL OR length(commit_hash) IN (20, 32)),
    worktree_state TEXT NOT NULL CHECK (worktree_state IN ('clean', 'dirty', 'unknown'))
) STRICT;

CREATE UNIQUE INDEX IF NOT EXISTS code_states_identity
ON code_states(coalesce(commit_hash, X''), worktree_state);

CREATE TABLE IF NOT EXISTS operations (
    id TEXT PRIMARY KEY,
    component_parts_json TEXT NOT NULL CHECK (json_valid(component_parts_json)),
    output_schema_version INTEGER NOT NULL CHECK (output_schema_version >= 1),
    code_provenance_json TEXT NOT NULL CHECK (json_valid(code_provenance_json)),
    invocation_json TEXT NOT NULL CHECK (json_valid(invocation_json)),
    configuration_json TEXT NOT NULL CHECK (json_valid(configuration_json)),
    state TEXT NOT NULL CHECK (state IN ('started', 'completed', 'failed')),
    started_at_utc TEXT NOT NULL,
    ended_at_utc TEXT,
    duration_ns INTEGER CHECK (duration_ns IS NULL OR duration_ns >= 0),
    result_json TEXT CHECK (result_json IS NULL OR json_valid(result_json)),
    error_json TEXT CHECK (error_json IS NULL OR json_valid(error_json)),
    code_state_id INTEGER REFERENCES code_states(id)
) STRICT;

CREATE INDEX IF NOT EXISTS operations_code_state ON operations(code_state_id);

CREATE TABLE IF NOT EXISTS objects (
    id TEXT PRIMARY KEY,
    object_type TEXT NOT NULL CHECK (object_type IN ('record', 'artifact')),
    kind_parts_json TEXT NOT NULL CHECK (json_valid(kind_parts_json)),
    created_by_operation_id TEXT NOT NULL,
    FOREIGN KEY (created_by_operation_id) REFERENCES operations(id)
) STRICT;

CREATE INDEX IF NOT EXISTS objects_kind
ON objects(kind_parts_json);

CREATE INDEX IF NOT EXISTS objects_operation_kind
ON objects(created_by_operation_id, kind_parts_json, id);

CREATE TABLE IF NOT EXISTS records (
    object_id TEXT PRIMARY KEY,
    schema_version INTEGER NOT NULL CHECK (schema_version >= 1),
    value_json TEXT NOT NULL CHECK (json_valid(value_json)),
    FOREIGN KEY (object_id) REFERENCES objects(id)
) STRICT;

CREATE TABLE IF NOT EXISTS saved_image_lookup (
    marketplace TEXT NOT NULL CHECK (marketplace IN ('facebook', 'ebay')),
    lookup_kind TEXT NOT NULL CHECK (lookup_kind IN ('url', 'photo_id', 'path')),
    lookup_value TEXT NOT NULL,
    result_object_id TEXT NOT NULL,
    PRIMARY KEY (marketplace, lookup_kind, lookup_value, result_object_id),
    FOREIGN KEY (result_object_id) REFERENCES objects(id)
) STRICT, WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS records_gallery_observation
ON records(json_extract(value_json, '$.listing_observation_record_identifier'), object_id)
WHERE json_extract(value_json, '$.listing_observation_record_identifier') IS NOT NULL;

CREATE INDEX IF NOT EXISTS records_ebay_gallery_observation
ON records(json_extract(value_json, '$.observation_record_identifier'), object_id)
WHERE json_extract(value_json, '$.observation_record_identifier') IS NOT NULL;

CREATE INDEX IF NOT EXISTS records_saved_image_reference
ON records(json_extract(value_json, '$.image_reference_record_identifier'), object_id)
WHERE json_extract(value_json, '$.state') = 'saved'
  AND json_extract(value_json, '$.image_reference_record_identifier') IS NOT NULL;

CREATE INDEX IF NOT EXISTS records_saved_image_rendition
ON records(
    json_extract(value_json, '$.original_url'),
    json_extract(value_json, '$.source_photo_id'),
    object_id
)
WHERE json_extract(value_json, '$.state') = 'saved'
  AND json_extract(value_json, '$.original_url') IS NOT NULL;

CREATE INDEX IF NOT EXISTS records_listing_observation_listing
ON records(json_extract(value_json, '$.listing_id'), object_id)
WHERE json_extract(value_json, '$.listing_id') IS NOT NULL;

CREATE INDEX IF NOT EXISTS records_search_occurrence_run_position
ON records(
    json_extract(value_json, '$.search_run_identifier'),
    json_extract(value_json, '$.page_ordinal'),
    json_extract(value_json, '$.edge_index'),
    json_extract(value_json, '$.listing_identifier'),
    object_id
)
WHERE json_extract(value_json, '$.listing_identifier') IS NOT NULL
  AND json_extract(value_json, '$.search_run_identifier') IS NOT NULL;

CREATE INDEX IF NOT EXISTS records_search_occurrence_listing
ON records(json_extract(value_json, '$.listing_identifier'), object_id)
WHERE json_extract(value_json, '$.listing_identifier') IS NOT NULL
  AND json_extract(value_json, '$.search_run_identifier') IS NOT NULL;

CREATE TABLE IF NOT EXISTS content (
    sha256 TEXT NOT NULL,
    size INTEGER NOT NULL CHECK (size >= 0),
    storage_backend TEXT NOT NULL CHECK (storage_backend IN ('sqlite')),
    inline_bytes BLOB NOT NULL,
    external_locator TEXT,
    PRIMARY KEY (sha256, size),
    CHECK (length(inline_bytes) = size),
    CHECK (external_locator IS NULL)
) STRICT;

CREATE TABLE IF NOT EXISTS artifacts (
    object_id TEXT PRIMARY KEY,
    sha256 TEXT NOT NULL,
    size INTEGER NOT NULL,
    media_type TEXT,
    representation_json TEXT NOT NULL CHECK (json_valid(representation_json)),
    FOREIGN KEY (object_id) REFERENCES objects(id),
    FOREIGN KEY (sha256, size) REFERENCES content(sha256, size)
) STRICT;

CREATE TABLE IF NOT EXISTS operation_inputs (
    operation_id TEXT NOT NULL,
    name_parts_json TEXT NOT NULL CHECK (json_valid(name_parts_json)),
    object_id TEXT NOT NULL,
    PRIMARY KEY (operation_id, name_parts_json, object_id),
    FOREIGN KEY (operation_id) REFERENCES operations(id),
    FOREIGN KEY (object_id) REFERENCES objects(id)
) STRICT;

CREATE INDEX IF NOT EXISTS operation_inputs_object_name_operation
ON operation_inputs(object_id, name_parts_json, operation_id);

CREATE TABLE IF NOT EXISTS operation_outputs (
    operation_id TEXT NOT NULL,
    name_parts_json TEXT NOT NULL CHECK (json_valid(name_parts_json)),
    object_id TEXT NOT NULL UNIQUE,
    PRIMARY KEY (operation_id, name_parts_json, object_id),
    FOREIGN KEY (operation_id) REFERENCES operations(id),
    FOREIGN KEY (object_id) REFERENCES objects(id)
) STRICT;

CREATE TABLE IF NOT EXISTS work_items (
    id TEXT PRIMARY KEY,
    kind_parts_json TEXT NOT NULL CHECK (json_valid(kind_parts_json)),
    payload_schema_version INTEGER NOT NULL CHECK (payload_schema_version >= 1),
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    acquisition_resource_identity_json TEXT CHECK (
        acquisition_resource_identity_json IS NULL OR json_valid(acquisition_resource_identity_json)
    ),
    deduplication_identity_json TEXT NOT NULL CHECK (json_valid(deduplication_identity_json)),
    state TEXT NOT NULL CHECK (
        state IN ('pending', 'leased', 'completed', 'terminal_failure', 'cancelling', 'cancelled')
    ),
    priority INTEGER NOT NULL,
    eligible_at_utc_ns INTEGER NOT NULL CHECK (eligible_at_utc_ns >= 0),
    created_at_utc_ns INTEGER NOT NULL CHECK (created_at_utc_ns >= 0),
    lease_token TEXT,
    lease_owner TEXT,
    lease_expires_at_utc_ns INTEGER CHECK (
        lease_expires_at_utc_ns IS NULL OR lease_expires_at_utc_ns >= 0
    ),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    result_json TEXT CHECK (result_json IS NULL OR json_valid(result_json)),
    error_json TEXT CHECK (error_json IS NULL OR json_valid(error_json)),
    CHECK (
        (state IN ('leased', 'cancelling') AND lease_token IS NOT NULL AND lease_owner IS NOT NULL
         AND lease_expires_at_utc_ns IS NOT NULL)
        OR
        (state NOT IN ('leased', 'cancelling') AND lease_token IS NULL AND lease_owner IS NULL
         AND lease_expires_at_utc_ns IS NULL)
    )
) STRICT;

CREATE INDEX IF NOT EXISTS work_items_claim
ON work_items(state, eligible_at_utc_ns, priority DESC, created_at_utc_ns);

CREATE INDEX IF NOT EXISTS work_items_dispatch
ON work_items(state, priority DESC, eligible_at_utc_ns, created_at_utc_ns, id);

CREATE INDEX IF NOT EXISTS work_items_expired
ON work_items(state, lease_expires_at_utc_ns, id);

CREATE INDEX IF NOT EXISTS work_items_live_acquisition
ON work_items(kind_parts_json, acquisition_resource_identity_json, lease_expires_at_utc_ns, id)
WHERE acquisition_resource_identity_json IS NOT NULL AND state IN ('leased', 'cancelling');

CREATE UNIQUE INDEX IF NOT EXISTS work_items_active_deduplication
ON work_items(kind_parts_json, deduplication_identity_json)
WHERE state IN ('pending', 'leased');

CREATE INDEX IF NOT EXISTS work_items_analysis_lookup
ON work_items(
    kind_parts_json,
    payload_schema_version,
    json_extract(payload_json, '$.evidence_set_record_identifier'),
    json_extract(payload_json, '$.product_guide_record_identifier'),
    state,
    created_at_utc_ns
);

CREATE TABLE IF NOT EXISTS work_operations (
    operation_id TEXT PRIMARY KEY,
    work_item_id TEXT NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt >= 1),
    lease_token TEXT NOT NULL,
    worker_identifier TEXT NOT NULL,
    UNIQUE (work_item_id, attempt),
    FOREIGN KEY (operation_id) REFERENCES operations(id),
    FOREIGN KEY (work_item_id) REFERENCES work_items(id)
) STRICT;

CREATE TABLE IF NOT EXISTS work_scopes (
    work_item_id TEXT NOT NULL,
    scope_kind TEXT NOT NULL CHECK (
        scope_kind IN (
            'overall', 'network_path', 'work_kind', 'network_activity_kind',
            'remote_origin'
        )
    ),
    scope_identity_json TEXT NOT NULL CHECK (json_valid(scope_identity_json)),
    PRIMARY KEY (work_item_id, scope_kind, scope_identity_json),
    FOREIGN KEY (work_item_id) REFERENCES work_items(id)
) STRICT;

CREATE INDEX IF NOT EXISTS work_scopes_lookup
ON work_scopes(scope_kind, scope_identity_json, work_item_id);

CREATE TABLE IF NOT EXISTS work_requests (
    id TEXT PRIMARY KEY,
    work_item_id TEXT NOT NULL,
    requester_kind_parts_json TEXT NOT NULL CHECK (json_valid(requester_kind_parts_json)),
    requester_identifier TEXT NOT NULL,
    requested_at_utc_ns INTEGER NOT NULL CHECK (requested_at_utc_ns >= 0),
    context_json TEXT NOT NULL CHECK (json_valid(context_json)),
    scheduling_json TEXT CHECK(scheduling_json IS NULL OR json_valid(scheduling_json)),
    parent_work_identifier TEXT REFERENCES work_items(id),
    FOREIGN KEY (work_item_id) REFERENCES work_items(id)
) STRICT;

CREATE INDEX IF NOT EXISTS work_requests_work_item
ON work_requests(work_item_id, requested_at_utc_ns);

CREATE INDEX IF NOT EXISTS work_requests_requester
ON work_requests(requester_identifier, requester_kind_parts_json, work_item_id);

CREATE TABLE IF NOT EXISTS work_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    work_item_id TEXT NOT NULL,
    event_kind TEXT NOT NULL CHECK (
        event_kind IN (
            'enqueued', 'requester_attached', 'claimed', 'lease_expired',
            'lease_renewed', 'released', 'completed', 'terminal_failure',
            'cancel_requested', 'cancelled'
        )
    ),
    recorded_at_utc_ns INTEGER NOT NULL CHECK (recorded_at_utc_ns >= 0),
    data_json TEXT NOT NULL CHECK (json_valid(data_json)),
    FOREIGN KEY (work_item_id) REFERENCES work_items(id)
) STRICT;

CREATE INDEX IF NOT EXISTS work_events_work_item
ON work_events(work_item_id, sequence);

CREATE TABLE IF NOT EXISTS work_cancellation_requests (
    work_item_id TEXT PRIMARY KEY REFERENCES work_items(id),
    requested_at_utc_ns INTEGER NOT NULL CHECK (requested_at_utc_ns >= 0),
    reason TEXT NOT NULL CHECK (length(reason) > 0),
    caller TEXT NOT NULL CHECK (length(caller) > 0)
) STRICT;

CREATE TABLE IF NOT EXISTS work_dependencies (
    parent_work_item_id TEXT NOT NULL REFERENCES work_items(id),
    child_work_item_id TEXT NOT NULL REFERENCES work_items(id),
    PRIMARY KEY (parent_work_item_id, child_work_item_id)
) STRICT, WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS work_dependencies_child ON work_dependencies(child_work_item_id);

CREATE TABLE IF NOT EXISTS scheduling_constraints (
    identifier_parts_json TEXT PRIMARY KEY CHECK (json_valid(identifier_parts_json)),
    constraint_kind TEXT NOT NULL CHECK (
        constraint_kind IN ('concurrency', 'sliding_window_rate', 'uniform_holdoff')
    ),
    subject_kind TEXT NOT NULL CHECK (
        subject_kind IN ('work_item', 'network_activity')
    ),
    scope_kind TEXT NOT NULL CHECK (
        scope_kind IN (
            'overall', 'network_path', 'work_kind', 'network_activity_kind',
            'remote_origin'
        )
    ),
    scope_identity_json TEXT NOT NULL CHECK (json_valid(scope_identity_json)),
    schema_version INTEGER NOT NULL CHECK (schema_version >= 1),
    definition_json TEXT NOT NULL CHECK (json_valid(definition_json)),
    registered_at_utc_ns INTEGER NOT NULL CHECK (registered_at_utc_ns >= 0)
) STRICT;

CREATE INDEX IF NOT EXISTS scheduling_constraints_scope
ON scheduling_constraints(
    subject_kind, scope_kind, scope_identity_json, constraint_kind
);

CREATE TABLE IF NOT EXISTS external_artifacts (
    object_id TEXT PRIMARY KEY,
    sha256 TEXT NOT NULL CHECK (length(sha256) = 64),
    size INTEGER NOT NULL CHECK (size >= 0),
    media_type TEXT,
    representation_json TEXT NOT NULL CHECK (json_valid(representation_json)),
    locator TEXT NOT NULL CHECK (length(locator) > 0),
    FOREIGN KEY (object_id) REFERENCES objects(id)
) STRICT;

CREATE TABLE IF NOT EXISTS scheduling_constraint_retirements (
    identifier_parts_json TEXT PRIMARY KEY CHECK (json_valid(identifier_parts_json)),
    retired_at_utc_ns INTEGER NOT NULL CHECK (retired_at_utc_ns >= 0),
    operation_id TEXT NOT NULL,
    reason TEXT NOT NULL CHECK (length(reason) > 0),
    FOREIGN KEY (identifier_parts_json)
        REFERENCES scheduling_constraints(identifier_parts_json),
    FOREIGN KEY (operation_id) REFERENCES operations(id)
) STRICT;

CREATE TABLE IF NOT EXISTS network_activities (
    id TEXT PRIMARY KEY,
    kind_parts_json TEXT NOT NULL CHECK (json_valid(kind_parts_json)),
    operation_id TEXT NOT NULL,
    network_session_identifier TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 1),
    attempt INTEGER NOT NULL CHECK (attempt >= 1),
    state TEXT NOT NULL CHECK (
        state IN ('pending', 'admitted', 'completed', 'skipped', 'failed', 'cancelled')
    ),
    eligible_at_utc_ns INTEGER NOT NULL CHECK (eligible_at_utc_ns >= 0),
    created_at_utc_ns INTEGER NOT NULL CHECK (created_at_utc_ns >= 0),
    admission_token TEXT,
    admitted_at_utc_ns INTEGER CHECK (
        admitted_at_utc_ns IS NULL OR admitted_at_utc_ns >= 0
    ),
    permit_expires_at_utc_ns INTEGER CHECK (
        permit_expires_at_utc_ns IS NULL OR permit_expires_at_utc_ns >= 0
    ),
    ended_at_utc_ns INTEGER CHECK (ended_at_utc_ns IS NULL OR ended_at_utc_ns >= 0),
    result_json TEXT CHECK (result_json IS NULL OR json_valid(result_json)),
    FOREIGN KEY (operation_id) REFERENCES operations(id),
    CHECK (
        (state = 'admitted' AND admission_token IS NOT NULL
         AND admitted_at_utc_ns IS NOT NULL AND permit_expires_at_utc_ns IS NOT NULL)
        OR
        (state != 'admitted' AND admission_token IS NULL
         AND permit_expires_at_utc_ns IS NULL)
    )
) STRICT;

CREATE INDEX IF NOT EXISTS network_activities_state
ON network_activities(state, eligible_at_utc_ns, permit_expires_at_utc_ns);

CREATE TABLE IF NOT EXISTS network_activity_scopes (
    network_activity_id TEXT NOT NULL,
    scope_kind TEXT NOT NULL CHECK (
        scope_kind IN (
            'overall', 'network_path', 'work_kind', 'network_activity_kind',
            'remote_origin'
        )
    ),
    scope_identity_json TEXT NOT NULL CHECK (json_valid(scope_identity_json)),
    PRIMARY KEY (network_activity_id, scope_kind, scope_identity_json),
    FOREIGN KEY (network_activity_id) REFERENCES network_activities(id)
) STRICT;

CREATE INDEX IF NOT EXISTS network_activity_scopes_lookup
ON network_activity_scopes(scope_kind, scope_identity_json, network_activity_id);

CREATE TABLE IF NOT EXISTS network_activity_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    network_activity_id TEXT NOT NULL,
    event_kind TEXT NOT NULL CHECK (
        event_kind IN ('created', 'admitted', 'completed', 'skipped', 'failed', 'cancelled')
    ),
    recorded_at_utc_ns INTEGER NOT NULL CHECK (recorded_at_utc_ns >= 0),
    data_json TEXT NOT NULL CHECK (json_valid(data_json)),
    FOREIGN KEY (network_activity_id) REFERENCES network_activities(id)
) STRICT;

CREATE INDEX IF NOT EXISTS network_activity_events_activity
ON network_activity_events(network_activity_id, sequence);

CREATE TABLE IF NOT EXISTS rate_starts (
    id TEXT PRIMARY KEY,
    constraint_identifier_parts_json TEXT NOT NULL,
    subject_kind TEXT NOT NULL CHECK (
        subject_kind IN ('work_item', 'network_activity')
    ),
    subject_identifier TEXT NOT NULL,
    permit_token TEXT NOT NULL,
    reserved_at_utc_ns INTEGER NOT NULL CHECK (reserved_at_utc_ns >= 0),
    FOREIGN KEY (constraint_identifier_parts_json)
        REFERENCES scheduling_constraints(identifier_parts_json)
) STRICT;

CREATE INDEX IF NOT EXISTS rate_starts_constraint_time
ON rate_starts(constraint_identifier_parts_json, reserved_at_utc_ns);

CREATE TABLE IF NOT EXISTS review_listing_claims (
    workspace_record_id TEXT NOT NULL,
    listing_identifier TEXT NOT NULL CHECK (
        length(listing_identifier) > 0
        AND (
            listing_identifier NOT GLOB '*[^0-9]*'
            OR (
                listing_identifier GLOB 'ebay:[0-9]*'
                AND length(substr(listing_identifier, 6)) BETWEEN 9 AND 15
                AND substr(listing_identifier, 6) NOT GLOB '*[^0-9]*'
            )
        )
    ),
    batch_record_id TEXT NOT NULL,
    claim_token TEXT NOT NULL CHECK (length(claim_token) > 0),
    owner_identifier TEXT NOT NULL CHECK (length(owner_identifier) > 0),
    acquired_at_utc_ns INTEGER NOT NULL CHECK (acquired_at_utc_ns >= 0),
    lease_expires_at_utc_ns INTEGER NOT NULL CHECK (
        lease_expires_at_utc_ns > acquired_at_utc_ns
    ),
    PRIMARY KEY (workspace_record_id, listing_identifier),
    FOREIGN KEY (workspace_record_id) REFERENCES objects(id),
    FOREIGN KEY (batch_record_id) REFERENCES objects(id)
) STRICT;

CREATE INDEX IF NOT EXISTS review_listing_claims_token
ON review_listing_claims(claim_token, owner_identifier, lease_expires_at_utc_ns);

CREATE INDEX IF NOT EXISTS review_listing_claims_batch
ON review_listing_claims(batch_record_id, claim_token, lease_expires_at_utc_ns);

CREATE TABLE IF NOT EXISTS review_mutation_requests (
    action TEXT NOT NULL CHECK (
        action IN (
            'acquire_batch', 'renew_claim', 'release_claim', 'record_reviews',
            'record_workspace_bulk_review'
        )
    ),
    request_identifier TEXT NOT NULL CHECK (length(request_identifier) > 0),
    request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
    request_schema_version INTEGER NOT NULL CHECK (request_schema_version >= 1),
    response_schema_version INTEGER NOT NULL CHECK (response_schema_version >= 1),
    response_json TEXT NOT NULL CHECK (json_valid(response_json)),
    operation_id TEXT NOT NULL,
    created_at_utc_ns INTEGER NOT NULL CHECK (created_at_utc_ns >= 0),
    PRIMARY KEY (action, request_identifier),
    FOREIGN KEY (operation_id) REFERENCES operations(id)
) STRICT;

CREATE INDEX IF NOT EXISTS review_mutation_requests_operation
ON review_mutation_requests(operation_id);

CREATE INDEX IF NOT EXISTS records_listing_review_workspace_listing
ON records(
    json_extract(value_json, '$.workspace_record_identifier'),
    json_extract(value_json, '$.listing_identifier'),
    object_id
)
WHERE json_extract(value_json, '$.workspace_record_identifier') IS NOT NULL
  AND json_extract(value_json, '$.listing_identifier') IS NOT NULL;

CREATE INDEX IF NOT EXISTS records_marketplace_item_observation
ON records(json_extract(value_json, '$.item_identifier'),
           json_extract(value_json, '$.observation_record_identifier'), object_id);

CREATE INDEX IF NOT EXISTS records_marketplace_source_run
ON records(json_extract(value_json, '$.search_run_record_identifier'), object_id);
"""
    + fair_sql.FAIR_SCHEMA
    + progress_sql.PROGRESS_SCHEMA
    + scheduler_sql.SCHEDULER_SCHEMA
    + _HISTORY_QUERY_INDEXES
)

_SCHEMA_DEFINITION_SHA256 = hashlib.sha256(_SCHEMA.encode()).hexdigest()


def _partial_facebook_search_publication(
    operation: str,
    *,
    run_identifier: str | None = None,
    internal_identifier: str | None = None,
) -> str:
    """SQL for an ended attempt that explicitly published a usable partial run.

    Arguments are internal SQL expressions, never caller-provided values. Failed
    acquisition checkpoints alone do not make their search evidence eligible.
    """

    run_match = "" if run_identifier is None else f"AND partial_run.id = {run_identifier}"
    internal_match = (
        ""
        if internal_identifier is None
        else f"AND json_extract(partial_record.value_json, '$.search_run_identifier') = {internal_identifier}"
    )
    return f"""(
        {operation}.state = 'failed'
        AND json_extract({operation}.result_json, '$.state') = 'partial'
        AND json_extract({operation}.result_json, '$.stopping_reason') = 'rate_limited'
        AND EXISTS (
            SELECT 1 FROM objects AS partial_run
            JOIN records AS partial_record ON partial_record.object_id = partial_run.id
            WHERE partial_run.id = json_extract(
                {operation}.result_json, '$.search_run_record_identifier'
            )
              AND partial_run.created_by_operation_id = {operation}.id
              AND partial_run.kind_parts_json = '["carl","facebook","search_run"]'
              AND json_extract(partial_record.value_json, '$.traversal.stopping_reason') = 'rate_limited'
              {run_match}
              {internal_match}
        )
    )"""


def _facebook_search_publication_event(
    operation: str,
    event: str,
    *,
    run_identifier: str | None = None,
    internal_identifier: str | None = None,
) -> str:
    """SQL preserving successful reads and admitting fenced partial publications."""

    partial = _partial_facebook_search_publication(
        operation, run_identifier=run_identifier, internal_identifier=internal_identifier
    )
    return f"""(
        ({operation}.state = 'completed' AND {event}.event_kind = 'completed')
        OR (
            {partial}
            AND {event}.event_kind IN ('released', 'terminal_failure')
            AND json_extract({event}.data_json, '$.operation_identifier') = {operation}.id
        )
    )"""


def _json(value: JsonValue) -> str:
    return encode_json(value)


def _ebay_search_stack_identifier(payload: JsonValue) -> str | None:
    """Read scheduling metadata without taking payload validation from the handler."""
    if not isinstance(payload, dict) or not isinstance((request := payload.get("request")), dict):
        return None
    stack = request.get("stack_identifier", "ebay_anonymous")
    return stack if isinstance(stack, str) else None


def _text(value: apsw.SQLiteValue) -> str:
    if not isinstance(value, str):
        raise ValueError("Expected SQLite text value")
    return value


def _integer(value: apsw.SQLiteValue) -> int:
    if not isinstance(value, int):
        raise ValueError("Expected SQLite integer value")
    return value


def _analysis_descriptor(row: Sequence[apsw.SQLiteValue]) -> AnalysisDescriptor:
    value = decode_json(_text(row[1]))
    payload = AnalyzeItemPayload.model_validate_json(_text(row[2]))
    if not isinstance(value, dict):
        raise ValueError("Stored item analysis is malformed")
    warnings = value.get("warnings")
    if not isinstance(warnings, list) or not all(isinstance(warning, str) for warning in warnings):
        raise ValueError("Stored item analysis warnings are malformed")
    return AnalysisDescriptor(
        analysis_record_identifier=_text(row[0]),
        evidence_set_record_identifier=payload.evidence_set_record_identifier,
        listing_observation_record_identifier=_text(row[3]),
        product_guide_record_identifier=payload.product_guide_record_identifier,
        completion_sequence=_integer(row[4]),
        completed_at_utc=None if row[5] is None else _text(row[5]),
        state=str(value.get("state", "unknown")),
        warnings=tuple(cast(list[str], warnings)),
        model=payload.model,
    )


def _external_artifact_path(database_path: Path, locator: str) -> Path:
    root = database_path.parent.resolve()
    candidate = (root / locator).resolve(strict=True)
    if not candidate.is_relative_to(root):
        raise ValueError("External artifact locator escapes the database data directory")
    info = candidate.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("External artifact is not a regular file")
    return candidate


def _read_external_artifact(
    database_path: Path,
    locator: str,
    expected_sha256: str,
    expected_size: int,
    maximum_bytes: int | None = None,
) -> tuple[Path, bytes]:
    if maximum_bytes is not None and expected_size > maximum_bytes:
        raise ValueError("Artifact exceeds maximum_bytes")
    path = _external_artifact_path(database_path, locator)
    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        if maximum_bytes is not None and os.fstat(descriptor).st_size > maximum_bytes:
            raise ValueError("Artifact exceeds maximum_bytes")
        chunks = bytearray()
        while True:
            read_size = (
                1024 * 1024
                if maximum_bytes is None
                else min(1024 * 1024, maximum_bytes - len(chunks) + 1)
            )
            chunk = os.read(descriptor, read_size)
            if not chunk:
                break
            if maximum_bytes is not None and len(chunks) + len(chunk) > maximum_bytes:
                raise ValueError("Artifact exceeds maximum_bytes")
            chunks.extend(chunk)
    finally:
        os.close(descriptor)
    content = bytes(chunks)
    if len(content) != expected_size or hashlib.sha256(content).hexdigest() != expected_sha256:
        raise ValueError("External artifact failed integrity verification")
    return path, content


def _utc_text_from_ns(value: int) -> str:
    seconds, nanoseconds = divmod(value, 1_000_000_000)
    prefix = datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{prefix}.{nanoseconds:09d}+00:00"


class LeaseLostError(RuntimeError):
    """A worker no longer owns an active lease for a work item."""


class _StageAdmissionDeferred(Exception):
    def __init__(self, result: StageClaimResult):
        super().__init__("Stage admission is not currently available")
        self.result = result


class _SuccessfulItemPageAvailable(Exception):
    def __init__(self, result: SuccessfulItemPageResult):
        super().__init__(result.listing_id)
        self.result = result


class Database:
    def __init__(self, path: Path, connections: DatabaseConnections):
        self.path = path
        self._connections = connections

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[None]:
        """Atomically combine database mutations using the existing nested writer."""

        async with self._connections.writer():
            yield

    async def work_progress_watermark(self) -> int:
        async with self._connections.reader() as connection:
            return await progress_sql.watermark(connection)

    def _scheduler_scope(self, coordination_scope: str | None) -> str:
        if coordination_scope is not None:
            if not coordination_scope or coordination_scope != coordination_scope.strip():
                raise ValueError("Scheduler status scope must be nonempty and trimmed")
            return coordination_scope
        from carl.io.resource_permits import current_resource_manager, resource_coordination_scope

        manager = current_resource_manager()
        return (
            manager.scope
            if manager is not None and manager.database is self
            else resource_coordination_scope()
        )

    async def scheduler_activity(
        self,
        *,
        coordination_scope: str | None = None,
        now_utc_ns: int,
        recent_window_ns: int = 300_000_000_000,
        maximum_samples: int = 8,
    ) -> SchedulerActivity:
        if now_utc_ns < 0 or recent_window_ns <= 0 or not 1 <= maximum_samples <= 8:
            raise ValueError("Scheduler activity bounds are invalid")
        async with self._connections.reader() as connection:
            return await scheduler_sql.snapshot(
                connection,
                self._scheduler_scope(coordination_scope),
                now_utc_ns,
                recent_window_ns,
                maximum_samples,
            )

    async def work_scheduler_status(
        self, work_identifier: str, *, coordination_scope: str | None = None, now_utc_ns: int
    ) -> SchedulerWorkStatus:
        if not work_identifier or now_utc_ns < 0:
            raise ValueError("Scheduler work status bounds are invalid")
        async with self._connections.reader() as connection:
            return await scheduler_sql.work_status(
                connection, work_identifier, self._scheduler_scope(coordination_scope), now_utc_ns
            )

    async def work_with_scheduler_status(
        self, work_identifier: str, *, coordination_scope: str | None = None, now_utc_ns: int
    ) -> tuple[dict[str, JsonValue], SchedulerWorkStatus]:
        async with self._connections.reader():
            work = await self.work(work_identifier)
            scheduler = await self.work_scheduler_status(
                work_identifier, coordination_scope=coordination_scope, now_utc_ns=now_utc_ns
            )
        return work, scheduler

    async def observe_scheduler_waits(
        self,
        *,
        coordination_scope: str,
        observations: Sequence[SchedulerWaitObservation],
        now_utc_ns: int,
    ) -> None:
        if len(observations) > 32 or now_utc_ns < 0:
            raise ValueError("Scheduler observation page exceeds its bound")
        async with self._connections.writer() as connection:
            await scheduler_sql.observe(
                connection,
                self._scheduler_scope(coordination_scope),
                tuple(observations),
                now_utc_ns,
            )

    async def prune_scheduler_telemetry(self, *, now_utc_ns: int, limit: int = 128) -> bool:
        if now_utc_ns < 0 or not 1 <= limit <= 128:
            raise ValueError("Scheduler retention bounds are invalid")
        async with self._connections.writer() as connection:
            return await scheduler_sql.prune(connection, now_utc_ns, limit)

    async def missing_consumer_edges(
        self,
        parent_work_identifier: str,
        requester_kind: tuple[str, ...],
        child_ids: tuple[str, ...],
    ) -> tuple[str, ...]:
        if len(child_ids) > 1000 or any(not value for value in child_ids):
            raise ValueError("Consumer edge lookup exceeds its bound")
        async with self._connections.reader() as connection:
            rows = await (
                await connection.execute(
                    """SELECT ids.value FROM json_each(?) ids
                WHERE NOT EXISTS(SELECT 1 FROM work_consumer_edges e WHERE e.parent_work_id=? AND e.kind_json=? AND e.child_work_id=ids.value)
                ORDER BY cast(ids.key AS INTEGER)""",
                    (_json(list(child_ids)), parent_work_identifier, _json(list(requester_kind))),
                )
            ).fetchall()
        return tuple(_text(row[0]) for row in rows)

    async def prepare_stage_progress_wait(
        self, lease: StageLease, wait: WaitWork, *, now_utc_ns: int
    ) -> None:
        progress = wait.progress_wait
        if progress is None:
            return
        if (wait.continuation.work_item_identifier, wait.continuation.operation_identifier) != (
            lease.definition.work_item_identifier,
            lease.definition.operation_identifier,
        ):
            raise ValueError("A progress continuation must retain its work and operation")
        nested = self._connections._current_writer_task_identifier == anyio.get_current_task().id  # pyright: ignore[reportPrivateUsage]
        if (
            nested
            and len(progress.watch.work_identifiers) > 128
            and "fingerprint" not in progress.__dict__
        ):
            raise ValueError("Large progress waits must be prepared before an outer writer")
        digest = progress_sql.wait_digest(progress)
        identifier = progress_sql.build_identifier(lease.token, wait.continuation.identifier)
        if (
            not nested or len(progress.watch.work_identifiers) <= 128
        ) and lease.definition.work_item_identifier in progress.watch.work_identifiers:
            raise ValueError("A stage cannot subscribe to its own checkpoint progress")
        async with self._connections.writer() as connection:
            if not await self.validate_stage(lease, now_utc_ns=now_utc_ns):
                raise LeaseLostError("Progress source no longer owns its stage")
            existing = await (
                await connection.execute(
                    "SELECT digest,state FROM progress_wait_builds WHERE id=?", (identifier,)
                )
            ).fetchone()
            if existing is not None:
                if existing[0] != digest:
                    raise ValueError("A prepared progress source cannot change its descriptor")
                if existing[1] in ("prepared", "building", "armed", "inactive"):
                    return
            if nested and len(progress.watch.work_identifiers) > 128:
                raise ValueError("Large progress waits require separately committed source chunks")
            if progress.observed_through_revision > await progress_sql.watermark(connection):
                raise ValueError("Progress baseline is ahead of committed progress")
            groups = _json([list(kind) for kind in progress.watch.consumer_requester_kinds])
            await connection.execute(
                """INSERT OR IGNORE INTO progress_wait_builds(
                id,work_id,operation_id,source_stage_id,source_token,continuation_id,baseline,digest,groups_json,state)
                VALUES(?,?,?,?,?,?,?,?,?,'source')""",
                (
                    identifier,
                    lease.definition.work_item_identifier,
                    lease.definition.operation_identifier,
                    lease.definition.identifier,
                    lease.token,
                    wait.continuation.identifier,
                    progress.observed_through_revision,
                    digest,
                    groups,
                ),
            )
            for kind in progress.watch.consumer_requester_kinds:
                encoded = _json(list(kind))
                if (
                    not progress.watch.work_identifiers
                    and await (
                        await connection.execute(
                            "SELECT 1 FROM work_consumer_edges WHERE parent_work_id=? AND kind_json=? LIMIT 1",
                            (lease.definition.work_item_identifier, encoded),
                        )
                    ).fetchone()
                    is None
                ):
                    raise ValueError(
                        "A legacy consumer group needs explicit unresolved-source fallback"
                    )
                await connection.execute(
                    "INSERT OR IGNORE INTO progress_wait_groups VALUES(?,?,?)",
                    (identifier, lease.definition.work_item_identifier, encoded),
                )
        for ordinal, start in enumerate(range(0, len(progress.watch.work_identifiers), 128)):
            chunk = progress.watch.work_identifiers[start : start + 128]
            encoded = _json(list(chunk))
            if len(encoded.encode()) > 1_048_576:
                raise ValueError("A progress source chunk exceeds its 1 MiB metadata bound")
            async with self._connections.writer() as connection:
                if not await self.validate_stage(lease, now_utc_ns=now_utc_ns):
                    raise LeaseLostError("Progress source ownership changed during preparation")
                count = await (
                    await connection.execute(
                        "SELECT count(DISTINCT ids.value) FROM json_each(?) ids JOIN work_items w ON w.id=ids.value",
                        (encoded,),
                    )
                ).fetchone()
                assert count is not None
                if _integer(count[0]) != len(set(chunk)):
                    raise KeyError("A watched progress source does not exist")
                old = await (
                    await connection.execute(
                        "SELECT identifiers_json FROM progress_wait_source_chunks WHERE build_id=? AND ordinal=?",
                        (identifier, ordinal),
                    )
                ).fetchone()
                if old is not None and old[0] != encoded:
                    raise ValueError("Progress source chunks are immutable")
                await connection.execute(
                    "INSERT OR IGNORE INTO progress_wait_source_chunks VALUES(?,?,?)",
                    (identifier, ordinal, encoded),
                )
        async with self._connections.writer() as connection:
            if not await self.validate_stage(lease, now_utc_ns=now_utc_ns):
                raise LeaseLostError("Progress source lost its final preparation fence")
            await connection.execute(
                "UPDATE progress_wait_builds SET state='prepared',chunk_count=? WHERE id=? AND state='source'",
                ((len(progress.watch.work_identifiers) + 127) // 128, identifier),
            )

    async def _activate_progress_wait(
        self, connection: AsyncConnection, lease: StageLease, wait: WaitWork, now: int
    ) -> None:
        if wait.progress_wait is None:
            return
        identifier = progress_sql.build_identifier(lease.token, wait.continuation.identifier)
        row = await (
            await connection.execute(
                "SELECT digest,chunk_count FROM progress_wait_builds WHERE id=? AND source_token=? AND state='prepared'",
                (identifier, lease.token),
            )
        ).fetchone()
        if row is None or row[0] != progress_sql.wait_digest(wait.progress_wait):
            raise ValueError("Progress registration requires its exact prepared descriptor")
        await connection.execute(
            "UPDATE progress_wait_builds SET stage_id=?,state='building',ticket=? WHERE id=?",
            (wait.continuation.identifier, await progress_sql.ticket(connection), identifier),
        )
        await connection.execute(
            "UPDATE work_stages SET progress_waiting=1 WHERE id=? AND state='pending'",
            (wait.continuation.identifier,),
        )
        if _integer(row[1]) <= 1:
            await progress_sql.build_batch(connection, identifier, 128, now)

    async def drain_progress_notifications(
        self,
        *,
        now_utc_ns: int,
        source_limit: int = 8,
        edge_limit: int = 64,
        total_limit: int = 128,
    ) -> ProgressDrainResult:
        if (
            now_utc_ns < 0
            or not 1 <= source_limit <= 32
            or not 1 <= edge_limit <= 128
            or not 1 <= total_limit <= 1024
        ):
            raise ValueError("Progress drain bounds are invalid")
        woke = 0
        remaining = total_limit
        async with self._connections.reader() as connection:
            builders = await (
                await connection.execute(
                    "SELECT id FROM progress_wait_builds WHERE state='building' ORDER BY ticket,id LIMIT ?",
                    (source_limit,),
                )
            ).fetchall()
            sources = await (
                await connection.execute(
                    "SELECT child_work_id FROM progress_notifications ORDER BY ticket,child_work_id LIMIT ?",
                    (source_limit,),
                )
            ).fetchall()
        build_budget = max(1, total_limit // 2) if sources else total_limit
        for row in builders:
            if build_budget <= 0 or remaining <= 0:
                break
            async with self._connections.writer() as connection:
                used, changed = await progress_sql.build_batch(
                    connection, _text(row[0]), min(edge_limit, build_budget, remaining), now_utc_ns
                )
            woke += changed
            build_budget -= max(1, used)
            remaining -= max(1, used)
        for row in sources:
            if remaining <= 0:
                break
            async with self._connections.writer() as connection:
                used, changed = await progress_sql.drain_source(
                    connection, _text(row[0]), min(edge_limit, remaining), now_utc_ns
                )
            woke += changed
            remaining -= max(1, used)
        async with self._connections.reader() as connection:
            pending = await (
                await connection.execute(
                    "SELECT EXISTS(SELECT 1 FROM progress_wait_builds WHERE state='building') OR EXISTS(SELECT 1 FROM progress_notifications)"
                )
            ).fetchone()
        assert pending is not None
        return ProgressDrainResult(more=bool(pending[0]), woke=woke)

    async def reconcile_progress_waits(
        self, *, after: str | None = None, limit: int = 32, now_utc_ns: int
    ) -> ProgressReconcilePage:
        if not 1 <= limit <= 128 or now_utc_ns < 0:
            raise ValueError("Progress reconciliation bounds are invalid")
        woke = 0
        async with self._connections.writer() as connection:
            rows = await (
                await connection.execute(
                    "SELECT id FROM work_stages INDEXED BY work_stages_progress_sleep WHERE state='pending' AND progress_waiting=1 AND id>? ORDER BY id LIMIT ?",
                    (after or "", limit + 1),
                )
            ).fetchall()
            for row in rows[:limit]:
                build = await (
                    await connection.execute(
                        "SELECT id,reconcile_after FROM progress_wait_builds WHERE stage_id=? AND state='armed'",
                        (row[0],),
                    )
                ).fetchone()
                if build is None:
                    continue
                members = await (
                    await connection.execute(
                        "SELECT m.child_work_id,coalesce(p.revision,0) FROM progress_wait_members m LEFT JOIN work_progress_revisions p ON p.work_id=m.child_work_id WHERE m.build_id=? AND m.child_work_id>? ORDER BY m.child_work_id LIMIT 33",
                        (build[0], build[1]),
                    )
                ).fetchall()
                maximum = max((_integer(member[1]) for member in members[:32]), default=0)
                await connection.execute(
                    "UPDATE progress_wait_builds SET reconcile_after=? WHERE id=?",
                    (members[31][0] if len(members) > 32 else "", build[0]),
                )
                groups = await (
                    await connection.execute(
                        "SELECT coalesce(max(p.revision),0) FROM progress_wait_groups g LEFT JOIN consumer_group_progress p ON p.parent_work_id=g.parent_work_id AND p.kind_json=g.kind_json WHERE g.build_id=?",
                        (build[0],),
                    )
                ).fetchone()
                assert groups is not None
                woke += await progress_sql.observe(
                    connection, _text(build[0]), max(maximum, _integer(groups[0])), now_utc_ns
                )
        more = len(rows) > limit
        return ProgressReconcilePage(
            next_cursor=_text(rows[limit - 1][0]) if more else None, more=more, woke=woke
        )

    @staticmethod
    async def _append_stage_event(
        connection: AsyncConnection,
        stage_identifier: str,
        event_kind: str,
        now_utc_ns: int,
        data: JsonValue,
    ) -> None:
        await connection.execute(
            "INSERT INTO stage_events(id, stage_id, event_kind, recorded_at_utc_ns, data_json) VALUES (?, ?, ?, ?, ?)",
            (str(uuid4()), stage_identifier, event_kind, now_utc_ns, _json(data)),
        )

    async def _register_stage(
        self,
        connection: AsyncConnection,
        definition: StageDefinition,
        created_at_utc_ns: int,
        *,
        continuation: bool = False,
        expected_snapshot: WorkDispatchSnapshot | None = None,
    ) -> None:
        serialized = definition.model_dump_json()
        current: WorkDispatchSnapshot | None = None
        if expected_snapshot is not None:
            if continuation:
                raise ValueError("Initial registration snapshots cannot fence continuations")
            try:
                current = await self._work_dispatch_snapshot(
                    connection, definition.work_item_identifier
                )
            except KeyError:
                raise WorkSnapshotStale from None
            exact_existing = (
                current.active_stage_identifier == definition.identifier
                and current.active_operation_identifier == definition.operation_identifier
                and current.active_stage_state == "pending"
                and current.active_attempt_state in ("pending", "active")
            )
            if (
                definition.work_item_identifier != expected_snapshot.definition.identifier
                or (
                    current.state is not WorkState.PENDING
                    and not (current.state is WorkState.LEASED and exact_existing)
                )
                or current.state is not expected_snapshot.state
                or current.fingerprint != expected_snapshot.fingerprint
                or current.definition != expected_snapshot.definition
                or current.attempt != expected_snapshot.attempt
                or current.active_operation_identifier
                != expected_snapshot.active_operation_identifier
                or current.active_stage_identifier != expected_snapshot.active_stage_identifier
                or current.active_stage_state != expected_snapshot.active_stage_state
                or current.active_attempt_state != expected_snapshot.active_attempt_state
                or (current.active_stage_identifier is not None and not exact_existing)
                or (
                    current.active_operation_identifier is not None
                    and current.active_stage_identifier != definition.identifier
                )
            ):
                raise WorkSnapshotStale(current.active_stage_identifier)
        cursor = await connection.execute(
            "SELECT definition_json FROM work_stages WHERE id = ?", (definition.identifier,)
        )
        existing = await cursor.fetchone()
        if existing is not None:
            if current is not None and current.active_stage_identifier != definition.identifier:
                raise WorkSnapshotStale(current.active_stage_identifier)
            if existing[0] != serialized:
                raise ValueError("A stage identifier cannot be reused for another definition")
            return
        cursor = await connection.execute(
            "SELECT state FROM work_items WHERE id = ?", (definition.work_item_identifier,)
        )
        work = await cursor.fetchone()
        if work is None:
            raise KeyError(definition.work_item_identifier)
        if work[0] not in {"pending", "leased"}:
            raise ValueError("Only pending or suspended work can receive a stage")
        cursor = await connection.execute(
            "SELECT work_item_id, state FROM stage_attempts WHERE operation_id = ?",
            (definition.operation_identifier,),
        )
        attempt = await cursor.fetchone()
        if attempt is None:
            if continuation or work[0] != "pending":
                raise ValueError("An initial stage requires pending logical work")
            cursor = await connection.execute(
                "SELECT 1 FROM stage_attempts WHERE work_item_id = ? AND state IN ('pending', 'active')",
                (definition.work_item_identifier,),
            )
            if await cursor.fetchone() is not None:
                raise ValueError("Logical work already has an active stage attempt")
            cursor = await connection.execute(
                "SELECT 1 FROM operations WHERE id = ?", (definition.operation_identifier,)
            )
            if await cursor.fetchone() is not None:
                raise ValueError("A stage's candidate operation identifier is already in use")
            await connection.execute(
                "INSERT INTO stage_attempts VALUES (?, ?, NULL, 'pending', ?)",
                (
                    definition.operation_identifier,
                    definition.work_item_identifier,
                    created_at_utc_ns,
                ),
            )
        elif (
            attempt[0] != definition.work_item_identifier
            or attempt[1] != "active"
            or not continuation
        ):
            raise ValueError("A continuation must belong to the current logical stage attempt")
        cursor = await connection.execute(
            "SELECT 1 FROM work_stages WHERE work_item_id = ? AND state IN ('pending', 'running')",
            (definition.work_item_identifier,),
        )
        if await cursor.fetchone() is not None:
            raise ValueError("Logical work already has a pending or running stage")
        await connection.execute(
            """
            INSERT INTO work_stages(
                id, work_item_id, operation_id, definition_json, state,
                eligible_at_utc_ns, created_at_utc_ns
            ) VALUES (?, ?, ?, ?, 'pending', ?, ?)
            """,
            (
                definition.identifier,
                definition.work_item_identifier,
                definition.operation_identifier,
                serialized,
                definition.not_before_utc_ns,
                created_at_utc_ns,
            ),
        )
        if definition.dependency_work_identifiers:
            await connection.executemany(
                "INSERT INTO stage_dependencies VALUES (?, ?)",
                [(definition.identifier, item) for item in definition.dependency_work_identifiers],
            )
        if definition.checkpoint_inputs:
            await connection.executemany(
                "INSERT INTO stage_inputs VALUES (?, ?, ?)",
                [
                    (definition.identifier, _json(list(item.name)), item.object_identifier)
                    for item in definition.checkpoint_inputs
                ],
            )
        if not continuation:
            await fair_sql.promote_initial_offers(
                connection,
                work_identifier=definition.work_item_identifier,
                stage_identifier=definition.identifier,
                serialized_definition=serialized,
                resources=definition.resources,
            )
        await self._append_stage_event(
            connection,
            definition.identifier,
            "registered",
            created_at_utc_ns,
            {"definition": definition.model_dump(mode="json")},
        )

    async def register_stage(
        self,
        definition: StageDefinition,
        *,
        created_at_utc_ns: int,
        expected_snapshot: WorkDispatchSnapshot | None = None,
    ) -> None:
        """Opt work into staged routing without claiming it or creating an operation."""

        if created_at_utc_ns < 0:
            raise ValueError("Stage registration time must be nonnegative")
        async with self._connections.writer() as connection:
            await self._register_stage(
                connection, definition, created_at_utc_ns, expected_snapshot=expected_snapshot
            )

    async def try_register_initial_stage(
        self,
        definition: StageDefinition,
        *,
        expected_snapshot: WorkDispatchSnapshot,
        created_at_utc_ns: int,
    ) -> InitialStageRegistration:
        """Decline optimistically, but never swallow an enclosing writer's rollback."""
        # Resident admission may already have inserted permits in an outer writer.
        # Such a caller must receive the exception and unwind that transaction.
        nested = self._connections._current_writer_task_identifier == anyio.get_current_task().id  # pyright: ignore[reportPrivateUsage]
        try:
            await self.register_stage(
                definition, created_at_utc_ns=created_at_utc_ns, expected_snapshot=expected_snapshot
            )
        except WorkSnapshotStale as error:
            if nested:
                raise
            return InitialStageRegistration(
                status="existing" if error.existing_stage_identifier is not None else "stale",
                stage_identifier=error.existing_stage_identifier,
            )
        return InitialStageRegistration(
            status="existing"
            if expected_snapshot.active_stage_identifier == definition.identifier
            else "registered",
            stage_identifier=definition.identifier,
        )

    @staticmethod
    def _dispatch_limit(limit: int, now_utc_ns: int) -> None:
        if (
            type(limit) is not int
            or not 1 <= limit <= 256
            or type(now_utc_ns) is not int
            or now_utc_ns < 0
        ):
            raise ValueError(
                "Dispatch queries require a page limit within 1..256 and a nonnegative time"
            )

    async def dispatch_candidates(
        self,
        *,
        supported_capabilities: Sequence[WorkCapability],
        now_utc_ns: int,
        limit: int,
        after: DispatchCursor | None = None,
        eligible_identifiers: Sequence[str] | None = None,
    ) -> DispatchPage:
        """Read bounded metadata only; admission remains authoritative and separate."""
        self._dispatch_limit(limit, now_utc_ns)
        if not 1 <= len(supported_capabilities) <= 256:
            raise ValueError("Dispatch requires 1..256 supported capabilities")
        predicate = " OR ".join(
            "(w.kind_parts_json=? AND w.payload_schema_version=?)" for _ in supported_capabilities
        )
        bindings: list[apsw.SQLiteValue] = [now_utc_ns, now_utc_ns]
        for capability in supported_capabilities:
            bindings.extend((_json(list(capability.kind)), capability.payload_schema_version))
        scope = ""
        source = "work_items AS w"
        order_columns = "w.priority DESC,w.eligible_at_utc_ns,w.created_at_utc_ns,w.id"
        if eligible_identifiers is not None:
            # Drive exact/scoped metadata reads from their IDs. An IN filter can
            # otherwise make SQLite walk a large sibling queue in dispatch order.
            source = "(SELECT DISTINCT value AS id FROM json_each(?)) AS selected CROSS JOIN work_items AS w ON w.id=selected.id"
            bindings.insert(0, _json(list(eligible_identifiers)))
        else:
            # The shared projection omits progress sleepers before a queue scan,
            # while retaining the original work priority/eligibility ordering.
            source = "fair_ready AS ready INDEXED BY fair_ready_dispatch CROSS JOIN work_items AS w ON w.id=ready.work_item_id"
            scope = "AND ready.active=1"
            order_columns = "ready.priority DESC,ready.work_eligible_at_utc_ns,ready.created_at_utc_ns,ready.work_item_id"
        frontier = ""
        if after is not None:
            frontier = "AND (w.priority<? OR (w.priority=? AND (w.eligible_at_utc_ns,w.created_at_utc_ns,w.id)>(?,?,?)))"
            bindings.extend(
                (
                    after.priority,
                    after.priority,
                    after.eligible_at_utc_ns,
                    after.created_at_utc_ns,
                    after.work_item_identifier,
                )
            )
        bindings.append(limit + 1)
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                f"""
                SELECT w.id,w.kind_parts_json,w.payload_schema_version,w.state,w.attempt,
                       w.priority,w.eligible_at_utc_ns,w.created_at_utc_ns,
                       s.id,s.operation_id,json_extract(s.definition_json,'$.kind'),
                       json_extract(s.definition_json,'$.schema_version'),
                       json_extract(s.definition_json,'$.resident_session_identifier'),
                       json_extract(s.definition_json,'$.resources')
                FROM {source}
                LEFT JOIN stage_attempts AS a ON a.work_item_id=w.id AND a.state IN ('pending','active')
                LEFT JOIN work_stages AS s ON s.work_item_id=w.id AND s.state IN ('pending','running')
                WHERE w.state IN ('pending','leased') AND w.eligible_at_utc_ns<=?
                  AND (s.id IS NULL OR (s.eligible_at_utc_ns<=? AND s.progress_waiting=0))
                  AND ((w.state='pending' AND a.operation_id IS NULL AND s.id IS NULL)
                       OR (s.state='pending' AND s.operation_id=a.operation_id))
                  AND ({predicate}) {scope} {frontier}
                ORDER BY {order_columns} LIMIT ?
            """,
                bindings,
            )
            rows = await cursor.fetchall()
        items = tuple(
            DispatchCandidate(
                work_item_identifier=_text(row[0]),
                capability=WorkCapability(
                    kind=tuple(cast(list[str], decode_json(_text(row[1])))),
                    payload_schema_version=_integer(row[2]),
                ),
                state=WorkState(_text(row[3])),
                attempt=_integer(row[4]),
                priority=_integer(row[5]),
                eligible_at_utc_ns=_integer(row[6]),
                created_at_utc_ns=_integer(row[7]),
                stage_identifier=None if row[8] is None else _text(row[8]),
                operation_identifier=None if row[9] is None else _text(row[9]),
                stage_kind=None
                if row[10] is None
                else tuple(cast(list[str], decode_json(_text(row[10])))),
                stage_schema_version=None if row[11] is None else _integer(row[11]),
                resident_session_identifier=None if row[12] is None else _text(row[12]),
                stage_resources=()
                if row[13] is None
                else tuple(
                    ResourceRequest.model_validate_json(encode_json(value))
                    for value in decode_json(_text(row[13]))
                ),
            )
            for row in rows[:limit]
        )
        more = len(rows) > limit
        return DispatchPage(
            items=items, has_more=more, next_cursor=items[-1].cursor if more else None
        )

    async def work_scheduling(self, work_identifier: str) -> SchedulingContext:
        """Read the immutable first owner's scheduling context, without work history."""
        async with self._connections.reader() as connection:
            return await fair_sql.scheduling(connection, work_identifier)

    @staticmethod
    async def _backfill_work_scheduling(
        connection: AsyncConnection, *, one_batch: bool = False
    ) -> int:
        """Resolve strong legacy parent/workspace edges in bounded work pages."""
        after = ""
        count = 0
        while True:
            cursor = await connection.execute(
                "SELECT work_item_id FROM fair_unowned WHERE work_item_id>? ORDER BY work_item_id LIMIT 128",
                (after,),
            )
            rows = await cursor.fetchall()
            if not rows:
                return count
            for row in rows:
                identifier = _text(row[0])
                await fair_sql.backfill_owner(connection, identifier)
                await connection.execute(fair_sql._refresh_sql("?"), (identifier,))
                after = identifier
                count += 1
            if one_batch:
                return count

    async def enable_fair_scheduling(
        self,
        scope: str,
        now_utc_ns: int,
        *,
        initial_resources: Sequence[tuple[WorkCapability, tuple[ResourceRequest, ...]]] = (),
        resident_capabilities: Sequence[WorkCapability] = (),
        resident_state_bytes: int = 4 * 1024 * 1024,
    ) -> None:
        """Enable fair admission for this DB/host resource scope; opt-in harnesses stay legacy."""
        if type(resident_state_bytes) is not int or resident_state_bytes < 1:
            raise ValueError("Resident scheduling requires a positive state bound")
        while True:
            async with self._connections.writer() as connection:
                count = await self._backfill_work_scheduling(connection, one_batch=True)
            if count == 0:
                break
            await anyio.lowlevel.checkpoint()
        resident_keys = {
            (capability.kind, capability.payload_schema_version)
            for capability in resident_capabilities
        }
        for capability, resources in initial_resources:
            async with self._connections.writer() as connection:
                key = (_json(list(capability.kind)), capability.payload_schema_version)
                is_resident = int(
                    (capability.kind, capability.payload_schema_version) in resident_keys
                )
                values = (
                    *key,
                    fair_sql.lane_for(resources).value,
                    _json(
                        [
                            request.model_dump(mode="json")
                            for request in (
                                (
                                    ResourceRequest(name="resident_sessions"),
                                    ResourceRequest(
                                        name="resident_state_bytes", units=resident_state_bytes
                                    ),
                                )
                                if is_resident
                                else resources
                            )
                        ]
                    ),
                    is_resident,
                )
                previous = await (
                    await connection.execute(
                        "SELECT kind_parts_json,payload_schema_version,lane,resources_json,resident FROM fair_initial_resources WHERE kind_parts_json=? AND payload_schema_version=?",
                        key,
                    )
                ).fetchone()
                if previous is None or tuple(previous) != values:
                    await connection.execute(
                        "INSERT INTO fair_initial_resources VALUES (?,?,?,?,?) ON CONFLICT(kind_parts_json,payload_schema_version) DO UPDATE SET lane=excluded.lane,resources_json=excluded.resources_json,resident=excluded.resident",
                        values,
                    )
                else:
                    continue
            after = ""
            while True:
                async with self._connections.writer() as connection:
                    rows = await (
                        await connection.execute(
                            "SELECT work_item_id FROM fair_ready INDEXED BY fair_ready_initial WHERE active=1 AND stage_id IS NULL AND kind_parts_json=? AND payload_schema_version=? AND work_item_id>? ORDER BY work_item_id LIMIT 128",
                            (*key, after),
                        )
                    ).fetchall()
                    if not rows:
                        break
                    await connection.execute(
                        "UPDATE fair_ready SET lane=? WHERE active=1 AND stage_id IS NULL AND work_item_id IN (SELECT value FROM json_each(?))",
                        (
                            "resident" if is_resident else fair_sql.lane_for(resources).value,
                            _json([_text(row[0]) for row in rows]),
                        ),
                    )
                    after = _text(rows[-1][0])
                await anyio.lowlevel.checkpoint()
        async with self._connections.writer() as connection:
            already = await fair_sql.enabled(connection, scope)
            await fair_sql.enable(connection, scope, now_utc_ns)
        if not already:
            after = ""
            while True:
                async with self._connections.writer() as connection:
                    rows = await (
                        await connection.execute(
                            "SELECT root_request_identifier,workspace_key,scheduling_json FROM fair_roots WHERE root_request_identifier>? ORDER BY root_request_identifier LIMIT 128",
                            (after,),
                        )
                    ).fetchall()
                    if not rows:
                        break
                    for row in rows:
                        await fair_sql.ensure_services(
                            connection,
                            SchedulingContext.model_validate_json(_text(row[2])),
                            _text(row[0]),
                            _text(row[1]),
                        )
                    after = _text(rows[-1][0])
                await anyio.lowlevel.checkpoint()
            async with self._connections.writer() as connection:
                await connection.execute("UPDATE fair_scopes SET enabled=1 WHERE scope=?", (scope,))

    async def fair_dispatch_candidates(
        self,
        *,
        scope: str,
        supported_capabilities: Sequence[WorkCapability],
        now_utc_ns: int,
        limit: int,
        after: FairDispatchCursor | None = None,
        eligible_identifiers: Sequence[str] | None = None,
        excluded_identifiers: Sequence[str] = (),
    ) -> FairDispatchPage[DispatchCandidate]:
        self._dispatch_limit(limit, now_utc_ns)
        if not 1 <= len(supported_capabilities) <= 256 or len(excluded_identifiers) > 1024:
            raise ValueError("Fair discovery scope exceeds its bound")
        async with self._connections.reader() as connection:
            if not await fair_sql.enabled(connection, scope):
                raise ValueError("Fair scheduling has not been enabled for this scope")
            identifiers, cursor, more = await fair_sql.discover(
                connection,
                scope=scope,
                now=now_utc_ns,
                limit=limit,
                capabilities=_json(
                    [
                        [_json(list(capability.kind)), capability.payload_schema_version]
                        for capability in supported_capabilities
                    ]
                ),
                eligible=None
                if eligible_identifiers is None
                else _json(list(eligible_identifiers)),
                excluded=tuple(excluded_identifiers),
                after=after,
            )
            if not identifiers:
                return FairDispatchPage(items=(), next_cursor=cursor, has_more=more)
            page = await self.dispatch_candidates(
                supported_capabilities=supported_capabilities,
                now_utc_ns=now_utc_ns,
                limit=len(identifiers),
                eligible_identifiers=identifiers,
            )
            selected = {item.work_item_identifier: item for item in page.items}
            return FairDispatchPage(
                items=tuple(
                    selected[identifier] for identifier in identifiers if identifier in selected
                ),
                next_cursor=cursor,
                has_more=more,
            )

    async def offer_fair_dispatch_candidates(
        self,
        *,
        scope: str,
        work_identifiers: Sequence[str],
        now_utc_ns: int,
    ) -> None:
        """Advertise a bounded page only while its driver has a matching idle runner.

        Discovery alone is read-only. These short-lived offers make actual driver
        readiness visible across processes, including before initial adaptation.
        """
        self._dispatch_limit(max(1, len(work_identifiers)), now_utc_ns)
        async with self._connections.writer() as connection:
            if not await fair_sql.enabled(connection, scope):
                raise ValueError("Fair scheduling has not been enabled for this scope")
            for identifier in dict.fromkeys(work_identifiers):
                row = await (
                    await connection.execute(
                        """SELECT f.active,f.available_at_utc_ns,f.stage_id,s.definition_json,
                    h.resources_json,h.resident,w.kind_parts_json,w.payload_schema_version,w.attempt
                    FROM work_items w JOIN fair_ready f ON f.work_item_id=w.id
                    LEFT JOIN work_stages s ON s.id=f.stage_id
                    LEFT JOIN fair_initial_resources h ON h.kind_parts_json=w.kind_parts_json
                        AND h.payload_schema_version=w.payload_schema_version
                    WHERE w.id=?""",
                        (identifier,),
                    )
                ).fetchone()
                if row is None or row[0] != 1 or _integer(row[1]) > now_utc_ns:
                    continue
                stage_identifier = None if row[2] is None else _text(row[2])
                resident = False
                if stage_identifier is not None:
                    definition = StageDefinition.model_validate_json(_text(row[3]))
                    if definition.resident_session_identifier is not None:
                        active = await (
                            await connection.execute(
                                "SELECT 1 FROM resident_sessions WHERE work_item_id=? AND state IN ('opening','ready','closing','quarantined') LIMIT 1",
                                (identifier,),
                            )
                        ).fetchone()
                        if active is not None:
                            # The owning actor advertises its own exact executions.
                            continue
                        resident = True
                    signature = definition.model_dump_json()
                    resources = definition.resources
                else:
                    resident = row[5] == 1
                    signature = fair_sql.initial_offer_signature(
                        _text(row[6]), _integer(row[7]), _integer(row[8])
                    )
                    resources = ()
                if stage_identifier is None or resident:
                    if row[4] is None:
                        continue
                    resources = tuple(
                        ResourceRequest.model_validate(value)
                        for value in cast(list[JsonValue], decode_json(_text(row[4])))
                    )
                await fair_sql.offer(
                    connection,
                    scope=scope,
                    lane=fair_sql.lane_for(resources, resident=resident),
                    work_identifier=identifier,
                    stage_identifier=stage_identifier,
                    signature=signature,
                    resources=resources,
                    now=now_utc_ns,
                    initial=stage_identifier is None,
                )

    async def fair_admissions(self, scope: str, *, limit: int = 100) -> tuple[FairAdmission, ...]:
        if not 1 <= limit <= 1000:
            raise ValueError("Fair admission history requires a bounded limit")
        async with self._connections.reader() as connection:
            rows = await (
                await connection.execute(
                    "SELECT scope,lane,sequence,work_item_id,token,scheduling_json,admitted_at_utc_ns FROM fair_admissions WHERE scope=? ORDER BY admitted_at_utc_ns DESC,lane,sequence DESC LIMIT ?",
                    (scope, limit),
                )
            ).fetchall()
        return tuple(
            FairAdmission(
                scope=_text(row[0]),
                lane=FairLane(_text(row[1])),
                sequence=_integer(row[2]),
                work_item_identifier=_text(row[3]),
                token=_text(row[4]),
                scheduling=SchedulingContext.model_validate_json(_text(row[5])),
                admitted_at_utc_ns=_integer(row[6]),
            )
            for row in rows
        )

    async def _fair_offer_eligible(
        self,
        connection: AsyncConnection,
        *,
        scope: str,
        work_identifier: str,
        stage_identifier: str | None,
        lane: FairLane,
        now_utc_ns: int,
    ) -> bool:
        """Check only an offered head; do not create operations or reservations."""
        row = await (
            await connection.execute(
                """SELECT state,lease_expires_at_utc_ns,kind_parts_json,
                CASE WHEN kind_parts_json=? THEN json_extract(payload_json,'$.request.stack_identifier') END,
                CASE WHEN kind_parts_json=? THEN json_extract(payload_json,'$.routing') END,
                acquisition_resource_identity_json,eligible_at_utc_ns,payload_schema_version,attempt
                FROM work_items WHERE id=?""",
                (
                    _json(list(COLLECT_EBAY_SEARCH_WORK_KIND)),
                    _json(list(COLLECT_SEARCH_WORK_KIND)),
                    work_identifier,
                ),
            )
        ).fetchone()
        if row is None or row[0] not in ("pending", "leased"):
            return False
        stored_offer = await (
            await connection.execute(
                """SELECT o.signature,o.resources_json,f.stage_id,h.resources_json FROM fair_offers o
            JOIN fair_ready f ON f.work_item_id=o.work_item_id
            LEFT JOIN fair_initial_resources h ON h.kind_parts_json=f.kind_parts_json
                AND h.payload_schema_version=f.payload_schema_version
            WHERE o.scope=? AND o.lane=? AND o.work_item_id=?""",
                (scope, lane.value, work_identifier),
            )
        ).fetchone()
        initial = stored_offer is not None and _text(stored_offer[0]).startswith("initial:")
        if initial:
            expected = (
                "initial:"
                + hashlib.sha256(
                    fair_sql.initial_offer_signature(
                        _text(row[2]), _integer(row[7]), _integer(row[8])
                    ).encode()
                ).hexdigest()
            )
            if (
                stored_offer is None
                or stored_offer[0] != expected
                or stored_offer[2] is not None
                or stored_offer[1] != stored_offer[3]
                or row[0] != "pending"
                or _integer(row[6]) > now_utc_ns
            ):
                return False
        if lane is FairLane.RESIDENT:
            if _integer(row[6]) > now_utc_ns:
                return False
            pending = await (
                await connection.execute(
                    "SELECT json_extract(definition_json,'$.not_before_utc_ns') FROM work_stages WHERE work_item_id=? AND state='pending'",
                    (work_identifier,),
                )
            ).fetchone()
            if pending is not None and _integer(pending[0]) > now_utc_ns:
                return False
            if stage_identifier is not None:
                stage_row = await (
                    await connection.execute(
                        "SELECT definition_json FROM work_stages WHERE id=? AND work_item_id=? AND state='pending'",
                        (stage_identifier, work_identifier),
                    )
                ).fetchone()
                if (
                    stage_row is None
                    or stored_offer is None
                    or stored_offer[0] != hashlib.sha256(_text(stage_row[0]).encode()).hexdigest()
                ):
                    return False
            if initial:
                available, _, _ = await self._constraint_availability(
                    connection,
                    work_item_identifier=work_identifier,
                    now_utc_ns=now_utc_ns,
                    check_rates=True,
                )
                if not available:
                    return False
            if row[2] == _json(list(COLLECT_EBAY_SEARCH_WORK_KIND)):
                cooldowns = await self._ebay_search_stack_cooldowns(connection, now_utc_ns)
                if (
                    cooldowns.get("ebay_anonymous" if row[3] is None else _text(row[3]), 0)
                    > now_utc_ns
                ):
                    return False
            elif row[2] == _json(list(COLLECT_SEARCH_WORK_KIND)):
                cooldowns = await self._facebook_search_route_cooldowns(connection, now_utc_ns)
                if cooldowns.get("null" if row[4] is None else _text(row[4]), 0) > now_utc_ns:
                    return False
            return (
                await (
                    await connection.execute(
                        "SELECT 1 FROM resident_sessions WHERE work_item_id=? AND state IN ('opening','ready','closing','quarantined') LIMIT 1",
                        (work_identifier,),
                    )
                ).fetchone()
                is None
            )
        if stage_identifier is None:
            fresh = await (
                await connection.execute(
                    """SELECT f.stage_id,o.signature FROM fair_ready f JOIN fair_offers o ON o.work_item_id=f.work_item_id
                WHERE f.work_item_id=? AND o.scope=? AND o.lane=?""",
                    (work_identifier, scope, lane.value),
                )
            ).fetchone()
            expected = (
                "initial:"
                + hashlib.sha256(
                    fair_sql.initial_offer_signature(
                        _text(row[2]), _integer(row[7]), _integer(row[8])
                    ).encode()
                ).hexdigest()
            )
            if (
                row[0] != "pending"
                or _integer(row[6]) > now_utc_ns
                or fresh is None
                or fresh[0] is not None
                or fresh[1] != expected
            ):
                return False
            active = await (
                await connection.execute(
                    "SELECT 1 FROM stage_attempts WHERE work_item_id=? AND state IN ('pending','active') LIMIT 1",
                    (work_identifier,),
                )
            ).fetchone()
            available, _, _ = await self._constraint_availability(
                connection,
                work_item_identifier=work_identifier,
                now_utc_ns=now_utc_ns,
                check_rates=True,
            )
            return active is None and available
        if row[0] == "leased" and row[1] is not None and _integer(row[1]) > now_utc_ns:
            return False
        stage = await (
            await connection.execute(
                "SELECT definition_json,execution,created_at_utc_ns FROM work_stages WHERE id=? AND work_item_id=? AND state='pending'",
                (stage_identifier, work_identifier),
            )
        ).fetchone()
        if stage is None:
            return False
        definition = StageDefinition.model_validate_json(_text(stage[0]))
        if definition.not_before_utc_ns > now_utc_ns:
            return False
        stored = await (
            await connection.execute(
                "SELECT signature FROM fair_offers WHERE scope=? AND lane=? AND work_item_id=?",
                (scope, lane.value, work_identifier),
            )
        ).fetchone()
        if (
            stored is None
            or stored[0] != hashlib.sha256(definition.model_dump_json().encode()).hexdigest()
        ):
            return False
        dependency = await (
            await connection.execute(
                "SELECT 1 FROM stage_dependencies d JOIN work_items w ON w.id=d.dependency_work_id WHERE d.stage_id=? AND w.state NOT IN ('completed','terminal_failure','cancelled') LIMIT 1",
                (stage_identifier,),
            )
        ).fetchone()
        if dependency is not None:
            return False
        attempt = await (
            await connection.execute(
                "SELECT state FROM stage_attempts WHERE operation_id=?",
                (definition.operation_identifier,),
            )
        ).fetchone()
        available, _, _ = await self._constraint_availability(
            connection,
            work_item_identifier=work_identifier,
            now_utc_ns=now_utc_ns,
            check_rates=attempt is not None and attempt[0] == "pending",
        )
        if not available:
            return False
        if row[5] is not None:
            peer = await (
                await connection.execute(
                    """SELECT 1 FROM work_items WHERE kind_parts_json=? AND acquisition_resource_identity_json=? AND id!=? AND state IN ('leased','cancelling') AND (lease_expires_at_utc_ns>? OR EXISTS(SELECT 1 FROM stage_attempts WHERE work_item_id=work_items.id AND state='active')) LIMIT 1""",
                    (row[2], row[5], work_identifier, now_utc_ns),
                )
            ).fetchone()
            if peer is not None:
                return False
        network = definition.network_activity is not None or any(
            request.name == "http" for request in definition.resources
        )
        if not network:
            return True
        connectivity = await (
            await connection.execute("SELECT paused FROM network_connectivity WHERE singleton=1")
        ).fetchone()
        if connectivity is not None and connectivity[0] == 1:
            return False
        if row[2] == _json(list(COLLECT_EBAY_SEARCH_WORK_KIND)):
            cooldowns = await self._ebay_search_stack_cooldowns(connection, now_utc_ns)
            stack = "ebay_anonymous" if row[3] is None else _text(row[3])
            if cooldowns.get(stack, 0) > now_utc_ns:
                return False
        elif row[2] == _json(list(COLLECT_SEARCH_WORK_KIND)):
            cooldowns = await self._facebook_search_route_cooldowns(connection, now_utc_ns)
            routing = "null" if row[4] is None else _text(row[4])
            if cooldowns.get(routing, 0) > now_utc_ns:
                return False
        template = definition.network_activity
        if template is None:
            return True
        if template.not_before_utc_ns > now_utc_ns:
            return False
        constraints = await (
            await connection.execute(
                """SELECT DISTINCT c.definition_json FROM scheduling_constraints c
            JOIN json_each(?) s ON c.scope_kind=json_extract(s.value,'$[0]') AND c.scope_identity_json=json_extract(s.value,'$[1]')
            WHERE c.subject_kind='network_activity' AND NOT EXISTS(SELECT 1 FROM scheduling_constraint_retirements r WHERE r.identifier_parts_json=c.identifier_parts_json)""",
                (
                    _json(
                        [
                            [scope.kind.value, _json(list(scope.identity))]
                            for scope in template.scopes
                        ]
                    ),
                ),
            )
        ).fetchall()
        for stored_constraint in constraints:
            constraint = CONSTRAINT_ADAPTER.validate_json(_text(stored_constraint[0]))
            identifier = _json(list(constraint.identifier))
            if isinstance(constraint, UniformHoldoffConstraint):
                delay = constraint.minimum_ns + int(
                    hashlib.sha256(
                        f"{stage_identifier}:{_integer(stage[1]) + 1}:{constraint.minimum_ns}:{constraint.maximum_ns}".encode()
                    ).hexdigest(),
                    16,
                ) % (constraint.maximum_ns - constraint.minimum_ns + 1)
                if _integer(stage[2]) + delay > now_utc_ns:
                    return False
            elif isinstance(constraint, SlidingWindowRateConstraint):
                count = await (
                    await connection.execute(
                        "SELECT count(*) FROM rate_starts WHERE constraint_identifier_parts_json=? AND reserved_at_utc_ns>?",
                        (identifier, now_utc_ns - constraint.period_ns),
                    )
                ).fetchone()
                assert count is not None
                if _integer(count[0]) >= constraint.maximum_starts:
                    return False
            elif isinstance(constraint, ConcurrencyConstraint):
                count = await (
                    await connection.execute(
                        """SELECT count(*) FROM network_activities a JOIN network_activity_scopes s ON s.network_activity_id=a.id
                    WHERE a.state='admitted' AND a.permit_expires_at_utc_ns>? AND s.scope_kind=? AND s.scope_identity_json=?""",
                        (
                            now_utc_ns,
                            constraint.scope.kind.value,
                            _json(list(constraint.scope.identity)),
                        ),
                    )
                ).fetchone()
                assert count is not None
                if _integer(count[0]) >= constraint.maximum_active:
                    return False
        return True

    async def _fair_gate(
        self,
        connection: AsyncConnection,
        *,
        scope: str,
        work_identifier: str,
        stage_identifier: str | None,
        signature: str,
        resources: tuple[ResourceRequest, ...],
        capacities: dict[str, int],
        now_utc_ns: int,
        resident: bool = False,
    ) -> tuple[bool, FairLane | None]:
        if not await fair_sql.enabled(connection, scope):
            return True, None
        lane = fair_sql.lane_for(resources, resident=resident)
        await fair_sql.offer(
            connection,
            scope=scope,
            lane=lane,
            work_identifier=work_identifier,
            stage_identifier=stage_identifier,
            signature=signature,
            resources=resources,
            now=now_utc_ns,
        )

        async def eligible(work: str, stage: str | None, offered_lane: FairLane) -> bool:
            return await self._fair_offer_eligible(
                connection,
                scope=scope,
                work_identifier=work,
                stage_identifier=stage,
                lane=offered_lane,
                now_utc_ns=now_utc_ns,
            )

        allowed = await fair_sql.admission_allowed(
            connection,
            scope=scope,
            lane=lane,
            work_identifier=work_identifier,
            capacities=capacities,
            now=now_utc_ns,
            eligible=eligible,
        )
        return allowed, lane

    async def _work_dispatch_snapshot(
        self, connection: AsyncConnection, identifier: str
    ) -> WorkDispatchSnapshot:
        cursor = await connection.execute(
            """
            SELECT kind_parts_json,payload_schema_version,payload_json,deduplication_identity_json,
                   priority,eligible_at_utc_ns,state,attempt,lease_token,lease_owner,lease_expires_at_utc_ns,created_at_utc_ns
            FROM work_items WHERE id=?
        """,
            (identifier,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise KeyError(identifier)
        cursor = await connection.execute(
            "SELECT scope_kind,scope_identity_json FROM work_scopes WHERE work_item_id=? ORDER BY scope_kind,scope_identity_json",
            (identifier,),
        )
        scopes = await cursor.fetchall()
        cursor = await connection.execute(
            """
            SELECT a.operation_id,a.state,s.id,s.state,s.definition_json,s.execution,s.token,s.lease_expires_at_utc_ns
            FROM stage_attempts AS a LEFT JOIN work_stages AS s
              ON s.work_item_id=a.work_item_id AND s.operation_id=a.operation_id AND s.state IN ('pending','running')
            WHERE a.work_item_id=? AND a.state IN ('pending','active')
        """,
            (identifier,),
        )
        active = await cursor.fetchone()
        fingerprint = hashlib.sha256(
            encode_json(
                {
                    "identifier": identifier,
                    "work": list(row),
                    "scopes": [list(scope) for scope in scopes],
                    "active": None if active is None else list(active),
                }
            ).encode()
        ).hexdigest()
        return WorkDispatchSnapshot(
            definition=WorkDefinition(
                identifier=identifier,
                kind=tuple(cast(list[str], decode_json(_text(row[0])))),
                payload_schema_version=_integer(row[1]),
                payload=decode_json(_text(row[2])),
                deduplication_identity=tuple(cast(list[str], decode_json(_text(row[3])))),
                priority=_integer(row[4]),
                not_before_utc_ns=_integer(row[5]),
                scopes=tuple(
                    SchedulingScope(
                        kind=SchedulingScopeKind(_text(scope[0])),
                        identity=tuple(cast(list[str], decode_json(_text(scope[1])))),
                    )
                    for scope in scopes
                ),
            ),
            state=WorkState(_text(row[6])),
            attempt=_integer(row[7]),
            fingerprint=fingerprint,
            active_operation_identifier=None if active is None else _text(active[0]),
            active_attempt_state=None if active is None else _text(active[1]),
            active_stage_identifier=None
            if active is None or active[2] is None
            else _text(active[2]),
            active_stage_state=None if active is None or active[3] is None else _text(active[3]),
        )

    async def work_dispatch_snapshot(self, work_identifier: str) -> WorkDispatchSnapshot:
        """Explicit selected point read, including active stage identity, in one snapshot."""
        async with self._connections.reader() as connection:
            return await self._work_dispatch_snapshot(connection, work_identifier)

    async def expired_stage_candidates(
        self,
        *,
        coordination_scope: str,
        now_utc_ns: int,
        limit: int,
        after: ExpiryCursor | None = None,
    ) -> ExpiredStagePage:
        self._dispatch_limit(limit, now_utc_ns)
        if not coordination_scope or coordination_scope != coordination_scope.strip():
            raise ValueError("Stage recovery requires a coordination scope")
        frontier = "" if after is None else "AND (lease_expires_at_utc_ns,id)>(?,?)"
        bindings: list[apsw.SQLiteValue] = [coordination_scope, now_utc_ns]
        if after is not None:
            bindings.extend((after.expires_at_utc_ns, after.identifier))
        bindings.append(limit + 1)
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                f"""
                SELECT id,token,worker_identifier,coordination_scope,execution,lease_expires_at_utc_ns
                FROM work_stages WHERE state='running' AND coordination_scope=? AND lease_expires_at_utc_ns<=?
                {frontier} ORDER BY lease_expires_at_utc_ns,id LIMIT ?
            """,
                bindings,
            )
            rows = await cursor.fetchall()
        items = tuple(
            ExpiredStageCandidate(
                stage_identifier=_text(row[0]),
                token=_text(row[1]),
                worker_identifier=_text(row[2]),
                coordination_scope=_text(row[3]),
                execution=_integer(row[4]),
                expires_at_utc_ns=_integer(row[5]),
            )
            for row in rows[:limit]
        )
        more = len(rows) > limit
        return ExpiredStagePage(
            items=items, has_more=more, next_cursor=items[-1].cursor if more else None
        )

    async def stage_execution_lease(
        self,
        stage_identifier: str,
        *,
        expected_token: str,
        expected_execution: int,
        coordination_scope: str | None = None,
    ) -> StageLease | None:
        """Point-load the matching lease only after the caller owns its physical guard."""
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT lease_json FROM work_stages WHERE id=? AND state='running' AND token=? AND execution=?
                  AND (? IS NULL OR coordination_scope=?)
            """,
                (
                    stage_identifier,
                    expected_token,
                    expected_execution,
                    coordination_scope,
                    coordination_scope,
                ),
            )
            row = await cursor.fetchone()
        return None if row is None else StageLease.model_validate_json(_text(row[0]))

    async def expired_legacy_work_candidates(
        self,
        *,
        now_utc_ns: int,
        limit: int,
        after: ExpiryCursor | None = None,
        eligible_identifiers: Sequence[str] | None = None,
    ) -> ExpiredLegacyWorkPage:
        self._dispatch_limit(limit, now_utc_ns)
        frontier = "" if after is None else "AND (w.lease_expires_at_utc_ns,w.id)>(?,?)"
        bindings: list[apsw.SQLiteValue] = [now_utc_ns]
        if after is not None:
            bindings.extend((after.expires_at_utc_ns, after.identifier))
        source = "fair_ready AS ready INDEXED BY fair_ready_legacy_expiry CROSS JOIN work_items AS w ON w.id=ready.work_item_id"
        order_columns = "ready.legacy_lease_expires_at_utc_ns,ready.work_item_id"
        if after is not None:
            frontier = "AND (ready.legacy_lease_expires_at_utc_ns,ready.work_item_id)>(?,?)"
        scope = "AND ready.legacy_lease_expires_at_utc_ns IS NOT NULL AND ready.legacy_lease_expires_at_utc_ns<=?"
        bindings.append(now_utc_ns)
        if eligible_identifiers is not None:
            source = "(SELECT DISTINCT value AS id FROM json_each(?)) AS selected CROSS JOIN work_items AS w ON w.id=selected.id"
            scope = ""
            order_columns = "w.lease_expires_at_utc_ns,w.id"
            frontier = "" if after is None else "AND (w.lease_expires_at_utc_ns,w.id)>(?,?)"
            bindings.pop()
            bindings.insert(0, _json(list(eligible_identifiers)))
        bindings.append(limit + 1)
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                f"""
                SELECT w.id,w.state,w.attempt,w.lease_token,w.lease_owner,w.lease_expires_at_utc_ns,
                       (SELECT operation_id FROM work_operations WHERE work_item_id=w.id AND attempt=w.attempt AND lease_token=w.lease_token AND worker_identifier=w.lease_owner)
                FROM {source} WHERE w.state IN ('leased','cancelling') AND w.lease_expires_at_utc_ns<=?
                  AND NOT EXISTS (SELECT 1 FROM stage_attempts WHERE work_item_id=w.id AND state IN ('pending','active'))
                  AND NOT EXISTS (SELECT 1 FROM work_stages WHERE work_item_id=w.id AND state IN ('pending','running'))
                {frontier} {scope} ORDER BY {order_columns} LIMIT ?
            """,
                bindings,
            )
            rows = await cursor.fetchall()
        items = tuple(
            ExpiredLegacyWorkCandidate(
                work_item_identifier=_text(row[0]),
                state=WorkState(_text(row[1])),
                attempt=_integer(row[2]),
                lease_token=_text(row[3]),
                worker_identifier=_text(row[4]),
                lease_expires_at_utc_ns=_integer(row[5]),
                operation_identifier=None if row[6] is None else _text(row[6]),
            )
            for row in rows[:limit]
        )
        more = len(rows) > limit
        return ExpiredLegacyWorkPage(
            items=items, has_more=more, next_cursor=items[-1].cursor if more else None
        )

    async def recover_expired_legacy_work(
        self,
        expected: ExpiredLegacyWorkCandidate,
        *,
        now_utc_ns: int,
        event_identifier: str | None = None,
    ) -> bool:
        if type(now_utc_ns) is not int or now_utc_ns < 0:
            raise ValueError("Legacy recovery requires a nonnegative time")
        async with self._connections.writer() as connection:
            cursor = await connection.execute(
                """
                SELECT state,attempt,lease_token,lease_owner,lease_expires_at_utc_ns FROM work_items
                WHERE id=? AND NOT EXISTS(SELECT 1 FROM stage_attempts WHERE work_item_id=work_items.id AND state IN ('pending','active'))
                  AND NOT EXISTS(SELECT 1 FROM work_stages WHERE work_item_id=work_items.id AND state IN ('pending','running'))
            """,
                (expected.work_item_identifier,),
            )
            row = await cursor.fetchone()
            if (
                row is None
                or tuple(row)
                != (
                    expected.state.value,
                    expected.attempt,
                    expected.lease_token,
                    expected.worker_identifier,
                    expected.lease_expires_at_utc_ns,
                )
                or expected.state not in (WorkState.LEASED, WorkState.CANCELLING)
                or expected.lease_expires_at_utc_ns > now_utc_ns
            ):
                return False
            cursor = await connection.execute(
                "SELECT operation_id FROM work_operations WHERE work_item_id=? AND attempt=? AND lease_token=? AND worker_identifier=?",
                (
                    expected.work_item_identifier,
                    expected.attempt,
                    expected.lease_token,
                    expected.worker_identifier,
                ),
            )
            operation = await cursor.fetchone()
            if expected.state is WorkState.CANCELLING:
                return await self.finalize_cancelled_work(
                    work_item_identifier=expected.work_item_identifier,
                    lease_token=expected.lease_token,
                    worker_identifier=expected.worker_identifier,
                    operation_id=None if operation is None else _text(operation[0]),
                    utc_now_ns=lambda: now_utc_ns,
                    event_identifier=event_identifier or str(uuid4()),
                    ended_at_utc=_utc_text_from_ns(now_utc_ns),
                    duration_ns=0,
                )
            recovered = await self._recover_expired_work_attempt(
                connection,
                work_item_identifier=expected.work_item_identifier,
                attempt=expected.attempt,
                recovered_at_utc_ns=now_utc_ns,
            )
            await connection.execute(
                "UPDATE work_items SET state='pending',lease_token=NULL,lease_owner=NULL,lease_expires_at_utc_ns=NULL WHERE id=?",
                (expected.work_item_identifier,),
            )
            await self._append_work_event(
                connection,
                event_identifier=event_identifier or str(uuid4()),
                work_item_identifier=expected.work_item_identifier,
                event_kind=WorkEventKind.LEASE_EXPIRED,
                recorded_at_utc_ns=now_utc_ns,
                data={
                    "expired_token": expected.lease_token,
                    "expired_owner": expected.worker_identifier,
                    "expired_at_utc_ns": expected.lease_expires_at_utc_ns,
                    "recovered_operation_identifiers": list(recovered),
                },
            )
            return True

    async def stage(self, identifier: str) -> StageDefinition:
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT definition_json FROM work_stages WHERE id = ?", (identifier,)
            )
            row = await cursor.fetchone()
        if row is None:
            raise KeyError(identifier)
        return StageDefinition.model_validate_json(_text(row[0]))

    async def active_stage_for_work(
        self, work_item_identifier: str, operation_identifier: str
    ) -> StageDefinition | None:
        """Point-read a continuation after filesystem/SQL publication interruption."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT definition_json FROM work_stages "
                "WHERE work_item_id = ? AND operation_id = ? AND state IN ('pending', 'running')",
                (work_item_identifier, operation_identifier),
            )
            row = await cursor.fetchone()
        return None if row is None else StageDefinition.model_validate_json(_text(row[0]))

    async def dataset_workflow_binding_state(
        self, work_item_identifier: str, operation_identifier: str
    ) -> tuple[str, str | None]:
        """Read only the states of a real work row and its exact bound operation."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT work.state, operation.state FROM work_items AS work "
                "LEFT JOIN work_operations AS binding ON binding.work_item_id=work.id "
                "AND binding.operation_id=? "
                "LEFT JOIN operations AS operation ON operation.id=binding.operation_id "
                "WHERE work.id=?",
                (operation_identifier, work_item_identifier),
            )
            row = await cursor.fetchone()
        if row is None:
            raise KeyError(work_item_identifier)
        return _text(row[0]), None if row[1] is None else _text(row[1])

    async def _stage_wait(
        self,
        connection: AsyncConnection,
        identifier: str,
        result: StageClaimResult,
        now_utc_ns: int,
        coordination_scope: str | None = None,
        observed_reason: str | None = None,
    ) -> StageClaimResult:
        cursor = await connection.execute(
            "SELECT state, wait_kind,work_item_id,json_extract(definition_json,'$.resources') FROM work_stages WHERE id = ?",
            (identifier,),
        )
        row = await cursor.fetchone()
        if row is not None and row[0] == "pending":
            kind = None if result.wait_kind is None else result.wait_kind.value
            await connection.execute(
                "UPDATE work_stages SET wait_entered_at_utc_ns=CASE WHEN wait_kind IS ? THEN coalesce(wait_entered_at_utc_ns,?) ELSE ? END,wait_kind = ?, eligible_at_utc_ns = ? WHERE id = ?",
                (
                    kind,
                    now_utc_ns,
                    now_utc_ns,
                    kind,
                    result.next_eligible_at_utc_ns or now_utc_ns,
                    identifier,
                ),
            )
            if row[1] != kind:
                await self._append_stage_event(
                    connection,
                    identifier,
                    "waiting",
                    now_utc_ns,
                    {"wait_kind": kind, "next_eligible_at_utc_ns": result.next_eligible_at_utc_ns},
                )
            if coordination_scope is not None:
                resources = cast(list[dict[str, JsonValue]], decode_json(_text(row[3])))
                reason = observed_reason or {
                    "resource": "resource",
                    "dependency": "dependency",
                    "time": "time",
                    "connectivity": "connectivity",
                    "ownership": "logical_policy",
                }.get(kind or "", "unknown")
                observation = SchedulerWaitObservation.model_validate(
                    {
                        "work_identifier": row[2],
                        "stage_identifier": identifier,
                        "reason": reason,
                        "resource_names": tuple(
                            request["name"]
                            for request in resources
                            if request.get("name") in scheduler_sql.RESOURCE_NAMES
                        ),
                        "source": "admission",
                    }
                )
                await scheduler_sql.observe(
                    connection, coordination_scope, (observation,), now_utc_ns
                )
        return result

    async def _resume_stage_work(
        self,
        connection: AsyncConnection,
        definition: StageDefinition,
        *,
        token: str,
        worker_identifier: str,
        now_utc_ns: int,
        lease_duration_ns: int,
        network_stage: bool,
    ) -> WorkLease:
        cursor = await connection.execute(
            """
            SELECT state, lease_expires_at_utc_ns, attempt, kind_parts_json,
                   payload_schema_version, payload_json, acquisition_resource_identity_json
            FROM work_items WHERE id = ?
            """,
            (definition.work_item_identifier,),
        )
        work = await cursor.fetchone()
        if work is None:
            raise KeyError(definition.work_item_identifier)
        if work[0] != "leased" or _integer(work[1]) > now_utc_ns:
            raise _StageAdmissionDeferred(
                StageClaimResult(
                    lease=None,
                    wait_kind=StageWaitKind.OWNERSHIP,
                    next_eligible_at_utc_ns=now_utc_ns + 100_000_000,
                )
            )
        kind = tuple(str(part) for part in cast(list[JsonValue], decode_json(_text(work[3]))))
        payload = decode_json(_text(work[5]))
        if network_stage:
            cooldown_until = 0
            if kind == COLLECT_EBAY_SEARCH_WORK_KIND:
                stack = _ebay_search_stack_identifier(payload)
                cooldowns = await self._ebay_search_stack_cooldowns(connection, now_utc_ns)
                cooldown_until = cooldowns.get(stack, 0) if stack is not None else 0
            elif kind == COLLECT_SEARCH_WORK_KIND:
                routing = payload.get("routing") if isinstance(payload, dict) else None
                cooldowns = await self._facebook_search_route_cooldowns(connection, now_utc_ns)
                cooldown_until = cooldowns.get(_json(routing), 0)
            if cooldown_until > now_utc_ns:
                raise _StageAdmissionDeferred(
                    StageClaimResult(
                        lease=None,
                        wait_kind=StageWaitKind.TIME,
                        next_eligible_at_utc_ns=cooldown_until,
                    )
                )
        available, next_eligible, _ = await self._constraint_availability(
            connection,
            work_item_identifier=definition.work_item_identifier,
            now_utc_ns=now_utc_ns,
            check_rates=False,
        )
        if not available:
            raise _StageAdmissionDeferred(
                StageClaimResult(
                    lease=None,
                    wait_kind=StageWaitKind.OWNERSHIP,
                    next_eligible_at_utc_ns=min(
                        next_eligible or now_utc_ns + 100_000_000, now_utc_ns + 100_000_000
                    ),
                )
            )
        resource_json = work[6]
        if resource_json is not None:
            cursor = await connection.execute(
                """
                SELECT 1 FROM work_items
                WHERE kind_parts_json = ? AND acquisition_resource_identity_json = ?
                  AND id != ? AND state IN ('leased','cancelling')
                  AND (lease_expires_at_utc_ns > ? OR EXISTS (
                      SELECT 1 FROM stage_attempts
                      WHERE work_item_id = work_items.id AND state = 'active'
                  ))
                LIMIT 1
                """,
                (
                    _text(work[3]),
                    _text(resource_json),
                    definition.work_item_identifier,
                    now_utc_ns,
                ),
            )
            if await cursor.fetchall():
                raise _StageAdmissionDeferred(
                    StageClaimResult(
                        lease=None,
                        wait_kind=StageWaitKind.OWNERSHIP,
                        next_eligible_at_utc_ns=now_utc_ns + 100_000_000,
                    )
                )
        expires = now_utc_ns + lease_duration_ns
        await connection.execute(
            "UPDATE work_items SET lease_token = ?, lease_owner = ?, lease_expires_at_utc_ns = ? WHERE id = ?",
            (token, worker_identifier, expires, definition.work_item_identifier),
        )
        await connection.execute(
            """
            UPDATE work_operations SET lease_token = ?, worker_identifier = ?
            WHERE operation_id = ? AND work_item_id = ? AND attempt = ?
              AND EXISTS (SELECT 1 FROM operations WHERE id = ? AND state = 'started')
            """,
            (
                token,
                worker_identifier,
                definition.operation_identifier,
                definition.work_item_identifier,
                _integer(work[2]),
                definition.operation_identifier,
            ),
        )
        if await connection.changes() != 1:
            raise LeaseLostError("The staged logical operation is no longer active")
        return WorkLease(
            work_item_identifier=definition.work_item_identifier,
            token=token,
            worker_identifier=worker_identifier,
            attempt=_integer(work[2]),
            expires_at_utc_ns=expires,
            kind=kind,
            payload_schema_version=_integer(work[4]),
            payload=decode_json(_text(work[5])),
        )

    async def try_claim_stage(
        self,
        *,
        stage_identifier: str,
        token: str,
        worker_identifier: str,
        resource_scope: str,
        capacities: dict[str, int],
        lease_duration_ns: int,
        now_utc_ns: int,
        component: Component,
        provenance: CodeProvenance,
        invocation: dict[str, JsonValue],
        resident_lease: ResidentLease | None = None,
    ) -> StageClaimResult:
        """Atomically reserve execution, work, resources and HTTP admission, or none."""

        if now_utc_ns < 0 or lease_duration_ns <= 0:
            raise ValueError("Stage claim timestamps are invalid")
        if any(
            not value or value != value.strip()
            for value in (token, worker_identifier, resource_scope)
        ):
            raise ValueError("Stage execution identities must be nonempty and trimmed")
        async with self._connections.writer() as connection:
            cursor = await connection.execute(
                "SELECT definition_json, state, execution, eligible_at_utc_ns, wait_kind, created_at_utc_ns, progress_waiting FROM work_stages WHERE id = ?",
                (stage_identifier,),
            )
            stage = await cursor.fetchone()
            if stage is None:
                raise KeyError(stage_identifier)
            definition = StageDefinition.model_validate_json(_text(stage[0]))
            if stage[1] != "pending":
                return StageClaimResult(
                    lease=None,
                    wait_kind=StageWaitKind.OWNERSHIP,
                    next_eligible_at_utc_ns=now_utc_ns + 100_000_000,
                )
            if stage[6] == 1:
                return StageClaimResult(lease=None, wait_kind=StageWaitKind.DEPENDENCY)
            cursor = await connection.execute(
                "SELECT state, eligible_at_utc_ns, kind_parts_json, payload_schema_version FROM work_items WHERE id = ?",
                (definition.work_item_identifier,),
            )
            work = await cursor.fetchone()
            assert work is not None
            if work[0] not in {"pending", "leased"}:
                return StageClaimResult(lease=None, wait_kind=StageWaitKind.OWNERSHIP)
            eligible = max(definition.not_before_utc_ns, _integer(work[1]))
            if eligible > now_utc_ns:
                return await self._stage_wait(
                    connection,
                    stage_identifier,
                    StageClaimResult(
                        lease=None, wait_kind=StageWaitKind.TIME, next_eligible_at_utc_ns=eligible
                    ),
                    now_utc_ns,
                    coordination_scope=resource_scope,
                )
            cursor = await connection.execute(
                """
                SELECT 1 FROM stage_dependencies AS dependency
                JOIN work_items AS work ON work.id = dependency.dependency_work_id
                WHERE dependency.stage_id = ?
                  AND work.state NOT IN ('completed','terminal_failure','cancelled') LIMIT 1
                """,
                (stage_identifier,),
            )
            if await cursor.fetchone() is not None:
                return await self._stage_wait(
                    connection,
                    stage_identifier,
                    StageClaimResult(
                        lease=None,
                        wait_kind=StageWaitKind.DEPENDENCY,
                        next_eligible_at_utc_ns=now_utc_ns + 100_000_000,
                    ),
                    now_utc_ns,
                    coordination_scope=resource_scope,
                )
            network_stage = definition.network_activity is not None or any(
                request.name == "http" for request in definition.resources
            )
            if network_stage:
                cursor = await connection.execute(
                    "SELECT paused, next_probe_utc_ns FROM network_connectivity WHERE singleton = 1"
                )
                connectivity = await cursor.fetchone()
                if connectivity is not None and connectivity[0] == 1:
                    return await self._stage_wait(
                        connection,
                        stage_identifier,
                        StageClaimResult(
                            lease=None,
                            wait_kind=StageWaitKind.CONNECTIVITY,
                            next_eligible_at_utc_ns=max(
                                now_utc_ns + 100_000_000, _integer(connectivity[1])
                            ),
                        ),
                        now_utc_ns,
                        coordination_scope=resource_scope,
                    )
            execution = _integer(stage[2]) + 1
            fair_allowed, fair_lane = await self._fair_gate(
                connection,
                scope=resource_scope,
                work_identifier=definition.work_item_identifier,
                stage_identifier=stage_identifier,
                signature=definition.model_dump_json(),
                resources=definition.resources,
                capacities=capacities,
                now_utc_ns=now_utc_ns,
            )
            if not fair_allowed:
                return await self._stage_wait(
                    connection,
                    stage_identifier,
                    StageClaimResult(
                        lease=None,
                        wait_kind=StageWaitKind.OWNERSHIP,
                        next_eligible_at_utc_ns=now_utc_ns + 100_000_000,
                    ),
                    now_utc_ns,
                    coordination_scope=resource_scope,
                    observed_reason="fairness",
                )
            try:
                # Nested writer is a savepoint: unavailable HTTP/resource admission
                # rolls back work attempts, operations and all reservations together.
                async with self._connections.writer():
                    resident = None
                    if definition.resident_session_identifier is not None:
                        resident = (
                            await self._owned_resident(connection, resident_lease)
                            if resident_lease is not None
                            else None
                        )
                        if (
                            resident is None
                            or resident.identifier != definition.resident_session_identifier
                            or resident.coordination_scope != resource_scope
                            or resident.owner != worker_identifier
                            or resident.definition.work_item_identifier
                            != definition.work_item_identifier
                            or resident.definition.operation_identifier
                            != definition.operation_identifier
                            or resident.active_stage_token is not None
                            or not await self.validate_resident_session(
                                resident, now_utc_ns=now_utc_ns
                            )
                        ):
                            raise _StageAdmissionDeferred(
                                StageClaimResult(
                                    lease=None,
                                    wait_kind=StageWaitKind.OWNERSHIP,
                                    next_eligible_at_utc_ns=now_utc_ns + 100_000_000,
                                )
                            )
                    elif resident_lease is not None:
                        raise ValueError("An unbound stage cannot borrow a resident session")
                    cursor = await connection.execute(
                        "SELECT 1 FROM stage_execution_tokens WHERE token = ? UNION ALL SELECT 1 FROM resource_permits WHERE token = ? LIMIT 1",
                        (token, token),
                    )
                    if await cursor.fetchone() is not None:
                        raise ValueError("Stage execution tokens may never be reused")
                    resource_lease = None
                    if definition.resources:
                        resource_lease = await self.try_acquire_resource_permit(
                            scope=resource_scope,
                            token=token,
                            owner=worker_identifier,
                            requests=definition.resources,
                            capacities=capacities,
                            now_utc_ns=now_utc_ns,
                            lease_duration_ns=lease_duration_ns,
                        )
                        if resource_lease is None:
                            raise _StageAdmissionDeferred(
                                StageClaimResult(
                                    lease=None,
                                    wait_kind=StageWaitKind.RESOURCE,
                                    next_eligible_at_utc_ns=now_utc_ns + 100_000_000,
                                )
                            )
                    cursor = await connection.execute(
                        "SELECT state FROM stage_attempts WHERE operation_id = ?",
                        (definition.operation_identifier,),
                    )
                    attempt = await cursor.fetchone()
                    assert attempt is not None
                    if attempt[0] == "pending":
                        claim = await self.claim_work(
                            supported_capabilities=(
                                WorkCapability(
                                    kind=tuple(
                                        str(part)
                                        for part in cast(
                                            list[JsonValue], decode_json(_text(work[2]))
                                        )
                                    ),
                                    payload_schema_version=_integer(work[3]),
                                ),
                            ),
                            worker_identifier=worker_identifier,
                            lease_token=token,
                            lease_duration_ns=lease_duration_ns,
                            utc_now_ns=lambda: now_utc_ns,
                            event_identifier=str(uuid4()),
                            eligible_identifiers=(definition.work_item_identifier,),
                            _include_staged=True,
                            _stage_has_network=network_stage,
                        )
                        if claim.lease is None:
                            raise _StageAdmissionDeferred(
                                StageClaimResult(
                                    lease=None,
                                    wait_kind=StageWaitKind.OWNERSHIP,
                                    next_eligible_at_utc_ns=claim.next_eligible_at_utc_ns
                                    or now_utc_ns + 100_000_000,
                                )
                            )
                        work_lease = claim.lease
                        await self.begin_leased_operation(
                            work_item_identifier=definition.work_item_identifier,
                            lease_token=token,
                            worker_identifier=worker_identifier,
                            lease_duration_ns=lease_duration_ns,
                            utc_now_ns=lambda: now_utc_ns,
                            event_identifier=str(uuid4()),
                            operation_id=definition.operation_identifier,
                            component=component,
                            provenance=provenance,
                            invocation=invocation,
                            configuration={"stage": definition.model_dump(mode="json")},
                            started_at_utc=_utc_text_from_ns(now_utc_ns),
                            inputs=tuple(
                                (item.name, item.object_identifier)
                                for item in definition.checkpoint_inputs
                            ),
                        )
                        # begin_leased_operation may extend the initial work fence.
                        cursor = await connection.execute(
                            "SELECT lease_expires_at_utc_ns FROM work_items WHERE id = ?",
                            (definition.work_item_identifier,),
                        )
                        expiry = await cursor.fetchone()
                        assert expiry is not None
                        work_lease = work_lease.model_copy(
                            update={"expires_at_utc_ns": _integer(expiry[0])}
                        )
                        await connection.execute(
                            "UPDATE stage_attempts SET state = 'active', attempt = ? WHERE operation_id = ?",
                            (work_lease.attempt, definition.operation_identifier),
                        )
                    elif attempt[0] == "active":
                        work_lease = await self._resume_stage_work(
                            connection,
                            definition,
                            token=token,
                            worker_identifier=worker_identifier,
                            now_utc_ns=now_utc_ns,
                            lease_duration_ns=lease_duration_ns,
                            network_stage=network_stage,
                        )
                    else:
                        raise LeaseLostError("The logical stage attempt has already finished")
                    network_admission = None
                    network = stage_network_activity(definition, execution)
                    if network is not None:
                        await self.create_network_activity(
                            network,
                            created_at_utc_ns=_integer(stage[5]),
                            event_identifier=str(uuid4()),
                            sample_uniform_holdoff_ns=lambda minimum, maximum: (
                                minimum
                                + int(
                                    hashlib.sha256(
                                        f"{stage_identifier}:{execution}:{minimum}:{maximum}".encode()
                                    ).hexdigest(),
                                    16,
                                )
                                % (maximum - minimum + 1)
                            ),
                        )
                        admitted = await self.try_admit_network_activity(
                            network_activity_identifier=network.identifier,
                            admission_token=token,
                            permit_duration_ns=lease_duration_ns,
                            now_utc_ns=now_utc_ns,
                            event_identifier=str(uuid4()),
                        )
                        if admitted.admission is None:
                            raise _StageAdmissionDeferred(
                                StageClaimResult(
                                    lease=None,
                                    wait_kind=StageWaitKind.TIME,
                                    next_eligible_at_utc_ns=admitted.next_eligible_at_utc_ns
                                    or now_utc_ns + 100_000_000,
                                )
                            )
                        network_admission = admitted.admission
                    lease = StageLease(
                        definition=definition,
                        coordination_scope=resource_scope,
                        token=token,
                        execution=execution,
                        work_lease=work_lease,
                        resource_lease=resource_lease,
                        network_admission=network_admission,
                    )
                    await connection.execute(
                        "INSERT INTO stage_execution_tokens VALUES (?, ?, ?)",
                        (token, stage_identifier, execution),
                    )
                    if resident is not None:
                        await connection.execute(
                            "UPDATE resident_sessions SET active_stage_token = ? WHERE id = ? AND token = ? AND generation = ? AND active_stage_token IS NULL",
                            (token, resident.identifier, resident.token, resident.generation),
                        )
                        if await connection.changes() != 1:
                            raise LeaseLostError("Resident ownership changed during admission")
                        await self._resident_event(
                            connection,
                            resident,
                            "stage_bound",
                            now_utc_ns,
                            {
                                "stage": stage_identifier,
                                "stage_token": token,
                                "execution": execution,
                            },
                        )
                    expires = min(
                        work_lease.expires_at_utc_ns,
                        resource_lease.expires_at_utc_ns
                        if resource_lease is not None
                        else work_lease.expires_at_utc_ns,
                        network_admission.permit_expires_at_utc_ns
                        if network_admission is not None
                        else work_lease.expires_at_utc_ns,
                    )
                    await connection.execute(
                        """
                        UPDATE work_stages SET state = 'running', execution = ?, token = ?,
                            worker_identifier = ?, coordination_scope = ?,
                            lease_expires_at_utc_ns = ?, lease_json = ?, wait_kind = NULL,wait_entered_at_utc_ns=NULL
                        WHERE id = ? AND state = 'pending'
                        """,
                        (
                            execution,
                            token,
                            worker_identifier,
                            resource_scope,
                            expires,
                            lease.model_dump_json(),
                            stage_identifier,
                        ),
                    )
                    await self._append_stage_event(
                        connection,
                        stage_identifier,
                        "claimed",
                        now_utc_ns,
                        {
                            "lease": lease.model_dump(mode="json"),
                            "component_parts": list(component.identifier.parts),
                            "output_schema_version": component.output_schema_version,
                            "code_provenance": provenance.model_dump(mode="json"),
                            "invocation": invocation,
                        },
                    )
                    if fair_lane is not None:
                        await fair_sql.charge(
                            connection,
                            scope=resource_scope,
                            lane=fair_lane,
                            work_identifier=definition.work_item_identifier,
                            token=token,
                            now=now_utc_ns,
                        )
                    await scheduler_sql.claimed(
                        connection,
                        stage=stage_identifier,
                        execution=execution,
                        scope=resource_scope,
                        work=definition.work_item_identifier,
                        now=now_utc_ns,
                    )
                    return StageClaimResult(lease=lease)
            except _StageAdmissionDeferred as deferred:
                if (
                    fair_lane is not None
                    and deferred.result.wait_kind is not StageWaitKind.RESOURCE
                ):
                    await fair_sql.clear_offer(
                        connection, resource_scope, fair_lane, definition.work_item_identifier
                    )
                return await self._stage_wait(
                    connection,
                    stage_identifier,
                    deferred.result,
                    now_utc_ns,
                    coordination_scope=resource_scope,
                )

    @staticmethod
    async def _stage_execution_owned(connection: AsyncConnection, lease: StageLease) -> bool:
        cursor = await connection.execute(
            """
            SELECT 1 FROM work_stages WHERE id = ? AND state = 'running'
              AND token = ? AND execution = ? AND worker_identifier = ?
              AND coordination_scope = ? AND definition_json = ?
            """,
            (
                lease.definition.identifier,
                lease.token,
                lease.execution,
                lease.work_lease.worker_identifier,
                lease.coordination_scope,
                lease.definition.model_dump_json(),
            ),
        )
        return await cursor.fetchone() is not None

    async def validate_stage(self, lease: StageLease, *, now_utc_ns: int) -> bool:
        """Validate all publication fences, joining an existing writer transaction."""

        if now_utc_ns < 0:
            raise ValueError("Stage validation time must be nonnegative")
        async with self._connections.reader() as connection:
            if not await self._stage_execution_owned(connection, lease):
                return False
            cursor = await connection.execute(
                "SELECT lease_expires_at_utc_ns FROM work_stages WHERE id = ?",
                (lease.definition.identifier,),
            )
            row = await cursor.fetchone()
            assert row is not None
            if _integer(row[0]) <= now_utc_ns:
                return False
            if lease.definition.resident_session_identifier is not None:
                resident = await self._resident_session(
                    connection, lease.definition.resident_session_identifier
                )
                if (
                    resident is None
                    or resident.active_stage_token != lease.token
                    or resident.coordination_scope != lease.coordination_scope
                    or resident.definition.work_item_identifier
                    != lease.definition.work_item_identifier
                    or resident.definition.operation_identifier
                    != lease.definition.operation_identifier
                    or not await self.validate_resident_session(resident, now_utc_ns=now_utc_ns)
                ):
                    return False
            try:
                attempt, _ = await self._require_active_lease(
                    connection,
                    work_item_identifier=lease.definition.work_item_identifier,
                    lease_token=lease.token,
                    worker_identifier=lease.work_lease.worker_identifier,
                    now_utc_ns=now_utc_ns,
                )
                if attempt != lease.work_lease.attempt:
                    return False
                await self._require_bound_operation(
                    connection,
                    operation_id=lease.definition.operation_identifier,
                    work_item_identifier=lease.definition.work_item_identifier,
                    attempt=attempt,
                    lease_token=lease.token,
                    worker_identifier=lease.work_lease.worker_identifier,
                )
            except (LeaseLostError, WorkCancellationRequestedError):
                return False
            cursor = await connection.execute(
                "SELECT 1 FROM operations WHERE id = ? AND state = 'started'",
                (lease.definition.operation_identifier,),
            )
            if await cursor.fetchone() is None:
                return False
            if lease.resource_lease is not None and not await self.validate_resource_permit(
                token=lease.token, owner=lease.resource_lease.owner, now_utc_ns=now_utc_ns
            ):
                return False
            if lease.network_admission is not None:
                cursor = await connection.execute(
                    """
                    SELECT 1 FROM network_activities
                    WHERE id = ? AND state = 'admitted' AND admission_token = ?
                      AND permit_expires_at_utc_ns > ?
                    """,
                    (lease.network_admission.activity_identifier, lease.token, now_utc_ns),
                )
                if await cursor.fetchone() is None:
                    return False
            return True

    async def renew_stage(
        self, lease: StageLease, *, now_utc_ns: int, lease_duration_ns: int
    ) -> StageLease | None:
        """Renew execution, work, resource and network fences in one transaction."""

        if now_utc_ns < 0 or lease_duration_ns <= 0:
            raise ValueError("Stage renewal timestamps are invalid")
        async with self._connections.writer() as connection:
            if not await self.validate_stage(lease, now_utc_ns=now_utc_ns):
                return None
            cursor = await connection.execute(
                "SELECT lease_json FROM work_stages WHERE id = ?", (lease.definition.identifier,)
            )
            row = await cursor.fetchone()
            assert row is not None
            current = StageLease.model_validate_json(_text(row[0]))
            expires = max(current.work_lease.expires_at_utc_ns, now_utc_ns + lease_duration_ns)
            resource = current.resource_lease
            if resource is not None:
                resource = await self.renew_resource_permit(
                    token=lease.token,
                    owner=resource.owner,
                    now_utc_ns=now_utc_ns,
                    lease_duration_ns=expires - now_utc_ns,
                )
                assert resource is not None
            network = current.network_admission
            if network is not None:
                await connection.execute(
                    "UPDATE network_activities SET permit_expires_at_utc_ns = ? WHERE id = ? AND admission_token = ? AND state = 'admitted'",
                    (expires, network.activity_identifier, lease.token),
                )
                if await connection.changes() != 1:
                    raise LeaseLostError("Network stage ownership changed during renewal")
                network = network.model_copy(update={"permit_expires_at_utc_ns": expires})
            await connection.execute(
                "UPDATE work_items SET lease_expires_at_utc_ns = ? WHERE id = ? AND lease_token = ? AND lease_owner = ?",
                (
                    expires,
                    lease.definition.work_item_identifier,
                    lease.token,
                    lease.work_lease.worker_identifier,
                ),
            )
            renewed = current.model_copy(
                update={
                    "work_lease": current.work_lease.model_copy(
                        update={"expires_at_utc_ns": expires}
                    ),
                    "resource_lease": resource,
                    "network_admission": network,
                }
            )
            await connection.execute(
                "UPDATE work_stages SET lease_expires_at_utc_ns = ?, lease_json = ? WHERE id = ? AND token = ?",
                (expires, renewed.model_dump_json(), lease.definition.identifier, lease.token),
            )
            await self._append_stage_event(
                connection,
                lease.definition.identifier,
                "renewed",
                now_utc_ns,
                {"token": lease.token, "execution": lease.execution, "expires_at_utc_ns": expires},
            )
            return renewed

    async def _finish_stage_network(
        self,
        connection: AsyncConnection,
        lease: StageLease,
        *,
        now_utc_ns: int,
        state: NetworkActivityState,
        result: JsonValue,
    ) -> None:
        if lease.network_admission is None:
            return
        cursor = await connection.execute(
            "SELECT state, admission_token FROM network_activities WHERE id = ?",
            (lease.network_admission.activity_identifier,),
        )
        row = await cursor.fetchone()
        if row is not None and row[0] == "admitted" and row[1] == lease.token:
            await self.finish_network_activity(
                network_activity_identifier=lease.network_admission.activity_identifier,
                admission_token=lease.token,
                state=state,
                ended_at_utc_ns=now_utc_ns,
                result=result,
                event_identifier=str(uuid4()),
            )

    @staticmethod
    async def _clear_stage_execution(
        connection: AsyncConnection,
        identifier: str,
        *,
        state: str,
        eligible_at_utc_ns: int,
        wait_kind: StageWaitKind | None = None,
    ) -> None:
        previous = await (
            await connection.execute(
                "SELECT execution FROM work_stages WHERE id=? AND state='running'", (identifier,)
            )
        ).fetchone()
        if previous is not None:
            await scheduler_sql.finished(
                connection,
                stage=identifier,
                execution=_integer(previous[0]),
                now=eligible_at_utc_ns,
                duration=None,
                outcome="cancelled" if state == "cancelled" else "released",
            )
        await connection.execute(
            "UPDATE resident_sessions SET active_stage_token = NULL WHERE active_stage_token = (SELECT token FROM work_stages WHERE id = ?)",
            (identifier,),
        )
        await connection.execute(
            """
            UPDATE work_stages SET state = ?, token = NULL, worker_identifier = NULL,
                coordination_scope = NULL, lease_expires_at_utc_ns = NULL, lease_json = NULL,
                eligible_at_utc_ns = ?, wait_kind = ?, wait_entered_at_utc_ns=? WHERE id = ?
            """,
            (
                state,
                eligible_at_utc_ns,
                None if wait_kind is None else wait_kind.value,
                eligible_at_utc_ns if wait_kind is not None else None,
                identifier,
            ),
        )

        await scheduler_sql.after_transition(connection, eligible_at_utc_ns)

    async def suspend_stage(
        self,
        lease: StageLease,
        wait: WaitWork,
        *,
        now_utc_ns: int,
        records: Sequence[RecordDraft] = (),
        artifacts: Sequence[ArtifactDraft] = (),
        outputs: Sequence[NamedOutput] = (),
        inputs: Sequence[NamedInput] = (),
        phase_duration_ns: int | None = None,
    ) -> None:
        """Publish a checkpoint and continuation without spending an acquisition attempt."""

        await self.prepare_stage_progress_wait(lease, wait, now_utc_ns=now_utc_ns)
        continuation = wait.continuation
        if (
            continuation.work_item_identifier != lease.definition.work_item_identifier
            or continuation.operation_identifier != lease.definition.operation_identifier
        ):
            raise ValueError("A continuation must retain its logical work and operation")
        if (
            continuation.identifier == lease.definition.identifier
            and continuation != lease.definition
        ):
            raise ValueError("A changed continuation requires a fresh stage identifier")
        async with self._connections.writer() as connection:
            if not await self.validate_stage(lease, now_utc_ns=now_utc_ns):
                raise LeaseLostError("The stage no longer owns its execution")
            await scheduler_sql.finished(
                connection,
                stage=lease.definition.identifier,
                execution=lease.execution,
                now=now_utc_ns,
                duration=phase_duration_ns,
                outcome="suspended",
            )
            await self.publish_leased_operation_checkpoint(
                work_item_identifier=lease.definition.work_item_identifier,
                lease_token=lease.token,
                worker_identifier=lease.work_lease.worker_identifier,
                utc_now_ns=lambda: now_utc_ns,
                operation_id=lease.definition.operation_identifier,
                records=records,
                artifacts=artifacts,
                outputs=outputs,
                inputs=inputs,
                checkpoint_result=wait.result,
            )
            await self._finish_stage_network(
                connection,
                lease,
                now_utc_ns=now_utc_ns,
                state=NetworkActivityState.COMPLETED,
                result={"kind": "stage_suspended"},
            )
            same_stage = continuation.identifier == lease.definition.identifier
            await self._clear_stage_execution(
                connection,
                lease.definition.identifier,
                state="pending" if same_stage else "completed",
                eligible_at_utc_ns=max(now_utc_ns, continuation.not_before_utc_ns),
                wait_kind=wait.wait_kind if same_stage else None,
            )
            await connection.execute(
                "UPDATE work_items SET lease_expires_at_utc_ns = ? WHERE id = ? AND lease_token = ?",
                (now_utc_ns, lease.definition.work_item_identifier, lease.token),
            )
            if not same_stage:
                await self._register_stage(connection, continuation, now_utc_ns, continuation=True)
                await connection.execute(
                    "UPDATE work_stages SET wait_kind = ?,wait_entered_at_utc_ns=? WHERE id = ?",
                    (wait.wait_kind.value, now_utc_ns, continuation.identifier),
                )
            await self._activate_progress_wait(connection, lease, wait, now_utc_ns)
            await self._append_stage_event(
                connection,
                lease.definition.identifier,
                "suspended",
                now_utc_ns,
                {
                    "token": lease.token,
                    "execution": lease.execution,
                    "continuation": continuation.identifier,
                    "wait_kind": wait.wait_kind.value,
                },
            )

    async def complete_stage(
        self,
        lease: StageLease,
        *,
        now_utc_ns: int,
        phase_duration_ns: int | None = None,
        phase_outcome: Literal["completed", "retry", "terminal_failure"] = "completed",
    ) -> None:
        """Fence finalization before logical publication in the caller's same transaction."""

        async with self._connections.writer() as connection:
            if not await self.validate_stage(lease, now_utc_ns=now_utc_ns):
                raise LeaseLostError("The stage no longer owns its execution")
            await scheduler_sql.finished(
                connection,
                stage=lease.definition.identifier,
                execution=lease.execution,
                now=now_utc_ns,
                duration=phase_duration_ns,
                outcome=phase_outcome,
            )
            await self._finish_stage_network(
                connection,
                lease,
                now_utc_ns=now_utc_ns,
                state=NetworkActivityState.COMPLETED,
                result={"kind": "stage_completed"},
            )
            await self._clear_stage_execution(
                connection,
                lease.definition.identifier,
                state="completed",
                eligible_at_utc_ns=now_utc_ns,
            )
            await connection.execute(
                "UPDATE stage_attempts SET state = 'completed' WHERE operation_id = ?",
                (lease.definition.operation_identifier,),
            )
            await self._append_stage_event(
                connection,
                lease.definition.identifier,
                "completed",
                now_utc_ns,
                {"token": lease.token, "execution": lease.execution},
            )

    async def release_stage_execution(
        self, lease: StageLease, *, now_utc_ns: int, reason: JsonValue
    ) -> bool:
        """Reset a current executor after physical cleanup, including after expiry.

        A stale finalizer never resets a replacement executor or masks its error.
        """

        if now_utc_ns < 0:
            raise ValueError("Stage release time must be nonnegative")
        async with self._connections.writer() as connection:
            if not await self._stage_execution_owned(connection, lease):
                return False
            cursor = await connection.execute(
                "SELECT state, lease_token, lease_owner FROM work_items WHERE id = ?",
                (lease.definition.work_item_identifier,),
            )
            work = await cursor.fetchone()
            if (
                work is None
                or work[1] != lease.token
                or work[2] != lease.work_lease.worker_identifier
            ):
                return False
            if work[0] == "cancelling":
                await self.finalize_cancelled_work(
                    work_item_identifier=lease.definition.work_item_identifier,
                    lease_token=lease.token,
                    worker_identifier=lease.work_lease.worker_identifier,
                    operation_id=lease.definition.operation_identifier,
                    utc_now_ns=lambda: now_utc_ns,
                    event_identifier=str(uuid4()),
                    ended_at_utc=_utc_text_from_ns(now_utc_ns),
                    duration_ns=0,
                )
                await self._cancel_staged_work(
                    connection, lease.definition.work_item_identifier, now_utc_ns
                )
                return True
            if work[0] != "leased":
                return False
            await self._finish_stage_network(
                connection,
                lease,
                now_utc_ns=now_utc_ns,
                state=NetworkActivityState.FAILED,
                result={"kind": "stage_released", "reason": reason},
            )
            await self._clear_stage_execution(
                connection,
                lease.definition.identifier,
                state="pending",
                eligible_at_utc_ns=now_utc_ns,
                wait_kind=StageWaitKind.OWNERSHIP,
            )
            await connection.execute(
                "UPDATE work_items SET lease_expires_at_utc_ns = ? WHERE id = ? AND lease_token = ?",
                (now_utc_ns, lease.definition.work_item_identifier, lease.token),
            )
            await self._append_stage_event(
                connection,
                lease.definition.identifier,
                "released",
                now_utc_ns,
                {"token": lease.token, "execution": lease.execution, "reason": reason},
            )
            return True

    async def expired_stage_executions(
        self, *, coordination_scope: str, now_utc_ns: int
    ) -> tuple[StageLease, ...]:
        """List expired executors only in the caller's local physical coordination scope."""

        if not coordination_scope or now_utc_ns < 0:
            raise ValueError("Expired stage lookup requires a scope and nonnegative time")
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT lease_json FROM work_stages
                WHERE state = 'running' AND coordination_scope = ? AND lease_expires_at_utc_ns <= ?
                ORDER BY lease_expires_at_utc_ns, id
                """,
                (coordination_scope, now_utc_ns),
            )
            return tuple(
                StageLease.model_validate_json(_text(row[0])) for row in await cursor.fetchall()
            )

    async def recover_stage_execution(self, lease: StageLease, *, now_utc_ns: int) -> bool:
        """Recover after the caller holds the expired execution's OS guard through commit."""

        if now_utc_ns < 0:
            raise ValueError("Stage recovery time must be nonnegative")
        async with self._connections.writer() as connection:
            if not await self._stage_execution_owned(connection, lease):
                return False
            cursor = await connection.execute(
                "SELECT lease_expires_at_utc_ns FROM work_stages WHERE id = ?",
                (lease.definition.identifier,),
            )
            stage = await cursor.fetchone()
            assert stage is not None
            if _integer(stage[0]) > now_utc_ns:
                return False
            cursor = await connection.execute(
                "SELECT lease_token, lease_owner FROM work_items WHERE id = ?",
                (lease.definition.work_item_identifier,),
            )
            work = await cursor.fetchone()
            if (
                work is None
                or work[0] != lease.token
                or work[1] != lease.work_lease.worker_identifier
            ):
                return False
            if lease.resource_lease is not None:
                # The held guard proves physical execution ended, independently
                # of timestamps or how the old process stopped renewing.
                await self.release_resource_permit(
                    token=lease.token, owner=lease.resource_lease.owner
                )
            released = await self.release_stage_execution(
                lease, now_utc_ns=now_utc_ns, reason={"kind": "expired_executor_guard_recovered"}
            )
            if released:
                await self._append_stage_event(
                    connection,
                    lease.definition.identifier,
                    "recovered",
                    now_utc_ns,
                    {"token": lease.token, "execution": lease.execution},
                )
            return released

    async def _cancel_staged_work(
        self, connection: AsyncConnection, work_item_identifier: str, now_utc_ns: int
    ) -> None:
        cursor = await connection.execute(
            "SELECT id FROM work_stages WHERE work_item_id = ? AND state IN ('pending','running')",
            (work_item_identifier,),
        )
        for stage in await cursor.fetchall():
            identifier = _text(stage[0])
            await self._clear_stage_execution(
                connection, identifier, state="cancelled", eligible_at_utc_ns=now_utc_ns
            )
            await self._append_stage_event(connection, identifier, "cancelled", now_utc_ns, {})
        await connection.execute(
            "UPDATE stage_attempts SET state = 'cancelled' WHERE work_item_id = ? AND state IN ('pending','active')",
            (work_item_identifier,),
        )

    async def charge_resident_request(
        self,
        context: StageAttemptContext,
        *,
        request_identity: tuple[str, ...],
        maximum_attempts: int,
        now_utc_ns: int,
    ) -> StageRequestCharge | None:
        """Keep the resident-only dispatch contract for existing collectors."""

        if context.resident_session_identifier is None:
            raise LeaseLostError("Request charge requires resident ownership")
        return await self.charge_stage_request(
            context,
            request_identity=request_identity,
            maximum_attempts=maximum_attempts,
            now_utc_ns=now_utc_ns,
        )

    async def charge_stage_request(
        self,
        context: StageAttemptContext,
        *,
        request_identity: tuple[str, ...],
        maximum_attempts: int,
        now_utc_ns: int,
    ) -> StageRequestCharge | None:
        """Commit a conservative dispatch charge before invoking HTTP.

        Budget identities contain only public request labels, never tokens or
        incarnation IDs. A repeated execution returns newly_charged=False and
        must not invoke HTTP again. An uncertain recovered execution has a fresh
        token and consumes another ordinal within the same logical budget.
        """
        return await self._charge_stage_dispatch(
            context,
            request_identity=request_identity,
            maximum_attempts=maximum_attempts,
            now_utc_ns=now_utc_ns,
            dispatch_resource="http",
        )

    async def charge_stage_invocation(
        self,
        context: StageAttemptContext,
        *,
        request_identity: tuple[str, ...],
        maximum_attempts: int,
        now_utc_ns: int,
    ) -> StageRequestCharge | None:
        """Commit an analysis invocation charge before starting its executor.

        Only a newly charged invocation may start. Recovery uses a fresh stage
        token and consumes another ordinal within the same logical budget.
        """
        return await self._charge_stage_dispatch(
            context,
            request_identity=request_identity,
            maximum_attempts=maximum_attempts,
            now_utc_ns=now_utc_ns,
            dispatch_resource="analysis",
        )

    async def _charge_stage_dispatch(
        self,
        context: StageAttemptContext,
        *,
        request_identity: tuple[str, ...],
        maximum_attempts: int,
        now_utc_ns: int,
        dispatch_resource: Literal["http", "analysis"],
    ) -> StageRequestCharge | None:
        if (
            now_utc_ns < 0
            or type(maximum_attempts) is not int
            or not 1 <= maximum_attempts <= 1000
            or not isinstance(request_identity, tuple)
            or not 1 <= len(request_identity) <= 16
            or any(
                not isinstance(part, str)
                or not 1 <= len(part) <= 256
                or part != part.strip()
                or any(ord(c) < 32 or ord(c) == 127 for c in part)
                for part in request_identity
            )
        ):
            raise ValueError("Request charge requires bounded public labels, budget and time")
        identity = encode_json(list(request_identity))
        if len(identity.encode()) > 4096:
            raise ValueError("Request budget identity exceeds its 4 KiB bound")
        if self._connections._current_writer_task_identifier == anyio.get_current_task().id:
            raise ValueError("Request charges must commit outside an existing writer transaction")
        async with self._connections.writer() as connection:
            cursor = await connection.execute(
                "SELECT lease_json FROM work_stages WHERE id = ? AND state = 'running' AND token = ? AND worker_identifier = ? AND execution = ?",
                (
                    context.stage_identifier,
                    context.lease_token,
                    context.worker_identifier,
                    context.execution,
                ),
            )
            row = await cursor.fetchone()
            if row is None:
                raise LeaseLostError("Request charge no longer owns its stage execution")
            lease = StageLease.model_validate_json(_text(row[0]))
            if (
                context.work_item_identifier != lease.definition.work_item_identifier
                or context.operation_identifier != lease.definition.operation_identifier
                or context.attempt != lease.work_lease.attempt
                or context.resident_session_identifier
                != lease.definition.resident_session_identifier
                or not await self.validate_stage(lease, now_utc_ns=now_utc_ns)
            ):
                raise LeaseLostError("Request charge lost its work or resident ownership")
            if dispatch_resource == "http":
                if (
                    lease.definition.network_activity is None
                    or lease.network_admission is None
                    or not any(request.name == "http" for request in lease.definition.resources)
                ):
                    raise ValueError("Request charges require an admitted HTTP/network stage")
                if (
                    context.network_admission is None
                    or context.network_admission.activity_identifier
                    != lease.network_admission.activity_identifier
                    or context.network_admission.token != lease.token
                ):
                    raise LeaseLostError("Request charge does not match its network admission")
            else:
                if (
                    not any(request.name == "analysis" for request in lease.definition.resources)
                    or lease.definition.network_activity is not None
                    or lease.network_admission is not None
                ):
                    raise ValueError(
                        "Invocation charges require an admitted offline analysis stage"
                    )
                if context.network_admission is not None:
                    raise LeaseLostError("Analysis invocation does not match its offline admission")
            cursor = await connection.execute(
                "SELECT maximum_attempts, max(ordinal) FROM resident_request_charges WHERE operation_id = ? AND request_identity_json = ? GROUP BY maximum_attempts",
                (context.operation_identifier, identity),
            )
            budget = await cursor.fetchone()
            if budget is not None and _integer(budget[0]) != maximum_attempts:
                raise ValueError("A logical request budget cannot change after its first charge")
            cursor = await connection.execute(
                "SELECT ordinal FROM resident_request_charges WHERE operation_id = ? AND request_identity_json = ? AND stage_token = ?",
                (context.operation_identifier, identity, context.lease_token),
            )
            previous = await cursor.fetchone()
            if previous is not None:
                return StageRequestCharge(ordinal=_integer(previous[0]), newly_charged=False)
            ordinal = 1 if budget is None else _integer(budget[1]) + 1
            if ordinal > maximum_attempts:
                return None
            await connection.execute(
                """INSERT INTO resident_request_charges(operation_id, request_identity_json,
                    stage_token, stage_id, execution, resident_id, ordinal, maximum_attempts,
                    charged_at_utc_ns) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    context.operation_identifier,
                    identity,
                    context.lease_token,
                    context.stage_identifier,
                    context.execution,
                    context.resident_session_identifier,
                    ordinal,
                    maximum_attempts,
                    now_utc_ns,
                ),
            )
            return StageRequestCharge(ordinal=ordinal, newly_charged=True)

    async def _resident_session(
        self,
        connection: AsyncConnection,
        identifier: str,
    ) -> ResidentLease | None:
        cursor = await connection.execute(
            """
            SELECT resident.definition_json, resident.generation, resident.state,
                   resident.active_stage_token, permit.token, permit.scope,
                   permit.owner, permit.acquired_at_utc_ns, permit.expires_at_utc_ns
            FROM resident_sessions AS resident
            JOIN resource_permits AS permit ON permit.token = resident.token
            WHERE resident.id = ?
            """,
            (identifier,),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        resource = await self._resource_lease_from_row(connection, row[4:])
        return ResidentLease(
            definition=ResidentDefinition.model_validate_json(_text(row[0])),
            generation=_integer(row[1]),
            state=ResidentState(_text(row[2])),
            active_stage_token=None if row[3] is None else _text(row[3]),
            token=resource.token,
            owner=resource.owner,
            coordination_scope=resource.scope,
            resource_lease=resource,
        )

    async def resident_session(self, identifier: str) -> ResidentLease | None:
        async with self._connections.reader() as connection:
            return await self._resident_session(connection, identifier)

    async def _owned_resident(
        self,
        connection: AsyncConnection,
        lease: ResidentLease,
    ) -> ResidentLease | None:
        current = await self._resident_session(connection, lease.identifier)
        if current is None or (
            current.definition,
            current.generation,
            current.token,
            current.owner,
            current.coordination_scope,
        ) != (
            lease.definition,
            lease.generation,
            lease.token,
            lease.owner,
            lease.coordination_scope,
        ):
            return None
        return current

    @staticmethod
    async def _resident_event(
        connection: AsyncConnection,
        lease: ResidentLease,
        kind: str,
        now_utc_ns: int,
        reason: JsonValue = None,
    ) -> None:
        data = {"generation": lease.generation, "token": lease.token, "reason": reason}
        encoded = encode_json(data)
        if len(encoded.encode()) > 65_536:
            raise ValueError("Resident event exceeds its 64 KiB bound")
        await connection.execute(
            "INSERT INTO resident_events(id, resident_id, event_kind, recorded_at_utc_ns, data_json) VALUES (?, ?, ?, ?, ?)",
            (str(uuid4()), lease.identifier, kind, now_utc_ns, encoded),
        )

    async def try_acquire_resident_session(
        self,
        definition: ResidentDefinition,
        *,
        token: str,
        owner: str,
        scope: str,
        capacities: dict[str, int],
        now_utc_ns: int,
        lease_duration_ns: int,
    ) -> ResidentLease | None:
        """Reserve one actor and its bounded state together, or allocate nothing."""
        if now_utc_ns < 0 or lease_duration_ns <= 0:
            raise ValueError("Resident admission timestamps are invalid")
        async with self._connections.writer() as connection:
            if await self._resident_session(connection, definition.identifier) is not None:
                raise ValueError("Resident incarnation identifiers may never be reused")
            cursor = await connection.execute(
                "SELECT state FROM work_items WHERE id = ?",
                (definition.work_item_identifier,),
            )
            work = await cursor.fetchone()
            if work is None:
                raise KeyError(definition.work_item_identifier)
            if work[0] not in {"pending", "leased"}:
                return None
            cursor = await connection.execute(
                "SELECT 1 FROM resident_sessions WHERE work_item_id = ? AND state IN ('opening','ready','closing','quarantined')",
                (definition.work_item_identifier,),
            )
            if await cursor.fetchone() is not None:
                return None
            requests = (
                ResourceRequest(name="resident_sessions"),
                ResourceRequest(name="resident_state_bytes", units=definition.state_bytes),
            )
            allowed, fair_lane = await self._fair_gate(
                connection,
                scope=scope,
                work_identifier=definition.work_item_identifier,
                stage_identifier=None,
                signature=definition.model_dump_json(),
                resources=requests,
                capacities=capacities,
                now_utc_ns=now_utc_ns,
                resident=True,
            )
            if not allowed:
                return None
            resource = await self.try_acquire_resource_permit(
                scope=scope,
                token=token,
                owner=owner,
                requests=requests,
                capacities=capacities,
                now_utc_ns=now_utc_ns,
                lease_duration_ns=lease_duration_ns,
            )
            if resource is None:
                return None
            cursor = await connection.execute(
                "SELECT coalesce(max(generation), 0) + 1 FROM resident_sessions WHERE work_item_id = ?",
                (definition.work_item_identifier,),
            )
            row = await cursor.fetchone()
            assert row is not None
            lease = ResidentLease(
                definition=definition,
                generation=_integer(row[0]),
                token=token,
                owner=owner,
                coordination_scope=scope,
                state=ResidentState.OPENING,
                resource_lease=resource,
            )
            await connection.execute(
                "INSERT INTO resident_sessions(id, work_item_id, operation_id, definition_json, generation, token, state) VALUES (?, ?, ?, ?, ?, ?, 'opening')",
                (
                    definition.identifier,
                    definition.work_item_identifier,
                    definition.operation_identifier,
                    definition.model_dump_json(),
                    lease.generation,
                    token,
                ),
            )
            await self._resident_event(connection, lease, "acquired", now_utc_ns)
            if fair_lane is not None:
                await fair_sql.charge(
                    connection,
                    scope=scope,
                    lane=fair_lane,
                    work_identifier=definition.work_item_identifier,
                    token=token,
                    now=now_utc_ns,
                )
            return lease

    async def validate_resident_session(self, lease: ResidentLease, *, now_utc_ns: int) -> bool:
        if now_utc_ns < 0:
            raise ValueError("Resident validation time must be nonnegative")
        async with self._connections.reader() as connection:
            current = await self._owned_resident(connection, lease)
            if current is None or current.state not in {ResidentState.OPENING, ResidentState.READY}:
                return False
            cursor = await connection.execute(
                "SELECT state FROM work_items WHERE id = ?",
                (lease.definition.work_item_identifier,),
            )
            work = await cursor.fetchone()
            return (
                work is not None
                and work[0] in {"pending", "leased"}
                and await self.validate_resource_permit(
                    token=lease.token, owner=lease.owner, now_utc_ns=now_utc_ns
                )
            )

    async def renew_resident_session(
        self,
        lease: ResidentLease,
        *,
        now_utc_ns: int,
        lease_duration_ns: int,
    ) -> ResidentLease | None:
        if now_utc_ns < 0 or lease_duration_ns <= 0:
            raise ValueError("Resident renewal timestamps are invalid")
        async with self._connections.writer() as connection:
            current = await self._owned_resident(connection, lease)
            if current is None or current.state in {
                ResidentState.CLOSED,
                ResidentState.PARKED,
                ResidentState.LOST,
            }:
                return None
            if current.state in {
                ResidentState.OPENING,
                ResidentState.READY,
            } and not await self.validate_resident_session(current, now_utc_ns=now_utc_ns):
                return None
            resource = await self.renew_resource_permit(
                token=lease.token,
                owner=lease.owner,
                now_utc_ns=now_utc_ns,
                lease_duration_ns=lease_duration_ns,
            )
            if resource is None:
                return None
            renewed = current.model_copy(update={"resource_lease": resource})
            await self._resident_event(connection, renewed, "renewed", now_utc_ns)
            return renewed

    async def transition_resident_session(
        self,
        lease: ResidentLease,
        *,
        state: ResidentState,
        now_utc_ns: int,
        reason: JsonValue = None,
    ) -> ResidentLease | None:
        """Fence actor admission before closing; quarantined state remains occupied."""
        if now_utc_ns < 0:
            raise ValueError("Resident transition time must be nonnegative")
        allowed = {
            ResidentState.OPENING: {
                ResidentState.READY,
                ResidentState.CLOSING,
                ResidentState.QUARANTINED,
            },
            ResidentState.READY: {ResidentState.CLOSING, ResidentState.QUARANTINED},
            ResidentState.CLOSING: {ResidentState.QUARANTINED},
            ResidentState.QUARANTINED: {ResidentState.CLOSING},
        }
        if state not in {ResidentState.READY, ResidentState.CLOSING, ResidentState.QUARANTINED}:
            raise ValueError("Terminal resident states require guarded finalization")
        async with self._connections.writer() as connection:
            current = await self._owned_resident(connection, lease)
            if current is None:
                return None
            if current.state == state:
                return (
                    current
                    if state != ResidentState.READY
                    or await self.validate_resident_session(current, now_utc_ns=now_utc_ns)
                    else None
                )
            if state not in allowed.get(current.state, set()):
                return None
            if state == ResidentState.READY and not await self.validate_resident_session(
                current, now_utc_ns=now_utc_ns
            ):
                return None
            await connection.execute(
                "UPDATE resident_sessions SET state = ? WHERE id = ?",
                (state.value, lease.identifier),
            )
            updated = current.model_copy(update={"state": state})
            await self._resident_event(
                connection,
                updated,
                "state_changed",
                now_utc_ns,
                {"previous": current.state.value, "state": state.value, "reason": reason},
            )
            return updated

    async def _finish_resident_session(
        self,
        connection: AsyncConnection,
        lease: ResidentLease,
        *,
        state: ResidentState,
        now_utc_ns: int,
        reason: JsonValue,
        reclaimed: bool = False,
    ) -> bool:
        current = await self._owned_resident(connection, lease)
        if (
            current is None
            or current.active_stage_token is not None
            or current.state
            in {
                ResidentState.CLOSED,
                ResidentState.PARKED,
                ResidentState.LOST,
            }
        ):
            return False
        await connection.execute(
            "UPDATE resource_permits SET state = ? WHERE token = ? AND owner = ? AND state = 'held'",
            ("reclaimed" if reclaimed else "released", lease.token, lease.owner),
        )
        if await connection.changes() != 1:
            return False
        await scheduler_sql.physical_release(
            connection, lease.token, now_utc_ns, reclaimed=reclaimed
        )
        await connection.execute(
            "UPDATE resident_sessions SET state = ? WHERE id = ?", (state.value, lease.identifier)
        )
        await self._resident_event(connection, current, state.value, now_utc_ns, reason)
        return True

    async def finish_resident_session(
        self,
        lease: ResidentLease,
        *,
        state: ResidentState,
        now_utc_ns: int,
        reason: JsonValue = None,
    ) -> bool:
        """Finalize only after the caller proves actor/children are gone under its guard."""
        if now_utc_ns < 0 or state not in {
            ResidentState.CLOSED,
            ResidentState.PARKED,
            ResidentState.LOST,
        }:
            raise ValueError("Resident finalization requires a terminal state and valid time")
        async with self._connections.writer() as connection:
            return await self._finish_resident_session(
                connection, lease, state=state, now_utc_ns=now_utc_ns, reason=reason
            )

    async def expired_resident_sessions(
        self,
        *,
        scope: str,
        now_utc_ns: int,
    ) -> tuple[ResidentLease, ...]:
        if not scope or scope != scope.strip() or now_utc_ns < 0:
            raise ValueError("Resident expiry lookup requires a scope and valid time")
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """SELECT resident.id FROM resident_sessions AS resident
                JOIN resource_permits AS permit ON permit.token = resident.token
                WHERE permit.scope = ? AND permit.state = 'held' AND permit.expires_at_utc_ns <= ?
                  AND resident.state IN ('opening','ready','closing','quarantined')
                ORDER BY permit.expires_at_utc_ns, resident.id""",
                (scope, now_utc_ns),
            )
            identifiers = [_text(row[0]) for row in await cursor.fetchall()]
            leases: list[ResidentLease] = []
            for identifier in identifiers:
                lease = await self._resident_session(connection, identifier)
                assert lease is not None
                leases.append(lease)
            return tuple(leases)

    async def recover_resident_session(self, lease: ResidentLease, *, now_utc_ns: int) -> bool:
        """Caller must hold both resident and captured active-stage guards through commit."""
        if now_utc_ns < 0:
            raise ValueError("Resident recovery time must be nonnegative")
        async with self._connections.writer() as connection:
            current = await self._owned_resident(connection, lease)
            if (
                current is None
                or current.expires_at_utc_ns > now_utc_ns
                or current.active_stage_token != lease.active_stage_token
            ):
                return False
            if current.active_stage_token is not None:
                cursor = await connection.execute(
                    "SELECT lease_json FROM work_stages WHERE token = ? AND state = 'running'",
                    (current.active_stage_token,),
                )
                row = await cursor.fetchone()
                if row is None:
                    return False
                stage = StageLease.model_validate_json(_text(row[0]))
                if not await self.release_stage_execution(
                    stage, now_utc_ns=now_utc_ns, reason={"kind": "resident_guard_recovered"}
                ):
                    return False
                if stage.resource_lease is not None:
                    await self.release_resource_permit(
                        token=stage.token, owner=stage.resource_lease.owner
                    )
            return await self._finish_resident_session(
                connection,
                current,
                state=ResidentState.LOST,
                now_utc_ns=now_utc_ns,
                reason={"kind": "expired_resident_guards_recovered"},
                reclaimed=True,
            )

    async def rebind_resident_stage(
        self,
        stage_identifier: str,
        previous_resident_identifier: str,
        lease: ResidentLease,
        *,
        now_utc_ns: int,
    ) -> bool:
        """Explicitly resume a pending checkpoint using a fresh resident incarnation."""
        async with self._connections.writer() as connection:
            if not await self.validate_resident_session(lease, now_utc_ns=now_utc_ns):
                return False
            current = await self._owned_resident(connection, lease)
            assert current is not None
            previous = await self._resident_session(connection, previous_resident_identifier)
            if (
                previous is None
                or previous.state
                not in {ResidentState.CLOSED, ResidentState.PARKED, ResidentState.LOST}
                or previous.active_stage_token is not None
                or current.active_stage_token is not None
            ):
                return False
            cursor = await connection.execute(
                "SELECT definition_json, state FROM work_stages WHERE id = ?", (stage_identifier,)
            )
            row = await cursor.fetchone()
            if row is None or row[1] != "pending":
                return False
            definition = StageDefinition.model_validate_json(_text(row[0]))
            if definition.resident_session_identifier != previous_resident_identifier or any(
                (resident.definition.work_item_identifier, resident.definition.operation_identifier)
                != (definition.work_item_identifier, definition.operation_identifier)
                for resident in (previous, current)
            ):
                return False
            updated = definition.model_copy(
                update={"resident_session_identifier": lease.identifier}
            )
            await connection.execute(
                "UPDATE work_stages SET definition_json = ? WHERE id = ?",
                (updated.model_dump_json(), stage_identifier),
            )
            await self._append_stage_event(
                connection,
                stage_identifier,
                "resident_rebound",
                now_utc_ns,
                {
                    "previous_resident": previous_resident_identifier,
                    "resident": lease.identifier,
                    "generation": lease.generation,
                },
            )
            return True

    @staticmethod
    async def _resource_lease_from_row(
        connection: AsyncConnection, row: Sequence[apsw.SQLiteValue]
    ) -> ResourceLease:
        cursor = await connection.execute(
            "SELECT name, units FROM resource_allocations WHERE permit_token = ? ORDER BY name",
            (_text(row[0]),),
        )
        return ResourceLease(
            token=_text(row[0]),
            scope=_text(row[1]),
            owner=_text(row[2]),
            acquired_at_utc_ns=_integer(row[3]),
            expires_at_utc_ns=_integer(row[4]),
            allocations=tuple(
                ResourceRequest(name=_text(allocation[0]), units=_integer(allocation[1]))
                for allocation in await cursor.fetchall()
            ),
        )

    async def resource_occupancy(self, scope: str) -> dict[str, int]:
        """Advisory occupied units; only atomic admission may grant execution.

        Expired held permits remain occupied until physical guard recovery proves
        their executor stopped. This snapshot can delay dispatch, never grant it.
        """
        async with self._connections.reader() as connection:
            # The composite foreign key guarantees the allocation's scope.
            # Joining by the globally unique token alone keeps the inner seek
            # on its primary key instead of scanning all historical allocations
            # through the scope index once for each held permit.
            cursor = await connection.execute(
                """SELECT a.name,SUM(a.units)
                FROM resource_permits AS p INDEXED BY resource_permits_occupied
                CROSS JOIN resource_allocations AS a ON a.permit_token=p.token
                WHERE p.scope=? AND p.state='held' GROUP BY a.name""",
                (scope,),
            )
            return {_text(row[0]): _integer(row[1]) for row in await cursor.fetchall()}

    @staticmethod
    async def _configure_resource_budgets(
        connection: AsyncConnection, scope: str, capacities: dict[str, int]
    ) -> None:
        cursor = await connection.execute(
            "SELECT name, capacity FROM resource_budgets WHERE scope = ?", (scope,)
        )
        registered = {_text(row[0]): _integer(row[1]) for row in await cursor.fetchall()}
        changed = any(
            name in registered and registered[name] != capacity
            for name, capacity in capacities.items()
        )
        if changed:
            cursor = await connection.execute(
                "SELECT 1 FROM resource_permits WHERE scope = ? AND state = 'held' LIMIT 1",
                (scope,),
            )
            if await cursor.fetchone() is not None:
                raise ValueError("Resource capacities cannot change while permits occupy the scope")
        await connection.executemany(
            """
            INSERT INTO resource_budgets(scope, name, capacity) VALUES (?, ?, ?)
            ON CONFLICT(scope, name) DO UPDATE SET capacity = excluded.capacity
            WHERE resource_budgets.capacity != excluded.capacity
            """,
            [(scope, name, capacity) for name, capacity in sorted(capacities.items())],
        )

    async def try_acquire_resource_permit(
        self,
        *,
        scope: str,
        token: str,
        owner: str,
        requests: Sequence[ResourceRequest],
        capacities: dict[str, int],
        now_utc_ns: int,
        lease_duration_ns: int,
    ) -> ResourceLease | None:
        """Reserve every requested budget atomically, or reserve none.

        Capacity is coordinated only inside this database and the supplied scope.
        Expired held permits remain occupied until guarded reclamation proves that
        their execution owner can no longer consume the resource.
        """

        if lease_duration_ns <= 0:
            raise ValueError("Resource lease duration must be positive")
        lease = ResourceLease(
            scope=scope,
            token=token,
            owner=owner,
            allocations=tuple(sorted(requests, key=lambda request: request.name)),
            acquired_at_utc_ns=now_utc_ns,
            expires_at_utc_ns=now_utc_ns + lease_duration_ns,
        )
        for name, capacity in capacities.items():
            ResourceRequest(name=name, units=capacity)
        for request in lease.allocations:
            if request.name not in capacities:
                raise ValueError(f"No capacity configured for resource {request.name!r}")
            if request.units > capacities[request.name]:
                raise ValueError(f"Requested units exceed resource {request.name!r} capacity")
        async with self._connections.writer() as connection:
            cursor = await connection.execute(
                "SELECT 1 FROM resource_permits WHERE token = ?", (token,)
            )
            if await cursor.fetchone() is not None:
                raise ValueError("Resource permit tokens may never be reused")
            await self._configure_resource_budgets(connection, scope, capacities)
            for request in lease.allocations:
                cursor = await connection.execute(
                    """
                    SELECT COALESCE(SUM(allocation.units), 0)
                    FROM resource_permits AS permit INDEXED BY resource_permits_occupied
                    JOIN resource_allocations AS allocation
                      ON allocation.permit_token = permit.token
                    WHERE permit.scope = ? AND permit.state = 'held' AND allocation.name = ?
                    """,
                    (scope, request.name),
                )
                row = await cursor.fetchone()
                assert row is not None
                if _integer(row[0]) + request.units > capacities[request.name]:
                    return None
            await connection.execute(
                """
                INSERT INTO resource_permits(
                    token, scope, owner, state, acquired_at_utc_ns, expires_at_utc_ns
                ) VALUES (?, ?, ?, 'held', ?, ?)
                """,
                (token, scope, owner, now_utc_ns, lease.expires_at_utc_ns),
            )
            await connection.executemany(
                "INSERT INTO resource_allocations(permit_token, scope, name, units) VALUES (?, ?, ?, ?)",
                [(token, scope, request.name, request.units) for request in lease.allocations],
            )
        return lease

    async def renew_resource_permit(
        self,
        *,
        token: str,
        owner: str,
        now_utc_ns: int,
        lease_duration_ns: int,
    ) -> ResourceLease | None:
        """Renew a current owner; expiry never grants a stale owner a new lease."""

        if now_utc_ns < 0 or lease_duration_ns <= 0:
            raise ValueError("Resource renewal timestamps are invalid")
        async with self._connections.writer() as connection:
            await connection.execute(
                """
                UPDATE resource_permits
                SET expires_at_utc_ns = MAX(expires_at_utc_ns, ?)
                WHERE token = ? AND owner = ? AND state = 'held' AND expires_at_utc_ns > ?
                """,
                (now_utc_ns + lease_duration_ns, token, owner, now_utc_ns),
            )
            if await connection.changes() != 1:
                return None
            cursor = await connection.execute(
                """
                SELECT token, scope, owner, acquired_at_utc_ns, expires_at_utc_ns
                FROM resource_permits WHERE token = ?
                """,
                (token,),
            )
            row = await cursor.fetchone()
            assert row is not None
            return await self._resource_lease_from_row(connection, row)

    async def validate_resource_permit(self, *, token: str, owner: str, now_utc_ns: int) -> bool:
        """Check ownership, joining a caller's writer transaction for publication fencing."""

        if now_utc_ns < 0:
            raise ValueError("Resource validation time must be nonnegative")
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT 1 FROM resource_permits
                WHERE token = ? AND owner = ? AND state = 'held' AND expires_at_utc_ns > ?
                """,
                (token, owner, now_utc_ns),
            )
            return await cursor.fetchone() is not None

    async def release_resource_permit(self, *, token: str, owner: str) -> bool:
        """Release only this owner, including cleanup after its lease expired."""

        async with self._connections.writer() as connection:
            await connection.execute(
                """UPDATE resource_permits SET state = 'released'
                WHERE token = ? AND owner = ? AND state = 'held'
                  AND NOT EXISTS (SELECT 1 FROM resident_sessions WHERE resident_sessions.token = resource_permits.token)""",
                (token, owner),
            )
            changed = await connection.changes() == 1
            if changed:
                await scheduler_sql.physical_release(connection, token, time_ns())
            return changed

    async def expired_resource_permits(
        self, *, scope: str, now_utc_ns: int
    ) -> tuple[ResourceLease, ...]:
        """List occupied expired permits for independent execution-owner guard checks."""

        if now_utc_ns < 0:
            raise ValueError("Resource expiry time must be nonnegative")
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT token, scope, owner, acquired_at_utc_ns, expires_at_utc_ns
                FROM resource_permits
                WHERE scope = ? AND state = 'held' AND expires_at_utc_ns <= ?
                  AND NOT EXISTS (SELECT 1 FROM resident_sessions WHERE resident_sessions.token = resource_permits.token)
                ORDER BY expires_at_utc_ns, token
                """,
                (scope, now_utc_ns),
            )
            return tuple(
                [
                    await self._resource_lease_from_row(connection, row)
                    for row in await cursor.fetchall()
                ]
            )

    async def reclaim_expired_resource_permit(
        self, *, token: str, owner: str, now_utc_ns: int
    ) -> bool:
        """Reclaim after the caller acquires and holds the owner's OS execution guard.

        A timestamp or a guessed dead PID is insufficient. The manager must keep
        the guard until this transaction commits; expiry is rechecked here.
        """

        if now_utc_ns < 0:
            raise ValueError("Resource reclamation time must be nonnegative")
        async with self._connections.writer() as connection:
            await connection.execute(
                """
                UPDATE resource_permits SET state = 'reclaimed'
                WHERE token = ? AND owner = ? AND state = 'held' AND expires_at_utc_ns <= ?
                  AND NOT EXISTS (SELECT 1 FROM resident_sessions WHERE resident_sessions.token = resource_permits.token)
                """,
                (token, owner, now_utc_ns),
            )
            changed = await connection.changes() == 1
            if changed:
                await scheduler_sql.physical_release(connection, token, now_utc_ns, reclaimed=True)
            return changed

    @classmethod
    @asynccontextmanager
    async def managed(
        cls,
        path: Path,
        *,
        initialize: bool = False,
        reader_count: int = 4,
        prepare_schema: bool = True,
        resource_capacities: dict[str, int] | None = None,
    ) -> AsyncGenerator[Database]:
        """Open the database, normally migrating and validating before yielding it.

        ``prepare_schema=False`` is reserved for service boundaries that must complete
        their transport handshake before a potentially long migration.  Such callers
        must finish :meth:`migrate` and :meth:`validate_schema` before issuing queries.
        """

        if initialize and not prepare_schema:
            raise ValueError("Database initialization requires schema preparation")
        path_exists = path.is_file()
        if path_exists:
            await cls._preflight_path(path, allow_empty=initialize)
        elif not initialize:
            raise FileNotFoundError(f"Evidence database does not exist: {path}")

        async with DatabaseConnections.managed(
            path,
            create=not path_exists,
            reader_count=reader_count,
        ) as connections:
            database = cls(path=path, connections=connections)
            if prepare_schema:
                if path_exists:
                    await database.migrate()
                if initialize:
                    await database.initialize()
                else:
                    await database.validate_schema()
            from carl.io.resource_permits import resource_manager_context

            async with resource_manager_context(database, capacities=resource_capacities):
                yield database

    @staticmethod
    def _expected_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": str(DATABASE_SCHEMA.version),
            "schema_definition_sha256": _SCHEMA_DEFINITION_SHA256,
        }

    @staticmethod
    def _v20_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(_V20_DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": "20",
            "schema_definition_sha256": _V20_SCHEMA_DEFINITION_SHA256,
        }

    @staticmethod
    def _v22_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(_V22_DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": "22",
            "schema_definition_sha256": _V22_SCHEMA_DEFINITION_SHA256,
        }

    @staticmethod
    def _v21_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(_V21_DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": "21",
            "schema_definition_sha256": _V21_SCHEMA_DEFINITION_SHA256,
        }

    @staticmethod
    def _v19_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(_V19_DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": "19",
            "schema_definition_sha256": _V19_SCHEMA_DEFINITION_SHA256,
        }

    @staticmethod
    def _v18_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(_V18_DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": str(_V18_DATABASE_SCHEMA.version),
            "schema_definition_sha256": _V18_SCHEMA_DEFINITION_SHA256,
        }

    @staticmethod
    def _v17_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(_V17_DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": str(_V17_DATABASE_SCHEMA.version),
            "schema_definition_sha256": _V17_SCHEMA_DEFINITION_SHA256,
        }

    @staticmethod
    def _v2_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(_V2_DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": str(_V2_DATABASE_SCHEMA.version),
            "schema_definition_sha256": _V2_SCHEMA_DEFINITION_SHA256,
        }

    @staticmethod
    def _v3_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(_V3_DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": str(_V3_DATABASE_SCHEMA.version),
            "schema_definition_sha256": _V3_SCHEMA_DEFINITION_SHA256,
        }

    @staticmethod
    def _v4_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(_V4_DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": str(_V4_DATABASE_SCHEMA.version),
            "schema_definition_sha256": _V4_SCHEMA_DEFINITION_SHA256,
        }

    @staticmethod
    def _v5_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(_V5_DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": str(_V5_DATABASE_SCHEMA.version),
            "schema_definition_sha256": _V5_SCHEMA_DEFINITION_SHA256,
        }

    @staticmethod
    def _v6_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(_V6_DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": str(_V6_DATABASE_SCHEMA.version),
            "schema_definition_sha256": _V6_SCHEMA_DEFINITION_SHA256,
        }

    @staticmethod
    def _v8_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(_V8_DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": str(_V8_DATABASE_SCHEMA.version),
            "schema_definition_sha256": _V8_SCHEMA_DEFINITION_SHA256,
        }

    @staticmethod
    def _v9_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(_V9_DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": str(_V9_DATABASE_SCHEMA.version),
            "schema_definition_sha256": _V9_SCHEMA_DEFINITION_SHA256,
        }

    @staticmethod
    def _v10_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(_V10_DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": str(_V10_DATABASE_SCHEMA.version),
            "schema_definition_sha256": _V10_SCHEMA_DEFINITION_SHA256,
        }

    @staticmethod
    def _v11_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(_V11_DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": str(_V11_DATABASE_SCHEMA.version),
            "schema_definition_sha256": _V11_SCHEMA_DEFINITION_SHA256,
        }

    @staticmethod
    def _v12_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(_V12_DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": "12",
            "schema_definition_sha256": _V12_SCHEMA_DEFINITION_SHA256,
        }

    @staticmethod
    def _v13_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(_V13_DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": "13",
            "schema_definition_sha256": _V13_SCHEMA_DEFINITION_SHA256,
        }

    @staticmethod
    def _v14_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(_V14_DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": "14",
            "schema_definition_sha256": _V14_SCHEMA_DEFINITION_SHA256,
        }

    @staticmethod
    def _v15_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(_V15_DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": "15",
            "schema_definition_sha256": _V15_SCHEMA_DEFINITION_SHA256,
        }

    @staticmethod
    def _v16_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(_V16_DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": "16",
            "schema_definition_sha256": _V16_SCHEMA_DEFINITION_SHA256,
        }

    @staticmethod
    def _v7_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(_V7_DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": str(_V7_DATABASE_SCHEMA.version),
            "schema_definition_sha256": _V7_SCHEMA_DEFINITION_SHA256,
        }

    @staticmethod
    def _v1_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(_V1_DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": str(_V1_DATABASE_SCHEMA.version),
            "schema_definition_sha256": _V1_SCHEMA_DEFINITION_SHA256,
        }

    @classmethod
    async def _metadata_matches(cls, connection: AsyncConnection, expected: dict[str, str]) -> bool:
        for key, value in expected.items():
            cursor = await connection.execute(
                "SELECT value FROM schema_metadata WHERE key = ?", (key,)
            )
            row = await cursor.fetchone()
            if row is None or row[0] != value:
                return False
        return True

    @classmethod
    async def _preflight_path(cls, path: Path, *, allow_empty: bool) -> None:
        connection = await apsw.Connection.as_async(
            str(path),
            flags=apsw.SQLITE_OPEN_READONLY,
        )
        try:
            # Startup queries can overlap another connection's WAL cleanup.
            await connection.set_busy_timeout(DEFAULT_BUSY_TIMEOUT_MS)
            cursor = await connection.execute(
                """
                SELECT name
                FROM sqlite_schema
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
            )
            tables = tuple([_text(row[0]) async for row in cursor])
            if not tables:
                if allow_empty:
                    return
                raise RuntimeError("Database has no Carl schema identity")
            if "schema_metadata" not in tables:
                raise RuntimeError("Database has no Carl schema identity")
            if (
                not await cls._metadata_matches(connection, cls._expected_metadata())
                and not await cls._metadata_matches(connection, cls._v22_metadata())
                and not await cls._metadata_matches(connection, cls._v21_metadata())
                and not await cls._metadata_matches(connection, cls._v20_metadata())
                and not await cls._metadata_matches(connection, cls._v19_metadata())
                and not await cls._metadata_matches(connection, cls._v18_metadata())
                and not await cls._metadata_matches(connection, cls._v17_metadata())
                and not await cls._metadata_matches(connection, cls._v16_metadata())
                and not await cls._metadata_matches(connection, cls._v15_metadata())
                and not await cls._metadata_matches(connection, cls._v14_metadata())
                and not await cls._metadata_matches(connection, cls._v13_metadata())
                and not await cls._metadata_matches(connection, cls._v12_metadata())
                and not await cls._metadata_matches(connection, cls._v11_metadata())
                and not await cls._metadata_matches(connection, cls._v10_metadata())
                and not await cls._metadata_matches(connection, cls._v9_metadata())
                and not await cls._metadata_matches(connection, cls._v8_metadata())
                and not await cls._metadata_matches(connection, cls._v7_metadata())
                and not await cls._metadata_matches(connection, cls._v6_metadata())
                and not await cls._metadata_matches(connection, cls._v5_metadata())
                and not await cls._metadata_matches(connection, cls._v4_metadata())
                and not await cls._metadata_matches(connection, cls._v3_metadata())
                and not await cls._metadata_matches(connection, cls._v2_metadata())
                and not await cls._metadata_matches(connection, cls._v1_metadata())
            ):
                for key, expected in cls._expected_metadata().items():
                    cursor = await connection.execute(
                        "SELECT value FROM schema_metadata WHERE key = ?", (key,)
                    )
                    row = await cursor.fetchone()
                    if row is None or row[0] != expected:
                        raise RuntimeError(f"Unsupported database schema metadata: {key}")
                raise RuntimeError("Unsupported database schema metadata")
        finally:
            with anyio.fail_after(10, shield=True):
                await connection.aclose()

    async def _schema_metadata_exists(self, connection: AsyncConnection) -> bool:
        cursor = await connection.execute(
            "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'schema_metadata'"
        )
        return await cursor.fetchone() is not None

    async def _validate_schema(self, connection: AsyncConnection) -> None:
        if not await self._schema_metadata_exists(connection):
            raise RuntimeError("Database has no Carl schema identity")
        for key, expected in self._expected_metadata().items():
            cursor = await connection.execute(
                "SELECT value FROM schema_metadata WHERE key = ?",
                (key,),
            )
            row = await cursor.fetchone()
            if row is None or row[0] != expected:
                raise RuntimeError(f"Unsupported database schema metadata: {key}")

    async def validate_schema(self) -> None:
        async with self._connections.reader() as connection:
            await self._validate_schema(connection)

    async def migrate(self) -> None:
        """Upgrade exact known schemas without discarding evidence."""

        async with self._connections.writer() as connection:
            if not await self._schema_metadata_exists(connection):
                return
            if await self._metadata_matches(connection, self._expected_metadata()):
                return
            if await self._metadata_matches(connection, self._v1_metadata()):
                await connection.execute(
                    """
                    CREATE TABLE scheduling_constraint_retirements (
                        identifier_parts_json TEXT PRIMARY KEY CHECK (json_valid(identifier_parts_json)),
                        retired_at_utc_ns INTEGER NOT NULL CHECK (retired_at_utc_ns >= 0),
                        operation_id TEXT NOT NULL,
                        reason TEXT NOT NULL CHECK (length(reason) > 0),
                        FOREIGN KEY (identifier_parts_json)
                            REFERENCES scheduling_constraints(identifier_parts_json),
                        FOREIGN KEY (operation_id) REFERENCES operations(id)
                    ) STRICT
                    """
                )
                for key, value in self._v2_metadata().items():
                    await connection.execute(
                        "UPDATE schema_metadata SET value = ? WHERE key = ?", (value, key)
                    )
            if await self._metadata_matches(connection, self._v2_metadata()):
                await connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS external_artifacts (
                        object_id TEXT PRIMARY KEY,
                        sha256 TEXT NOT NULL CHECK (length(sha256) = 64),
                        size INTEGER NOT NULL CHECK (size >= 0),
                        media_type TEXT,
                        representation_json TEXT NOT NULL CHECK (json_valid(representation_json)),
                        locator TEXT NOT NULL CHECK (length(locator) > 0),
                        FOREIGN KEY (object_id) REFERENCES objects(id)
                    ) STRICT
                    """
                )
                for key, value in self._v3_metadata().items():
                    await connection.execute(
                        "UPDATE schema_metadata SET value = ? WHERE key = ?", (value, key)
                    )
            if await self._metadata_matches(
                connection, self._v3_metadata()
            ) or await self._metadata_matches(connection, self._v4_metadata()):
                for statement in (
                    "CREATE INDEX IF NOT EXISTS objects_kind ON objects(kind_parts_json)",
                    """
                CREATE INDEX IF NOT EXISTS objects_operation_kind
                ON objects(created_by_operation_id, kind_parts_json, id)
                """,
                    """
                CREATE INDEX IF NOT EXISTS records_gallery_observation
                ON records(
                    json_extract(value_json, '$.listing_observation_record_identifier'),
                    object_id
                )
                WHERE json_extract(
                    value_json, '$.listing_observation_record_identifier'
                ) IS NOT NULL
                """,
                    """
                CREATE INDEX IF NOT EXISTS records_saved_image_reference
                ON records(
                    json_extract(value_json, '$.image_reference_record_identifier'),
                    object_id
                )
                WHERE json_extract(value_json, '$.state') = 'saved'
                  AND json_extract(
                      value_json, '$.image_reference_record_identifier'
                  ) IS NOT NULL
                """,
                    """
                CREATE INDEX IF NOT EXISTS records_saved_image_rendition
                ON records(
                    json_extract(value_json, '$.original_url'),
                    json_extract(value_json, '$.source_photo_id'),
                    object_id
                )
                WHERE json_extract(value_json, '$.state') = 'saved'
                  AND json_extract(value_json, '$.original_url') IS NOT NULL
                """,
                    """
                CREATE INDEX IF NOT EXISTS operation_inputs_object_name_operation
                ON operation_inputs(object_id, name_parts_json, operation_id)
                """,
                    """
                CREATE INDEX IF NOT EXISTS work_items_analysis_lookup
                ON work_items(
                    kind_parts_json,
                    payload_schema_version,
                    json_extract(payload_json, '$.evidence_set_record_identifier'),
                    json_extract(payload_json, '$.product_guide_record_identifier'),
                    state,
                    created_at_utc_ns
                )
                """,
                    """
                CREATE INDEX IF NOT EXISTS records_listing_observation_listing
                ON records(json_extract(value_json, '$.listing_id'), object_id)
                WHERE json_extract(value_json, '$.listing_id') IS NOT NULL
                """,
                    """
                CREATE INDEX IF NOT EXISTS records_search_occurrence_run_position
                ON records(
                    json_extract(value_json, '$.search_run_identifier'),
                    json_extract(value_json, '$.page_ordinal'),
                    json_extract(value_json, '$.edge_index'),
                    json_extract(value_json, '$.listing_identifier'),
                    object_id
                )
                WHERE json_extract(value_json, '$.listing_identifier') IS NOT NULL
                  AND json_extract(value_json, '$.search_run_identifier') IS NOT NULL
                """,
                    """
                CREATE INDEX IF NOT EXISTS records_search_occurrence_listing
                ON records(json_extract(value_json, '$.listing_identifier'), object_id)
                WHERE json_extract(value_json, '$.listing_identifier') IS NOT NULL
                  AND json_extract(value_json, '$.search_run_identifier') IS NOT NULL
                """,
                ):
                    await connection.execute(statement)
                for key, value in self._v5_metadata().items():
                    await connection.execute(
                        "UPDATE schema_metadata SET value = ? WHERE key = ?", (value, key)
                    )
            if await self._metadata_matches(connection, self._v5_metadata()):
                for statement in (
                    """
                    CREATE TABLE IF NOT EXISTS review_listing_claims (
                        workspace_record_id TEXT NOT NULL,
                        listing_identifier TEXT NOT NULL CHECK (
                            length(listing_identifier) > 0
                            AND listing_identifier NOT GLOB '*[^0-9]*'
                        ),
                        batch_record_id TEXT NOT NULL,
                        claim_token TEXT NOT NULL CHECK (length(claim_token) > 0),
                        owner_identifier TEXT NOT NULL CHECK (length(owner_identifier) > 0),
                        acquired_at_utc_ns INTEGER NOT NULL CHECK (acquired_at_utc_ns >= 0),
                        lease_expires_at_utc_ns INTEGER NOT NULL CHECK (
                            lease_expires_at_utc_ns > acquired_at_utc_ns
                        ),
                        PRIMARY KEY (workspace_record_id, listing_identifier),
                        FOREIGN KEY (workspace_record_id) REFERENCES objects(id),
                        FOREIGN KEY (batch_record_id) REFERENCES objects(id)
                    ) STRICT
                    """,
                    """
                    CREATE INDEX IF NOT EXISTS review_listing_claims_token
                    ON review_listing_claims(
                        claim_token, owner_identifier, lease_expires_at_utc_ns
                    )
                    """,
                    """
                    CREATE INDEX IF NOT EXISTS review_listing_claims_batch
                    ON review_listing_claims(
                        batch_record_id, claim_token, lease_expires_at_utc_ns
                    )
                    """,
                    """
                    CREATE TABLE IF NOT EXISTS review_mutation_requests (
                        action TEXT NOT NULL CHECK (
                            action IN (
                                'acquire_batch', 'renew_claim', 'release_claim', 'record_reviews'
                            )
                        ),
                        request_identifier TEXT NOT NULL CHECK (
                            length(request_identifier) > 0
                        ),
                        request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
                        request_schema_version INTEGER NOT NULL CHECK (
                            request_schema_version >= 1
                        ),
                        response_schema_version INTEGER NOT NULL CHECK (
                            response_schema_version >= 1
                        ),
                        response_json TEXT NOT NULL CHECK (json_valid(response_json)),
                        operation_id TEXT NOT NULL,
                        created_at_utc_ns INTEGER NOT NULL CHECK (created_at_utc_ns >= 0),
                        PRIMARY KEY (action, request_identifier),
                        FOREIGN KEY (operation_id) REFERENCES operations(id)
                    ) STRICT
                    """,
                    """
                    CREATE INDEX IF NOT EXISTS review_mutation_requests_operation
                    ON review_mutation_requests(operation_id)
                    """,
                ):
                    await connection.execute(statement)
                for key, value in self._v6_metadata().items():
                    await connection.execute(
                        "UPDATE schema_metadata SET value = ? WHERE key = ?", (value, key)
                    )
            if await self._metadata_matches(connection, self._v6_metadata()):
                await connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS work_requests_requester
                    ON work_requests(
                        requester_identifier, requester_kind_parts_json, work_item_id
                    )
                    """
                )
                for key, value in self._v7_metadata().items():
                    await connection.execute(
                        "UPDATE schema_metadata SET value = ? WHERE key = ?", (value, key)
                    )
            if await self._metadata_matches(connection, self._v7_metadata()):
                await connection.execute(
                    "ALTER TABLE review_mutation_requests RENAME TO review_mutation_requests_v7"
                )
                await connection.execute(
                    """
                    CREATE TABLE review_mutation_requests (
                        action TEXT NOT NULL CHECK (
                            action IN (
                                'acquire_batch', 'renew_claim', 'release_claim',
                                'record_reviews', 'record_workspace_bulk_review'
                            )
                        ),
                        request_identifier TEXT NOT NULL CHECK (
                            length(request_identifier) > 0
                        ),
                        request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
                        request_schema_version INTEGER NOT NULL CHECK (
                            request_schema_version >= 1
                        ),
                        response_schema_version INTEGER NOT NULL CHECK (
                            response_schema_version >= 1
                        ),
                        response_json TEXT NOT NULL CHECK (json_valid(response_json)),
                        operation_id TEXT NOT NULL,
                        created_at_utc_ns INTEGER NOT NULL CHECK (created_at_utc_ns >= 0),
                        PRIMARY KEY (action, request_identifier),
                        FOREIGN KEY (operation_id) REFERENCES operations(id)
                    ) STRICT
                    """
                )
                await connection.execute(
                    """
                    INSERT INTO review_mutation_requests
                    SELECT * FROM review_mutation_requests_v7
                    """
                )
                await connection.execute("DROP TABLE review_mutation_requests_v7")
                await connection.execute(
                    """
                    CREATE INDEX review_mutation_requests_operation
                    ON review_mutation_requests(operation_id)
                    """
                )
                await connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS records_listing_review_workspace_listing
                    ON records(
                        json_extract(value_json, '$.workspace_record_identifier'),
                        json_extract(value_json, '$.listing_identifier'),
                        object_id
                    )
                    WHERE json_extract(
                              value_json, '$.workspace_record_identifier'
                          ) IS NOT NULL
                      AND json_extract(value_json, '$.listing_identifier') IS NOT NULL
                    """
                )
                for key, value in self._v8_metadata().items():
                    await connection.execute(
                        "UPDATE schema_metadata SET value = ? WHERE key = ?", (value, key)
                    )
            if await self._metadata_matches(connection, self._v8_metadata()):
                await connection.execute(
                    "ALTER TABLE review_listing_claims RENAME TO review_listing_claims_v8"
                )
                await connection.execute(
                    """
                    CREATE TABLE review_listing_claims (
                        workspace_record_id TEXT NOT NULL,
                        listing_identifier TEXT NOT NULL CHECK (
                            length(listing_identifier) > 0 AND (
                                listing_identifier NOT GLOB '*[^0-9]*'
                                OR (
                                    listing_identifier GLOB 'ebay:[0-9]*'
                                    AND length(substr(listing_identifier, 6)) BETWEEN 9 AND 15
                                    AND substr(listing_identifier, 6) NOT GLOB '*[^0-9]*'
                                )
                            )
                        ),
                        batch_record_id TEXT NOT NULL,
                        claim_token TEXT NOT NULL CHECK (length(claim_token) > 0),
                        owner_identifier TEXT NOT NULL CHECK (length(owner_identifier) > 0),
                        acquired_at_utc_ns INTEGER NOT NULL CHECK (acquired_at_utc_ns >= 0),
                        lease_expires_at_utc_ns INTEGER NOT NULL CHECK (
                            lease_expires_at_utc_ns > acquired_at_utc_ns
                        ),
                        PRIMARY KEY (workspace_record_id, listing_identifier),
                        FOREIGN KEY (workspace_record_id) REFERENCES objects(id),
                        FOREIGN KEY (batch_record_id) REFERENCES objects(id)
                    ) STRICT
                    """
                )
                await connection.execute(
                    "INSERT INTO review_listing_claims SELECT * FROM review_listing_claims_v8"
                )
                await connection.execute("DROP TABLE review_listing_claims_v8")
                await connection.execute(
                    """
                    CREATE INDEX review_listing_claims_token ON review_listing_claims(
                        claim_token, owner_identifier, lease_expires_at_utc_ns
                    )
                    """
                )
                await connection.execute(
                    """
                    CREATE INDEX review_listing_claims_batch ON review_listing_claims(
                        batch_record_id, claim_token, lease_expires_at_utc_ns
                    )
                    """
                )
                for key, value in self._v9_metadata().items():
                    await connection.execute(
                        "UPDATE schema_metadata SET value = ? WHERE key = ?", (value, key)
                    )
                await connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS records_marketplace_item_observation
                    ON records(json_extract(value_json, '$.item_identifier'),
                               json_extract(value_json, '$.observation_record_identifier'), object_id)
                    """
                )
                await connection.execute(
                    """
                    CREATE INDEX IF NOT EXISTS records_marketplace_source_run
                    ON records(json_extract(value_json, '$.search_run_record_identifier'), object_id)
                    """
                )
            if await self._metadata_matches(connection, self._v9_metadata()):
                await connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS network_connectivity (
                        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                        paused INTEGER NOT NULL CHECK (paused IN (0, 1)),
                        next_probe_utc_ns INTEGER NOT NULL CHECK (next_probe_utc_ns >= 0),
                        probe_token TEXT,
                        probe_expires_utc_ns INTEGER,
                        checked_at_utc_ns INTEGER,
                        result_json TEXT CHECK (result_json IS NULL OR json_valid(result_json))
                    ) STRICT
                    """
                )
                for key, value in self._v10_metadata().items():
                    await connection.execute(
                        "UPDATE schema_metadata SET value = ? WHERE key = ?", (value, key)
                    )
            if await self._metadata_matches(connection, self._v10_metadata()):
                await self._migrate_code_states(connection)
                for key, value in self._v11_metadata().items():
                    await connection.execute(
                        "UPDATE schema_metadata SET value = ? WHERE key = ?", (value, key)
                    )
            if await self._metadata_matches(connection, self._v11_metadata()):
                await self._migrate_saved_image_lookup(connection)
                for key, value in self._v12_metadata().items():
                    await connection.execute(
                        "UPDATE schema_metadata SET value = ? WHERE key = ?", (value, key)
                    )
            if await self._metadata_matches(connection, self._v12_metadata()):
                await self._migrate_work_cancellation(connection)
                for key, value in self._v13_metadata().items():
                    await connection.execute(
                        "UPDATE schema_metadata SET value = ? WHERE key = ?", (value, key)
                    )
            if await self._metadata_matches(connection, self._v13_metadata()):
                await self._apply_schema(connection)
                for key, value in self._v14_metadata().items():
                    await connection.execute(
                        "UPDATE schema_metadata SET value = ? WHERE key = ?", (value, key)
                    )
            if await self._metadata_matches(connection, self._v14_metadata()):
                await self._apply_schema(connection)
                for key, value in self._v18_metadata().items():
                    await connection.execute(
                        "UPDATE schema_metadata SET value = ? WHERE key = ?", (value, key)
                    )
            if await self._metadata_matches(connection, self._v15_metadata()):
                await self._apply_schema(connection)
                for key, value in self._v18_metadata().items():
                    await connection.execute(
                        "UPDATE schema_metadata SET value = ? WHERE key = ?", (value, key)
                    )
            if await self._metadata_matches(connection, self._v16_metadata()):
                await connection.execute(
                    "ALTER TABLE resident_request_charges RENAME TO resident_request_charges_v16"
                )
                await self._apply_schema(connection)
                await connection.execute(
                    "INSERT INTO resident_request_charges SELECT * FROM resident_request_charges_v16"
                )
                await connection.execute("DROP TABLE resident_request_charges_v16")
                for key, value in self._v18_metadata().items():
                    await connection.execute(
                        "UPDATE schema_metadata SET value = ? WHERE key = ?", (value, key)
                    )
            if await self._metadata_matches(connection, self._v17_metadata()):
                await self._apply_schema(connection)
                for key, value in self._v18_metadata().items():
                    await connection.execute(
                        "UPDATE schema_metadata SET value = ? WHERE key = ?", (value, key)
                    )
            if await self._metadata_matches(connection, self._v18_metadata()):
                await self._apply_schema(connection)
                await self._migrate_acquisition_resource_identities(connection)
                for key, value in self._v19_metadata().items():
                    await connection.execute(
                        "UPDATE schema_metadata SET value = ? WHERE key = ?", (value, key)
                    )
            if await self._metadata_matches(connection, self._v19_metadata()):
                await self._apply_schema(connection)
                await connection.execute(
                    "INSERT OR IGNORE INTO fair_unowned SELECT id FROM work_items WHERE NOT EXISTS(SELECT 1 FROM work_scheduling WHERE work_item_id=work_items.id)"
                )
                await self._backfill_work_scheduling(connection)
                for key, value in self._v20_metadata().items():
                    await connection.execute(
                        "UPDATE schema_metadata SET value = ? WHERE key = ?", (value, key)
                    )
            if await self._metadata_matches(connection, self._v20_metadata()):
                triggers = await (
                    await connection.execute(
                        "SELECT name FROM sqlite_schema WHERE type='trigger' AND name GLOB 'fair_refresh_*'"
                    )
                ).fetchall()
                for trigger in triggers:
                    name = _text(trigger[0]).replace('"', '""')
                    await connection.execute(f'DROP TRIGGER "{name}"')
                await self._apply_schema(connection)
                after = 0
                while True:
                    requests = await (
                        await connection.execute(
                            "SELECT rowid,id,requested_at_utc_ns FROM work_requests WHERE rowid>? ORDER BY rowid LIMIT 128",
                            (after,),
                        )
                    ).fetchall()
                    if not requests:
                        break
                    for request in requests:
                        await progress_sql.publish_consumer_edge(
                            connection, _text(request[1]), _integer(request[2])
                        )
                    after = _integer(requests[-1][0])
                for key, value in self._v21_metadata().items():
                    await connection.execute(
                        "UPDATE schema_metadata SET value=? WHERE key=?", (value, key)
                    )
            if await self._metadata_matches(connection, self._v21_metadata()):
                await self._apply_schema(connection)
                for key, value in self._v22_metadata().items():
                    await connection.execute(
                        "UPDATE schema_metadata SET value=? WHERE key=?", (value, key)
                    )
            if await self._metadata_matches(connection, self._v22_metadata()):
                await connection.execute(_HISTORY_QUERY_INDEXES)
                for key, value in self._expected_metadata().items():
                    await connection.execute(
                        "UPDATE schema_metadata SET value=? WHERE key=?", (value, key)
                    )
            if not await self._metadata_matches(connection, self._expected_metadata()):
                raise RuntimeError("Unsupported database schema metadata")
            await self._validate_schema(connection)

    @staticmethod
    async def _apply_schema(connection: AsyncConnection) -> None:
        """Prepare older work tables before applying indexes from the current schema."""

        legacy_projection_added = False
        for table, column, definition in (
            ("work_stages", "wait_entered_at_utc_ns", "INTEGER"),
            (
                "work_stages",
                "progress_waiting",
                "INTEGER NOT NULL DEFAULT 0 CHECK(progress_waiting IN (0,1))",
            ),
            ("fair_workspace_service", "ready_count", "INTEGER NOT NULL DEFAULT 0"),
            ("fair_root_service", "ready_count", "INTEGER NOT NULL DEFAULT 0"),
            ("fair_ready", "work_eligible_at_utc_ns", "INTEGER NOT NULL DEFAULT 0"),
            ("fair_ready", "legacy_lease_expires_at_utc_ns", "INTEGER"),
        ):
            existing = {
                _text(row[1])
                for row in await (
                    await connection.execute(f"PRAGMA table_info({table})")
                ).fetchall()
            }
            if existing and column not in existing:
                await connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
                if table == "fair_ready" and column == "work_eligible_at_utc_ns":
                    await connection.execute(
                        "UPDATE fair_ready SET work_eligible_at_utc_ns=coalesce((SELECT eligible_at_utc_ns FROM work_items WHERE id=fair_ready.work_item_id),0)"
                    )
                if column == "legacy_lease_expires_at_utc_ns":
                    legacy_projection_added = True

        cursor = await connection.execute("PRAGMA table_info(work_items)")
        columns = {_text(row[1]) for row in await cursor.fetchall()}
        if columns and "acquisition_resource_identity_json" not in columns:
            await connection.execute(
                "ALTER TABLE work_items ADD COLUMN acquisition_resource_identity_json TEXT "
                "CHECK (acquisition_resource_identity_json IS NULL OR "
                "json_valid(acquisition_resource_identity_json))"
            )
        request_columns = {
            _text(row[1])
            for row in await (
                await connection.execute("PRAGMA table_info(work_requests)")
            ).fetchall()
        }
        if request_columns and "scheduling_json" not in request_columns:
            await connection.execute(
                "ALTER TABLE work_requests ADD COLUMN scheduling_json TEXT CHECK(scheduling_json IS NULL OR json_valid(scheduling_json))"
            )
        if request_columns and "parent_work_identifier" not in request_columns:
            await connection.execute(
                "ALTER TABLE work_requests ADD COLUMN parent_work_identifier TEXT REFERENCES work_items(id)"
            )
        await connection.execute(_SCHEMA)
        await scheduler_sql.initialize(connection)
        await fair_sql.initialize_service_counts(connection)
        if legacy_projection_added:
            await fair_sql.initialize_legacy_lease_projection(connection)

    @staticmethod
    async def _migrate_acquisition_resource_identities(connection: AsyncConnection) -> None:
        """Backfill all acquisition history without retaining the full queue's payloads."""

        after: str | None = None
        while True:
            cursor = await connection.execute(
                """
                SELECT id, kind_parts_json,
                       CASE WHEN kind_parts_json IN (
                         '["carl","ebay","collect","item"]',
                         '["carl","ebay","collect","description"]',
                         '["carl","ebay","collect","image"]',
                         '["carl","facebook","work","collect_item"]',
                         '["carl","facebook","work","collect_image"]'
                       ) THEN payload_json END
                FROM work_items
                """
                + ("WHERE id > ? " if after is not None else "")
                + "ORDER BY id LIMIT 128",
                (after,) if after is not None else (),
            )
            rows = await cursor.fetchall()
            if not rows:
                return
            for row in rows:
                if row[2] is not None:
                    identity = acquisition_resource_identity_json(
                        tuple(cast(list[str], decode_json(_text(row[1])))),
                        decode_json(_text(row[2])),
                    )
                    await connection.execute(
                        "UPDATE work_items SET acquisition_resource_identity_json=? WHERE id=?",
                        (identity, _text(row[0])),
                    )
            after = _text(rows[-1][0])

    async def _migrate_work_cancellation(self, connection: AsyncConnection) -> None:
        """Rebuild mutable scheduling tables, retaining all identifiers and evidence."""
        await connection.execute("PRAGMA defer_foreign_keys = ON")
        for name in ("work_items", "work_events"):
            start = _SCHEMA.index(f"CREATE TABLE IF NOT EXISTS {name} (")
            end = _SCHEMA.index(") STRICT;", start) + len(") STRICT;")
            definition = _SCHEMA[start:end].replace(
                f"CREATE TABLE IF NOT EXISTS {name} (", f"CREATE TABLE {name}_new (", 1
            )
            await connection.execute(definition)
            if name == "work_items":
                columns = (
                    "id,kind_parts_json,payload_schema_version,payload_json,"
                    "deduplication_identity_json,state,priority,eligible_at_utc_ns,"
                    "created_at_utc_ns,lease_token,lease_owner,lease_expires_at_utc_ns,"
                    "attempt,result_json,error_json"
                )
                await connection.execute(
                    f"INSERT INTO {name}_new({columns}) SELECT {columns} FROM {name}"
                )
            else:
                await connection.execute(f"INSERT INTO {name}_new SELECT * FROM {name}")
            await connection.execute(f"DROP TABLE {name}")
            await connection.execute(f"ALTER TABLE {name}_new RENAME TO {name}")
        # Recreate indexes and the new request table from the authoritative schema.
        await self._apply_schema(connection)
        await self._publish_work_request_dependencies(connection)
        cursor = await connection.execute("PRAGMA foreign_key_check")
        if await cursor.fetchone() is not None:
            raise RuntimeError("Cancellation migration violated a foreign key")
        await connection.execute("PRAGMA defer_foreign_keys = OFF")

    async def _migrate_saved_image_lookup(self, connection: AsyncConnection) -> None:
        """Derive indexed reuse keys once, without modifying retained image evidence."""

        await connection.execute(
            """
            CREATE TABLE IF NOT EXISTS saved_image_lookup (
                marketplace TEXT NOT NULL CHECK (marketplace IN ('facebook', 'ebay')),
                lookup_kind TEXT NOT NULL CHECK (lookup_kind IN ('url', 'photo_id', 'path')),
                lookup_value TEXT NOT NULL,
                result_object_id TEXT NOT NULL,
                PRIMARY KEY (marketplace, lookup_kind, lookup_value, result_object_id),
                FOREIGN KEY (result_object_id) REFERENCES objects(id)
            ) STRICT, WITHOUT ROWID
            """
        )
        await connection.execute(
            """
            CREATE INDEX IF NOT EXISTS records_ebay_gallery_observation
            ON records(json_extract(value_json, '$.observation_record_identifier'), object_id)
            WHERE json_extract(value_json, '$.observation_record_identifier') IS NOT NULL
            """
        )
        cursor = await connection.execute(
            """
            SELECT object.id, object.kind_parts_json, record.value_json
            FROM objects AS object
            JOIN records AS record ON record.object_id = object.id
            WHERE object.kind_parts_json IN (
                '["carl","facebook","image_result"]', '["carl","ebay","image_result"]'
            ) AND json_extract(record.value_json, '$.state') = 'saved'
            ORDER BY object.rowid
            """
        )
        # Publish keys in bounded batches, rather than retaining all historical
        # image-result JSON or derived URLs in memory during the migration.
        rows: list[tuple[str, str, str, str]] = []
        async for row in cursor:
            rows.extend(
                self._saved_image_lookup_rows(
                    _text(row[0]),
                    tuple(cast(list[str], decode_json(_text(row[1])))),
                    decode_json(_text(row[2])),
                )
            )
            if len(rows) >= 1000:
                await self._publish_saved_image_lookup(connection, rows)
                rows.clear()
        await self._publish_saved_image_lookup(connection, rows)

    @staticmethod
    def _saved_image_lookup_rows(
        identifier: str, kind: tuple[str, ...], value: JsonValue
    ) -> tuple[tuple[str, str, str, str], ...]:
        if (
            kind not in {("carl", "facebook", "image_result"), ("carl", "ebay", "image_result")}
            or not isinstance(value, dict)
            or value.get("state") != "saved"
        ):
            return ()
        marketplace = kind[1]
        url = value.get("original_url" if marketplace == "facebook" else "url")
        photo_id = value.get("source_photo_id") if marketplace == "facebook" else None
        rows: list[tuple[str, str, str, str]] = []
        if isinstance(photo_id, str):
            rows.append((marketplace, "photo_id", photo_id, identifier))
        if isinstance(url, str):
            rows.append((marketplace, "url", url, identifier))
            identity = (
                facebook_image_identity(url, photo_id if isinstance(photo_id, str) else None)
                if marketplace == "facebook"
                else ebay_image_identity(url)
            )
            if identity[2] == "rendition":
                rows.append(
                    (
                        marketplace,
                        "path",
                        identity[4 if marketplace == "facebook" else 3],
                        identifier,
                    )
                )
        return tuple(rows)

    @staticmethod
    async def _publish_saved_image_lookup(
        connection: AsyncConnection, rows: Sequence[tuple[str, str, str, str]]
    ) -> None:
        if rows:
            await connection.executemany(
                "INSERT OR IGNORE INTO saved_image_lookup VALUES (?, ?, ?, ?)", rows
            )

    async def _migrate_code_states(self, connection: AsyncConnection) -> None:
        """Normalize retained operation provenance without changing output identities."""

        await connection.execute(
            """
            CREATE TABLE IF NOT EXISTS code_states (
                id INTEGER PRIMARY KEY,
                commit_hash BLOB CHECK (commit_hash IS NULL OR length(commit_hash) IN (20, 32)),
                worktree_state TEXT NOT NULL CHECK (worktree_state IN ('clean', 'dirty', 'unknown'))
            ) STRICT;
            CREATE UNIQUE INDEX IF NOT EXISTS code_states_identity
            ON code_states(coalesce(commit_hash, X''), worktree_state);
            """
        )
        cursor = await connection.execute("PRAGMA table_info(operations)")
        if not any(row[1] == "code_state_id" for row in await cursor.fetchall()):
            await connection.execute(
                "ALTER TABLE operations ADD COLUMN code_state_id INTEGER REFERENCES code_states(id)"
            )
        await connection.execute(
            "CREATE INDEX IF NOT EXISTS operations_code_state ON operations(code_state_id)"
        )
        cursor = await connection.execute(
            """
            SELECT DISTINCT json_extract(code_provenance_json, '$.commit_hash'),
                   coalesce(json_extract(code_provenance_json, '$.worktree_state'), 'unknown')
            FROM operations WHERE code_state_id IS NULL
            """
        )
        for commit_hash, worktree_state in await cursor.fetchall():
            code_state_id = await self._intern_code_state(connection, commit_hash, worktree_state)
            await connection.execute(
                """
                UPDATE operations SET code_state_id = ?,
                    code_provenance_json = json_remove(
                        code_provenance_json, '$.commit_hash', '$.worktree_state'
                    )
                WHERE code_state_id IS NULL
                  AND json_extract(code_provenance_json, '$.commit_hash') IS ?
                  AND coalesce(json_extract(code_provenance_json, '$.worktree_state'), 'unknown') = ?
                """,
                (code_state_id, commit_hash, worktree_state),
            )

    @staticmethod
    async def _intern_code_state(
        connection: AsyncConnection,
        commit_hash: apsw.SQLiteValue,
        worktree_state: apsw.SQLiteValue,
    ) -> int:
        if worktree_state not in {"clean", "dirty", "unknown"}:
            raise ValueError("Invalid code provenance worktree state")
        binary_hash: bytes | None = None
        if commit_hash is not None:
            if (
                not isinstance(commit_hash, str)
                or len(commit_hash) not in (40, 64)
                or any(character not in "0123456789abcdefABCDEF" for character in commit_hash)
            ):
                raise ValueError("Code provenance commit hash must be a full Git hexadecimal hash")
            binary_hash = bytes.fromhex(commit_hash)
        await connection.execute(
            "INSERT INTO code_states(commit_hash, worktree_state) VALUES (?, ?) ON CONFLICT DO NOTHING",
            (binary_hash, worktree_state),
        )
        cursor = await connection.execute(
            """
            SELECT id FROM code_states
            WHERE coalesce(commit_hash, X'') = coalesce(?, X'') AND worktree_state = ?
            """,
            (binary_hash, worktree_state),
        )
        row = await cursor.fetchone()
        if row is None:
            raise AssertionError("Code state was not retained")
        return _integer(row[0])

    async def initialize(self) -> None:
        async with self._connections.writer() as connection:
            cursor = await connection.execute(
                """
                SELECT name
                FROM sqlite_schema
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
            )
            table_names = tuple([_text(row[0]) async for row in cursor])
            if table_names:
                await self._validate_schema(connection)

            await self._apply_schema(connection)
            for key, expected in self._expected_metadata().items():
                await connection.execute(
                    "INSERT OR IGNORE INTO schema_metadata(key, value) VALUES (?, ?)",
                    (key, expected),
                )
            await self._validate_schema(connection)

    async def register_constraint(
        self,
        constraint: Constraint,
        *,
        registered_at_utc_ns: int,
    ) -> None:
        """Register one immutable scheduler constraint.

        Re-registering the same identity and definition is idempotent. Reusing
        an identity for different behavior is rejected so retained permit
        decisions remain interpretable.
        """

        async with self._connections.writer() as connection:
            await self._register_constraint(
                connection, constraint=constraint, registered_at_utc_ns=registered_at_utc_ns
            )

    async def _register_constraint(
        self,
        connection: AsyncConnection,
        *,
        constraint: Constraint,
        registered_at_utc_ns: int,
    ) -> None:
        identity_json = _json(list(constraint.identifier))
        definition_json = _json(constraint.model_dump(mode="json"))
        cursor = await connection.execute(
            "SELECT definition_json FROM scheduling_constraints WHERE identifier_parts_json = ?",
            (identity_json,),
        )
        row = await cursor.fetchone()
        if row is not None:
            if row[0] != definition_json:
                raise ValueError("A scheduler constraint identity cannot be redefined")
            return
        await connection.execute(
            """
            INSERT INTO scheduling_constraints(
                identifier_parts_json, constraint_kind, subject_kind, scope_kind,
                scope_identity_json, schema_version, definition_json,
                registered_at_utc_ns
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                identity_json,
                constraint.kind.value,
                constraint.subject_kind.value,
                constraint.scope.kind.value,
                _json(list(constraint.scope.identity)),
                constraint.schema_version,
                definition_json,
                registered_at_utc_ns,
            ),
        )

    @staticmethod
    def _facebook_route_capacity_range(family: tuple[str, ...]) -> tuple[str, str]:
        if family not in FACEBOOK_ROUTE_CAPACITY_FAMILIES:
            raise ValueError("Unknown Facebook route capacity family")
        prefix = _json(list(family))[:-1]
        # The comma extends this exact JSON-array prefix. Its next codepoint
        # bounds all such extensions without scanning neighboring families.
        return prefix + ",", prefix + "-"

    async def facebook_route_capacity_high_water(self, family: tuple[str, ...]) -> str | None:
        """Freeze one indexed metadata range before a bounded retirement pass."""
        lower, upper = self._facebook_route_capacity_range(family)
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT max(identifier_parts_json) FROM scheduling_constraints "
                "WHERE identifier_parts_json >= ? AND identifier_parts_json < ?",
                (lower, upper),
            )
            row = await cursor.fetchone()
        return None if row is None or row[0] is None else _text(row[0])

    async def facebook_route_capacity_constraint_page(
        self,
        *,
        family: tuple[str, ...],
        through: str,
        after: str | None = None,
        limit: int = 128,
    ) -> tuple[tuple[str, Constraint, bool], ...]:
        """Read at most 128 definitions, advancing over retired/custom history too.

        Cursors are the exact stored JSON keys, not decoded route aliases. The
        retirement join does not filter rows, so a page cannot walk an unbounded
        retired prefix while searching for a live candidate.
        """
        if isinstance(limit, bool) or not 1 <= limit <= 128:
            raise ValueError("A route capacity page must contain 1 to 128 rows")
        lower, upper = self._facebook_route_capacity_range(family)
        if not lower <= through < upper or (after is not None and not lower <= after <= through):
            raise ValueError("Route capacity cursor is outside its family range")
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT c.identifier_parts_json,c.definition_json,r.identifier_parts_json IS NOT NULL
                FROM scheduling_constraints c
                LEFT JOIN scheduling_constraint_retirements r
                  ON r.identifier_parts_json=c.identifier_parts_json
                WHERE c.identifier_parts_json > ? AND c.identifier_parts_json <= ?
                ORDER BY c.identifier_parts_json LIMIT ?
                """,
                (lower if after is None else after, through, limit),
            )
            rows = await cursor.fetchall()
        return tuple(
            (_text(row[0]), CONSTRAINT_ADAPTER.validate_json(_text(row[1])), bool(row[2]))
            for row in rows
        )

    async def legacy_facebook_search_constraint_identifiers(self) -> tuple[tuple[str, ...], ...]:
        """Find persisted superseded requested and effective search limits.

        Route names can outlive their configuration, so discover old definitions
        rather than guessing names from the current route configuration.
        """

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT definition_json FROM scheduling_constraints
                WHERE json_extract(identifier_parts_json, '$[0]') = 'carl'
                  AND json_extract(identifier_parts_json, '$[1]') = 'facebook'
                  AND json_extract(identifier_parts_json, '$[2]') = 'search'
                  AND (
                      json_extract(identifier_parts_json, '$[4]') = 'v1'
                      OR (
                          json_extract(identifier_parts_json, '$[4]') = 'v2'
                          AND json_extract(identifier_parts_json, '$[5]') = 'decodo'
                      )
                  )
                  AND json_extract(identifier_parts_json, '$[3]') IN
                      ('route_concurrency', 'effective_route_concurrency')
                ORDER BY identifier_parts_json
                """
            )
            rows = await cursor.fetchall()
        return tuple(CONSTRAINT_ADAPTER.validate_json(_text(row[0])).identifier for row in rows)

    async def legacy_facebook_request_budget_routes(self) -> tuple[tuple[str, ...], ...]:
        """Find persisted route-wide v2 budgets, including routes with no queued searches."""
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT scope_identity_json FROM scheduling_constraints
                WHERE scope_kind='network_path' AND subject_kind='network_activity'
                  AND constraint_kind='sliding_window_rate'
                  AND json_extract(identifier_parts_json, '$[0]')='carl'
                  AND json_extract(identifier_parts_json, '$[1]')='facebook'
                  AND json_extract(identifier_parts_json, '$[2]')='network_path'
                  AND json_extract(identifier_parts_json, '$[3]')='network_activity_rate'
                  AND json_extract(identifier_parts_json, '$[4]')='v2'
                ORDER BY scope_identity_json
                """
            )
            return tuple(
                tuple(str(part) for part in cast(list[JsonValue], decode_json(_text(row[0]))))
                for row in await cursor.fetchall()
            )

    async def facebook_search_work_routes(self) -> tuple[tuple[str, ...], ...]:
        """Discover still-active requested/effective routes, including retired aliases."""
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT DISTINCT scope.scope_identity_json
                FROM work_items AS work JOIN work_scopes AS scope ON scope.work_item_id=work.id
                WHERE work.kind_parts_json=? AND work.state IN ('pending','leased','cancelling')
                  AND scope.scope_kind='network_path'
                  AND json_extract(scope.scope_identity_json,'$[0]') IN
                      ('search_acquisition','effective_search_acquisition')
                ORDER BY scope.scope_identity_json
                """,
                (_json(list(COLLECT_SEARCH_WORK_KIND)),),
            )
            routes = {
                tuple(str(part) for part in cast(list[JsonValue], decode_json(_text(row[0])))[1:])
                for row in await cursor.fetchall()
            }
        return tuple(sorted(routes))

    async def supersede_constraints(
        self,
        *,
        retired_identifiers: tuple[tuple[str, ...], ...],
        replacements: tuple[Constraint, ...],
        operation_identifier: str,
        at_utc_ns: int,
        reason: str,
        retired_definitions: tuple[Constraint, ...] = (),
    ) -> None:
        """Atomically retire known constraints and register immutable replacements.

        Retired definitions and rate reservations remain available for audit.
        Repeating a retirement leaves its original operation, reason, and timestamp
        intact. A missing old constraint needs no retirement row. Definitions can
        restrict retirement to exact shipped policies, preserving customized
        policies even when they reuse a known identifier.
        """

        if at_utc_ns < 0 or not reason or not operation_identifier:
            raise ValueError("A constraint supersession needs time, reason, and operation")
        if len(set(retired_identifiers)) != len(retired_identifiers):
            raise ValueError("Duplicate retired constraint identifier")
        replacement_identifiers = [constraint.identifier for constraint in replacements]
        if len(set(replacement_identifiers)) != len(replacement_identifiers):
            raise ValueError("Duplicate replacement constraint identifier")
        expected_definitions: dict[tuple[str, ...], set[str]] = {}
        for constraint in retired_definitions:
            expected_definitions.setdefault(constraint.identifier, set()).add(
                _json(constraint.model_dump(mode="json"))
            )
        retirement_identifiers = tuple(dict.fromkeys((*retired_identifiers, *expected_definitions)))
        if set(retirement_identifiers) & set(replacement_identifiers):
            raise ValueError("A retired constraint cannot also be a replacement")

        async with self._connections.writer() as connection:
            for constraint in replacements:
                await self._register_constraint(
                    connection, constraint=constraint, registered_at_utc_ns=at_utc_ns
                )
                cursor = await connection.execute(
                    """
                    SELECT 1 FROM scheduling_constraint_retirements
                    WHERE identifier_parts_json = ?
                    """,
                    (_json(list(constraint.identifier)),),
                )
                if await cursor.fetchone() is not None:
                    raise ValueError("A retired constraint cannot be reactivated")
            for identifier in retirement_identifiers:
                identity_json = _json(list(identifier))
                cursor = await connection.execute(
                    """
                    SELECT 1 FROM scheduling_constraint_retirements
                    WHERE identifier_parts_json = ?
                    """,
                    (identity_json,),
                )
                retired = await cursor.fetchone()
                if retired is not None:
                    continue
                cursor = await connection.execute(
                    "SELECT definition_json FROM scheduling_constraints WHERE identifier_parts_json = ?",
                    (identity_json,),
                )
                row = await cursor.fetchone()
                if row is None:
                    continue
                if (
                    identifier not in retired_identifiers
                    and _text(row[0]) not in expected_definitions[identifier]
                ):
                    continue
                await connection.execute(
                    """
                    INSERT INTO scheduling_constraint_retirements(
                        identifier_parts_json, retired_at_utc_ns, operation_id, reason
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (identity_json, at_utc_ns, operation_identifier, reason),
                )

    async def _constraints_for_work(
        self, connection: AsyncConnection, work_item_identifier: str
    ) -> tuple[Constraint, ...]:
        cursor = await connection.execute(
            """
            SELECT DISTINCT scheduling_constraints.definition_json
            FROM scheduling_constraints
            JOIN work_scopes
              ON work_scopes.scope_kind = scheduling_constraints.scope_kind
             AND work_scopes.scope_identity_json = scheduling_constraints.scope_identity_json
            WHERE work_scopes.work_item_id = ?
              AND scheduling_constraints.subject_kind = 'work_item'
              AND NOT EXISTS (
                  SELECT 1 FROM scheduling_constraint_retirements
                  WHERE scheduling_constraint_retirements.identifier_parts_json =
                        scheduling_constraints.identifier_parts_json
              )
            ORDER BY scheduling_constraints.identifier_parts_json
            """,
            (work_item_identifier,),
        )
        rows = await cursor.fetchall()
        return tuple(CONSTRAINT_ADAPTER.validate_json(_text(row[0])) for row in rows)

    async def _constraints_for_network_activity(
        self,
        connection: AsyncConnection,
        network_activity_identifier: str,
    ) -> tuple[Constraint, ...]:
        cursor = await connection.execute(
            """
            SELECT DISTINCT scheduling_constraints.definition_json
            FROM scheduling_constraints
            JOIN network_activity_scopes
              ON network_activity_scopes.scope_kind = scheduling_constraints.scope_kind
             AND network_activity_scopes.scope_identity_json =
                 scheduling_constraints.scope_identity_json
            WHERE network_activity_scopes.network_activity_id = ?
              AND scheduling_constraints.subject_kind = 'network_activity'
              AND NOT EXISTS (
                  SELECT 1 FROM scheduling_constraint_retirements
                  WHERE scheduling_constraint_retirements.identifier_parts_json =
                        scheduling_constraints.identifier_parts_json
              )
            ORDER BY scheduling_constraints.identifier_parts_json
            """,
            (network_activity_identifier,),
        )
        rows = await cursor.fetchall()
        return tuple(CONSTRAINT_ADAPTER.validate_json(_text(row[0])) for row in rows)

    async def _append_network_activity_event(
        self,
        connection: AsyncConnection,
        *,
        event_identifier: str,
        network_activity_identifier: str,
        event_kind: NetworkActivityEventKind,
        recorded_at_utc_ns: int,
        data: JsonValue,
    ) -> None:
        await connection.execute(
            """
            INSERT INTO network_activity_events(
                id, network_activity_id, event_kind, recorded_at_utc_ns, data_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                event_identifier,
                network_activity_identifier,
                event_kind.value,
                recorded_at_utc_ns,
                _json(data),
            ),
        )

    async def create_network_activity(
        self,
        definition: NetworkActivityDefinition,
        *,
        created_at_utc_ns: int,
        event_identifier: str,
        sample_uniform_holdoff_ns: Callable[[int, int], int],
    ) -> int:
        """Persist an activity and sample each applicable holdoff exactly once."""

        async with self._connections.writer() as connection:
            cursor = await connection.execute(
                "SELECT state FROM operations WHERE id = ?",
                (definition.operation_identifier,),
            )
            operation = await cursor.fetchone()
            if operation is None or operation[0] != "started":
                raise RuntimeError("Only a started operation can own network activity")
            await connection.execute(
                """
                INSERT INTO network_activities(
                    id, kind_parts_json, operation_id, network_session_identifier,
                    ordinal, attempt, state, eligible_at_utc_ns, created_at_utc_ns
                ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                """,
                (
                    definition.identifier,
                    _json(list(definition.kind)),
                    definition.operation_identifier,
                    definition.network_session_identifier,
                    definition.ordinal,
                    definition.attempt,
                    max(definition.not_before_utc_ns, created_at_utc_ns),
                    created_at_utc_ns,
                ),
            )
            for scope in definition.scopes:
                await connection.execute(
                    "INSERT INTO network_activity_scopes VALUES (?, ?, ?)",
                    (
                        definition.identifier,
                        scope.kind.value,
                        _json(list(scope.identity)),
                    ),
                )
            constraints = await self._constraints_for_network_activity(
                connection,
                definition.identifier,
            )
            eligible_at = max(definition.not_before_utc_ns, created_at_utc_ns)
            holdoff_decisions: list[dict[str, JsonValue]] = []
            for constraint in constraints:
                if not isinstance(constraint, UniformHoldoffConstraint):
                    continue
                sampled_delay_ns = sample_uniform_holdoff_ns(
                    constraint.minimum_ns,
                    constraint.maximum_ns,
                )
                if not constraint.minimum_ns <= sampled_delay_ns <= constraint.maximum_ns:
                    raise ValueError("Sampled holdoff is outside the registered range")
                eligible_at = max(eligible_at, created_at_utc_ns + sampled_delay_ns)
                holdoff_decisions.append(
                    {
                        "constraint_identifier": list(constraint.identifier),
                        "sampled_delay_ns": sampled_delay_ns,
                    }
                )
            await connection.execute(
                "UPDATE network_activities SET eligible_at_utc_ns = ? WHERE id = ?",
                (eligible_at, definition.identifier),
            )
            await self._append_network_activity_event(
                connection,
                event_identifier=event_identifier,
                network_activity_identifier=definition.identifier,
                event_kind=NetworkActivityEventKind.CREATED,
                recorded_at_utc_ns=created_at_utc_ns,
                data={
                    "definition": definition.model_dump(mode="json"),
                    "holdoff_decisions": holdoff_decisions,
                    "effective_eligible_at_utc_ns": eligible_at,
                },
            )
        return eligible_at

    async def _network_activity_constraint_availability(
        self,
        connection: AsyncConnection,
        *,
        network_activity_identifier: str,
        now_utc_ns: int,
    ) -> tuple[bool, int | None, tuple[Constraint, ...]]:
        constraints = await self._constraints_for_network_activity(
            connection,
            network_activity_identifier,
        )
        next_eligible: int | None = None
        available = True
        for constraint in constraints:
            if isinstance(constraint, ConcurrencyConstraint):
                # Common scopes retain all historical owners. Drive this join
                # from admitted activities so history cannot grow admission cost.
                cursor = await connection.execute(
                    """
                    SELECT count(*), min(network_activities.permit_expires_at_utc_ns)
                    FROM network_activities INDEXED BY network_activities_state
                    CROSS JOIN network_activity_scopes
                      ON network_activity_scopes.network_activity_id = network_activities.id
                    WHERE network_activity_scopes.scope_kind = ?
                      AND network_activity_scopes.scope_identity_json = ?
                      AND network_activities.state = 'admitted'
                      AND network_activities.permit_expires_at_utc_ns > ?
                    """,
                    (
                        constraint.scope.kind.value,
                        _json(list(constraint.scope.identity)),
                        now_utc_ns,
                    ),
                )
                row = await cursor.fetchone()
                assert row is not None
                if row[0] >= constraint.maximum_active:
                    available = False
                    expiry = row[1]
                    if isinstance(expiry, int):
                        next_eligible = (
                            expiry if next_eligible is None else min(next_eligible, expiry)
                        )
            elif isinstance(constraint, SlidingWindowRateConstraint):
                threshold = now_utc_ns - constraint.period_ns
                cursor = await connection.execute(
                    """
                    SELECT count(*), min(reserved_at_utc_ns)
                    FROM rate_starts
                    WHERE constraint_identifier_parts_json = ?
                      AND reserved_at_utc_ns > ?
                    """,
                    (_json(list(constraint.identifier)), threshold),
                )
                row = await cursor.fetchone()
                assert row is not None
                if row[0] >= constraint.maximum_starts:
                    available = False
                    oldest = row[1]
                    if isinstance(oldest, int):
                        rate_eligible = oldest + constraint.period_ns
                        next_eligible = (
                            rate_eligible
                            if next_eligible is None
                            else min(next_eligible, rate_eligible)
                        )
        return available, next_eligible, constraints

    async def try_admit_network_activity(
        self,
        *,
        network_activity_identifier: str,
        admission_token: str,
        permit_duration_ns: int,
        now_utc_ns: int,
        event_identifier: str,
    ) -> NetworkActivityAdmissionResult:
        """Atomically admit one pending network activity when all constraints allow it."""

        if permit_duration_ns <= 0:
            raise ValueError("Network activity permit duration must be positive")
        async with self._connections.writer() as connection:
            cursor = await connection.execute(
                """
                SELECT activity.state, activity.eligible_at_utc_ns, operation.state,
                       activity.kind_parts_json
                FROM network_activities AS activity
                JOIN operations AS operation ON operation.id = activity.operation_id
                WHERE activity.id = ?
                """,
                (network_activity_identifier,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise KeyError(network_activity_identifier)
            if row[2] != "started":
                raise RuntimeError("Only a started operation can own network activity")
            if _text(row[0]) != NetworkActivityState.PENDING.value:
                raise RuntimeError("Only a pending network activity can be admitted")
            eligible_at = _integer(row[1])
            if (
                tuple(cast(list[str], decode_json(_text(row[3]))))
                in FACEBOOK_SEARCH_NETWORK_ACTIVITY_KINDS
            ):
                path_cursor = await connection.execute(
                    "SELECT scope_identity_json FROM network_activity_scopes WHERE network_activity_id=? AND scope_kind='network_path'",
                    (network_activity_identifier,),
                )
                cooldowns = await self._facebook_search_route_cooldowns(connection, now_utc_ns)
                for path_row in await path_cursor.fetchall():
                    eligible_at = max(eligible_at, cooldowns.get(_text(path_row[0]), 0))
            if eligible_at > now_utc_ns:
                return NetworkActivityAdmissionResult(
                    admission=None,
                    next_eligible_at_utc_ns=eligible_at,
                )
            (
                available,
                next_eligible,
                constraints,
            ) = await self._network_activity_constraint_availability(
                connection,
                network_activity_identifier=network_activity_identifier,
                now_utc_ns=now_utc_ns,
            )
            if not available:
                return NetworkActivityAdmissionResult(
                    admission=None,
                    next_eligible_at_utc_ns=next_eligible,
                )
            expires_at = now_utc_ns + permit_duration_ns
            await connection.execute(
                """
                UPDATE network_activities
                SET state = 'admitted', admission_token = ?, admitted_at_utc_ns = ?,
                    permit_expires_at_utc_ns = ?
                WHERE id = ?
                """,
                (
                    admission_token,
                    now_utc_ns,
                    expires_at,
                    network_activity_identifier,
                ),
            )
            rate_reservations: list[dict[str, JsonValue]] = []
            for constraint in constraints:
                if not isinstance(constraint, SlidingWindowRateConstraint):
                    continue
                reservation_identifier = str(uuid4())
                await connection.execute(
                    """
                    INSERT INTO rate_starts(
                        id, constraint_identifier_parts_json, subject_kind,
                        subject_identifier, permit_token, reserved_at_utc_ns
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        reservation_identifier,
                        _json(list(constraint.identifier)),
                        SchedulingSubjectKind.NETWORK_ACTIVITY.value,
                        network_activity_identifier,
                        admission_token,
                        now_utc_ns,
                    ),
                )
                rate_reservations.append(
                    {
                        "identifier": reservation_identifier,
                        "constraint_identifier": list(constraint.identifier),
                        "reserved_at_utc_ns": now_utc_ns,
                    }
                )
            await self._append_network_activity_event(
                connection,
                event_identifier=event_identifier,
                network_activity_identifier=network_activity_identifier,
                event_kind=NetworkActivityEventKind.ADMITTED,
                recorded_at_utc_ns=now_utc_ns,
                data={
                    "admission_token": admission_token,
                    "permit_expires_at_utc_ns": expires_at,
                    "constraints": [
                        constraint.model_dump(mode="json") for constraint in constraints
                    ],
                    "rate_reservations": rate_reservations,
                },
            )
        return NetworkActivityAdmissionResult(
            admission=NetworkActivityAdmission(
                activity_identifier=network_activity_identifier,
                token=admission_token,
                admitted_at_utc_ns=now_utc_ns,
                permit_expires_at_utc_ns=expires_at,
            ),
            next_eligible_at_utc_ns=None,
        )

    async def finish_network_activity(
        self,
        *,
        network_activity_identifier: str,
        admission_token: str | None,
        state: NetworkActivityState,
        ended_at_utc_ns: int,
        result: JsonValue,
        event_identifier: str,
    ) -> None:
        """Finish an admitted activity, or cancel one that is still pending."""

        if state not in {
            NetworkActivityState.COMPLETED,
            NetworkActivityState.SKIPPED,
            NetworkActivityState.FAILED,
            NetworkActivityState.CANCELLED,
        }:
            raise ValueError("Network activity must finish in a terminal state")
        async with self._connections.writer() as connection:
            cursor = await connection.execute(
                "SELECT state, admission_token FROM network_activities WHERE id = ?",
                (network_activity_identifier,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise KeyError(network_activity_identifier)
            current_state = _text(row[0])
            current_token = row[1]
            if current_state == NetworkActivityState.PENDING.value:
                if (
                    state
                    not in {
                        NetworkActivityState.FAILED,
                        NetworkActivityState.CANCELLED,
                    }
                    or admission_token is not None
                ):
                    raise RuntimeError("A pending network activity may only fail or be cancelled")
            elif (
                current_state != NetworkActivityState.ADMITTED.value
                or current_token != admission_token
            ):
                raise RuntimeError("Network activity admission is no longer owned")
            await connection.execute(
                """
                UPDATE network_activities
                SET state = ?, admission_token = NULL, permit_expires_at_utc_ns = NULL,
                    ended_at_utc_ns = ?, result_json = ?
                WHERE id = ?
                """,
                (state.value, ended_at_utc_ns, _json(result), network_activity_identifier),
            )
            await self._append_network_activity_event(
                connection,
                event_identifier=event_identifier,
                network_activity_identifier=network_activity_identifier,
                event_kind=NetworkActivityEventKind(state.value),
                recorded_at_utc_ns=ended_at_utc_ns,
                data=result,
            )

    async def _reconcile_terminal_network_activities(
        self,
        connection: AsyncConnection,
        *,
        recorded_at_utc_ns: int,
        operation_id: str | None = None,
        activity_identifiers: Sequence[str] | None = None,
    ) -> tuple[str, ...]:
        """Close unknown outcomes, not fabricate request successes or failures."""

        if recorded_at_utc_ns < 0:
            raise ValueError("Reconciliation time cannot be negative")
        if activity_identifiers is not None and not activity_identifiers:
            return ()
        operation_predicate = "AND activity.operation_id = ?" if operation_id is not None else ""
        identifier_predicate = (
            "AND activity.id IN (SELECT value FROM json_each(?))"
            if activity_identifiers is not None
            else ""
        )
        bindings: list[apsw.SQLiteValue] = []
        if operation_id is not None:
            bindings.append(operation_id)
        if activity_identifiers is not None:
            bindings.append(_json(list(activity_identifiers)))
        # The state index restricts this to unfinished activities, rather than
        # scanning the complete network history on every operation completion.
        cursor = await connection.execute(
            f"""
            SELECT activity.id, activity.state, operation.state
            FROM network_activities AS activity
            JOIN operations AS operation ON operation.id = activity.operation_id
            WHERE activity.state IN ('pending', 'admitted')
              AND operation.state IN ('completed', 'failed')
              {operation_predicate} {identifier_predicate}
            ORDER BY activity.created_at_utc_ns, activity.id
            """,
            bindings,
        )
        rows = await cursor.fetchall()
        reconciled: list[str] = []
        for row in rows:
            identifier = _text(row[0])
            previous_state = _text(row[1])
            result: dict[str, JsonValue] = {
                "kind": "owning_operation_finished",
                "operation_state": _text(row[2]),
                "previous_state": previous_state,
                "possibly_dispatched": previous_state == "admitted",
                "outcome": "unknown",
            }
            await connection.execute(
                """
                UPDATE network_activities
                SET state = 'cancelled', admission_token = NULL,
                    permit_expires_at_utc_ns = NULL, ended_at_utc_ns = ?, result_json = ?
                WHERE id = ? AND state = ?
                """,
                (recorded_at_utc_ns, _json(result), identifier, previous_state),
            )
            if await connection.changes() != 1:
                raise RuntimeError("Network activity changed during reconciliation")
            await self._append_network_activity_event(
                connection,
                event_identifier=str(uuid4()),
                network_activity_identifier=identifier,
                event_kind=NetworkActivityEventKind.CANCELLED,
                recorded_at_utc_ns=recorded_at_utc_ns,
                data=result,
            )
            reconciled.append(identifier)
        return tuple(reconciled)

    async def reconcile_terminal_network_activities(
        self,
        *,
        recorded_at_utc_ns: int,
        activity_identifiers: Sequence[str] | None = None,
    ) -> tuple[str, ...]:
        """Repair legacy orphans; active owners and finished activities stay untouched."""

        async with self._connections.writer() as connection:
            return await self._reconcile_terminal_network_activities(
                connection,
                recorded_at_utc_ns=recorded_at_utc_ns,
                activity_identifiers=activity_identifiers,
            )

    async def network_activity(self, identifier: str) -> dict[str, JsonValue]:
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT kind_parts_json, operation_id, network_session_identifier,
                       ordinal, attempt, state, eligible_at_utc_ns, created_at_utc_ns,
                       admitted_at_utc_ns, ended_at_utc_ns, result_json
                FROM network_activities WHERE id = ?
                """,
                (identifier,),
            )
            row = await cursor.fetchone()
        if row is None:
            raise KeyError(identifier)
        return {
            "identifier": identifier,
            "kind": decode_json(_text(row[0])),
            "operation_identifier": _text(row[1]),
            "network_session_identifier": _text(row[2]),
            "ordinal": _integer(row[3]),
            "attempt": _integer(row[4]),
            "state": _text(row[5]),
            "eligible_at_utc_ns": _integer(row[6]),
            "created_at_utc_ns": _integer(row[7]),
            "admitted_at_utc_ns": row[8],
            "ended_at_utc_ns": row[9],
            "result": None if row[10] is None else decode_json(_text(row[10])),
        }

    async def enqueue_work(
        self,
        definition: WorkDefinition,
        requester: WorkRequester,
        *,
        event_identifier: str,
        enqueued_at_utc_ns: int,
        holdoff_decisions: Sequence[HoldoffDecision] = (),
    ) -> EnqueueResult:
        """Create or share work and retain the individual requester edge."""

        kind_json = _json(list(definition.kind))
        deduplication_json = _json(list(definition.deduplication_identity))
        async with self._connections.writer() as connection:
            if requester.kind in (
                ("carl", "mcp", "request_workspace_refresh"),
                ("carl", "mcp", "create_workspace_search_revision"),
            ) and isinstance(requester.context, dict):
                track = requester.context.get("track_identifier")
                version = requester.context.get("search_specification_version")
                if (
                    isinstance(track, str)
                    and isinstance(version, int)
                    and not isinstance(version, bool)
                ):
                    await self._require_workspace_track_version(
                        connection, requester.identifier, track, version
                    )
            eligible_at = max(definition.not_before_utc_ns, enqueued_at_utc_ns)
            own_eligible_at = eligible_at
            shared_cooldown_until = 0
            if definition.kind == COLLECT_EBAY_SEARCH_WORK_KIND:
                stack = _ebay_search_stack_identifier(definition.payload)
                cooldowns = await self._ebay_search_stack_cooldowns(connection, enqueued_at_utc_ns)
                shared_cooldown_until = cooldowns.get(stack, 0) if stack is not None else 0
                eligible_at = max(eligible_at, shared_cooldown_until)
            cursor = await connection.execute(
                """
                SELECT id, eligible_at_utc_ns
                FROM work_items
                WHERE kind_parts_json = ? AND deduplication_identity_json = ?
                  AND state IN ('pending', 'leased')
                """,
                (kind_json, deduplication_json),
            )
            row = await cursor.fetchone()
            created = row is None
            if created:
                work_item_identifier = definition.identifier
                await connection.execute(
                    """
                    INSERT INTO work_items(
                        id, kind_parts_json, payload_schema_version, payload_json,
                        acquisition_resource_identity_json, deduplication_identity_json, state, priority,
                        eligible_at_utc_ns, created_at_utc_ns
                    ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                    """,
                    (
                        work_item_identifier,
                        kind_json,
                        definition.payload_schema_version,
                        _json(definition.payload),
                        acquisition_resource_identity_json(definition.kind, definition.payload),
                        deduplication_json,
                        definition.priority,
                        eligible_at,
                        enqueued_at_utc_ns,
                    ),
                )
                for scope in definition.scopes:
                    await connection.execute(
                        "INSERT INTO work_scopes VALUES (?, ?, ?)",
                        (
                            work_item_identifier,
                            scope.kind.value,
                            _json(list(scope.identity)),
                        ),
                    )

                constraints = await self._constraints_for_work(connection, work_item_identifier)
                holdoffs = {
                    constraint.identifier: constraint
                    for constraint in constraints
                    if isinstance(constraint, UniformHoldoffConstraint)
                }
                decisions = {
                    decision.constraint_identifier: decision for decision in holdoff_decisions
                }
                if len(decisions) != len(holdoff_decisions):
                    raise ValueError("Holdoff constraints may be sampled only once")
                if decisions.keys() != holdoffs.keys():
                    raise ValueError(
                        "Holdoff decisions must exactly match applicable holdoff constraints"
                    )
                for identity, constraint in holdoffs.items():
                    sampled_delay = decisions[identity].sampled_delay_ns
                    if not constraint.minimum_ns <= sampled_delay <= constraint.maximum_ns:
                        raise ValueError("Sampled holdoff is outside the registered range")
                    eligible_at = max(eligible_at, enqueued_at_utc_ns + sampled_delay)
                    own_eligible_at = max(own_eligible_at, enqueued_at_utc_ns + sampled_delay)
                await connection.execute(
                    "UPDATE work_items SET eligible_at_utc_ns = ? WHERE id = ?",
                    (eligible_at, work_item_identifier),
                )
                event_kind = WorkEventKind.ENQUEUED
                event_data: JsonValue = {
                    "request_identifier": requester.request_identifier,
                    "holdoff_decisions": [
                        decision.model_dump(mode="json") for decision in holdoff_decisions
                    ],
                    "effective_eligible_at_utc_ns": eligible_at,
                    "own_eligible_at_utc_ns": own_eligible_at,
                    "shared_cooldown_until_utc_ns": shared_cooldown_until,
                }
            else:
                work_item_identifier = _text(row[0])
                eligible_at = row[1]
                if holdoff_decisions:
                    raise ValueError("Holdoff is not resampled when work is deduplicated")
                event_kind = WorkEventKind.REQUESTER_ATTACHED
                event_data = {"request_identifier": requester.request_identifier}

            await connection.execute(
                """
                INSERT INTO work_requests(
                    id, work_item_id, requester_kind_parts_json,
                    requester_identifier, requested_at_utc_ns, context_json,
                    scheduling_json,parent_work_identifier
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    requester.request_identifier,
                    work_item_identifier,
                    _json(list(requester.kind)),
                    requester.identifier,
                    enqueued_at_utc_ns,
                    _json(requester.context),
                    None
                    if requester.scheduling is None
                    else requester.scheduling.model_dump_json(),
                    requester.parent_work_identifier,
                ),
            )
            await self._publish_work_request_dependencies(connection, requester.request_identifier)
            await fair_sql.assign_owner(connection, work_item_identifier, requester)
            await self._require_requester_not_cancelled(connection, requester.request_identifier)
            await self._append_work_event(
                connection,
                event_identifier=event_identifier,
                work_item_identifier=work_item_identifier,
                event_kind=event_kind,
                recorded_at_utc_ns=enqueued_at_utc_ns,
                data=event_data,
            )
        return EnqueueResult(
            work_item_identifier=work_item_identifier,
            created=created,
            eligible_at_utc_ns=eligible_at,
        )

    async def enqueue_work_unless_facebook_item_page_is_usable(
        self,
        definition: WorkDefinition,
        requester: WorkRequester,
        *,
        listing_identifier: str,
        event_identifier: str,
        enqueued_at_utc_ns: int,
    ) -> tuple[EnqueueResult | None, SuccessfulItemPageResult | None]:
        """Atomically enqueue collection or return the latest usable retained item page."""

        try:
            async with self._connections.writer():
                enqueued = await self.enqueue_work(
                    definition,
                    requester,
                    event_identifier=event_identifier,
                    enqueued_at_utc_ns=enqueued_at_utc_ns,
                )
                successful = await self.successful_facebook_item_page_results((listing_identifier,))
                if successful:
                    raise _SuccessfulItemPageAvailable(successful[-1])
        except _SuccessfulItemPageAvailable as available:
            return None, available.result
        return enqueued, None

    async def attach_work_request(
        self,
        work_item_identifier: str,
        requester: WorkRequester,
        *,
        event_identifier: str,
        requested_at_utc_ns: int,
    ) -> None:
        """Retain another requester edge to existing work, including terminal work."""

        async with self._connections.writer() as connection:
            cursor = await connection.execute(
                "SELECT 1 FROM work_items WHERE id = ?",
                (work_item_identifier,),
            )
            if await cursor.fetchone() is None:
                raise KeyError(work_item_identifier)
            await connection.execute(
                """
                INSERT INTO work_requests(
                    id, work_item_id, requester_kind_parts_json,
                    requester_identifier, requested_at_utc_ns, context_json,
                    scheduling_json,parent_work_identifier
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    requester.request_identifier,
                    work_item_identifier,
                    _json(list(requester.kind)),
                    requester.identifier,
                    requested_at_utc_ns,
                    _json(requester.context),
                    None
                    if requester.scheduling is None
                    else requester.scheduling.model_dump_json(),
                    requester.parent_work_identifier,
                ),
            )
            await self._publish_work_request_dependencies(connection, requester.request_identifier)
            await fair_sql.assign_owner(connection, work_item_identifier, requester)
            await self._require_requester_not_cancelled(connection, requester.request_identifier)
            await self._append_work_event(
                connection,
                event_identifier=event_identifier,
                work_item_identifier=work_item_identifier,
                event_kind=WorkEventKind.REQUESTER_ATTACHED,
                recorded_at_utc_ns=requested_at_utc_ns,
                data={"request_identifier": requester.request_identifier},
            )

    async def _append_work_event(
        self,
        connection: AsyncConnection,
        *,
        event_identifier: str,
        work_item_identifier: str,
        event_kind: WorkEventKind,
        recorded_at_utc_ns: int,
        data: JsonValue,
    ) -> None:
        await connection.execute(
            """
            INSERT INTO work_events(
                id, work_item_id, event_kind, recorded_at_utc_ns, data_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                event_identifier,
                work_item_identifier,
                event_kind.value,
                recorded_at_utc_ns,
                _json(data),
            ),
        )
        await scheduler_sql.logical_event(
            connection, work_item_identifier, event_kind.value, data, recorded_at_utc_ns
        )

    @staticmethod
    async def _require_requester_not_cancelled(
        connection: AsyncConnection, request_identifier: str
    ) -> None:
        cursor = await connection.execute(
            """
            WITH request AS (SELECT * FROM work_requests WHERE id=?), immediate(id) AS (
                SELECT work.id FROM request JOIN work_items AS work ON work.id=request.requester_identifier
                UNION
                SELECT owner.work_item_id FROM request
                JOIN objects AS object ON object.id=request.requester_identifier
                JOIN work_operations AS owner ON owner.operation_id=object.created_by_operation_id
                UNION
                SELECT owner.work_item_id FROM request,json_tree(request.context_json) AS context
                JOIN work_operations AS owner ON owner.operation_id=context.value
                WHERE context.key IN ('operation_identifier','source_operation_identifier')
            ), parents(id) AS (
                SELECT id FROM immediate
                UNION
                SELECT context.value FROM request,json_tree(request.context_json) AS context
                WHERE context.type='text' AND NOT EXISTS (SELECT 1 FROM immediate)
            ) SELECT parent.id FROM parents JOIN work_items AS parent ON parent.id=parents.id
            WHERE parent.state IN ('cancelled','cancelling') LIMIT 1
            """,
            (request_identifier,),
        )
        row = await cursor.fetchone()
        if row is not None:
            raise WorkCancellationRequestedError(_text(row[0]))

    @staticmethod
    async def _publish_work_request_dependencies(
        connection: AsyncConnection,
        request_identifier: str | None = None,
    ) -> None:
        predicate = "" if request_identifier is None else "WHERE id=?"
        await connection.execute(
            f"""
            WITH requested AS (
                SELECT * FROM work_requests {predicate}
            ), edges(parent, child) AS (
                SELECT requester_identifier, work_item_id FROM requested
                UNION
                SELECT owner.work_item_id, request.work_item_id
                FROM requested AS request JOIN objects AS object ON object.id=request.requester_identifier
                JOIN work_operations AS owner ON owner.operation_id=object.created_by_operation_id
                UNION
                SELECT context.value, request.work_item_id
                FROM requested AS request, json_tree(request.context_json) AS context
                WHERE context.type='text'
                UNION
                SELECT owner.work_item_id, request.work_item_id
                FROM requested AS request, json_tree(request.context_json) AS context
                JOIN work_operations AS owner ON owner.operation_id=context.value
                WHERE context.type='text'
            )
            INSERT OR IGNORE INTO work_dependencies
            SELECT parent,child FROM edges JOIN work_items ON work_items.id=edges.parent
            WHERE parent!=child
            """,
            () if request_identifier is None else (request_identifier,),
        )
        if request_identifier is not None:
            await progress_sql.publish_consumer_edge(connection, request_identifier, time_ns())

    @staticmethod
    async def _work_descendants(
        connection: AsyncConnection, identifiers: Sequence[str]
    ) -> tuple[str, ...]:
        """Follow durable requests, operation ownership, and retained root contexts."""
        cursor = await connection.execute(
            """
            WITH RECURSIVE selected(id) AS (
                SELECT value FROM json_each(?)
                UNION
                SELECT edges.child_work_item_id FROM work_dependencies AS edges
                JOIN selected ON edges.parent_work_item_id=selected.id
            )
            SELECT work.id FROM selected JOIN work_items AS work ON work.id=selected.id
            ORDER BY work.created_at_utc_ns, work.id
            """,
            (_json(list(identifiers)),),
        )
        return tuple(_text(row[0]) for row in await cursor.fetchall())

    @staticmethod
    async def _workspace_work_identifiers(
        connection: AsyncConnection, workspace_record_identifier: str
    ) -> tuple[str, ...]:
        cursor = await connection.execute(
            """
            SELECT DISTINCT work_item_id FROM work_requests WHERE requester_identifier=?
            UNION
            SELECT json_extract(execution.value_json, '$.work_identifier')
            FROM records AS execution JOIN objects AS object ON object.id=execution.object_id
            WHERE object.kind_parts_json='["carl","marketplace","search_execution"]'
              AND json_extract(execution.value_json, '$.search_record_identifier') = (
                  SELECT json_extract(workspace.value_json, '$.search_run_record_identifier')
                  FROM records AS workspace WHERE workspace.object_id=?
              )
            """,
            (workspace_record_identifier, workspace_record_identifier),
        )
        return tuple(_text(row[0]) for row in await cursor.fetchall() if row[0] is not None)

    async def _cancel_work_identifiers(
        self,
        connection: AsyncConnection,
        identifiers: Sequence[str],
        *,
        reason: str,
        caller: str,
        requested_at_utc_ns: int,
    ) -> dict[str, JsonValue]:
        if not reason.strip() or not caller.strip() or requested_at_utc_ns < 0:
            raise ValueError("Cancellation requires a reason, caller, and nonnegative time")
        cancelled: list[JsonValue] = []
        cancelling: list[JsonValue] = []
        terminal: list[JsonValue] = []
        for identifier in identifiers:
            cursor = await connection.execute(
                "SELECT state FROM work_items WHERE id=?", (identifier,)
            )
            row = await cursor.fetchone()
            if row is None:
                raise KeyError(identifier)
            state = _text(row[0])
            if state in ("completed", "terminal_failure", "cancelled"):
                terminal.append(identifier)
                continue
            if state == "cancelling":
                cancelling.append(identifier)
                continue
            data: JsonValue = {"reason": reason, "caller": caller}
            await connection.execute(
                "INSERT INTO work_cancellation_requests VALUES (?, ?, ?, ?)",
                (identifier, requested_at_utc_ns, reason, caller),
            )
            await self._append_work_event(
                connection,
                event_identifier=str(uuid4()),
                work_item_identifier=identifier,
                event_kind=WorkEventKind.CANCEL_REQUESTED,
                recorded_at_utc_ns=requested_at_utc_ns,
                data=data,
            )
            cursor = await connection.execute(
                """
                SELECT attempt.operation_id FROM stage_attempts AS attempt
                WHERE attempt.work_item_id = ? AND attempt.state = 'active'
                  AND NOT EXISTS (
                      SELECT 1 FROM work_stages AS stage
                      WHERE stage.work_item_id = attempt.work_item_id AND stage.state = 'running'
                  )
                """,
                (identifier,),
            )
            suspended_stage = await cursor.fetchone()
            if state == "pending" or suspended_stage is not None:
                await connection.execute(
                    "UPDATE work_items SET state='cancelled',error_json=NULL,lease_token=NULL,lease_owner=NULL,lease_expires_at_utc_ns=NULL WHERE id=?",
                    (identifier,),
                )
                if suspended_stage is not None:
                    await connection.execute(
                        """
                        UPDATE operations SET state = 'completed', ended_at_utc = ?, duration_ns = 0,
                            result_json = ?, error_json = NULL
                        WHERE id = ? AND state = 'started'
                        """,
                        (
                            _utc_text_from_ns(requested_at_utc_ns),
                            _json({"state": "cancelled"}),
                            suspended_stage[0],
                        ),
                    )
                await self._cancel_staged_work(connection, identifier, requested_at_utc_ns)
                await self._append_work_event(
                    connection,
                    event_identifier=str(uuid4()),
                    work_item_identifier=identifier,
                    event_kind=WorkEventKind.CANCELLED,
                    recorded_at_utc_ns=requested_at_utc_ns,
                    data=data,
                )
                cancelled.append(identifier)
            else:
                await connection.execute(
                    "UPDATE work_items SET state='cancelling',error_json=NULL WHERE id=?",
                    (identifier,),
                )
                cancelling.append(identifier)
        return {
            "cancelled_pending": len(cancelled),
            "cancelling_leased": len(cancelling),
            "already_terminal": len(terminal),
            "cancelled_pending_identifiers": cancelled,
            "cancelling_leased_identifiers": cancelling,
            "already_terminal_identifiers": terminal,
        }

    async def cancel_work(
        self,
        work_item_identifier: str,
        *,
        reason: str,
        caller: str,
        requested_at_utc_ns: int,
        include_descendants: bool = True,
    ) -> dict[str, JsonValue]:
        async with self._connections.writer() as connection:
            cursor = await connection.execute(
                "SELECT state FROM work_items WHERE id=?", (work_item_identifier,)
            )
            root = await cursor.fetchone()
            if root is None:
                raise KeyError(work_item_identifier)
            identifiers = (
                await self._work_descendants(connection, (work_item_identifier,))
                if include_descendants and root[0] in ("pending", "leased", "cancelling")
                else (work_item_identifier,)
            )
            return await self._cancel_work_identifiers(
                connection,
                identifiers,
                reason=reason,
                caller=caller,
                requested_at_utc_ns=requested_at_utc_ns,
            )

    async def cancel_workspace_work(
        self,
        workspace_record_identifier: str,
        *,
        reason: str,
        caller: str,
        requested_at_utc_ns: int,
    ) -> dict[str, JsonValue]:
        async with self._connections.writer() as connection:
            roots = await self._workspace_work_identifiers(connection, workspace_record_identifier)
            cursor = await connection.execute(
                "SELECT id,state FROM work_items WHERE id IN (SELECT value FROM json_each(?))",
                (_json(list(roots)),),
            )
            rows = await cursor.fetchall()
            active = tuple(
                _text(row[0]) for row in rows if row[1] in ("pending", "leased", "cancelling")
            )
            terminal = tuple(
                _text(row[0]) for row in rows if row[1] not in ("pending", "leased", "cancelling")
            )
            descendants = await self._work_descendants(connection, active)
            identifiers = (*descendants, *terminal)
            return await self._cancel_work_identifiers(
                connection,
                identifiers,
                reason=reason,
                caller=caller,
                requested_at_utc_ns=requested_at_utc_ns,
            )

    async def cancel_all_work(
        self,
        *,
        reason: str,
        caller: str,
        requested_at_utc_ns: int,
    ) -> dict[str, JsonValue]:
        async with self._connections.writer() as connection:
            cursor = await connection.execute(
                "SELECT id FROM work_items WHERE state IN ('pending','leased','cancelling') ORDER BY created_at_utc_ns,id"
            )
            identifiers = tuple(_text(row[0]) for row in await cursor.fetchall())
            return await self._cancel_work_identifiers(
                connection,
                identifiers,
                reason=reason,
                caller=caller,
                requested_at_utc_ns=requested_at_utc_ns,
            )

    async def work_cancellation_requested(self, work_item_identifier: str) -> bool:
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT 1 FROM work_cancellation_requests WHERE work_item_id=?",
                (work_item_identifier,),
            )
            return await cursor.fetchone() is not None

    async def finalize_cancelled_work(
        self,
        *,
        work_item_identifier: str,
        lease_token: str,
        worker_identifier: str,
        operation_id: str | None,
        utc_now_ns: Callable[[], int],
        event_identifier: str,
        ended_at_utc: str,
        duration_ns: int,
        records: Sequence[RecordDraft] = (),
        artifacts: Sequence[ArtifactDraft] = (),
        outputs: Sequence[NamedOutput] = (),
        inputs: Sequence[NamedInput] = (),
    ) -> bool:
        """Acknowledge cancellation, preserving already committed evidence/checkpoints."""
        async with self._connections.writer() as connection:
            now = utc_now_ns()
            cursor = await connection.execute(
                "SELECT state,lease_token,lease_owner FROM work_items WHERE id=?",
                (work_item_identifier,),
            )
            row = await cursor.fetchone()
            if row is None or tuple(row) != ("cancelling", lease_token, worker_identifier):
                return False
            if operation_id is not None:
                cursor = await connection.execute(
                    "SELECT operation.state FROM operations AS operation JOIN work_operations AS owner ON owner.operation_id=operation.id WHERE operation.id=? AND owner.work_item_id=? AND owner.lease_token=? AND owner.worker_identifier=?",
                    (operation_id, work_item_identifier, lease_token, worker_identifier),
                )
                operation = await cursor.fetchone()
                if operation is not None and operation[0] == "started":
                    if inputs:
                        await connection.executemany(
                            "INSERT OR IGNORE INTO operation_inputs VALUES (?, ?, ?)",
                            (
                                (operation_id, _json(list(item.name)), item.object_identifier)
                                for item in inputs
                            ),
                        )
                    await self._publish_operation_objects(
                        connection,
                        operation_id=operation_id,
                        records=records,
                        artifacts=artifacts,
                        outputs=outputs,
                    )
                    await connection.execute(
                        "UPDATE operations SET state='completed', ended_at_utc=?, duration_ns=?, result_json=?,error_json=NULL WHERE id=?",
                        (ended_at_utc, duration_ns, _json({"state": "cancelled"}), operation_id),
                    )
                cursor = await connection.execute(
                    "SELECT id FROM network_activities WHERE operation_id=? AND state IN ('pending','admitted')",
                    (operation_id,),
                )
                for activity in await cursor.fetchall():
                    await connection.execute(
                        "UPDATE network_activities SET state='cancelled',admission_token=NULL,permit_expires_at_utc_ns=NULL,ended_at_utc_ns=?,result_json=? WHERE id=?",
                        (now, _json({"kind": "work_cancelled"}), activity[0]),
                    )
                    await self._append_network_activity_event(
                        connection,
                        event_identifier=str(uuid4()),
                        network_activity_identifier=_text(activity[0]),
                        event_kind=NetworkActivityEventKind.CANCELLED,
                        recorded_at_utc_ns=now,
                        data={"kind": "work_cancelled"},
                    )
            await connection.execute(
                "UPDATE work_items SET state='cancelled',lease_token=NULL,lease_owner=NULL,lease_expires_at_utc_ns=NULL WHERE id=?",
                (work_item_identifier,),
            )
            await self._cancel_staged_work(connection, work_item_identifier, now)
            cursor = await connection.execute(
                "SELECT reason,caller FROM work_cancellation_requests WHERE work_item_id=?",
                (work_item_identifier,),
            )
            request = await cursor.fetchone()
            assert request is not None
            await self._append_work_event(
                connection,
                event_identifier=event_identifier,
                work_item_identifier=work_item_identifier,
                event_kind=WorkEventKind.CANCELLED,
                recorded_at_utc_ns=now,
                data={"reason": _text(request[0]), "caller": _text(request[1])},
            )
            return True

    async def has_cancelled_work_dependencies(self, work_item_identifier: str) -> bool:
        async with self._connections.reader() as connection:
            return await self._has_cancelled_work_dependencies(connection, work_item_identifier)

    @staticmethod
    async def _has_cancelled_work_dependencies(
        connection: AsyncConnection, work_item_identifier: str
    ) -> bool:
        cursor = await connection.execute(
            """
            WITH RECURSIVE descendants(id) AS (
                SELECT child_work_item_id FROM work_dependencies WHERE parent_work_item_id=?
                UNION
                SELECT dependency.child_work_item_id FROM work_dependencies AS dependency
                JOIN descendants ON dependency.parent_work_item_id=descendants.id
            ) SELECT 1 FROM descendants JOIN work_items AS work ON work.id=descendants.id
              WHERE work.state IN ('cancelled','cancelling') LIMIT 1
            """,
            (work_item_identifier,),
        )
        return await cursor.fetchone() is not None

    async def _clear_cancelled_cooldowns(
        self, connection: AsyncConnection, now_utc_ns: int
    ) -> None:
        # Restore only deadlines whose current value came from a cancelled source.
        cursor = await connection.execute(
            """
            SELECT work.id, work.payload_json, work.created_at_utc_ns, event.sequence,
                   NULL
            FROM work_items AS work JOIN work_events AS event ON event.work_item_id=work.id
            JOIN work_items AS source ON source.id=json_extract(event.data_json,'$.reason.source_work_identifier')
            WHERE work.state='pending' AND source.state IN ('cancelled','cancelling')
              AND json_extract(event.data_json,'$.reason.kind')='ebay_search_shared_cooldown'
              AND work.eligible_at_utc_ns<=json_extract(event.data_json,'$.eligible_at_utc_ns')
            UNION
            SELECT work.id, work.payload_json, work.created_at_utc_ns, event.sequence,
                   json_extract(event.data_json,'$.own_eligible_at_utc_ns')
            FROM work_items AS work JOIN work_events AS event ON event.work_item_id=work.id
            WHERE work.state='pending'
              AND json_extract(event.data_json,'$.shared_cooldown_until_utc_ns')>0
              AND work.eligible_at_utc_ns<=json_extract(event.data_json,'$.shared_cooldown_until_utc_ns')
              AND EXISTS (
                SELECT 1 FROM work_items AS source JOIN work_events AS failure ON failure.work_item_id=source.id
                WHERE source.state IN ('cancelled','cancelling')
                  AND coalesce(json_extract(failure.data_json,'$.reason.cooldown.until_utc_ns'),json_extract(failure.data_json,'$.error.cooldown.until_utc_ns'))=json_extract(event.data_json,'$.shared_cooldown_until_utc_ns')
                  AND coalesce(json_extract(failure.data_json,'$.reason.cooldown.stack_identifier'),json_extract(failure.data_json,'$.error.cooldown.stack_identifier'))=coalesce(json_extract(work.payload_json,'$.request.stack_identifier'),'ebay_anonymous')
              )
            """
        )
        cooldowns = await self._ebay_search_stack_cooldowns(connection, now_utc_ns)
        for row in await cursor.fetchall():
            previous = await connection.execute(
                "SELECT max(coalesce(json_extract(data_json,'$.effective_eligible_at_utc_ns'),json_extract(data_json,'$.eligible_at_utc_ns'))) FROM work_events WHERE work_item_id=? AND sequence<? AND coalesce(json_extract(data_json,'$.reason.kind'),'')!='ebay_search_shared_cooldown'",
                (row[0], row[3]),
            )
            prior = await previous.fetchone()
            baseline = (
                _integer(row[4])
                if row[4] is not None
                else _integer(row[2])
                if prior is None or prior[0] is None
                else _integer(prior[0])
            )
            stack = _ebay_search_stack_identifier(decode_json(_text(row[1])))
            eligible = max(
                now_utc_ns, baseline, cooldowns.get(stack, 0) if stack is not None else 0
            )
            await connection.execute(
                "UPDATE work_items SET eligible_at_utc_ns=? WHERE id=?", (eligible, row[0])
            )

    async def _recover_expired_work_attempt(
        self,
        connection: AsyncConnection,
        *,
        work_item_identifier: str,
        attempt: int,
        recovered_at_utc_ns: int,
    ) -> tuple[str, ...]:
        cursor = await connection.execute(
            """
            SELECT operations.id
            FROM work_operations
            JOIN operations ON operations.id = work_operations.operation_id
            WHERE work_operations.work_item_id = ?
              AND work_operations.attempt = ?
              AND operations.state = 'started'
            """,
            (work_item_identifier, attempt),
        )
        operation_identifiers = tuple([_text(row[0]) async for row in cursor])
        ended_at_utc = _utc_text_from_ns(recovered_at_utc_ns)
        for operation_identifier in operation_identifiers:
            activity_cursor = await connection.execute(
                """
                SELECT id, state
                FROM network_activities
                WHERE operation_id = ? AND state IN ('pending', 'admitted')
                ORDER BY ordinal, id
                """,
                (operation_identifier,),
            )
            activities = tuple(
                [
                    (_text(row[0]), NetworkActivityState(_text(row[1])))
                    async for row in activity_cursor
                ]
            )
            for activity_identifier, previous_state in activities:
                await connection.execute(
                    """
                    UPDATE network_activities
                    SET state = 'cancelled', admission_token = NULL,
                        permit_expires_at_utc_ns = NULL, ended_at_utc_ns = ?,
                        result_json = ?
                    WHERE id = ? AND state = ?
                    """,
                    (
                        recovered_at_utc_ns,
                        _json(
                            {
                                "kind": "owning_work_lease_expired",
                                "possibly_dispatched": (
                                    previous_state is NetworkActivityState.ADMITTED
                                ),
                            }
                        ),
                        activity_identifier,
                        previous_state.value,
                    ),
                )
                if await connection.changes() != 1:
                    raise RuntimeError("Interrupted network activity changed during recovery")
                await self._append_network_activity_event(
                    connection,
                    event_identifier=str(uuid4()),
                    network_activity_identifier=activity_identifier,
                    event_kind=NetworkActivityEventKind.CANCELLED,
                    recorded_at_utc_ns=recovered_at_utc_ns,
                    data={
                        "kind": "owning_work_lease_expired",
                        "previous_state": previous_state.value,
                        "possibly_dispatched": (previous_state is NetworkActivityState.ADMITTED),
                    },
                )
            await self._fail_operation(
                connection,
                operation_id=operation_identifier,
                error={
                    "kind": "work_lease_expired",
                    "duration": {
                        "state": "unavailable",
                        "reason": "worker_process_interrupted",
                    },
                },
                result={"state": "interrupted"},
                ended_at_utc=ended_at_utc,
                duration_ns=None,
            )
        return operation_identifiers

    async def _constraint_availability(
        self,
        connection: AsyncConnection,
        *,
        work_item_identifier: str,
        now_utc_ns: int,
        check_rates: bool = True,
    ) -> tuple[bool, int | None, tuple[Constraint, ...]]:
        constraints = await self._constraints_for_work(connection, work_item_identifier)
        next_eligible: int | None = None
        available = True
        for constraint in constraints:
            if isinstance(constraint, ConcurrencyConstraint):
                # Count live leases first, then seek their scope memberships;
                # the opposite order walks every retained job in common scopes.
                cursor = await connection.execute(
                    """
                    SELECT count(*), min(work_items.lease_expires_at_utc_ns)
                    FROM work_items INDEXED BY work_items_claim
                    CROSS JOIN work_scopes ON work_scopes.work_item_id = work_items.id
                    WHERE work_scopes.scope_kind = ?
                      AND work_scopes.scope_identity_json = ?
                      AND work_items.state IN ('leased','cancelling')
                      AND work_items.lease_expires_at_utc_ns > ?
                    """,
                    (
                        constraint.scope.kind.value,
                        _json(list(constraint.scope.identity)),
                        now_utc_ns,
                    ),
                )
                row = await cursor.fetchone()
                assert row is not None
                if row[0] >= constraint.maximum_active:
                    available = False
                    expiry = row[1]
                    if isinstance(expiry, int):
                        next_eligible = (
                            expiry if next_eligible is None else min(next_eligible, expiry)
                        )
            elif check_rates and isinstance(constraint, SlidingWindowRateConstraint):
                threshold = now_utc_ns - constraint.period_ns
                cursor = await connection.execute(
                    """
                    SELECT count(*), min(reserved_at_utc_ns)
                    FROM rate_starts
                    WHERE constraint_identifier_parts_json = ?
                      AND reserved_at_utc_ns > ?
                    """,
                    (_json(list(constraint.identifier)), threshold),
                )
                row = await cursor.fetchone()
                assert row is not None
                if row[0] >= constraint.maximum_starts:
                    available = False
                    oldest = row[1]
                    if isinstance(oldest, int):
                        next_rate = oldest + constraint.period_ns
                        next_eligible = (
                            next_rate if next_eligible is None else min(next_eligible, next_rate)
                        )
        return available, next_eligible, constraints

    async def claim_work(
        self,
        *,
        supported_capabilities: Sequence[WorkCapability],
        worker_identifier: str,
        lease_token: str,
        lease_duration_ns: int,
        utc_now_ns: Callable[[], int],
        event_identifier: str,
        eligible_identifiers: Sequence[str] | None = None,
        _include_staged: bool = False,
        _stage_has_network: bool | None = None,
    ) -> ClaimResult:
        """Claim one eligible item and reserve all applicable permits atomically."""

        if lease_duration_ns <= 0:
            raise ValueError("Lease duration must be positive")
        if not supported_capabilities:
            raise ValueError("A worker must declare at least one supported capability")
        capability_predicate = " OR ".join(
            "(kind_parts_json = ? AND payload_schema_version = ?)" for _ in supported_capabilities
        )
        capability_bindings: list[apsw.SQLiteValue] = []
        for capability in supported_capabilities:
            capability_bindings.extend(
                (_json(list(capability.kind)), capability.payload_schema_version)
            )
        exact_identifier = (
            eligible_identifiers[0]
            if eligible_identifiers is not None and len(eligible_identifiers) == 1
            else None
        )
        identifier_predicate = (
            "AND id = ?"
            if exact_identifier is not None
            else "AND id IN (SELECT value FROM json_each(?))"
            if eligible_identifiers is not None
            else ""
        )
        staged_predicate = (
            ""
            if _include_staged
            else "AND NOT EXISTS (SELECT 1 FROM stage_attempts WHERE work_item_id = work_items.id)"
        )
        identifier_bindings: tuple[apsw.SQLiteValue, ...] = (
            (exact_identifier,)
            if exact_identifier is not None
            else (_json(list(eligible_identifiers)),)
            if eligible_identifiers is not None
            else ()
        )
        async with self._connections.writer() as connection:
            now_utc_ns = utc_now_ns()
            expired = await connection.execute(
                """
                SELECT id,lease_token,lease_owner FROM work_items
                WHERE state='cancelling' AND lease_expires_at_utc_ns<=?
                  AND NOT EXISTS (
                      SELECT 1 FROM stage_attempts WHERE work_item_id = work_items.id
                  )
                """,
                (now_utc_ns,),
            )
            for cancelled in await expired.fetchall():
                operation_cursor = await connection.execute(
                    "SELECT operation_id FROM work_operations WHERE work_item_id=? ORDER BY attempt DESC LIMIT 1",
                    (cancelled[0],),
                )
                operation = await operation_cursor.fetchone()
                await self.finalize_cancelled_work(
                    work_item_identifier=_text(cancelled[0]),
                    lease_token=_text(cancelled[1]),
                    worker_identifier=_text(cancelled[2]),
                    operation_id=None if operation is None else _text(operation[0]),
                    utc_now_ns=lambda: now_utc_ns,
                    event_identifier=str(uuid4()),
                    ended_at_utc=_utc_text_from_ns(now_utc_ns),
                    duration_ns=0,
                )
            ebay_cooldowns = (
                await self._ebay_search_stack_cooldowns(connection, now_utc_ns)
                if any(
                    capability.kind == COLLECT_EBAY_SEARCH_WORK_KIND
                    for capability in supported_capabilities
                )
                else {}
            )
            facebook_cooldowns = (
                await self._facebook_search_route_cooldowns(connection, now_utc_ns)
                if any(
                    capability.kind == COLLECT_SEARCH_WORK_KIND
                    for capability in supported_capabilities
                )
                else {}
            )
            cursor = await connection.execute(
                f"""
                WITH eligible AS (
                    SELECT id, kind_parts_json, payload_schema_version, payload_json,
                           state, lease_token, lease_owner, lease_expires_at_utc_ns,
                           attempt, priority, eligible_at_utc_ns, created_at_utc_ns,
                           acquisition_resource_identity_json,
                           coalesce(
                               (
                                   SELECT json_group_array(
                                       json_array(scope_kind, json(scope_identity_json))
                                   )
                                   FROM (
                                       SELECT scope_kind, scope_identity_json
                                       FROM work_scopes
                                       WHERE work_item_id = work_items.id
                                       ORDER BY scope_kind, scope_identity_json
                                   )
                               ),
                               '[]'
                           ) AS scope_profile
                    FROM work_items
                    WHERE eligible_at_utc_ns <= ?
                      AND (
                          state = 'pending'
                          OR (state = 'leased' AND lease_expires_at_utc_ns <= ?)
                      )
                      AND ({capability_predicate})
                      {identifier_predicate}
                      {staged_predicate}
                ),
                representatives AS (
                    SELECT *, row_number() OVER (
                        PARTITION BY scope_profile,
                            CASE WHEN kind_parts_json='["carl","ebay","collect","search"]'
                                 THEN COALESCE(json_extract(payload_json,'$.request.stack_identifier'),
                                               'ebay_anonymous') END,
                            CASE kind_parts_json
                              WHEN '["carl","ebay","collect","item"]' THEN
                                json_array(json_extract(payload_json, '$.request.item_identifier'),
                                           json_extract(payload_json, '$.request.stack_identifier'))
                              WHEN '["carl","ebay","collect","image"]' THEN
                                json_extract(payload_json, '$.url')
                              WHEN '["carl","ebay","collect","description"]' THEN
                                json_array(json_extract(payload_json, '$.url'),
                                           json_extract(payload_json, '$.request.stack_identifier'))
                              WHEN '["carl","facebook","work","collect_item"]' THEN
                                json_extract(payload_json, '$.request_plan')
                              WHEN '["carl","facebook","work","collect_image"]' THEN
                                json_array(json_extract(payload_json, '$.reference.photo_id'),
                                           json_extract(payload_json, '$.request_plan'))
                            END
                        ORDER BY priority DESC, eligible_at_utc_ns, created_at_utc_ns, id
                    ) AS profile_rank
                    FROM eligible
                )
                SELECT id, kind_parts_json, payload_schema_version, payload_json,
                       state, lease_token, lease_owner, lease_expires_at_utc_ns,
                       attempt, acquisition_resource_identity_json
                FROM representatives
                WHERE profile_rank = 1
                ORDER BY priority DESC, eligible_at_utc_ns, created_at_utc_ns, id
                """,
                (now_utc_ns, now_utc_ns, *capability_bindings, *identifier_bindings),
            )
            rows = await cursor.fetchall()
            next_checks: list[int] = []
            connectivity_cursor = await connection.execute(
                "SELECT paused, next_probe_utc_ns, probe_expires_utc_ns FROM network_connectivity WHERE singleton = 1"
            )
            connectivity = await connectivity_cursor.fetchone()
            for row in rows:
                work_item_identifier = _text(row[0])
                if (
                    connectivity is not None
                    and connectivity[0] == 1
                    and _stage_has_network is not False
                    and network_work_kind(tuple(decode_json(_text(row[1]))))
                ):
                    next_checks.append(max(now_utc_ns + 1_000_000_000, _integer(connectivity[1])))
                    continue
                if _stage_has_network is not False and _text(row[1]) == _json(
                    list(COLLECT_EBAY_SEARCH_WORK_KIND)
                ):
                    stack = _ebay_search_stack_identifier(decode_json(_text(row[3])))
                    cooldown_until = ebay_cooldowns.get(stack, 0) if stack is not None else 0
                    if cooldown_until > now_utc_ns:
                        next_checks.append(cooldown_until)
                        continue
                if _stage_has_network is not False and _text(row[1]) == _json(
                    list(COLLECT_SEARCH_WORK_KIND)
                ):
                    payload = decode_json(_text(row[3]))
                    routing = payload.get("routing") if isinstance(payload, dict) else None
                    cooldown_until = facebook_cooldowns.get(_json(routing), 0)
                    if cooldown_until > now_utc_ns:
                        next_checks.append(cooldown_until)
                        continue
                kind_json = _text(row[1])
                resource_json = row[9]
                if resource_json is not None:
                    # Caller-bound jobs have different payload/evidence identities.
                    # Serialize their actual fetch resource under this same writer
                    # transaction, until reusable evidence is durably published.
                    leases_cursor = await connection.execute(
                        """
                        SELECT min(CASE WHEN lease_expires_at_utc_ns > ?
                                        THEN lease_expires_at_utc_ns ELSE ? END)
                        FROM work_items
                        WHERE kind_parts_json = ? AND acquisition_resource_identity_json = ?
                          AND state IN ('leased','cancelling')
                          AND (lease_expires_at_utc_ns > ? OR EXISTS (
                              SELECT 1 FROM stage_attempts
                              WHERE work_item_id = work_items.id AND state = 'active'
                          ))
                        """,
                        (
                            now_utc_ns,
                            now_utc_ns + 100_000_000,
                            kind_json,
                            _text(resource_json),
                            now_utc_ns,
                        ),
                    )
                    blockers = await leases_cursor.fetchall()
                    if blockers and blockers[0][0] is not None:
                        next_checks.append(_integer(blockers[0][0]))
                        continue
                available, next_eligible, constraints = await self._constraint_availability(
                    connection,
                    work_item_identifier=work_item_identifier,
                    now_utc_ns=now_utc_ns,
                )
                if not available:
                    if next_eligible is not None:
                        next_checks.append(next_eligible)
                    continue

                if _text(row[4]) == WorkState.LEASED.value:
                    recovered_operations = await self._recover_expired_work_attempt(
                        connection,
                        work_item_identifier=work_item_identifier,
                        attempt=_integer(row[8]),
                        recovered_at_utc_ns=now_utc_ns,
                    )
                    await self._append_work_event(
                        connection,
                        event_identifier=str(uuid4()),
                        work_item_identifier=work_item_identifier,
                        event_kind=WorkEventKind.LEASE_EXPIRED,
                        recorded_at_utc_ns=now_utc_ns,
                        data={
                            "expired_token": row[5],
                            "expired_owner": row[6],
                            "expired_at_utc_ns": row[7],
                            "recovered_operation_identifiers": list(recovered_operations),
                        },
                    )

                attempt = _integer(row[8]) + 1
                expires_at = now_utc_ns + lease_duration_ns
                await connection.execute(
                    """
                    UPDATE work_items
                    SET state = 'leased', lease_token = ?, lease_owner = ?,
                        lease_expires_at_utc_ns = ?, attempt = ?
                    WHERE id = ?
                    """,
                    (
                        lease_token,
                        worker_identifier,
                        expires_at,
                        attempt,
                        work_item_identifier,
                    ),
                )
                rate_reservations = []
                for constraint in constraints:
                    if isinstance(constraint, SlidingWindowRateConstraint):
                        reservation_identifier = str(uuid4())
                        await connection.execute(
                            """
                            INSERT INTO rate_starts(
                                id, constraint_identifier_parts_json, subject_kind,
                                subject_identifier, permit_token, reserved_at_utc_ns
                            ) VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            (
                                reservation_identifier,
                                _json(list(constraint.identifier)),
                                SchedulingSubjectKind.WORK_ITEM.value,
                                work_item_identifier,
                                lease_token,
                                now_utc_ns,
                            ),
                        )
                        rate_reservations.append(
                            {
                                "identifier": reservation_identifier,
                                "constraint_identifier": list(constraint.identifier),
                                "reserved_at_utc_ns": now_utc_ns,
                            }
                        )
                await self._append_work_event(
                    connection,
                    event_identifier=event_identifier,
                    work_item_identifier=work_item_identifier,
                    event_kind=WorkEventKind.CLAIMED,
                    recorded_at_utc_ns=now_utc_ns,
                    data={
                        "worker_identifier": worker_identifier,
                        "lease_token": lease_token,
                        "lease_expires_at_utc_ns": expires_at,
                        "attempt": attempt,
                        "constraints": [
                            constraint.model_dump(mode="json") for constraint in constraints
                        ],
                        "rate_reservations": rate_reservations,
                    },
                )
                return ClaimResult(
                    lease=WorkLease(
                        work_item_identifier=work_item_identifier,
                        token=lease_token,
                        worker_identifier=worker_identifier,
                        attempt=attempt,
                        expires_at_utc_ns=expires_at,
                        kind=tuple(decode_json(_text(row[1]))),
                        payload_schema_version=_integer(row[2]),
                        payload=decode_json(_text(row[3])),
                    ),
                    next_eligible_at_utc_ns=None,
                )

            cursor = await connection.execute(
                """
                SELECT min(candidate_time)
                FROM (
                    SELECT eligible_at_utc_ns AS candidate_time
                    FROM work_items WHERE state = 'pending' AND eligible_at_utc_ns > ?
                    UNION ALL
                    SELECT lease_expires_at_utc_ns AS candidate_time
                    FROM work_items WHERE state = 'leased' AND lease_expires_at_utc_ns > ?
                )
                """,
                (now_utc_ns, now_utc_ns),
            )
            row = await cursor.fetchone()
            assert row is not None
            future = row[0]
            if isinstance(future, int):
                next_checks.append(future)
            return ClaimResult(
                lease=None,
                next_eligible_at_utc_ns=min(next_checks) if next_checks else None,
            )

    async def work_state(self, work_item_identifier: str) -> WorkState:
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT state FROM work_items WHERE id = ?",
                (work_item_identifier,),
            )
            row = await cursor.fetchone()
        if row is None:
            raise KeyError(work_item_identifier)
        return WorkState(_text(row[0]))

    async def work_kind(self, work_item_identifier: str) -> tuple[str, ...]:
        """Read only kind metadata when applying a retained outcome policy."""
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT kind_parts_json FROM work_items WHERE id = ?", (work_item_identifier,)
            )
            row = await cursor.fetchone()
        if row is None:
            raise KeyError(work_item_identifier)
        return tuple(cast(list[str], decode_json(_text(row[0]))))

    async def work_definition(self, work_item_identifier: str) -> WorkDefinition:
        """Point-load retained work without history; not_before is current eligibility."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT kind_parts_json, payload_schema_version, payload_json,
                       deduplication_identity_json, priority, eligible_at_utc_ns
                FROM work_items WHERE id = ?
                """,
                (work_item_identifier,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise KeyError(work_item_identifier)
            cursor = await connection.execute(
                """
                SELECT scope_kind, scope_identity_json FROM work_scopes
                WHERE work_item_id = ? ORDER BY scope_kind, scope_identity_json
                """,
                (work_item_identifier,),
            )
            scopes = tuple(
                SchedulingScope(
                    kind=SchedulingScopeKind(_text(scope[0])),
                    identity=tuple(cast(list[str], decode_json(_text(scope[1])))),
                )
                for scope in await cursor.fetchall()
            )
        return WorkDefinition(
            identifier=work_item_identifier,
            kind=tuple(cast(list[str], decode_json(_text(row[0])))),
            payload_schema_version=_integer(row[1]),
            payload=decode_json(_text(row[2])),
            deduplication_identity=tuple(cast(list[str], decode_json(_text(row[3])))),
            priority=_integer(row[4]),
            not_before_utc_ns=_integer(row[5]),
            scopes=scopes,
        )

    async def work(self, work_item_identifier: str) -> dict[str, JsonValue]:
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT kind_parts_json, payload_schema_version, payload_json,
                       state, attempt, result_json, error_json,
                       created_at_utc_ns, eligible_at_utc_ns, lease_owner,
                       lease_expires_at_utc_ns
                FROM work_items
                WHERE id = ?
                """,
                (work_item_identifier,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise KeyError(work_item_identifier)
            operation_cursor = await connection.execute(
                """
                SELECT operation_id, attempt
                FROM work_operations
                WHERE work_item_id = ?
                ORDER BY attempt
                """,
                (work_item_identifier,),
            )
            operations = await operation_cursor.fetchall()
            event_cursor = await connection.execute(
                """
                SELECT sequence, event_kind, recorded_at_utc_ns,
                       (
                           SELECT MAX(lease_event.recorded_at_utc_ns)
                           FROM work_events AS lease_event
                           WHERE lease_event.work_item_id = work_events.work_item_id
                             AND lease_event.event_kind IN ('claimed', 'lease_renewed')
                       )
                FROM work_events
                WHERE work_item_id = ?
                ORDER BY sequence DESC
                LIMIT 1
                """,
                (work_item_identifier,),
            )
            latest_event = await event_cursor.fetchone()
            if latest_event is None:
                raise ValueError("Durable work has no event history")
            cursor = await connection.execute(
                "SELECT requested_at_utc_ns,reason,caller FROM work_cancellation_requests WHERE work_item_id=?",
                (work_item_identifier,),
            )
            cancellation = await cursor.fetchone()
        return {
            "identifier": work_item_identifier,
            "kind": decode_json(_text(row[0])),
            "payload_schema_version": _integer(row[1]),
            "payload": decode_json(_text(row[2])),
            "state": _text(row[3]),
            "cancellation": None
            if cancellation is None
            else {
                "requested_at_utc_ns": _integer(cancellation[0]),
                "reason": _text(cancellation[1]),
                "caller": _text(cancellation[2]),
            },
            "attempt": _integer(row[4]),
            "result": None if row[5] is None else decode_json(_text(row[5])),
            "error": None if row[6] is None else decode_json(_text(row[6])),
            "created_at_utc_ns": _integer(row[7]),
            "eligible_at_utc_ns": _integer(row[8]),
            "worker_identifier": None if row[9] is None else _text(row[9]),
            "lease_expires_at_utc_ns": None if row[10] is None else _integer(row[10]),
            "latest_event_sequence": _integer(latest_event[0]),
            "latest_event_kind": _text(latest_event[1]),
            "latest_event_at_utc_ns": _integer(latest_event[2]),
            "last_lease_activity_at_utc_ns": (
                None if latest_event[3] is None else _integer(latest_event[3])
            ),
            "operations": [
                {"operation_identifier": _text(operation[0]), "attempt": _integer(operation[1])}
                for operation in operations
            ],
        }

    async def search_refresh_child_work_states(
        self,
        *,
        refresh_work_identifier: str,
        refreshed_search_run_record_identifier: str | None,
    ) -> dict[str, tuple[WorkState, ...]]:
        """Summarize child work linked to one search-refresh coordinator."""

        async with self._connections.reader() as connection:

            async def children(
                requester_kind: tuple[str, ...],
            ) -> tuple[tuple[WorkState, WorkState | None], ...]:
                cursor = await connection.execute(
                    """
                    SELECT DISTINCT work.id, work.state, extraction.state
                    FROM work_requests AS request
                    JOIN work_items AS work ON work.id = request.work_item_id
                    LEFT JOIN work_items AS extraction
                      ON extraction.id = json_extract(
                          work.result_json,
                          '$.extraction_work_identifier'
                      )
                    WHERE request.requester_kind_parts_json = ?
                      AND (
                          json_extract(
                              request.context_json,
                              '$.search_refresh_work_identifier'
                          ) = ?
                          OR (
                              ? IS NOT NULL
                              AND json_extract(
                                  request.context_json,
                                  '$.refreshed_search_run_record_identifier'
                              ) = ?
                          )
                      )
                    ORDER BY work.id
                    """,
                    (
                        _json(list(requester_kind)),
                        refresh_work_identifier,
                        refreshed_search_run_record_identifier,
                        refreshed_search_run_record_identifier,
                    ),
                )
                rows = await cursor.fetchall()
                return tuple(
                    (
                        WorkState(_text(row[1])),
                        None if row[2] is None else WorkState(_text(row[2])),
                    )
                    for row in rows
                )

            item_children = await children(("carl", "facebook", "search_refresh", "item"))
            image_children = await children(("carl", "facebook", "search_refresh", "image"))

        def referenced_extractions(
            children: tuple[tuple[WorkState, WorkState | None], ...],
        ) -> tuple[WorkState, ...]:
            return tuple(state for _, state in children if state is not None)

        return {
            "item_pages": tuple(state for state, _ in item_children),
            "item_extractions": referenced_extractions(item_children),
            "images": tuple(state for state, _ in image_children),
            "image_extractions": referenced_extractions(image_children),
        }

    async def activity_snapshot(
        self,
        *,
        captured_at_utc_ns: int,
        recent_window_ns: int,
        maximum_rows: int,
        coordination_scope: str | None = None,
    ) -> ActivitySnapshot:
        """Read one consistent, bounded view of durable work and network activity."""

        if captured_at_utc_ns < 0 or recent_window_ns <= 0 or not 1 <= maximum_rows <= 100:
            raise ValueError("Activity snapshot bounds are invalid")
        recent_since_utc_ns = max(0, captured_at_utc_ns - recent_window_ns)
        async with self._connections.reader() as connection:
            work_count_cursor = await connection.execute(
                """
                SELECT kind_parts_json, state, count(*)
                FROM work_items
                WHERE kind_parts_json IN (SELECT kind_parts_json FROM work_items GROUP BY kind_parts_json ORDER BY kind_parts_json LIMIT 33)
                GROUP BY kind_parts_json, state
                ORDER BY kind_parts_json, state
                """
            )
            work_count_rows = await work_count_cursor.fetchall()
            recent_work_cursor = await connection.execute(
                """
                SELECT work.kind_parts_json, event.event_kind, count(*)
                FROM work_events AS event INDEXED BY work_events_terminal_time
                CROSS JOIN work_items AS work ON work.id = event.work_item_id
                WHERE event.recorded_at_utc_ns >= ?
                  AND work.kind_parts_json IN (SELECT value FROM json_each(?))
                  AND event.event_kind IN ('completed', 'terminal_failure', 'cancelled')
                GROUP BY work.kind_parts_json, event.event_kind
                ORDER BY work.kind_parts_json, event.event_kind
                """,
                (
                    recent_since_utc_ns,
                    _json(list(dict.fromkeys(_text(row[0]) for row in work_count_rows))),
                ),
            )
            recent_work_rows = await recent_work_cursor.fetchall()
            active_work_cursor = await connection.execute(
                f"""
                SELECT substr(id,1,161), kind_parts_json, state, attempt, created_at_utc_ns,
                       eligible_at_utc_ns, lease_expires_at_utc_ns, lease_owner,
                       {scheduler_sql.payload_projection("payload_json")}, {scheduler_sql.result_projection("result_json")}, {scheduler_sql.error_projection("error_json")}
                FROM work_items
                WHERE state IN ('pending', 'leased', 'cancelling')
                ORDER BY state = 'leased' DESC, priority DESC, created_at_utc_ns, id
                LIMIT ?
                """,
                (maximum_rows,),
            )
            active_work_rows = await active_work_cursor.fetchall()
            terminal_work_cursor = await connection.execute(
                f"""
                SELECT substr(work.id,1,161), work.kind_parts_json, work.state, work.attempt,
                       work.created_at_utc_ns, work.eligible_at_utc_ns,
                       {scheduler_sql.payload_projection("work.payload_json")}, {scheduler_sql.result_projection("work.result_json")}, {scheduler_sql.error_projection("work.error_json")},
                       event.recorded_at_utc_ns
                FROM work_events AS event INDEXED BY work_events_terminal_time
                CROSS JOIN work_items AS work ON work.id = event.work_item_id
                WHERE event.event_kind IN ('completed', 'terminal_failure', 'cancelled')
                  AND event.recorded_at_utc_ns >= ?
                ORDER BY event.sequence DESC
                LIMIT ?
                """,
                (recent_since_utc_ns, maximum_rows),
            )
            terminal_work_rows = await terminal_work_cursor.fetchall()
            network_count_cursor = await connection.execute(
                """
                SELECT state, count(*)
                FROM network_activities
                GROUP BY state
                ORDER BY state
                """
            )
            network_count_rows = await network_count_cursor.fetchall()
            recent_network_cursor = await connection.execute(
                """
                SELECT state, count(*)
                FROM network_activities
                WHERE ended_at_utc_ns >= ?
                GROUP BY state
                ORDER BY state
                """,
                (recent_since_utc_ns,),
            )
            recent_network_rows = await recent_network_cursor.fetchall()
            network_path_cursor = await connection.execute(
                """
                SELECT scope.scope_identity_json, activity.state,
                       count(*),
                       sum(CASE WHEN activity.ended_at_utc_ns >= ? THEN 1 ELSE 0 END)
                FROM network_activities AS activity
                JOIN network_activity_scopes AS scope
                  ON scope.network_activity_id = activity.id
                 AND scope.scope_kind = 'network_path'
                 AND scope.scope_identity_json IN (SELECT scope_identity_json FROM network_activity_scopes WHERE scope_kind='network_path' GROUP BY scope_identity_json ORDER BY scope_identity_json LIMIT 17)
                GROUP BY scope.scope_identity_json, activity.state
                ORDER BY scope.scope_identity_json, activity.state
                """,
                (recent_since_utc_ns,),
            )
            network_path_rows = await network_path_cursor.fetchall()
            active_network_cursor = await connection.execute(
                """
                SELECT substr(activity.id,1,161), activity.kind_parts_json,
                       scope.scope_identity_json, activity.state,
                       substr(activity.network_session_identifier,1,161), activity.ordinal,
                       activity.attempt, activity.created_at_utc_ns,
                       activity.eligible_at_utc_ns, activity.admitted_at_utc_ns,
                       activity.permit_expires_at_utc_ns
                FROM network_activities AS activity
                JOIN network_activity_scopes AS scope
                  ON scope.network_activity_id = activity.id
                 AND scope.scope_kind = 'network_path'
                WHERE activity.state IN ('pending', 'admitted')
                ORDER BY activity.state = 'admitted' DESC,
                         activity.created_at_utc_ns, activity.id
                LIMIT ?
                """,
                (maximum_rows,),
            )
            active_network_rows = await active_network_cursor.fetchall()
            connectivity_cursor = await connection.execute(
                f"""
                SELECT paused, next_probe_utc_ns, probe_token, probe_expires_utc_ns,
                       checked_at_utc_ns, {scheduler_sql.probe_projection("result_json")}
                FROM network_connectivity WHERE singleton = 1
                """
            )
            connectivity_row = await connectivity_cursor.fetchone()
            scheduler = await scheduler_sql.snapshot(
                connection,
                self._scheduler_scope(coordination_scope),
                captured_at_utc_ns,
                recent_window_ns,
                8,
            )

        work_counts: dict[tuple[str, ...], dict[WorkState, int]] = {}
        for row in work_count_rows:
            kind = tuple(str(part) for part in decode_json(_text(row[0])))
            work_counts.setdefault(kind, {})[WorkState(_text(row[1]))] = _integer(row[2])
        recent_work_counts: dict[tuple[str, ...], dict[WorkState, int]] = {}
        for row in recent_work_rows:
            kind = tuple(str(part) for part in decode_json(_text(row[0])))
            recent_work_counts.setdefault(kind, {})[WorkState(_text(row[1]))] = _integer(row[2])
        work_kinds = tuple(
            WorkKindActivity(
                kind=scheduler_sql.bounded_parts(_json(list(kind)))[0],
                pending=counts.get(WorkState.PENDING, 0),
                leased=counts.get(WorkState.LEASED, 0),
                completed=counts.get(WorkState.COMPLETED, 0),
                cancelling=counts.get(WorkState.CANCELLING, 0),
                cancelled=counts.get(WorkState.CANCELLED, 0),
                recent_cancelled=recent_work_counts.get(kind, {}).get(WorkState.CANCELLED, 0),
                terminal_failure=counts.get(WorkState.TERMINAL_FAILURE, 0),
                recent_completed=recent_work_counts.get(kind, {}).get(WorkState.COMPLETED, 0),
                recent_terminal_failure=recent_work_counts.get(kind, {}).get(
                    WorkState.TERMINAL_FAILURE, 0
                ),
                labels_truncated=scheduler_sql.bounded_parts(_json(list(kind)))[1],
            )
            for kind, counts in sorted(work_counts.items())[:32]
        )

        def activity_from_row(
            row: Sequence[apsw.SQLiteValue], *, terminal: bool
        ) -> BoundedWorkActivity:
            raw_kind = _text(row[1])
            kind, kind_truncated = scheduler_sql.bounded_parts(raw_kind)
            payload = decode_json(_text(row[8] if not terminal else row[6]))
            result_value = row[9] if not terminal else row[7]
            error_value = row[10] if not terminal else row[8]
            result = None if result_value is None else decode_json(_text(result_value))
            error = None if error_value is None else decode_json(_text(error_value))
            return BoundedWorkActivity(
                identifier=_text(row[0])[:160],
                kind=kind,
                state=WorkState(_text(row[2])),
                attempt=_integer(row[3]),
                created_at_utc_ns=_integer(row[4]),
                eligible_at_utc_ns=_integer(row[5]),
                lease_expires_at_utc_ns=(None if terminal or row[6] is None else _integer(row[6])),
                worker_identifier=(None if terminal or row[7] is None else _text(row[7])[:160]),
                subject=scheduler_sql.label(work_subject(kind, payload)),
                stage=scheduler_sql.label(work_stage(result)),
                error_kind=scheduler_sql.label(work_error_kind(error)),
                terminal_at_utc_ns=(None if not terminal else _integer(row[9])),
                identity_truncated=len(_text(row[0])) > 160
                or (not terminal and row[7] is not None and len(_text(row[7])) > 160),
                labels_truncated=kind_truncated
                or any(
                    value is not None and len(value) > 160
                    for value in (
                        work_subject(kind, payload),
                        work_stage(result),
                        work_error_kind(error),
                    )
                ),
            )

        network_counts = {state: 0 for state in NetworkActivityState}
        for row in network_count_rows:
            network_counts[NetworkActivityState(_text(row[0]))] = _integer(row[1])
        recent_network_counts = {state: 0 for state in NetworkActivityState}
        for row in recent_network_rows:
            recent_network_counts[NetworkActivityState(_text(row[0]))] = _integer(row[1])
        path_counts: dict[
            tuple[str, ...], tuple[dict[NetworkActivityState, int], dict[NetworkActivityState, int]]
        ] = {}
        for row in network_path_rows:
            path = tuple(str(part) for part in decode_json(_text(row[0])))
            all_counts, recent_counts = path_counts.setdefault(
                path,
                (
                    {state: 0 for state in NetworkActivityState},
                    {state: 0 for state in NetworkActivityState},
                ),
            )
            state = NetworkActivityState(_text(row[1]))
            all_counts[state] = _integer(row[2])
            recent_counts[state] = _integer(row[3])

        probe_in_progress = (
            connectivity_row is not None
            and connectivity_row[2] is not None
            and connectivity_row[3] is not None
            and _integer(connectivity_row[3]) > captured_at_utc_ns
        )
        return ActivitySnapshot(
            captured_at_utc_ns=captured_at_utc_ns,
            recent_window_ns=recent_window_ns,
            work_kinds=work_kinds,
            work_kind_groups_omitted=None if len(work_counts) > 32 else 0,
            work_kind_groups_truncated=len(work_counts) > 32,
            network_path_groups_omitted=None if len(path_counts) > 16 else 0,
            network_path_groups_truncated=len(path_counts) > 16,
            scheduler=scheduler,
            active_work=tuple(activity_from_row(row, terminal=False) for row in active_work_rows),
            recent_terminal_work=tuple(
                activity_from_row(row, terminal=True) for row in terminal_work_rows
            ),
            network=NetworkActivityCounts(
                pending=network_counts[NetworkActivityState.PENDING],
                admitted=network_counts[NetworkActivityState.ADMITTED],
                completed=network_counts[NetworkActivityState.COMPLETED],
                skipped=network_counts[NetworkActivityState.SKIPPED],
                failed=network_counts[NetworkActivityState.FAILED],
                cancelled=network_counts[NetworkActivityState.CANCELLED],
                recent_completed=recent_network_counts[NetworkActivityState.COMPLETED],
                recent_skipped=recent_network_counts[NetworkActivityState.SKIPPED],
                recent_failed=recent_network_counts[NetworkActivityState.FAILED],
                recent_cancelled=recent_network_counts[NetworkActivityState.CANCELLED],
            ),
            network_paths=tuple(
                NetworkPathActivity(
                    path=scheduler_sql.bounded_parts(_json(list(path)))[0],
                    pending=all_counts[NetworkActivityState.PENDING],
                    admitted=all_counts[NetworkActivityState.ADMITTED],
                    completed=all_counts[NetworkActivityState.COMPLETED],
                    skipped=all_counts[NetworkActivityState.SKIPPED],
                    failed=all_counts[NetworkActivityState.FAILED],
                    cancelled=all_counts[NetworkActivityState.CANCELLED],
                    recent_completed=recent_counts[NetworkActivityState.COMPLETED],
                    recent_skipped=recent_counts[NetworkActivityState.SKIPPED],
                    recent_failed=recent_counts[NetworkActivityState.FAILED],
                    recent_cancelled=recent_counts[NetworkActivityState.CANCELLED],
                    labels_truncated=scheduler_sql.bounded_parts(_json(list(path)))[1],
                )
                for path, (all_counts, recent_counts) in sorted(path_counts.items())[:16]
            ),
            active_network=tuple(
                ActiveNetworkActivity(
                    identifier=_text(row[0])[:160],
                    kind=scheduler_sql.bounded_parts(_text(row[1]))[0],
                    path=scheduler_sql.bounded_parts(_text(row[2]))[0],
                    state=NetworkActivityState(_text(row[3])),
                    session_identifier=_text(row[4])[:160],
                    ordinal=_integer(row[5]),
                    attempt=_integer(row[6]),
                    created_at_utc_ns=_integer(row[7]),
                    eligible_at_utc_ns=_integer(row[8]),
                    admitted_at_utc_ns=None if row[9] is None else _integer(row[9]),
                    permit_expires_at_utc_ns=(None if row[10] is None else _integer(row[10])),
                    identity_truncated=len(_text(row[0])) > 160 or len(_text(row[4])) > 160,
                    labels_truncated=scheduler_sql.bounded_parts(_text(row[1]))[1]
                    or scheduler_sql.bounded_parts(_text(row[2]))[1],
                )
                for row in active_network_rows
            ),
            connectivity=(
                NetworkConnectivityActivity()
                if connectivity_row is None
                else NetworkConnectivityActivity(
                    paused=bool(connectivity_row[0]),
                    reason=(
                        "checking_connectivity"
                        if probe_in_progress
                        else "connectivity_outage"
                        if connectivity_row[0]
                        else None
                    ),
                    probe_in_progress=probe_in_progress,
                    next_probe_at_utc_ns=(
                        _integer(connectivity_row[1]) if connectivity_row[0] else None
                    ),
                    last_probe_at_utc_ns=(
                        None if connectivity_row[4] is None else _integer(connectivity_row[4])
                    ),
                    last_probe_result=(
                        None
                        if connectivity_row[5] is None
                        else decode_json(_text(connectivity_row[5]))
                    ),
                )
            ),
        )

    async def requested_work_for_requesters(
        self,
        *,
        requester_kind: tuple[str, ...],
        requester_identifiers: Sequence[str],
    ) -> tuple[tuple[str, str], ...]:
        """Seek bounded requester edges without one database round trip per image."""

        if len(requester_identifiers) > 1000 or any(
            not identifier for identifier in requester_identifiers
        ):
            raise ValueError("Work requester scope is invalid")
        if not requester_identifiers:
            return ()
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT requested.value, request.work_item_id
                FROM json_each(?) AS requested
                CROSS JOIN work_requests AS request INDEXED BY work_requests_requester
                  ON request.requester_identifier = requested.value
                 AND request.requester_kind_parts_json = ?
                GROUP BY requested.key, request.work_item_id
                ORDER BY CAST(requested.key AS INTEGER),
                         MIN(request.requested_at_utc_ns), MIN(request.id)
                """,
                (_json(list(requester_identifiers)), _json(list(requester_kind))),
            )
            rows = await cursor.fetchall()
        return tuple((_text(row[0]), _text(row[1])) for row in rows)

    async def requested_work_identifiers(
        self, *, requester_kind: tuple[str, ...], requester_identifier: str
    ) -> tuple[str, ...]:
        """Return work items linked to one structured durable requester."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT DISTINCT work_item_id
                FROM work_requests
                WHERE requester_kind_parts_json = ? AND requester_identifier = ?
                ORDER BY work_item_id
                """,
                (_json(list(requester_kind)), requester_identifier),
            )
            rows = await cursor.fetchall()
        return tuple(_text(row[0]) for row in rows)

    async def workspace_active_work(
        self,
        *,
        workspace_record_identifier: str,
        maximum_rows: int,
        maximum_failure_rows: int,
    ) -> tuple[int, int, int, tuple[WorkActivity, ...], tuple[WorkActivity, ...]]:
        """Return active and terminal root work requested by one review workspace."""

        if maximum_rows < 1 or maximum_failure_rows < 1:
            raise ValueError("Workspace work limits must be positive")
        async with self._connections.reader() as connection:
            count_cursor = await connection.execute(
                """
                WITH requested(work_item_id) AS (
                    SELECT DISTINCT work_item_id
                    FROM work_requests
                    WHERE requester_identifier = ?
                    UNION
                    SELECT json_extract(execution.value_json, '$.work_identifier')
                    FROM records AS execution JOIN objects AS object ON object.id=execution.object_id
                    WHERE object.kind_parts_json='["carl","marketplace","search_execution"]'
                      AND json_extract(execution.value_json, '$.search_record_identifier') = (
                          SELECT json_extract(workspace.value_json, '$.search_run_record_identifier')
                          FROM records AS workspace WHERE workspace.object_id=?
                      )
                )
                SELECT
                    count(*) FILTER (WHERE work.state = 'pending'),
                    count(*) FILTER (WHERE work.state IN ('leased','cancelling')),
                    count(*) FILTER (WHERE work.state = 'terminal_failure')
                FROM requested
                JOIN work_items AS work ON work.id = requested.work_item_id
                WHERE work.state IN ('pending', 'leased', 'cancelling', 'terminal_failure')
                """,
                (workspace_record_identifier, workspace_record_identifier),
            )
            count_row = await count_cursor.fetchone()
            active_cursor = await connection.execute(
                """
                WITH requested(work_item_id) AS (
                    SELECT DISTINCT work_item_id
                    FROM work_requests
                    WHERE requester_identifier = ?
                    UNION
                    SELECT json_extract(execution.value_json, '$.work_identifier')
                    FROM records AS execution JOIN objects AS object ON object.id=execution.object_id
                    WHERE object.kind_parts_json='["carl","marketplace","search_execution"]'
                      AND json_extract(execution.value_json, '$.search_record_identifier') = (
                          SELECT json_extract(workspace.value_json, '$.search_run_record_identifier')
                          FROM records AS workspace WHERE workspace.object_id=?
                      )
                )
                SELECT work.id, work.kind_parts_json, work.state, work.attempt,
                       work.created_at_utc_ns, work.eligible_at_utc_ns,
                       work.lease_expires_at_utc_ns, work.lease_owner,
                       work.payload_json, work.result_json, work.error_json
                FROM requested
                JOIN work_items AS work ON work.id = requested.work_item_id
                WHERE work.state IN ('pending', 'leased', 'cancelling')
                ORDER BY work.state = 'leased' DESC, work.priority DESC,
                         work.created_at_utc_ns, work.id
                LIMIT ?
                """,
                (workspace_record_identifier, workspace_record_identifier, maximum_rows),
            )
            rows = await active_cursor.fetchall()
            failure_cursor = await connection.execute(
                """
                WITH requested(work_item_id) AS (
                    SELECT DISTINCT work_item_id
                    FROM work_requests
                    WHERE requester_identifier = ?
                    UNION
                    SELECT json_extract(execution.value_json, '$.work_identifier')
                    FROM records AS execution JOIN objects AS object ON object.id=execution.object_id
                    WHERE object.kind_parts_json='["carl","marketplace","search_execution"]'
                      AND json_extract(execution.value_json, '$.search_record_identifier') = (
                          SELECT json_extract(workspace.value_json, '$.search_run_record_identifier')
                          FROM records AS workspace WHERE workspace.object_id=?
                      )
                )
                SELECT work.id, work.kind_parts_json, work.state, work.attempt,
                       work.created_at_utc_ns, work.eligible_at_utc_ns,
                       NULL, NULL, work.payload_json, work.result_json, work.error_json,
                       (
                           SELECT max(event.recorded_at_utc_ns)
                           FROM work_events AS event
                           WHERE event.work_item_id = work.id
                             AND event.event_kind = 'terminal_failure'
                       )
                FROM requested
                JOIN work_items AS work ON work.id = requested.work_item_id
                WHERE work.state = 'terminal_failure' OR (
                    work.state = 'completed'
                    AND json_extract(work.result_json, '$.state') = 'completed_with_failures'
                )
                ORDER BY 12 DESC, work.created_at_utc_ns DESC, work.id
                LIMIT ?
                """,
                (workspace_record_identifier, workspace_record_identifier, maximum_failure_rows),
            )
            failure_rows = await failure_cursor.fetchall()

        if count_row is None:
            raise RuntimeError("Workspace work count query returned no row")
        queued_count = _integer(count_row[0])
        in_progress_count = _integer(count_row[1])
        terminal_failure_count = _integer(count_row[2])

        def activity_from_row(row: Sequence[apsw.SQLiteValue]) -> WorkActivity:
            kind = tuple(str(part) for part in decode_json(_text(row[1])))
            payload = decode_json(_text(row[8]))
            result = None if row[9] is None else decode_json(_text(row[9]))
            error = None if row[10] is None else decode_json(_text(row[10]))
            return WorkActivity(
                identifier=_text(row[0]),
                kind=kind,
                state=WorkState(_text(row[2])),
                attempt=_integer(row[3]),
                created_at_utc_ns=_integer(row[4]),
                eligible_at_utc_ns=_integer(row[5]),
                lease_expires_at_utc_ns=None if row[6] is None else _integer(row[6]),
                worker_identifier=None if row[7] is None else _text(row[7]),
                subject=work_subject(kind, payload),
                stage=work_stage(result),
                error_kind=(
                    "completed_with_failures"
                    if isinstance(result, dict) and result.get("state") == "completed_with_failures"
                    else work_error_kind(error)
                ),
                terminal_at_utc_ns=None if len(row) < 12 or row[11] is None else _integer(row[11]),
            )

        return (
            queued_count,
            in_progress_count,
            terminal_failure_count,
            tuple(activity_from_row(row) for row in rows),
            tuple(activity_from_row(row) for row in failure_rows),
        )

    async def workspace_cancellation_status(
        self,
        *,
        workspace_record_identifier: str,
        maximum_rows: int,
    ) -> tuple[int, int, tuple[WorkActivity, ...]]:
        if maximum_rows < 1:
            raise ValueError("Workspace cancellation limit must be positive")
        async with self._connections.reader() as connection:
            identifiers = await self._workspace_work_identifiers(
                connection, workspace_record_identifier
            )
            scope = _json(list(identifiers))
            cursor = await connection.execute(
                "SELECT count(*) FILTER (WHERE state='cancelling'),count(*) FILTER (WHERE state='cancelled') FROM work_items WHERE id IN (SELECT value FROM json_each(?))",
                (scope,),
            )
            counts = await cursor.fetchone()
            assert counts is not None
            cursor = await connection.execute(
                """
                SELECT work.id,work.kind_parts_json,work.attempt,work.created_at_utc_ns,
                       work.eligible_at_utc_ns,work.payload_json,work.result_json,
                       (SELECT max(recorded_at_utc_ns) FROM work_events WHERE work_item_id=work.id AND event_kind='cancelled')
                FROM work_items AS work WHERE work.state='cancelled'
                  AND work.id IN (SELECT value FROM json_each(?))
                ORDER BY 8 DESC,work.id LIMIT ?
                """,
                (scope, maximum_rows),
            )
            rows = await cursor.fetchall()
        return (
            _integer(counts[0]),
            _integer(counts[1]),
            tuple(
                WorkActivity(
                    identifier=_text(row[0]),
                    kind=tuple(str(part) for part in decode_json(_text(row[1]))),
                    state=WorkState.CANCELLED,
                    attempt=_integer(row[2]),
                    created_at_utc_ns=_integer(row[3]),
                    eligible_at_utc_ns=_integer(row[4]),
                    lease_expires_at_utc_ns=None,
                    worker_identifier=None,
                    subject=work_subject(
                        tuple(str(part) for part in decode_json(_text(row[1]))),
                        decode_json(_text(row[5])),
                    ),
                    stage=work_stage(None if row[6] is None else decode_json(_text(row[6]))),
                    error_kind=None,
                    terminal_at_utc_ns=None if row[7] is None else _integer(row[7]),
                )
                for row in rows
            ),
        )

    async def workspace_partial_failure_count(self, workspace_record_identifier: str) -> int:
        """Count settled coordinators that explicitly retained child failures."""
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT count(*) FROM work_items AS work
                WHERE work.state='completed'
                  AND json_extract(work.result_json,'$.state')='completed_with_failures'
                  AND EXISTS (SELECT 1 FROM work_requests AS request
                    WHERE request.work_item_id=work.id AND request.requester_identifier=?)
                """,
                (workspace_record_identifier,),
            )
            row = await cursor.fetchone()
        assert row is not None
        return _integer(row[0])

    async def cutover_work_routes(
        self,
        *,
        replacements: dict[tuple[str, ...], tuple[str, ...]],
        operation_identifier: str,
        audit_record_identifier: str,
        at_utc_ns: int,
        started_monotonic_ns: int,
    ) -> int:
        """Explicitly revise executable routes and publish their audit atomically.

        Completed work and immutable acquisition evidence are not rewritten.
        Unexpired leases fence the entire cutover. Definitions are rebuilt while
        holding the writer reservation, keeping payloads, deduplication and scopes
        consistent; collisions abort the entire transaction.
        """

        async with self._connections.writer() as connection:
            cursor = await connection.execute(
                """
                SELECT id, kind_parts_json, payload_json, eligible_at_utc_ns,
                       state, lease_expires_at_utc_ns, attempt, deduplication_identity_json
                FROM work_items WHERE state != 'completed' ORDER BY id
                """
            )
            rows = await cursor.fetchall()
            revisions: list[tuple[WorkDefinition, tuple[apsw.SQLiteValue, ...]]] = []
            for row in rows:
                definition = rebuild_work_definition(
                    identifier=_text(row[0]),
                    kind=tuple(str(part) for part in decode_json(_text(row[1]))),
                    payload=decode_json(_text(row[2])),
                    not_before_utc_ns=_integer(row[3]),
                    replacements=replacements,
                )
                if definition is None:
                    continue
                staged_cursor = await connection.execute(
                    "SELECT 1 FROM stage_attempts WHERE work_item_id=? AND state IN ('pending','active') "
                    "UNION ALL SELECT 1 FROM work_stages WHERE work_item_id=? AND state IN ('pending','running') LIMIT 1",
                    (_text(row[0]), _text(row[0])),
                )
                if await staged_cursor.fetchone() is not None:
                    raise ValueError(
                        f"Route cutover requires staged work {_text(row[0])} to finish or cancel"
                    )
                if _text(row[4]) == "leased" and _integer(row[5]) > at_utc_ns:
                    raise ValueError(
                        f"Route cutover requires work {_text(row[0])} to release its lease"
                    )
                revisions.append((definition, row))
            audit: list[JsonValue] = []
            for definition, row in revisions:
                identifier = definition.identifier
                previous_state = _text(row[4])
                if previous_state == "leased":
                    _ = await self._recover_expired_work_attempt(
                        connection,
                        work_item_identifier=identifier,
                        attempt=_integer(row[6]),
                        recovered_at_utc_ns=at_utc_ns,
                    )
                    await connection.execute(
                        """
                        UPDATE work_items SET state='pending', lease_token=NULL,
                            lease_owner=NULL, lease_expires_at_utc_ns=NULL WHERE id=?
                        """,
                        (identifier,),
                    )
                    await self._append_work_event(
                        connection,
                        event_identifier=str(uuid4()),
                        work_item_identifier=identifier,
                        event_kind=WorkEventKind.LEASE_EXPIRED,
                        recorded_at_utc_ns=at_utc_ns,
                        data={
                            "reason": "explicit_route_cutover",
                            "operation_identifier": operation_identifier,
                        },
                    )
                try:
                    await connection.execute(
                        """
                        UPDATE work_items SET payload_json=?, payload_schema_version=?,
                            deduplication_identity_json=?, acquisition_resource_identity_json=?
                        WHERE id=?
                        """,
                        (
                            _json(definition.payload),
                            definition.payload_schema_version,
                            _json(list(definition.deduplication_identity)),
                            acquisition_resource_identity_json(definition.kind, definition.payload),
                            identifier,
                        ),
                    )
                except apsw.ConstraintError as error:
                    raise ValueError(
                        f"Route cutover would collide with existing work: {identifier}"
                    ) from error
                await connection.execute(
                    "DELETE FROM work_scopes WHERE work_item_id=?", (identifier,)
                )
                for scope in definition.scopes:
                    await connection.execute(
                        "INSERT INTO work_scopes VALUES (?, ?, ?)",
                        (identifier, scope.kind.value, _json(list(scope.identity))),
                    )
                audit.append(
                    {
                        "work_identifier": identifier,
                        "previous_state": previous_state,
                        "previous_payload": decode_json(_text(row[2])),
                        "previous_payload_sha256": hashlib.sha256(
                            _text(row[2]).encode()
                        ).hexdigest(),
                        "previous_deduplication_identity": decode_json(_text(row[7])),
                        "definition": definition.model_dump(mode="json"),
                    }
                )
            await self._complete_operation(
                connection,
                operation_id=operation_identifier,
                records=(
                    RecordDraft(
                        identifier=audit_record_identifier,
                        kind=("carl", "work", "route_cutover"),
                        schema_version=1,
                        value={"revisions": audit},
                    ),
                ),
                artifacts=(),
                inputs=(),
                outputs=(
                    NamedOutput(name=("route_cutover",), object_identifier=audit_record_identifier),
                ),
                result={"state": "completed", "revised_work_count": len(revisions)},
                ended_at_utc=_utc_text_from_ns(at_utc_ns),
                duration_ns=max(0, perf_counter_ns() - started_monotonic_ns),
            )
        return len(revisions)

    async def retry_terminal_collect_search_work(
        self,
        *,
        work_item_identifier: str,
        retried_at_utc_ns: int,
        event_identifier: str,
        reason: JsonValue,
        payload_schema_version: int,
    ) -> int:
        """Atomically give one terminal search work item a fresh retry budget."""

        if retried_at_utc_ns < 0 or payload_schema_version < 1:
            raise ValueError("Search retry arguments are invalid")
        async with self._connections.writer() as connection:
            cursor = await connection.execute(
                """
                SELECT attempt, payload_json
                FROM work_items
                WHERE id = ? AND kind_parts_json = ? AND state = 'terminal_failure'
                """,
                (work_item_identifier, _json(list(COLLECT_SEARCH_WORK_KIND))),
            )
            row = await cursor.fetchone()
            if row is None:
                raise ValueError("Search work is not in terminal failure")
            previous_attempt = _integer(row[0])
            payload = decode_json(_text(row[1]))
            routing = payload.get("routing") if isinstance(payload, dict) else None
            if not isinstance(routing, list) or not all(
                isinstance(part, str) and part for part in routing
            ):
                raise ValueError("Search work has no valid network route")
            await connection.execute(
                """
                INSERT OR IGNORE INTO work_scopes(
                    work_item_id, scope_kind, scope_identity_json
                ) VALUES (?, 'network_path', ?)
                """,
                (
                    work_item_identifier,
                    _json(["search_acquisition", *routing]),
                ),
            )
            await connection.execute(
                """
                UPDATE work_items
                SET state = 'pending', eligible_at_utc_ns = ?,
                    payload_schema_version = ?,
                    payload_json = json_set(payload_json, '$.retry_attempt_offset', attempt),
                    acquisition_resource_identity_json = ?,
                    result_json = NULL, error_json = NULL,
                    lease_token = NULL, lease_owner = NULL,
                    lease_expires_at_utc_ns = NULL
                WHERE id = ? AND state = 'terminal_failure'
                """,
                (
                    retried_at_utc_ns,
                    payload_schema_version,
                    acquisition_resource_identity_json(COLLECT_SEARCH_WORK_KIND, payload),
                    work_item_identifier,
                ),
            )
            if await connection.changes() != 1:
                raise RuntimeError("Terminal search work changed during retry")
            await self._append_work_event(
                connection,
                event_identifier=event_identifier,
                work_item_identifier=work_item_identifier,
                event_kind=WorkEventKind.ENQUEUED,
                recorded_at_utc_ns=retried_at_utc_ns,
                data={
                    "reason": reason,
                    "previous_attempt": previous_attempt,
                    "retry_attempt_offset": previous_attempt,
                    "payload_schema_version": payload_schema_version,
                },
            )
        return previous_attempt

    async def retry_terminal_ebay_search_work(
        self,
        *,
        work_item_identifier: str,
        retried_at_utc_ns: int,
        event_identifier: str,
        reason: JsonValue,
        payload_schema_version: int,
        acquisition_stack: str | None = None,
    ) -> int:
        """Give an exact eBay search a new retry budget without erasing history."""
        async with self._connections.writer() as connection:
            cursor = await connection.execute(
                "SELECT attempt,payload_json FROM work_items WHERE id=? AND kind_parts_json=? AND state='terminal_failure'",
                (work_item_identifier, _json(["carl", "ebay", "collect", "search"])),
            )
            row = await cursor.fetchone()
            if row is None:
                raise ValueError("eBay search work is not in terminal failure")
            previous_attempt = _integer(row[0])
            payload = CollectEbaySearchPayload.model_validate_json(_text(row[1]))
            previous_stack = payload.request.stack_identifier
            request = payload.request
            if acquisition_stack is not None:
                request = type(payload.request).model_validate(
                    {**payload.request.model_dump(), "stack_identifier": acquisition_stack}
                )
            payload = CollectEbaySearchPayload(
                request=request, retry_attempt_offset=previous_attempt
            )
            definition = collect_ebay_search_work(
                identifier=work_item_identifier,
                payload=payload,
                not_before_utc_ns=retried_at_utc_ns,
            )
            cursor = await connection.execute(
                "SELECT id FROM work_items WHERE kind_parts_json=? AND deduplication_identity_json=? AND state IN ('pending','leased') AND id<>?",
                (
                    _json(list(definition.kind)),
                    _json(list(definition.deduplication_identity)),
                    work_item_identifier,
                ),
            )
            if await cursor.fetchone() is not None:
                raise ValueError("An equivalent eBay search is already active; no retry performed")
            cooldowns = await self._ebay_search_stack_cooldowns(connection, retried_at_utc_ns)
            eligible_at = max(retried_at_utc_ns, cooldowns.get(payload.request.stack_identifier, 0))
            await connection.execute(
                """
                UPDATE work_items SET state='pending', eligible_at_utc_ns=?,
                    payload_schema_version=?,
                    payload_json=?,deduplication_identity_json=?,
                    acquisition_resource_identity_json=?,
                    result_json=NULL,error_json=NULL,lease_token=NULL,lease_owner=NULL,
                    lease_expires_at_utc_ns=NULL
                WHERE id=? AND state='terminal_failure'
                """,
                (
                    eligible_at,
                    payload_schema_version,
                    _json(payload.as_json()),
                    _json(list(definition.deduplication_identity)),
                    acquisition_resource_identity_json(definition.kind, definition.payload),
                    work_item_identifier,
                ),
            )
            for scope in definition.scopes:
                await connection.execute(
                    "INSERT OR IGNORE INTO work_scopes VALUES (?,?,?)",
                    (work_item_identifier, scope.kind.value, _json(list(scope.identity))),
                )
            if previous_stack != payload.request.stack_identifier:
                await connection.execute(
                    "DELETE FROM work_scopes WHERE work_item_id=? AND scope_kind='network_path' AND scope_identity_json=?",
                    (work_item_identifier, _json(["ebay", "search", previous_stack])),
                )
            await self._append_work_event(
                connection,
                event_identifier=event_identifier,
                work_item_identifier=work_item_identifier,
                event_kind=WorkEventKind.ENQUEUED,
                recorded_at_utc_ns=retried_at_utc_ns,
                data={
                    "reason": reason,
                    "previous_attempt": previous_attempt,
                    "retry_attempt_offset": previous_attempt,
                    "payload_schema_version": payload_schema_version,
                    "previous_acquisition_stack": previous_stack,
                    "acquisition_stack": payload.request.stack_identifier,
                    "eligible_at_utc_ns": eligible_at,
                },
            )
        return previous_attempt

    async def _facebook_search_route_cooldowns(
        self, connection: AsyncConnection, now_utc_ns: int
    ) -> dict[str, int]:
        """Historical Marketplace cooldown markers no longer gate acquisition."""
        return {}

    async def _ebay_search_stack_cooldowns(
        self, connection: AsyncConnection, now_utc_ns: int
    ) -> dict[str, int]:
        """Retain the compatibility query without reapplying retired cooldowns."""
        return {}

    async def _defer_ebay_search_siblings(
        self,
        connection: AsyncConnection,
        work_item_identifier: str,
        failure: JsonValue,
        now_utc_ns: int,
        event_identifier: str,
    ) -> None:
        if not isinstance(failure, dict) or not isinstance(
            (cooldown := failure.get("cooldown")), dict
        ):
            return
        stack, until_ns = cooldown.get("stack_identifier"), cooldown.get("until_utc_ns")
        if (
            not isinstance(stack, str)
            or not isinstance(until_ns, int)
            or isinstance(until_ns, bool)
        ):
            raise ValueError("Invalid eBay search cooldown marker")
        if until_ns <= now_utc_ns:
            return
        cursor = await connection.execute(
            "SELECT kind_parts_json,payload_json FROM work_items WHERE id=?",
            (work_item_identifier,),
        )
        row = await cursor.fetchone()
        if row is None or _text(row[0]) != _json(list(COLLECT_EBAY_SEARCH_WORK_KIND)):
            raise ValueError("eBay search cooldown belongs to another work kind")
        payload = CollectEbaySearchPayload.model_validate_json(_text(row[1]))
        if payload.request.stack_identifier != stack:
            raise ValueError("eBay search cooldown stack disagrees with its attempt")
        cursor = await connection.execute(
            "SELECT id FROM work_items WHERE kind_parts_json=? AND state='pending' AND COALESCE(json_extract(payload_json,'$.request.stack_identifier'),'ebay_anonymous')=? AND eligible_at_utc_ns<? ORDER BY id",
            (_json(list(COLLECT_EBAY_SEARCH_WORK_KIND)), stack, until_ns),
        )
        for index, row in enumerate(await cursor.fetchall()):
            identifier = _text(row[0])
            await connection.execute(
                "UPDATE work_items SET eligible_at_utc_ns=MAX(eligible_at_utc_ns,?) WHERE id=?",
                (until_ns, identifier),
            )
            await self._append_work_event(
                connection,
                event_identifier=f"{event_identifier}-cooldown-{index}",
                work_item_identifier=identifier,
                event_kind=WorkEventKind.ENQUEUED,
                recorded_at_utc_ns=now_utc_ns,
                data={
                    "reason": {
                        "kind": "ebay_search_shared_cooldown",
                        "source_work_identifier": work_item_identifier,
                        "cooldown": cooldown,
                    },
                    "eligible_at_utc_ns": until_ns,
                },
            )

    async def defer_pending_collect_search_work_for_route(
        self,
        *,
        routing: tuple[str, ...],
        eligible_at_utc_ns: int,
        recorded_at_utc_ns: int,
        event_batch_identifier: str,
        reason: JsonValue,
    ) -> tuple[str, ...]:
        """Apply a shared-route backoff to queued searches after a transport failure."""

        if (
            not routing
            or any(not part for part in routing)
            or eligible_at_utc_ns < recorded_at_utc_ns
            or recorded_at_utc_ns < 0
            or not event_batch_identifier
        ):
            raise ValueError("Search route backoff arguments are invalid")
        async with self._connections.writer() as connection:
            cursor = await connection.execute(
                """
                SELECT id
                FROM work_items
                WHERE kind_parts_json = ? AND state = 'pending'
                  AND json_extract(payload_json, '$.routing') = json(?)
                  AND eligible_at_utc_ns < ?
                ORDER BY created_at_utc_ns, id
                """,
                (
                    _json(list(COLLECT_SEARCH_WORK_KIND)),
                    _json(list(routing)),
                    eligible_at_utc_ns,
                ),
            )
            identifiers = tuple(_text(row[0]) for row in await cursor.fetchall())
            if not identifiers:
                return ()
            identifiers_json = _json(list(identifiers))
            await connection.execute(
                """
                UPDATE work_items
                SET eligible_at_utc_ns = ?
                WHERE id IN (SELECT value FROM json_each(?))
                  AND state = 'pending' AND eligible_at_utc_ns < ?
                """,
                (eligible_at_utc_ns, identifiers_json, eligible_at_utc_ns),
            )
            if await connection.changes() != len(identifiers):
                raise RuntimeError("Pending search work changed during route backoff")
            await connection.execute(
                """
                INSERT INTO work_events(
                    id, work_item_id, event_kind, recorded_at_utc_ns, data_json
                )
                SELECT ? || '-' || printf('%08d', key), value, 'enqueued', ?,
                       json_object(
                           'reason', json(?),
                           'eligible_at_utc_ns', ?
                       )
                FROM json_each(?)
                ORDER BY key
                """,
                (
                    event_batch_identifier,
                    recorded_at_utc_ns,
                    _json(reason),
                    eligible_at_utc_ns,
                    identifiers_json,
                ),
            )
        return identifiers

    async def requested_work_edges(
        self, *, requester_kind: tuple[str, ...], requester_identifier: str
    ) -> tuple[dict[str, JsonValue], ...]:
        """Return ordered durable requester edges with their retained contexts."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT work_item_id, context_json
                FROM work_requests
                WHERE requester_kind_parts_json = ? AND requester_identifier = ?
                ORDER BY requested_at_utc_ns, id
                """,
                (_json(list(requester_kind)), requester_identifier),
            )
            rows = await cursor.fetchall()
        return tuple(
            {
                "work_identifier": _text(row[0]),
                "context": decode_json(_text(row[1])),
            }
            for row in rows
        )

    async def work_states(self, identifiers: tuple[str, ...]) -> tuple[WorkState, ...]:
        """Read work states in caller order without SQLite parameter-count limits."""

        if not identifiers:
            return ()
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT requested.key, work.state
                FROM json_each(?) AS requested
                JOIN work_items AS work ON work.id = requested.value
                ORDER BY requested.key
                """,
                (_json(list(identifiers)),),
            )
            rows = await cursor.fetchall()
        if len(rows) != len(identifiers):
            raise KeyError("One or more requested work items do not exist")
        return tuple(WorkState(_text(row[1])) for row in rows)

    async def work_summaries(
        self, identifiers: tuple[str, ...]
    ) -> tuple[dict[str, JsonValue], ...]:
        """Read bounded status inputs in caller order without loading work payloads or results."""

        if not identifiers:
            return ()
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT requested.key, work.id, work.state, work.error_json,
                       (
                           SELECT max(event.sequence)
                           FROM work_events AS event
                           WHERE event.work_item_id = work.id
                             AND event.event_kind IN ('completed', 'terminal_failure', 'cancel_requested', 'cancelled')
                       )
                FROM json_each(?) AS requested
                JOIN work_items AS work ON work.id = requested.value
                ORDER BY requested.key
                """,
                (_json(list(identifiers)),),
            )
            rows = await cursor.fetchall()
        if len(rows) != len(identifiers):
            raise KeyError("One or more requested work items do not exist")
        return tuple(
            {
                "identifier": _text(row[1]),
                "state": _text(row[2]),
                "error": None if row[3] is None else decode_json(_text(row[3])),
                "latest_event_sequence": 0 if row[4] is None else _integer(row[4]),
            }
            for row in rows
        )

    async def retry_terminal_facebook_image_work_for_source(
        self,
        *,
        source_identifier: str,
        maximum_items: int,
        retried_at_utc_ns: int,
        retry_batch_identifier: str,
        reason: JsonValue,
    ) -> tuple[int, tuple[str, ...]]:
        """Atomically requeue terminal image work linked to a refresh or search run."""

        if maximum_items < 1 or retried_at_utc_ns < 0 or not retry_batch_identifier:
            raise ValueError("Image retry selection arguments are invalid")
        async with self._connections.writer() as connection:
            cursor = await connection.execute(
                """
                SELECT work.id, work_operation.operation_id,
                       count(*) OVER () AS matched_terminal_failures
                FROM work_items AS work
                JOIN work_operations AS work_operation
                  ON work_operation.work_item_id = work.id
                 AND work_operation.attempt = work.attempt
                JOIN operations AS operation
                  ON operation.id = work_operation.operation_id
                 AND operation.result_json IS NOT NULL
                WHERE work.kind_parts_json = ?
                  AND work.state = 'terminal_failure'
                  AND EXISTS (
                      SELECT 1
                      FROM work_requests AS request
                      WHERE request.work_item_id = work.id
                        AND (
                            json_extract(
                                request.context_json,
                                '$.search_refresh_work_identifier'
                            ) = ?
                            OR EXISTS (
                                SELECT 1
                                FROM objects AS plan
                                JOIN records AS plan_record
                                  ON plan_record.object_id = plan.id
                                JOIN json_each(
                                    plan_record.value_json,
                                    '$.search_run_record_identifiers'
                                ) AS source_search_run
                                WHERE plan.id = json_extract(
                                    request.context_json,
                                    '$.image_followup_plan_record_identifier'
                                )
                                  AND plan.kind_parts_json =
                                      '["carl","facebook","image_followup_plan"]'
                                  AND source_search_run.value = ?
                            )
                        )
                  )
                ORDER BY work.created_at_utc_ns, work.id
                LIMIT ?
                """,
                (
                    _json(list(COLLECT_IMAGE_WORK_KIND)),
                    source_identifier,
                    source_identifier,
                    maximum_items,
                ),
            )
            rows = await cursor.fetchall()
            if not rows:
                return 0, ()
            matched = _integer(rows[0][2])
            retries = tuple(
                {
                    "work_identifier": _text(row[0]),
                    "checkpoint_operation_identifier": _text(row[1]),
                    "event_identifier": f"{retry_batch_identifier}-{index:08d}",
                }
                for index, row in enumerate(rows)
            )
            retries_json = _json(list(retries))
            await connection.execute(
                """
                WITH retry AS (
                    SELECT
                        json_extract(value, '$.work_identifier') AS work_identifier,
                        json_extract(
                            value,
                            '$.checkpoint_operation_identifier'
                        ) AS checkpoint_operation_identifier
                    FROM json_each(?)
                )
                UPDATE work_items
                SET state = 'pending', eligible_at_utc_ns = ?,
                    payload_schema_version = ?,
                    result_json = (
                        SELECT operation.result_json
                        FROM retry
                        JOIN operations AS operation
                          ON operation.id = retry.checkpoint_operation_identifier
                        WHERE retry.work_identifier = work_items.id
                    ),
                    error_json = NULL, lease_token = NULL, lease_owner = NULL,
                    lease_expires_at_utc_ns = NULL
                WHERE id IN (SELECT work_identifier FROM retry)
                  AND state = 'terminal_failure'
                """,
                (
                    retries_json,
                    retried_at_utc_ns,
                    COLLECT_IMAGE_WORK_SCHEMA_VERSION,
                ),
            )
            if await connection.changes() != len(retries):
                raise RuntimeError("Terminal image work changed during bulk recovery")
            await connection.execute(
                """
                WITH retry AS (
                    SELECT
                        json_extract(value, '$.work_identifier') AS work_identifier,
                        json_extract(
                            value,
                            '$.checkpoint_operation_identifier'
                        ) AS checkpoint_operation_identifier,
                        json_extract(value, '$.event_identifier') AS event_identifier
                    FROM json_each(?)
                )
                INSERT INTO work_events(
                    id, work_item_id, event_kind, recorded_at_utc_ns, data_json
                )
                SELECT event_identifier, work_identifier, 'enqueued', ?,
                       json_object(
                           'reason', json(?),
                           'recovered_checkpoint_operation_identifier',
                           checkpoint_operation_identifier,
                           'payload_schema_version', ?
                       )
                FROM retry
                ORDER BY work_identifier
                """,
                (
                    retries_json,
                    retried_at_utc_ns,
                    _json(reason),
                    COLLECT_IMAGE_WORK_SCHEMA_VERSION,
                ),
            )
        return matched, tuple(str(retry["work_identifier"]) for retry in retries)

    async def retry_terminal_work_from_operation(
        self,
        *,
        work_item_identifier: str,
        checkpoint_operation_identifier: str,
        retried_at_utc_ns: int,
        event_identifier: str,
        reason: JsonValue,
        payload_schema_version: int | None = None,
    ) -> None:
        """Retry terminal work from an earlier result produced by that same work item."""

        if payload_schema_version is not None and payload_schema_version < 1:
            raise ValueError("Payload schema version must be positive")

        async with self._connections.writer() as connection:
            cursor = await connection.execute(
                """
                SELECT operation.result_json
                FROM work_operations AS work_operation
                JOIN operations AS operation
                  ON operation.id = work_operation.operation_id
                JOIN work_items AS work
                  ON work.id = work_operation.work_item_id
                WHERE work.id = ? AND work.state = 'terminal_failure'
                  AND work_operation.operation_id = ?
                  AND operation.result_json IS NOT NULL
                """,
                (work_item_identifier, checkpoint_operation_identifier),
            )
            row = await cursor.fetchone()
            if row is None:
                raise ValueError(
                    "Recovery requires terminal work and a result from one of its operations"
                )
            checkpoint_result = decode_json(_text(row[0]))
            await connection.execute(
                """
                UPDATE work_items
                SET state = 'pending', eligible_at_utc_ns = ?, result_json = ?,
                    error_json = NULL, lease_token = NULL, lease_owner = NULL,
                    lease_expires_at_utc_ns = NULL,
                    payload_schema_version = COALESCE(?, payload_schema_version)
                WHERE id = ? AND state = 'terminal_failure'
                """,
                (
                    retried_at_utc_ns,
                    _json(checkpoint_result),
                    payload_schema_version,
                    work_item_identifier,
                ),
            )
            if await connection.changes() != 1:
                raise RuntimeError("Terminal work changed during recovery")
            await self._append_work_event(
                connection,
                event_identifier=event_identifier,
                work_item_identifier=work_item_identifier,
                event_kind=WorkEventKind.ENQUEUED,
                recorded_at_utc_ns=retried_at_utc_ns,
                data={
                    "reason": reason,
                    "recovered_checkpoint_operation_identifier": (checkpoint_operation_identifier),
                },
            )

    async def _require_active_lease(
        self,
        connection: AsyncConnection,
        *,
        work_item_identifier: str,
        lease_token: str,
        worker_identifier: str,
        now_utc_ns: int,
    ) -> tuple[int, int]:
        cancellation = await connection.execute(
            "SELECT 1 FROM work_items WHERE id=? AND state='cancelling' AND lease_token=? AND lease_owner=?",
            (work_item_identifier, lease_token, worker_identifier),
        )
        if await cancellation.fetchone() is not None:
            raise WorkCancellationRequestedError(work_item_identifier)
        cursor = await connection.execute(
            """
            SELECT attempt, lease_expires_at_utc_ns
            FROM work_items
            WHERE id = ? AND state = 'leased' AND lease_token = ?
              AND lease_owner = ? AND lease_expires_at_utc_ns > ?
            """,
            (work_item_identifier, lease_token, worker_identifier, now_utc_ns),
        )
        row = await cursor.fetchone()
        if row is None:
            raise LeaseLostError("The work lease is missing, expired, or owned by another worker")
        if await self._has_cancelled_work_dependencies(connection, work_item_identifier):
            raise WorkCancellationRequestedError(work_item_identifier)
        return _integer(row[0]), _integer(row[1])

    async def bind_facebook_search_route(
        self,
        *,
        work_item_identifier: str,
        lease_token: str,
        worker_identifier: str,
        routing: tuple[str, ...],
        utc_now_ns: Callable[[], int],
    ) -> None:
        """Bind effective-route evidence to an active lease without a capacity cap.

        Requested scopes and payloads remain intact. Only previous effective-route
        scopes are removed. Physical resource permits and request policy govern
        execution; route provenance remains available across processes.
        """

        if not routing or any(not isinstance(part, str) or not part for part in routing):
            raise ValueError("An effective route needs nonempty string parts")
        async with self._connections.writer() as connection:
            scope_json = _json(list(facebook_effective_search_scope(routing).identity))
            # Acquire SQLite's writer reservation before reading admission state.
            # A stale/invalid lease rolls this deletion back with the transaction.
            await connection.execute(
                """
                DELETE FROM work_scopes
                WHERE work_item_id = ? AND scope_kind = 'network_path'
                  AND json_extract(scope_identity_json, '$[0]') = ?
                  AND scope_identity_json != ?
                """,
                (work_item_identifier, FACEBOOK_EFFECTIVE_SEARCH_SCOPE_FAMILY, scope_json),
            )
            now_utc_ns = utc_now_ns()
            await self._require_active_lease(
                connection,
                work_item_identifier=work_item_identifier,
                lease_token=lease_token,
                worker_identifier=worker_identifier,
                now_utc_ns=now_utc_ns,
            )
            await connection.execute(
                "INSERT OR IGNORE INTO work_scopes VALUES (?, 'network_path', ?)",
                (work_item_identifier, scope_json),
            )

    async def renew_lease(
        self,
        *,
        work_item_identifier: str,
        lease_token: str,
        worker_identifier: str,
        lease_duration_ns: int,
        utc_now_ns: Callable[[], int],
        event_identifier: str,
    ) -> None:
        if lease_duration_ns <= 0:
            raise ValueError("Lease duration must be positive")
        async with self._connections.writer() as connection:
            now_utc_ns = utc_now_ns()
            new_expires_at_utc_ns = now_utc_ns + lease_duration_ns
            attempt, previous_expiry = await self._require_active_lease(
                connection,
                work_item_identifier=work_item_identifier,
                lease_token=lease_token,
                worker_identifier=worker_identifier,
                now_utc_ns=now_utc_ns,
            )
            if new_expires_at_utc_ns <= previous_expiry:
                raise ValueError("A lease renewal must extend the existing lease")
            await connection.execute(
                """
                UPDATE work_items
                SET lease_expires_at_utc_ns = ?
                WHERE id = ? AND lease_token = ? AND lease_owner = ?
                """,
                (
                    new_expires_at_utc_ns,
                    work_item_identifier,
                    lease_token,
                    worker_identifier,
                ),
            )
            await self._append_work_event(
                connection,
                event_identifier=event_identifier,
                work_item_identifier=work_item_identifier,
                event_kind=WorkEventKind.LEASE_RENEWED,
                recorded_at_utc_ns=now_utc_ns,
                data={
                    "worker_identifier": worker_identifier,
                    "lease_token": lease_token,
                    "attempt": attempt,
                    "previous_expires_at_utc_ns": previous_expiry,
                    "new_expires_at_utc_ns": new_expires_at_utc_ns,
                },
            )

    async def release_lease(
        self,
        *,
        work_item_identifier: str,
        lease_token: str,
        worker_identifier: str,
        utc_now_ns: Callable[[], int],
        eligible_at_utc_ns: int,
        reason: JsonValue,
        event_identifier: str,
    ) -> None:
        async with self._connections.writer() as connection:
            now_utc_ns = utc_now_ns()
            if eligible_at_utc_ns < now_utc_ns:
                raise ValueError("Released work cannot become eligible in the past")
            attempt, _ = await self._require_active_lease(
                connection,
                work_item_identifier=work_item_identifier,
                lease_token=lease_token,
                worker_identifier=worker_identifier,
                now_utc_ns=now_utc_ns,
            )
            await connection.execute(
                """
                UPDATE work_items
                SET state = 'pending', eligible_at_utc_ns = ?, lease_token = NULL,
                    lease_owner = NULL, lease_expires_at_utc_ns = NULL
                WHERE id = ? AND lease_token = ? AND lease_owner = ?
                """,
                (
                    eligible_at_utc_ns,
                    work_item_identifier,
                    lease_token,
                    worker_identifier,
                ),
            )
            await self._append_work_event(
                connection,
                event_identifier=event_identifier,
                work_item_identifier=work_item_identifier,
                event_kind=WorkEventKind.RELEASED,
                recorded_at_utc_ns=now_utc_ns,
                data={
                    "worker_identifier": worker_identifier,
                    "lease_token": lease_token,
                    "attempt": attempt,
                    "eligible_at_utc_ns": eligible_at_utc_ns,
                    "reason": reason,
                },
            )

    async def finalize_interrupted_work(
        self,
        *,
        work_item_identifier: str,
        lease_token: str,
        worker_identifier: str,
        operation_id: str,
        utc_now_ns: Callable[[], int],
        event_identifier: str,
        reason: str,
        ended_at_utc: str,
        duration_ns: int,
        possibly_dispatched: bool,
        inputs: Sequence[NamedInput] = (),
        records: Sequence[RecordDraft] = (),
        artifacts: Sequence[ArtifactDraft] = (),
        outputs: Sequence[NamedOutput] = (),
    ) -> bool:
        """Reconcile interruption atomically; committed outcomes and replacement leases win.

        Setup may not have created an operation yet. Conversely, cancellation
        may hide a successful commit. Neither case is inferred from local flags.
        """

        async with self._connections.writer() as connection:
            now_utc_ns = utc_now_ns()
            cursor = await connection.execute(
                "SELECT state,lease_token,lease_owner FROM work_items WHERE id=?",
                (work_item_identifier,),
            )
            current = await cursor.fetchone()
            owns_lease = current is not None and tuple(current[1:]) == (
                lease_token,
                worker_identifier,
            )
            if (
                owns_lease
                and current is not None
                and (
                    current[0] == "cancelling"
                    or (
                        current[0] == "leased"
                        and await self._has_cancelled_work_dependencies(
                            connection, work_item_identifier
                        )
                    )
                )
            ):
                if current[0] == "leased":
                    await self.cancel_work(
                        work_item_identifier,
                        reason="child_cancelled",
                        caller=f"worker:{worker_identifier}",
                        requested_at_utc_ns=now_utc_ns,
                    )
                return await self.finalize_cancelled_work(
                    work_item_identifier=work_item_identifier,
                    lease_token=lease_token,
                    worker_identifier=worker_identifier,
                    operation_id=operation_id,
                    utc_now_ns=lambda: now_utc_ns,
                    event_identifier=event_identifier,
                    ended_at_utc=ended_at_utc,
                    duration_ns=duration_ns,
                    inputs=inputs,
                    records=records,
                    artifacts=artifacts,
                    outputs=outputs,
                )
            cursor = await connection.execute(
                """
                SELECT operations.state, work_operations.work_item_id,
                       work_operations.lease_token, work_operations.worker_identifier
                FROM operations
                LEFT JOIN work_operations ON work_operations.operation_id = operations.id
                WHERE operations.id = ?
                """,
                (operation_id,),
            )
            operation = await cursor.fetchone()
            if operation is not None:
                if tuple(operation[1:]) != (work_item_identifier, lease_token, worker_identifier):
                    raise LeaseLostError("The interrupted operation belongs to another attempt")
                if operation[0] != "started":
                    return False
                await self._fail_operation(
                    connection,
                    operation_id=operation_id,
                    error={"kind": reason, "possibly_dispatched": possibly_dispatched},
                    result={"state": reason},
                    ended_at_utc=ended_at_utc,
                    duration_ns=duration_ns,
                )
            try:
                attempt, _ = await self._require_active_lease(
                    connection,
                    work_item_identifier=work_item_identifier,
                    lease_token=lease_token,
                    worker_identifier=worker_identifier,
                    now_utc_ns=now_utc_ns,
                )
            except LeaseLostError:
                # A bound old operation can be failed, but never release its successor.
                return False
            await connection.execute(
                """
                UPDATE work_items
                SET state = 'pending', eligible_at_utc_ns = ?, lease_token = NULL,
                    lease_owner = NULL, lease_expires_at_utc_ns = NULL
                WHERE id = ? AND lease_token = ? AND lease_owner = ?
                """,
                (now_utc_ns, work_item_identifier, lease_token, worker_identifier),
            )
            await self._append_work_event(
                connection,
                event_identifier=event_identifier,
                work_item_identifier=work_item_identifier,
                event_kind=WorkEventKind.RELEASED,
                recorded_at_utc_ns=now_utc_ns,
                data={
                    "worker_identifier": worker_identifier,
                    "lease_token": lease_token,
                    "attempt": attempt,
                    "eligible_at_utc_ns": now_utc_ns,
                    "reason": {
                        "kind": "worker_cancelled" if reason == "cancelled" else reason,
                        "operation_identifier": operation_id,
                        "possibly_dispatched": possibly_dispatched,
                        "decision": "retry",
                    },
                },
            )
            return True

    async def _begin_operation(
        self,
        connection: AsyncConnection,
        *,
        operation_id: str,
        component: Component,
        provenance: CodeProvenance,
        invocation: dict[str, JsonValue],
        configuration: JsonValue,
        started_at_utc: str,
        inputs: Sequence[tuple[tuple[str, ...], str]] = (),
    ) -> None:
        code_state_id = await self._intern_code_state(
            connection, provenance.commit_hash, provenance.worktree_state
        )
        stored_provenance = provenance.as_json()
        del stored_provenance["commit_hash"]
        del stored_provenance["worktree_state"]
        await connection.execute(
            """
            INSERT INTO operations(
                id, component_parts_json, output_schema_version,
                code_provenance_json, invocation_json, configuration_json,
                state, started_at_utc, code_state_id
            ) VALUES (?, ?, ?, ?, ?, ?, 'started', ?, ?)
            """,
            (
                operation_id,
                _json(list(component.identifier.parts)),
                component.output_schema_version,
                _json(stored_provenance),
                _json(invocation),
                _json(configuration),
                started_at_utc,
                code_state_id,
            ),
        )
        for name, object_identifier in inputs:
            await connection.execute(
                "INSERT INTO operation_inputs VALUES (?, ?, ?)",
                (operation_id, _json(list(name)), object_identifier),
            )

    async def begin_operation(
        self,
        *,
        operation_id: str,
        component: Component,
        provenance: CodeProvenance,
        invocation: dict[str, JsonValue],
        configuration: JsonValue,
        started_at_utc: str,
        inputs: Sequence[tuple[tuple[str, ...], str]] = (),
    ) -> None:
        async with self._connections.writer() as connection:
            await self._begin_operation(
                connection,
                operation_id=operation_id,
                component=component,
                provenance=provenance,
                invocation=invocation,
                configuration=configuration,
                started_at_utc=started_at_utc,
                inputs=inputs,
            )

    async def begin_leased_operation(
        self,
        *,
        work_item_identifier: str,
        lease_token: str,
        worker_identifier: str,
        lease_duration_ns: int,
        utc_now_ns: Callable[[], int],
        event_identifier: str,
        operation_id: str,
        component: Component,
        provenance: CodeProvenance,
        invocation: dict[str, JsonValue],
        configuration: JsonValue,
        started_at_utc: str,
        inputs: Sequence[tuple[tuple[str, ...], str]] = (),
    ) -> None:
        """Validate and extend a lease while binding its operation before dispatch."""

        if lease_duration_ns <= 0:
            raise ValueError("Lease duration must be positive")
        async with self._connections.writer() as connection:
            now_utc_ns = utc_now_ns()
            attempt, previous_expiry = await self._require_active_lease(
                connection,
                work_item_identifier=work_item_identifier,
                lease_token=lease_token,
                worker_identifier=worker_identifier,
                now_utc_ns=now_utc_ns,
            )
            new_expiry = max(previous_expiry + 1, now_utc_ns + lease_duration_ns)
            await connection.execute(
                """
                UPDATE work_items
                SET lease_expires_at_utc_ns = ?
                WHERE id = ? AND lease_token = ? AND lease_owner = ?
                """,
                (new_expiry, work_item_identifier, lease_token, worker_identifier),
            )
            await self._append_work_event(
                connection,
                event_identifier=event_identifier,
                work_item_identifier=work_item_identifier,
                event_kind=WorkEventKind.LEASE_RENEWED,
                recorded_at_utc_ns=now_utc_ns,
                data={
                    "worker_identifier": worker_identifier,
                    "lease_token": lease_token,
                    "attempt": attempt,
                    "previous_expires_at_utc_ns": previous_expiry,
                    "new_expires_at_utc_ns": new_expiry,
                    "reason": "operation_dispatch",
                },
            )
            await self._begin_operation(
                connection,
                operation_id=operation_id,
                component=component,
                provenance=provenance,
                invocation=invocation,
                configuration=configuration,
                started_at_utc=started_at_utc,
                inputs=inputs,
            )
            await connection.execute(
                "INSERT INTO work_operations VALUES (?, ?, ?, ?, ?)",
                (
                    operation_id,
                    work_item_identifier,
                    attempt,
                    lease_token,
                    worker_identifier,
                ),
            )

    async def _require_bound_operation(
        self,
        connection: AsyncConnection,
        *,
        operation_id: str,
        work_item_identifier: str,
        attempt: int,
        lease_token: str,
        worker_identifier: str,
    ) -> None:
        cursor = await connection.execute(
            """
            SELECT 1 FROM work_operations
            WHERE operation_id = ? AND work_item_id = ? AND attempt = ?
              AND lease_token = ? AND worker_identifier = ?
            """,
            (
                operation_id,
                work_item_identifier,
                attempt,
                lease_token,
                worker_identifier,
            ),
        )
        if await cursor.fetchone() is None:
            raise LeaseLostError("The operation is not bound to this leased work attempt")

    async def _publish_operation_objects(
        self,
        connection: AsyncConnection,
        *,
        operation_id: str,
        records: Sequence[RecordDraft],
        artifacts: Sequence[ArtifactDraft],
        outputs: Sequence[NamedOutput],
    ) -> None:
        if records:
            await connection.executemany(
                "INSERT INTO objects VALUES (?, 'record', ?, ?)",
                ((record.identifier, _json(list(record.kind)), operation_id) for record in records),
            )
            await connection.executemany(
                "INSERT INTO records VALUES (?, ?, ?)",
                (
                    (record.identifier, record.schema_version, _json(record.value))
                    for record in records
                ),
            )
            await self._publish_saved_image_lookup(
                connection,
                tuple(
                    row
                    for record in records
                    for row in self._saved_image_lookup_rows(
                        record.identifier, record.kind, record.value
                    )
                ),
            )
        for artifact in artifacts:
            await connection.execute(
                "INSERT INTO objects VALUES (?, 'artifact', ?, ?)",
                (artifact.identifier, _json(list(artifact.kind)), operation_id),
            )
            if isinstance(artifact, BytesDraft):
                digest = hashlib.sha256(artifact.content).hexdigest()
                size = len(artifact.content)
                await connection.execute(
                    """
                    INSERT OR IGNORE INTO content(
                        sha256, size, storage_backend, inline_bytes, external_locator
                    ) VALUES (?, ?, 'sqlite', ?, NULL)
                    """,
                    (digest, size, artifact.content),
                )
                await connection.execute(
                    "INSERT INTO artifacts VALUES (?, ?, ?, ?, ?)",
                    (
                        artifact.identifier,
                        digest,
                        size,
                        artifact.media_type,
                        _json(artifact.representation),
                    ),
                )
            elif isinstance(artifact, ExternalFileDraft):
                await connection.execute(
                    "INSERT INTO external_artifacts VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        artifact.identifier,
                        artifact.sha256,
                        artifact.size,
                        artifact.media_type,
                        _json(artifact.representation),
                        artifact.locator,
                    ),
                )
            else:
                raise TypeError("Unsupported artifact draft")
        if outputs:
            await connection.executemany(
                "INSERT INTO operation_outputs VALUES (?, ?, ?)",
                (
                    (operation_id, _json(list(output.name)), output.object_identifier)
                    for output in outputs
                ),
            )
        if records or artifacts or outputs:
            owner = await (
                await connection.execute(
                    "SELECT work_item_id FROM work_operations WHERE operation_id=?", (operation_id,)
                )
            ).fetchone()
            if owner is not None:
                await progress_sql.mark(connection, _text(owner[0]))

    async def _complete_operation(
        self,
        connection: AsyncConnection,
        *,
        operation_id: str,
        records: Sequence[RecordDraft],
        artifacts: Sequence[ArtifactDraft],
        inputs: Sequence[NamedInput],
        outputs: Sequence[NamedOutput],
        result: JsonValue,
        ended_at_utc: str,
        duration_ns: int,
    ) -> None:
        for operation_input in inputs:
            await connection.execute(
                "INSERT INTO operation_inputs VALUES (?, ?, ?)",
                (
                    operation_id,
                    _json(list(operation_input.name)),
                    operation_input.object_identifier,
                ),
            )
        await self._publish_operation_objects(
            connection,
            operation_id=operation_id,
            records=records,
            artifacts=artifacts,
            outputs=outputs,
        )
        await connection.execute(
            """
            UPDATE operations
            SET state = 'completed', ended_at_utc = ?, duration_ns = ?, result_json = ?
            WHERE id = ? AND state = 'started'
            """,
            (ended_at_utc, duration_ns, _json(result), operation_id),
        )
        if await connection.changes() != 1:
            raise RuntimeError("Operation was not in started state")

        await self._reconcile_terminal_network_activities(
            connection, operation_id=operation_id, recorded_at_utc_ns=time_ns()
        )

    async def publish_leased_operation_checkpoint(
        self,
        *,
        work_item_identifier: str,
        lease_token: str,
        worker_identifier: str,
        utc_now_ns: Callable[[], int],
        operation_id: str,
        records: Sequence[RecordDraft],
        artifacts: Sequence[ArtifactDraft],
        outputs: Sequence[NamedOutput],
        checkpoint_result: JsonValue,
        inputs: Sequence[NamedInput] = (),
    ) -> None:
        """Publish immutable partial evidence while a fenced work attempt remains active."""

        async with self._connections.writer() as connection:
            now_utc_ns = utc_now_ns()
            attempt, _ = await self._require_active_lease(
                connection,
                work_item_identifier=work_item_identifier,
                lease_token=lease_token,
                worker_identifier=worker_identifier,
                now_utc_ns=now_utc_ns,
            )
            await self._require_bound_operation(
                connection,
                operation_id=operation_id,
                work_item_identifier=work_item_identifier,
                attempt=attempt,
                lease_token=lease_token,
                worker_identifier=worker_identifier,
            )
            for operation_input in inputs:
                await connection.execute(
                    "INSERT OR IGNORE INTO operation_inputs VALUES (?, ?, ?)",
                    (
                        operation_id,
                        _json(list(operation_input.name)),
                        operation_input.object_identifier,
                    ),
                )
            await self._publish_operation_objects(
                connection,
                operation_id=operation_id,
                records=records,
                artifacts=artifacts,
                outputs=outputs,
            )
            await connection.execute(
                """
                UPDATE operations
                SET result_json = ?
                WHERE id = ? AND state = 'started'
                """,
                (_json(checkpoint_result), operation_id),
            )
            if await connection.changes() != 1:
                raise RuntimeError("Operation was not in started state")
            await connection.execute(
                """
                UPDATE work_items
                SET result_json = ?
                WHERE id = ? AND lease_token = ? AND lease_owner = ?
                """,
                (
                    _json(checkpoint_result),
                    work_item_identifier,
                    lease_token,
                    worker_identifier,
                ),
            )
            if await connection.changes() != 1:
                raise LeaseLostError("The lease changed during checkpoint publication")

    async def complete_operation(
        self,
        *,
        operation_id: str,
        records: Sequence[RecordDraft],
        artifacts: Sequence[ArtifactDraft],
        outputs: Sequence[NamedOutput],
        result: JsonValue,
        ended_at_utc: str,
        duration_ns: int,
        inputs: Sequence[NamedInput] = (),
    ) -> None:
        async with self._connections.writer() as connection:
            await self._complete_operation(
                connection,
                operation_id=operation_id,
                records=records,
                artifacts=artifacts,
                inputs=inputs,
                outputs=outputs,
                result=result,
                ended_at_utc=ended_at_utc,
                duration_ns=duration_ns,
            )

    async def complete_leased_operation(
        self,
        *,
        work_item_identifier: str,
        lease_token: str,
        worker_identifier: str,
        utc_now_ns: Callable[[], int],
        event_identifier: str,
        operation_id: str,
        records: Sequence[RecordDraft],
        artifacts: Sequence[ArtifactDraft],
        outputs: Sequence[NamedOutput],
        result: JsonValue,
        ended_at_utc: str,
        duration_ns: int,
        inputs: Sequence[NamedInput] = (),
        follow_on_work: Sequence[FollowOnWork] = (),
    ) -> None:
        """Fence output publication and work completion in one transaction."""

        async with self._connections.writer() as connection:
            now_utc_ns = utc_now_ns()
            attempt, _ = await self._require_active_lease(
                connection,
                work_item_identifier=work_item_identifier,
                lease_token=lease_token,
                worker_identifier=worker_identifier,
                now_utc_ns=now_utc_ns,
            )
            await self._require_bound_operation(
                connection,
                operation_id=operation_id,
                work_item_identifier=work_item_identifier,
                attempt=attempt,
                lease_token=lease_token,
                worker_identifier=worker_identifier,
            )
            await self._complete_operation(
                connection,
                operation_id=operation_id,
                records=records,
                artifacts=artifacts,
                inputs=inputs,
                outputs=outputs,
                result=result,
                ended_at_utc=ended_at_utc,
                duration_ns=duration_ns,
            )
            await connection.execute(
                """
                UPDATE work_items
                SET state = 'completed', result_json = ?, lease_token = NULL,
                    lease_owner = NULL, lease_expires_at_utc_ns = NULL
                WHERE id = ? AND lease_token = ? AND lease_owner = ?
                """,
                (
                    _json(result),
                    work_item_identifier,
                    lease_token,
                    worker_identifier,
                ),
            )
            if await connection.changes() != 1:
                raise LeaseLostError("The lease changed during fenced completion")
            for submission in follow_on_work:
                await self.enqueue_work(
                    submission.definition,
                    submission.requester,
                    event_identifier=submission.event_identifier,
                    enqueued_at_utc_ns=now_utc_ns,
                    holdoff_decisions=submission.holdoff_decisions,
                )
            await self._append_work_event(
                connection,
                event_identifier=event_identifier,
                work_item_identifier=work_item_identifier,
                event_kind=WorkEventKind.COMPLETED,
                recorded_at_utc_ns=now_utc_ns,
                data={
                    "worker_identifier": worker_identifier,
                    "lease_token": lease_token,
                    "attempt": attempt,
                    "operation_identifier": operation_id,
                    "result": result,
                },
            )

    async def _fail_operation(
        self,
        connection: AsyncConnection,
        *,
        operation_id: str,
        error: JsonValue,
        result: JsonValue,
        ended_at_utc: str,
        duration_ns: int | None,
        inputs: Sequence[NamedInput] = (),
        records: Sequence[RecordDraft] = (),
        artifacts: Sequence[ArtifactDraft] = (),
        outputs: Sequence[NamedOutput] = (),
    ) -> None:
        for operation_input in inputs:
            await connection.execute(
                "INSERT INTO operation_inputs VALUES (?, ?, ?)",
                (
                    operation_id,
                    _json(list(operation_input.name)),
                    operation_input.object_identifier,
                ),
            )
        await self._publish_operation_objects(
            connection,
            operation_id=operation_id,
            records=records,
            artifacts=artifacts,
            outputs=outputs,
        )
        await connection.execute(
            """
            UPDATE operations
            SET state = 'failed', ended_at_utc = ?, duration_ns = ?,
                result_json = ?, error_json = ?
            WHERE id = ? AND state = 'started'
            """,
            (ended_at_utc, duration_ns, _json(result), _json(error), operation_id),
        )
        if await connection.changes() != 1:
            raise RuntimeError("Operation was not in started state")

        await self._reconcile_terminal_network_activities(
            connection, operation_id=operation_id, recorded_at_utc_ns=time_ns()
        )

    async def retry_leased_operation(
        self,
        *,
        work_item_identifier: str,
        lease_token: str,
        worker_identifier: str,
        utc_now_ns: Callable[[], int],
        delay_ns: int,
        event_identifier: str,
        operation_id: str,
        reason: JsonValue,
        result: JsonValue,
        ended_at_utc: str,
        duration_ns: int,
        inputs: Sequence[NamedInput] = (),
        records: Sequence[RecordDraft] = (),
        artifacts: Sequence[ArtifactDraft] = (),
        outputs: Sequence[NamedOutput] = (),
    ) -> None:
        if delay_ns < 0:
            raise ValueError("Retry delay cannot be negative")
        async with self._connections.writer() as connection:
            now_utc_ns = utc_now_ns()
            eligible_at_utc_ns = now_utc_ns + delay_ns
            attempt, _ = await self._require_active_lease(
                connection,
                work_item_identifier=work_item_identifier,
                lease_token=lease_token,
                worker_identifier=worker_identifier,
                now_utc_ns=now_utc_ns,
            )
            await self._require_bound_operation(
                connection,
                operation_id=operation_id,
                work_item_identifier=work_item_identifier,
                attempt=attempt,
                lease_token=lease_token,
                worker_identifier=worker_identifier,
            )
            await self._fail_operation(
                connection,
                operation_id=operation_id,
                error={"kind": "retry_scheduled", "reason": reason},
                result=result,
                ended_at_utc=ended_at_utc,
                duration_ns=duration_ns,
                inputs=inputs,
                records=records,
                artifacts=artifacts,
                outputs=outputs,
            )
            await connection.execute(
                """
                UPDATE work_items
                SET state = 'pending', eligible_at_utc_ns = ?, lease_token = NULL,
                    lease_owner = NULL, lease_expires_at_utc_ns = NULL,
                    result_json = ?, error_json = NULL
                WHERE id = ? AND lease_token = ? AND lease_owner = ?
                """,
                (
                    eligible_at_utc_ns,
                    _json(result),
                    work_item_identifier,
                    lease_token,
                    worker_identifier,
                ),
            )
            if await connection.changes() != 1:
                raise LeaseLostError("The lease changed while scheduling a retry")
            await self._append_work_event(
                connection,
                event_identifier=event_identifier,
                work_item_identifier=work_item_identifier,
                event_kind=WorkEventKind.RELEASED,
                recorded_at_utc_ns=now_utc_ns,
                data={
                    "worker_identifier": worker_identifier,
                    "lease_token": lease_token,
                    "attempt": attempt,
                    "operation_identifier": operation_id,
                    "eligible_at_utc_ns": eligible_at_utc_ns,
                    "reason": reason,
                },
            )

    async def terminally_fail_leased_operation(
        self,
        *,
        work_item_identifier: str,
        lease_token: str,
        worker_identifier: str,
        utc_now_ns: Callable[[], int],
        event_identifier: str,
        operation_id: str,
        error: JsonValue,
        result: JsonValue,
        ended_at_utc: str,
        duration_ns: int,
        inputs: Sequence[NamedInput] = (),
        records: Sequence[RecordDraft] = (),
        artifacts: Sequence[ArtifactDraft] = (),
        outputs: Sequence[NamedOutput] = (),
    ) -> None:
        async with self._connections.writer() as connection:
            now_utc_ns = utc_now_ns()
            attempt, _ = await self._require_active_lease(
                connection,
                work_item_identifier=work_item_identifier,
                lease_token=lease_token,
                worker_identifier=worker_identifier,
                now_utc_ns=now_utc_ns,
            )
            await self._require_bound_operation(
                connection,
                operation_id=operation_id,
                work_item_identifier=work_item_identifier,
                attempt=attempt,
                lease_token=lease_token,
                worker_identifier=worker_identifier,
            )
            await self._fail_operation(
                connection,
                operation_id=operation_id,
                error=error,
                result=result,
                ended_at_utc=ended_at_utc,
                duration_ns=duration_ns,
                inputs=inputs,
                records=records,
                artifacts=artifacts,
                outputs=outputs,
            )
            await connection.execute(
                """
                UPDATE work_items
                SET state = 'terminal_failure', error_json = ?, result_json = ?,
                    lease_token = NULL, lease_owner = NULL,
                    lease_expires_at_utc_ns = NULL
                WHERE id = ? AND lease_token = ? AND lease_owner = ?
                """,
                (
                    _json(error),
                    _json(result),
                    work_item_identifier,
                    lease_token,
                    worker_identifier,
                ),
            )
            if await connection.changes() != 1:
                raise LeaseLostError("The lease changed during terminal failure publication")
            await self._append_work_event(
                connection,
                event_identifier=event_identifier,
                work_item_identifier=work_item_identifier,
                event_kind=WorkEventKind.TERMINAL_FAILURE,
                recorded_at_utc_ns=now_utc_ns,
                data={
                    "worker_identifier": worker_identifier,
                    "lease_token": lease_token,
                    "attempt": attempt,
                    "operation_identifier": operation_id,
                    "error": error,
                    "result": result,
                },
            )

    async def fail_operation(
        self,
        *,
        operation_id: str,
        error: JsonValue,
        result: JsonValue,
        ended_at_utc: str,
        duration_ns: int,
        records: Sequence[RecordDraft] = (),
        artifacts: Sequence[ArtifactDraft] = (),
        outputs: Sequence[NamedOutput] = (),
    ) -> None:
        async with self._connections.writer() as connection:
            await self._fail_operation(
                connection,
                operation_id=operation_id,
                error=error,
                result=result,
                ended_at_utc=ended_at_utc,
                duration_ns=duration_ns,
                records=records,
                artifacts=artifacts,
                outputs=outputs,
            )

    async def get_record(self, identifier: str) -> tuple[tuple[str, ...], int, JsonValue]:
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT objects.kind_parts_json, records.schema_version, records.value_json
                FROM records JOIN objects ON objects.id = records.object_id
                WHERE records.object_id = ?
                """,
                (identifier,),
            )
            row = await cursor.fetchone()
        if row is None:
            raise KeyError(identifier)
        kind = decode_json(_text(row[0]))
        if not isinstance(kind, list) or not all(isinstance(part, str) for part in kind):
            raise ValueError("Invalid stored object kind")
        return tuple(kind), _integer(row[1]), decode_json(_text(row[2]))

    async def records_by_identifiers(
        self, identifiers: Sequence[str]
    ) -> tuple[tuple[str, tuple[str, ...], int, JsonValue], ...]:
        """Read a bounded caller-ordered record scope, retaining duplicate requests."""

        if len(identifiers) > 1000 or any(not identifier for identifier in identifiers):
            raise ValueError("Record identifier scope is invalid")
        if not identifiers:
            return ()
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT record.object_id, object.kind_parts_json,
                       record.schema_version, record.value_json
                FROM json_each(?) AS requested
                CROSS JOIN records AS record ON record.object_id = requested.value
                CROSS JOIN objects AS object ON object.id = record.object_id
                ORDER BY CAST(requested.key AS INTEGER)
                """,
                (_json(list(identifiers)),),
            )
            rows = await cursor.fetchall()
        if len(rows) != len(identifiers):
            raise KeyError("One or more requested records do not exist")
        results: list[tuple[str, tuple[str, ...], int, JsonValue]] = []
        for row in rows:
            kind = decode_json(_text(row[1]))
            if not isinstance(kind, list) or not all(isinstance(part, str) for part in kind):
                raise ValueError("Invalid stored object kind")
            results.append(
                (_text(row[0]), tuple(kind), _integer(row[2]), decode_json(_text(row[3])))
            )
        return tuple(results)

    async def work_progress(self, identifiers: Sequence[str]) -> tuple[dict[str, JsonValue], ...]:
        """Read current work state without loading operations or event histories."""

        if len(identifiers) > 1000 or any(not identifier for identifier in identifiers):
            raise ValueError("Work progress scope is invalid")
        if not identifiers:
            return ()
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT work.id, work.kind_parts_json, work.state, work.attempt,
                       work.result_json, work.error_json, work.eligible_at_utc_ns
                FROM json_each(?) AS requested
                CROSS JOIN work_items AS work ON work.id = requested.value
                ORDER BY CAST(requested.key AS INTEGER)
                """,
                (_json(list(identifiers)),),
            )
            rows = await cursor.fetchall()
        if len(rows) != len(identifiers):
            raise KeyError("One or more requested work items do not exist")
        return tuple(
            {
                "identifier": _text(row[0]),
                "kind": decode_json(_text(row[1])),
                "state": _text(row[2]),
                "attempt": _integer(row[3]),
                "result": None if row[4] is None else decode_json(_text(row[4])),
                "error": None if row[5] is None else decode_json(_text(row[5])),
                "eligible_at_utc_ns": _integer(row[6]),
            }
            for row in rows
        )

    async def ebay_gallery_references(
        self, observation_identifiers: Sequence[str]
    ) -> tuple[tuple[str, JsonValue], ...]:
        """Seek gallery references attached to selected retained observations."""

        if len(observation_identifiers) > 1000 or any(
            not identifier for identifier in observation_identifiers
        ):
            raise ValueError("Gallery observation scope is invalid")
        if not observation_identifiers:
            return ()
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT object.id, record.value_json
                FROM json_each(?) AS requested
                CROSS JOIN records AS record INDEXED BY records_ebay_gallery_observation
                  ON json_extract(record.value_json, '$.observation_record_identifier') =
                     requested.value
                CROSS JOIN objects AS object ON object.id = record.object_id
                WHERE json_extract(record.value_json, '$.observation_record_identifier') IS NOT NULL
                  AND object.kind_parts_json = '["carl","ebay","gallery_image_reference"]'
                ORDER BY object.rowid
                """,
                (_json(list(dict.fromkeys(observation_identifiers))),),
            )
            rows = await cursor.fetchall()
        return tuple((_text(row[0]), decode_json(_text(row[1]))) for row in rows)

    async def ebay_item_observations(
        self, item_identifier: str
    ) -> tuple[tuple[str, JsonValue], ...]:
        """Order observations by acquisition recency, then offline derivation recency."""
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT observation.id, records.value_json
                FROM objects AS observation JOIN records ON records.object_id = observation.id
                LEFT JOIN objects AS acquisition
                  ON acquisition.id = json_extract(records.value_json, '$.acquisition_record_identifier')
                WHERE observation.kind_parts_json = ?
                  AND json_extract(records.value_json, '$.item_identifier') = ?
                ORDER BY COALESCE(acquisition.rowid, observation.rowid), observation.rowid
                """,
                (_json(["carl", "ebay", "listing_observation"]), item_identifier),
            )
            rows = await cursor.fetchall()
        return tuple((_text(row[0]), decode_json(_text(row[1]))) for row in rows)

    async def records_by_kind(self, kind: tuple[str, ...]) -> tuple[tuple[str, JsonValue], ...]:
        """Return immutable records of one kind in publication order."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT objects.id, records.value_json
                FROM objects
                JOIN records ON records.object_id = objects.id
                WHERE objects.kind_parts_json = ?
                ORDER BY objects.rowid, objects.id
                """,
                (_json(list(kind)),),
            )
            rows = await cursor.fetchall()
        return tuple((_text(row[0]), decode_json(_text(row[1]))) for row in rows)

    async def record_identifiers_by_kind_page(
        self, kind: tuple[str, ...], *, after_identifier: str | None = None, limit: int = 100
    ) -> tuple[str, ...]:
        """Read a keyset page of identities without loading growing record payloads."""

        if not 1 <= limit <= 1_000:
            raise ValueError("Record identity page limit must be between one and 1,000")
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """SELECT id FROM objects WHERE kind_parts_json=? AND id>?
                ORDER BY id LIMIT ?""",
                (_json(list(kind)), after_identifier or "", limit),
            )
            rows = await cursor.fetchall()
        return tuple(_text(row[0]) for row in rows)

    async def pipeline_completed_analysis_exists(
        self,
        marketplace: str,
        external_identifier: str,
        product_guide_record_identifier: str | None = None,
    ) -> bool:
        """Find successful analysis history through exact observation evidence edges."""

        if (
            marketplace not in ("facebook", "ebay")
            or not external_identifier
            or product_guide_record_identifier == ""
        ):
            raise ValueError("Pipeline analysis history scope is invalid")
        observation_path = "$.listing_id" if marketplace == "facebook" else "$.item_identifier"
        observation_index = (
            "records_listing_observation_listing"
            if marketplace == "facebook"
            else "records_marketplace_item_observation"
        )
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                f"""
                SELECT EXISTS (
                    SELECT 1
                    FROM records AS observation_record INDEXED BY {observation_index}
                    CROSS JOIN objects AS observation ON observation.id = observation_record.object_id
                      AND observation.kind_parts_json = ?
                    CROSS JOIN operation_inputs AS evidence_input
                      INDEXED BY operation_inputs_object_name_operation
                      ON evidence_input.object_id = observation.id
                     AND evidence_input.name_parts_json = '["listing_observation"]'
                    CROSS JOIN objects AS evidence INDEXED BY objects_operation_kind
                      ON evidence.created_by_operation_id = evidence_input.operation_id
                     AND evidence.kind_parts_json = ?
                    CROSS JOIN operation_inputs AS analysis_input
                      INDEXED BY operation_inputs_object_name_operation
                      ON analysis_input.object_id = evidence.id
                     AND analysis_input.name_parts_json = '["listing_analysis_evidence"]'
                    JOIN work_operations AS owner ON owner.operation_id = analysis_input.operation_id
                    JOIN work_items AS work ON work.id = owner.work_item_id
                    JOIN operations AS operation ON operation.id = owner.operation_id
                    JOIN objects AS analysis ON analysis.created_by_operation_id = owner.operation_id
                      AND analysis.kind_parts_json = ?
                    JOIN records AS analysis_record ON analysis_record.object_id = analysis.id
                    WHERE json_extract(observation_record.value_json, '{observation_path}') = ?
                      AND json_extract(observation_record.value_json, '{observation_path}') IS NOT NULL
                      AND work.kind_parts_json = ? AND work.state = 'completed'
                      AND operation.state = 'completed'
                      AND json_extract(analysis_record.value_json, '$.state') = 'completed'
                      AND json_extract(work.payload_json, '$.evidence_set_record_identifier') = evidence.id
                      AND (? IS NULL OR json_extract(work.payload_json, '$.product_guide_record_identifier') = ?)
                )
                """,
                (
                    _json(["carl", marketplace, "listing_observation"]),
                    _json(["carl", marketplace, "listing_analysis_evidence"]),
                    _json(["carl", marketplace, "item_analysis"]),
                    external_identifier,
                    _json(["carl", marketplace, "work", "analyze_item"]),
                    product_guide_record_identifier,
                    product_guide_record_identifier,
                ),
            )
            row = await cursor.fetchone()
        assert row is not None
        return bool(row[0])

    async def retained_ebay_item_acquisitions(
        self,
        work_item_identifier: str,
        *,
        before_attempt: int,
        retry_budget_start_attempt: int = 0,
    ) -> tuple[str, ...]:
        """Read committed item responses, including failed attempts and checkpoints."""

        if (
            not work_item_identifier
            or before_attempt < 1
            or retry_budget_start_attempt < 0
            or retry_budget_start_attempt >= before_attempt
        ):
            raise ValueError("Item acquisition retry budget bounds are invalid")
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT acquisition.id
                FROM work_operations AS owner
                JOIN work_items AS work ON work.id = owner.work_item_id
                JOIN objects AS acquisition
                  ON acquisition.created_by_operation_id = owner.operation_id
                JOIN records AS record ON record.object_id = acquisition.id
                WHERE owner.work_item_id = ?
                  AND work.kind_parts_json = '["carl","ebay","collect","item"]'
                  AND owner.attempt > ? AND owner.attempt < ?
                  AND acquisition.kind_parts_json = '["carl","http","acquisition"]'
                  AND json_extract(record.value_json, '$.purpose') = 'ebay_item'
                ORDER BY owner.attempt, acquisition.rowid
                """,
                (work_item_identifier, retry_budget_start_attempt, before_attempt),
            )
            return tuple(_text(row[0]) for row in await cursor.fetchall())

    async def retained_ebay_item_request_attempts(
        self,
        work_item_identifier: str,
        *,
        before_attempt: int,
        retry_budget_start_attempt: int = 0,
    ) -> int:
        """Count uncertain charged requests and legacy responses without duplication."""
        if (
            not work_item_identifier
            or before_attempt < 1
            or retry_budget_start_attempt < 0
            or retry_budget_start_attempt >= before_attempt
        ):
            raise ValueError("Item acquisition retry budget bounds are invalid")
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT coalesce(sum(max(
                    (SELECT count(*) FROM resident_request_charges AS charge
                     WHERE charge.operation_id = owner.operation_id
                       AND charge.request_identity_json = '["ebay","item"]'),
                    (SELECT count(*) FROM objects AS acquisition
                     JOIN records AS record ON record.object_id = acquisition.id
                     WHERE acquisition.created_by_operation_id = owner.operation_id
                       AND acquisition.kind_parts_json = '["carl","http","acquisition"]'
                       AND json_extract(record.value_json, '$.purpose') = 'ebay_item')
                )), 0)
                FROM work_operations AS owner
                JOIN work_items AS work ON work.id = owner.work_item_id
                WHERE owner.work_item_id = ?
                  AND work.kind_parts_json = '["carl","ebay","collect","item"]'
                  AND owner.attempt > ? AND owner.attempt < ?
                """,
                (work_item_identifier, retry_budget_start_attempt, before_attempt),
            )
            row = await cursor.fetchone()
            assert row is not None
            return _integer(row[0])

    async def expedite_pending_ebay_item_retries(
        self,
        *,
        at_utc_ns: int,
        event_batch_identifier: str,
        stack_identifier: str | None = "ebay_anonymous",
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Audit an immediate-retry policy cutover without resetting work history."""

        if at_utc_ns < 0 or not event_batch_identifier or stack_identifier == "":
            raise ValueError("Item retry policy cutover arguments are invalid")
        async with self._connections.writer() as connection:
            cursor = await connection.execute(
                """
                SELECT work.id, work.attempt, work.eligible_at_utc_ns
                FROM work_items AS work
                JOIN work_events AS released ON released.sequence = (
                    SELECT MAX(event.sequence) FROM work_events AS event
                    WHERE event.work_item_id = work.id AND event.event_kind = 'released'
                )
                WHERE work.kind_parts_json = '["carl","ebay","collect","item"]'
                  AND work.state = 'pending' AND work.eligible_at_utc_ns > ?
                  AND (? IS NULL OR COALESCE(
                      json_extract(work.payload_json, '$.request.stack_identifier'),
                      'ebay_anonymous') = ?)
                  AND json_extract(released.data_json, '$.reason.kind') =
                      'ebay_item_response_failure'
                  AND json_extract(released.data_json, '$.reason.classification') = 'challenge'
                ORDER BY work.created_at_utc_ns, work.id
                """,
                (at_utc_ns, stack_identifier, stack_identifier),
            )
            items = await cursor.fetchall()
            identifiers = tuple(_text(row[0]) for row in items)
            if not identifiers:
                return (), ()
            cursor = await connection.execute(
                """
                SELECT work.id, work.attempt, work.eligible_at_utc_ns
                FROM work_items AS work
                WHERE work.kind_parts_json = '["carl","ebay","work","refresh_search"]'
                  AND work.state = 'pending' AND work.eligible_at_utc_ns > ?
                  AND json_extract(work.result_json, '$.stage') = 'collecting_items'
                  AND EXISTS (
                      SELECT 1 FROM json_each(
                          work.result_json, '$.item_collection_work_identifiers'
                      ) AS child
                      JOIN json_each(?) AS selected ON selected.value = child.value
                  )
                ORDER BY work.created_at_utc_ns, work.id
                """,
                (at_utc_ns, _json(list(identifiers))),
            )
            parents = await cursor.fetchall()
            for index, row in enumerate((*items, *parents)):
                identifier, attempt, previous_eligible_at = (
                    _text(row[0]),
                    _integer(row[1]),
                    _integer(row[2]),
                )
                await connection.execute(
                    """UPDATE work_items SET eligible_at_utc_ns = ?
                       WHERE id = ? AND state = 'pending' AND eligible_at_utc_ns = ?""",
                    (at_utc_ns, identifier, previous_eligible_at),
                )
                if await connection.changes() != 1:
                    raise RuntimeError("Pending item retry work changed during policy cutover")
                await self._append_work_event(
                    connection,
                    event_identifier=f"{event_batch_identifier}-{index}",
                    work_item_identifier=identifier,
                    event_kind=WorkEventKind.ENQUEUED,
                    recorded_at_utc_ns=at_utc_ns,
                    data={
                        "reason": {
                            "kind": "ebay_item_retry_policy_change",
                            "stack_identifier": stack_identifier,
                            "role": "item" if index < len(items) else "refresh_parent",
                        },
                        "previous_attempt": attempt,
                        "previous_eligible_at_utc_ns": previous_eligible_at,
                        "eligible_at_utc_ns": at_utc_ns,
                    },
                )
        return identifiers, tuple(_text(row[0]) for row in parents)

    async def completed_acquisition_candidates(
        self,
        kind: tuple[str, ...],
        resource_identity: tuple[str, ...],
        *,
        work_identifier: str,
        since_work_created: bool = True,
    ) -> tuple[dict[str, JsonValue], ...]:
        """Find same-resource committed work without swallowing an explicit refresh.

        Item jobs only share acquisitions completed after their own enqueue time;
        this bridges the collection/extraction gap without reusing an old page.
        Matching caller-bound payloads may have different downstream image limits.
        """

        current = await self.work(work_identifier)
        current_payload = current["payload"]
        if acquisition_resource_identity(kind, current_payload) != resource_identity:
            raise ValueError("Acquisition lookup identity disagrees with its work")
        payload = cast(dict[str, JsonValue], current_payload)
        if kind == ("carl", "facebook", "work", "collect_item"):
            selector = "$.listing_id"
            selected = payload.get("listing_id")
        elif kind == ("carl", "ebay", "collect", "item"):
            selector = "$.request.item_identifier"
            request = cast(dict[str, JsonValue], payload["request"])
            selected = request.get("item_identifier")
        elif kind == ("carl", "ebay", "collect", "description"):
            selector = "$.url"
            selected = payload.get("url")
        else:
            raise ValueError("Unsupported acquisition candidate kind")
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT work.id, work.payload_json, work.result_json
                FROM work_items AS work INDEXED BY work_items_completed_acquisition
                WHERE work.kind_parts_json = ? AND work.state = 'completed'
                  AND work.acquisition_resource_identity_json = ?
                  AND work.id != ? AND json_extract(work.payload_json, ?) = ?
                  AND EXISTS (
                      SELECT 1 FROM work_events AS event
                      WHERE event.work_item_id = work.id AND event.event_kind = 'completed'
                        AND event.recorded_at_utc_ns >= ?
                  )
                  AND (? = 0 OR EXISTS (
                      SELECT 1 FROM objects AS acquisition
                      JOIN work_operations AS owner
                        ON owner.operation_id = acquisition.created_by_operation_id
                      JOIN work_events AS acquired
                        ON acquired.work_item_id = owner.work_item_id
                       AND acquired.event_kind = 'completed'
                       AND json_extract(acquired.data_json, '$.operation_identifier') =
                           acquisition.created_by_operation_id
                      WHERE acquisition.id = coalesce(
                            json_extract(work.result_json, '$.acquisition_record_identifier'),
                            (SELECT json_extract(description.value_json, '$.acquisition_record_identifier')
                             FROM records AS description
                             WHERE description.object_id = json_extract(
                                work.result_json, '$.description_result_record_identifier'))
                            )
                        AND acquired.recorded_at_utc_ns >= ?
                  ))
                ORDER BY work.created_at_utc_ns DESC, work.id
                """,
                (
                    _json(list(kind)),
                    acquisition_resource_identity_json(kind, current_payload),
                    work_identifier,
                    selector,
                    selected,
                    current["created_at_utc_ns"] if since_work_created else 0,
                    int(since_work_created),
                    current["created_at_utc_ns"],
                ),
            )
            rows = await cursor.fetchall()
        return tuple(
            {
                "identifier": _text(row[0]),
                "payload": decode_json(_text(row[1])),
                "result": None if row[2] is None else decode_json(_text(row[2])),
            }
            for row in rows
            if acquisition_resource_identity(kind, decode_json(_text(row[1]))) == resource_identity
        )

    async def pipeline_work_rows(
        self, identifiers: tuple[str, ...]
    ) -> tuple[dict[str, JsonValue], ...]:
        """Read bounded selected work in caller order without loading its history."""

        if len(identifiers) > 1000 or any(not identifier for identifier in identifiers):
            raise ValueError("Pipeline work scope is invalid")
        if not identifiers:
            return ()
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT work.id, work.state, work.payload_json, work.result_json, work.error_json,
                       coalesce(progress.revision,0)
                FROM json_each(?) AS requested
                JOIN work_items AS work ON work.id = requested.value
                LEFT JOIN work_progress_revisions AS progress ON progress.work_id=work.id
                ORDER BY CAST(requested.key AS INTEGER)
                """,
                (_json(list(identifiers)),),
            )
            rows = await cursor.fetchall()
        if len(rows) != len(identifiers):
            raise KeyError("One or more requested pipeline work items do not exist")
        return tuple(
            {
                "identifier": _text(row[0]),
                "state": _text(row[1]),
                "payload": decode_json(_text(row[2])),
                "result": None if row[3] is None else decode_json(_text(row[3])),
                "error": None if row[4] is None else decode_json(_text(row[4])),
                "progress_revision": _integer(row[5]),
            }
            for row in rows
        )

    async def pipeline_search_occurrences(
        self,
        *,
        search_work_identifiers: tuple[str, ...],
        search_run_record_identifiers: tuple[str, ...] = (),
        after_object_rowid: int = 0,
        limit: int = 100,
    ) -> tuple[dict[str, JsonValue], ...]:
        """Read incremental cards belonging to exact work attempts or published runs."""

        if (
            len(search_work_identifiers) > 100
            or len(search_run_record_identifiers) > 100
            or any(
                not identifier
                for identifier in (*search_work_identifiers, *search_run_record_identifiers)
            )
            or after_object_rowid < 0
            or not 1 <= limit <= 1000
        ):
            raise ValueError("Pipeline search occurrence scope or bounds are invalid")
        if not search_work_identifiers and not search_run_record_identifiers:
            return ()
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                f"""
                WITH requested_work AS MATERIALIZED (
                    SELECT DISTINCT value AS identifier FROM json_each(?)
                ), requested_runs AS MATERIALIZED (
                    SELECT DISTINCT value AS identifier FROM json_each(?)
                ), owned_operations AS MATERIALIZED (
                    SELECT owner.operation_id
                    FROM requested_work AS requested
                    JOIN work_operations AS owner ON owner.work_item_id = requested.identifier
                ), completed_runs AS MATERIALIZED (
                    SELECT run.id, run.kind_parts_json, record.value_json
                    FROM requested_runs AS requested
                    JOIN objects AS run ON run.id = requested.identifier
                    JOIN records AS record ON record.object_id = run.id
                    JOIN operations AS operation ON operation.id = run.created_by_operation_id
                    WHERE (operation.state = 'completed' OR (
                        {_partial_facebook_search_publication("operation", run_identifier="run.id")}
                        AND EXISTS (
                            SELECT 1 FROM work_operations AS owner
                            JOIN work_events AS event ON event.work_item_id = owner.work_item_id
                            WHERE owner.operation_id = operation.id
                              AND {_facebook_search_publication_event("operation", "event", run_identifier="run.id")}
                        )
                    ))
                      AND run.kind_parts_json IN (
                          '["carl","facebook","search_run"]',
                          '["carl","ebay","search_run"]'
                      )
                ), ebay_runs AS MATERIALIZED (
                    SELECT json_extract(record.value_json, '$.search_run_record_identifier') AS id
                    FROM owned_operations AS owner
                    JOIN objects AS binding ON binding.created_by_operation_id = owner.operation_id
                      AND binding.kind_parts_json = '["carl","ebay","search_attempt"]'
                    JOIN records AS record ON record.object_id = binding.id
                    UNION
                    SELECT json_extract(operation.result_json, '$.search_run_record_identifier')
                    FROM owned_operations AS owner
                    JOIN operations AS operation ON operation.id = owner.operation_id
                    JOIN work_operations AS work_owner ON work_owner.operation_id = owner.operation_id
                    JOIN work_items AS work ON work.id = work_owner.work_item_id
                    WHERE operation.state = 'completed'
                      AND work.kind_parts_json = '["carl","ebay","collect","search"]'
                    UNION
                    SELECT id FROM completed_runs
                    WHERE kind_parts_json = '["carl","ebay","search_run"]'
                ), candidates AS (
                    SELECT object.rowid AS object_rowid, object.id, 'facebook' AS marketplace,
                           record.value_json
                    FROM owned_operations AS owner
                    JOIN objects AS object ON object.created_by_operation_id = owner.operation_id
                      AND object.kind_parts_json = '["carl","facebook","search_listing_occurrence"]'
                    JOIN records AS record ON record.object_id = object.id
                    WHERE object.rowid > ?
                    UNION
                    SELECT object.rowid, object.id, 'facebook', occurrence.value_json
                    FROM completed_runs AS run
                    CROSS JOIN records AS occurrence INDEXED BY records_search_occurrence_run_position
                      ON json_extract(occurrence.value_json, '$.search_run_identifier') =
                         json_extract(run.value_json, '$.search_run_identifier')
                    JOIN objects AS object ON object.id = occurrence.object_id
                    WHERE run.kind_parts_json = '["carl","facebook","search_run"]'
                      AND object.kind_parts_json = '["carl","facebook","search_listing_occurrence"]'
                      AND json_extract(occurrence.value_json, '$.listing_identifier') IS NOT NULL
                      AND json_extract(occurrence.value_json, '$.search_run_identifier') IS NOT NULL
                      AND object.rowid > ?
                    UNION
                    SELECT object.rowid, object.id, 'ebay', occurrence.value_json
                    FROM ebay_runs AS run
                    CROSS JOIN records AS occurrence INDEXED BY records_marketplace_source_run
                      ON json_extract(occurrence.value_json, '$.search_run_record_identifier') = run.id
                    JOIN objects AS object ON object.id = occurrence.object_id
                    JOIN operations AS operation ON operation.id = object.created_by_operation_id
                    WHERE object.kind_parts_json = '["carl","ebay","search_listing_occurrence"]'
                      AND operation.state = 'completed'
                      AND object.rowid > ?
                )
                SELECT object_rowid, id, marketplace, value_json FROM candidates
                ORDER BY object_rowid, id LIMIT ?
                """,
                (
                    _json(list(search_work_identifiers)),
                    _json(list(search_run_record_identifiers)),
                    after_object_rowid,
                    after_object_rowid,
                    after_object_rowid,
                    limit,
                ),
            )
            rows = await cursor.fetchall()
        results: list[dict[str, JsonValue]] = []
        for row in rows:
            value = decode_json(_text(row[3]))
            if not isinstance(value, dict):
                raise ValueError("Stored pipeline search occurrence is malformed")
            value = cast(dict[str, JsonValue], value)
            marketplace = _text(row[2])
            original = value.get("original")
            title = (
                search_card_field_value("title", cast(dict[str, JsonValue], original))
                if marketplace == "facebook" and isinstance(original, dict)
                else value.get("title")
            )
            external_identifier = value.get(
                "listing_identifier" if marketplace == "facebook" else "item_identifier"
            )
            source_identifier = value.get(
                "search_run_identifier"
                if marketplace == "facebook"
                else "search_run_record_identifier"
            )
            if not isinstance(external_identifier, str) or not isinstance(source_identifier, str):
                raise ValueError("Stored pipeline search occurrence has invalid identifiers")
            results.append(
                {
                    "object_rowid": _integer(row[0]),
                    "occurrence_record_identifier": _text(row[1]),
                    "marketplace": marketplace,
                    "external_identifier": external_identifier,
                    "title": title if isinstance(title, str) else None,
                    "source_search_run_identifier": source_identifier,
                    "listing_status": (
                        {"sold": "sold", "completed": "unavailable"}.get(
                            str(value.get("listing_state"))
                        )
                        if marketplace == "ebay"
                        else (
                            status.value
                            if (
                                status := normalize_listing_status(
                                    is_sold=original.get("is_sold")
                                    if isinstance(original, dict)
                                    and isinstance(original.get("is_sold"), bool)
                                    else None,
                                    is_pending=original.get("is_pending")
                                    if isinstance(original, dict)
                                    and isinstance(original.get("is_pending"), bool)
                                    else None,
                                    is_live=original.get("is_live")
                                    if isinstance(original, dict)
                                    and isinstance(original.get("is_live"), bool)
                                    else None,
                                )
                            )
                            is not None
                            else None
                        )
                    ),
                }
            )
        return tuple(results)

    async def search_listing_occurrences_for_runs(
        self,
        marketplace: str,
        run_identifiers: Sequence[str],
        *,
        as_of_completion_sequence: int,
        maximum_object_rowid: int | None = None,
    ) -> tuple[tuple[str, JsonValue], ...]:
        """Read published cards for exact source runs, without other-source scans."""
        if marketplace not in ("facebook", "ebay"):
            raise ValueError("Search occurrence marketplace is invalid")
        if len(run_identifiers) > 100 or any(not identifier for identifier in run_identifiers):
            raise ValueError("Search occurrence run scope exceeds its supported bounds")
        if as_of_completion_sequence < 0 or (
            maximum_object_rowid is not None and maximum_object_rowid < 0
        ):
            raise ValueError("Search occurrence snapshot boundary is invalid")
        if not run_identifiers:
            return ()
        source_identifiers: list[str] = []
        for identifier in dict.fromkeys(run_identifiers):
            kind, _, value = await self.get_record(identifier)
            if kind != ("carl", marketplace, "search_run") or not isinstance(value, dict):
                raise ValueError("Search occurrence scope contains a different source kind")
            if marketplace == "facebook":
                internal = value.get("search_run_identifier")
                if not isinstance(internal, str) or not internal:
                    raise ValueError("Stored Facebook search run has no internal identifier")
                source_identifiers.append(internal)
            else:
                source_identifiers.append(identifier)
        # Paths and index names are internal constants, never user-provided SQL.
        source_path = (
            "$.search_run_identifier"
            if marketplace == "facebook"
            else "$.search_run_record_identifier"
        )
        index = (
            "records_search_occurrence_run_position"
            if marketplace == "facebook"
            else "records_marketplace_source_run"
        )
        partial_predicate = (
            "AND json_extract(record.value_json, '$.listing_identifier') IS NOT NULL "
            "AND json_extract(record.value_json, '$.search_run_identifier') IS NOT NULL"
            if marketplace == "facebook"
            else ""
        )
        publication_event = (
            _facebook_search_publication_event(
                "operation", "event", internal_identifier="requested.value"
            )
            if marketplace == "facebook"
            else "event.event_kind = 'completed'"
        )
        partial_publication = (
            _partial_facebook_search_publication("operation", internal_identifier="requested.value")
            if marketplace == "facebook"
            else "0"
        )
        async with self._connections.reader() as connection:
            if maximum_object_rowid is None:
                cursor = await connection.execute("SELECT COALESCE(MAX(rowid), 0) FROM objects")
                row = await cursor.fetchone()
                assert row is not None
                maximum_object_rowid = _integer(row[0])
            cursor = await connection.execute(
                f"""
                SELECT object.id, record.value_json
                FROM json_each(?) AS requested
                CROSS JOIN records AS record INDEXED BY {index}
                  ON json_extract(record.value_json, '{source_path}') = requested.value
                CROSS JOIN objects AS object
                  ON object.id = record.object_id
                CROSS JOIN operations AS operation
                  ON operation.id = object.created_by_operation_id
                WHERE object.kind_parts_json = ?
                  AND object.rowid <= ?
                  AND (operation.state = 'completed' OR (
                    {partial_publication}
                    AND EXISTS (
                        SELECT 1 FROM work_operations AS work
                        JOIN work_events AS event ON event.work_item_id = work.work_item_id
                        WHERE work.operation_id = operation.id
                          AND {publication_event}
                    )
                  ))
                  {partial_predicate}
                  AND COALESCE((SELECT MAX(event.sequence)
                    FROM work_operations AS work
                    JOIN work_events AS event ON event.work_item_id = work.work_item_id
                    WHERE work.operation_id = object.created_by_operation_id
                      AND {publication_event}
                      AND json_extract(event.data_json, '$.operation_identifier') =
                          object.created_by_operation_id), 0) <= ?
                ORDER BY object.rowid, object.id
                LIMIT 100001
                """,
                (
                    _json(list(dict.fromkeys(source_identifiers))),
                    _json(["carl", marketplace, "search_listing_occurrence"]),
                    maximum_object_rowid,
                    as_of_completion_sequence,
                ),
            )
            rows = await cursor.fetchall()
        if len(rows) > 100_000:
            raise ValueError("Retained search cards exceed the supported safety bound")
        return tuple((_text(row[0]), decode_json(_text(row[1]))) for row in rows)

    @staticmethod
    async def _review_mutation_replay(
        connection: AsyncConnection,
        *,
        action: str,
        request_identifier: str,
        request_sha256: str,
    ) -> JsonValue | None:
        cursor = await connection.execute(
            """
            SELECT request_sha256, response_json
            FROM review_mutation_requests
            WHERE action = ? AND request_identifier = ?
            """,
            (action, request_identifier),
        )
        row = await cursor.fetchone()
        if row is None:
            return None
        if _text(row[0]) != request_sha256:
            raise ValueError(
                "The review mutation request identifier was already used for a different request"
            )
        return decode_json(_text(row[1]))

    async def review_mutation_replay(
        self,
        *,
        action: str,
        request_identifier: str,
        request_sha256: str,
    ) -> JsonValue | None:
        """Return an exact prior mutation response after validating caller intent."""

        async with self._connections.reader() as connection:
            return await self._review_mutation_replay(
                connection,
                action=action,
                request_identifier=request_identifier,
                request_sha256=request_sha256,
            )

    @staticmethod
    async def _latest_listing_reviews(
        connection: AsyncConnection,
        *,
        workspace_record_identifier: str,
        listing_identifiers: Sequence[str],
    ) -> dict[str, ListingReviewRecord]:
        """Return current review heads for one workspace and exact listing subset."""

        if len(listing_identifiers) > 10_000 or any(
            not is_listing_identifier(identifier) for identifier in listing_identifiers
        ):
            raise ValueError("Listing-review lookup identifiers are invalid")
        if not listing_identifiers:
            return {}
        parameters: list[apsw.SQLiteValue] = [
            _json(list(LISTING_REVIEW_KIND)),
            workspace_record_identifier,
            _json(list(dict.fromkeys(listing_identifiers))),
        ]
        cursor = await connection.execute(
            """
            SELECT review_record.value_json
            FROM records AS review_record
                INDEXED BY records_listing_review_workspace_listing
            CROSS JOIN objects AS review
              ON review.id = review_record.object_id
             AND review.kind_parts_json = ?
            WHERE json_extract(
                      review_record.value_json, '$.workspace_record_identifier'
                  ) = ?
              AND json_extract(
                      review_record.value_json, '$.workspace_record_identifier'
                  ) IS NOT NULL
              AND json_extract(
                      review_record.value_json, '$.listing_identifier'
                  ) IS NOT NULL
              AND json_extract(review_record.value_json, '$.listing_identifier')
                  IN (SELECT value FROM json_each(?))
            ORDER BY review.rowid, review.id
            """,
            parameters,
        )
        rows = await cursor.fetchall()
        latest: dict[str, ListingReviewRecord] = {}
        for row in rows:
            review = ListingReviewRecord.model_validate_json(_text(row[0]))
            latest[review.listing_identifier] = review
        return latest

    async def latest_listing_reviews(
        self,
        *,
        workspace_record_identifier: str,
        listing_identifiers: Sequence[str],
    ) -> dict[str, ListingReviewRecord]:
        """Return current review heads for one workspace and exact listing subset."""

        async with self._connections.reader() as connection:
            return await self._latest_listing_reviews(
                connection,
                workspace_record_identifier=workspace_record_identifier,
                listing_identifiers=listing_identifiers,
            )

    async def listing_review_scalar_boundaries(
        self, review_record_identifiers: Sequence[str]
    ) -> dict[str, int]:
        """Recover immutable bulk-review evidence boundaries without modifying old reviews."""

        if not review_record_identifiers:
            return {}
        if len(review_record_identifiers) > 10_000:
            raise ValueError("Review scalar history lookup is too large")
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT object.id,
                       json_extract(mutation.response_json, '$.as_of_completion_sequence')
                FROM objects AS object
                JOIN review_mutation_requests AS mutation
                  ON mutation.operation_id = object.created_by_operation_id
                 AND mutation.action = 'record_workspace_bulk_review'
                WHERE object.id IN (SELECT value FROM json_each(?))
                """,
                (_json(list(review_record_identifiers)),),
            )
            rows = await cursor.fetchall()
        return {_text(row[0]): row[1] for row in rows if isinstance(row[1], int) and row[1] >= 0}

    @staticmethod
    async def _record_review_mutation_response(
        connection: AsyncConnection,
        *,
        action: str,
        request_identifier: str,
        request_sha256: str,
        response: JsonValue,
        operation_identifier: str,
        created_at_utc_ns: int,
    ) -> None:
        await connection.execute(
            """
            INSERT INTO review_mutation_requests(
                action, request_identifier, request_sha256,
                request_schema_version, response_schema_version,
                response_json, operation_id, created_at_utc_ns
            ) VALUES (?, ?, ?, 1, 1, ?, ?, ?)
            """,
            (
                action,
                request_identifier,
                request_sha256,
                _json(response),
                operation_identifier,
                created_at_utc_ns,
            ),
        )

    async def active_review_claims(
        self,
        *,
        workspace_record_identifier: str,
        now_utc_ns: int,
        listing_identifiers: Sequence[str] = (),
    ) -> tuple[ReviewClaimLease, ...]:
        """Return active claim groups, optionally limited to exact listing IDs."""

        bindings: list[apsw.SQLiteValue] = [workspace_record_identifier, now_utc_ns]
        listing_filter = ""
        if listing_identifiers:
            placeholders = ", ".join("?" for _ in listing_identifiers)
            listing_filter = f" AND listing_identifier IN ({placeholders})"
            bindings.extend(listing_identifiers)
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                f"""
                SELECT claim_token, owner_identifier, workspace_record_id,
                       batch_record_id, acquired_at_utc_ns,
                       lease_expires_at_utc_ns, listing_identifier
                FROM review_listing_claims
                WHERE workspace_record_id = ?
                  AND lease_expires_at_utc_ns > ?
                  {listing_filter}
                ORDER BY claim_token, listing_identifier
                """,
                bindings,
            )
            rows = await cursor.fetchall()
        grouped: dict[tuple[str, str, str, str, int, int], list[str]] = {}
        for row in rows:
            key = (
                _text(row[0]),
                _text(row[1]),
                _text(row[2]),
                _text(row[3]),
                _integer(row[4]),
                _integer(row[5]),
            )
            grouped.setdefault(key, []).append(_text(row[6]))
        return tuple(
            ReviewClaimLease(
                claim_token=key[0],
                owner_identifier=key[1],
                workspace_record_identifier=key[2],
                batch_record_identifier=key[3],
                acquired_at_utc_ns=key[4],
                lease_expires_at_utc_ns=key[5],
                listing_identifiers=tuple(identifiers),
            )
            for key, identifiers in grouped.items()
        )

    async def publish_acquired_review_batch(
        self,
        *,
        candidate_batch: ReviewBatch,
        claim_token: str,
        owner_identifier: str,
        utc_now_ns: Callable[[], int],
        lease_duration_ns: int,
        action: str,
        request_identifier: str,
        request_sha256: str,
        component: Component,
        operation_identifier: str,
        provenance: CodeProvenance,
        invocation: dict[str, JsonValue],
        started_at_utc: str,
        ended_at_utc: str,
        duration_ns: int,
    ) -> ReviewBatchAcquisition:
        """Atomically publish a batch containing only listings this caller claimed."""

        if lease_duration_ns <= 0:
            raise ValueError("Review claim duration must be positive")
        async with self._connections.writer() as connection:
            replay = await self._review_mutation_replay(
                connection,
                action=action,
                request_identifier=request_identifier,
                request_sha256=request_sha256,
            )
            if replay is not None:
                return ReviewBatchAcquisition.model_validate_json(encode_json(replay))

            acquired_at_utc_ns = utc_now_ns()
            lease_expires_at_utc_ns = acquired_at_utc_ns + lease_duration_ns
            await connection.execute(
                """
                DELETE FROM review_listing_claims
                WHERE workspace_record_id = ? AND lease_expires_at_utc_ns <= ?
                """,
                (candidate_batch.workspace_record_identifier, acquired_at_utc_ns),
            )
            candidate_identifiers = tuple(
                item.projection.listing_identifier for item in candidate_batch.items
            )
            active_identifiers: set[str] = set()
            if candidate_identifiers:
                placeholders = ", ".join("?" for _ in candidate_identifiers)
                cursor = await connection.execute(
                    f"""
                    SELECT listing_identifier
                    FROM review_listing_claims
                    WHERE workspace_record_id = ?
                      AND lease_expires_at_utc_ns > ?
                      AND listing_identifier IN ({placeholders})
                    """,
                    (
                        candidate_batch.workspace_record_identifier,
                        acquired_at_utc_ns,
                        *candidate_identifiers,
                    ),
                )
                active_identifiers = {_text(row[0]) async for row in cursor}
            batch = candidate_batch.model_copy(
                update={
                    "items": tuple(
                        item
                        for item in candidate_batch.items
                        if item.projection.listing_identifier not in active_identifiers
                    )
                }
            )
            lease = (
                None
                if not batch.items
                else ReviewClaimLease(
                    claim_token=claim_token,
                    owner_identifier=owner_identifier,
                    workspace_record_identifier=batch.workspace_record_identifier,
                    batch_record_identifier=batch.record_identifier,
                    acquired_at_utc_ns=acquired_at_utc_ns,
                    lease_expires_at_utc_ns=lease_expires_at_utc_ns,
                    listing_identifiers=tuple(
                        item.projection.listing_identifier for item in batch.items
                    ),
                )
            )
            acquisition = ReviewBatchAcquisition(batch=batch, lease=lease)
            await self._begin_operation(
                connection,
                operation_id=operation_identifier,
                component=component,
                provenance=provenance,
                invocation=invocation,
                configuration={},
                started_at_utc=started_at_utc,
            )
            await self._complete_operation(
                connection,
                operation_id=operation_identifier,
                records=(
                    RecordDraft(
                        identifier=batch.record_identifier,
                        kind=REVIEW_BATCH_KIND,
                        schema_version=1,
                        value=batch.model_dump(mode="json"),
                    ),
                ),
                artifacts=(),
                inputs=(
                    NamedInput(
                        name=("workspace",),
                        object_identifier=batch.workspace_record_identifier,
                    ),
                ),
                outputs=(
                    NamedOutput(name=("review_batch",), object_identifier=batch.record_identifier),
                ),
                result={"state": "completed", "item_count": len(batch.items)},
                ended_at_utc=ended_at_utc,
                duration_ns=duration_ns,
            )
            if lease is not None:
                await connection.executemany(
                    """
                    INSERT INTO review_listing_claims(
                        workspace_record_id, listing_identifier, batch_record_id,
                        claim_token, owner_identifier, acquired_at_utc_ns,
                        lease_expires_at_utc_ns
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        (
                            lease.workspace_record_identifier,
                            listing_identifier,
                            lease.batch_record_identifier,
                            lease.claim_token,
                            lease.owner_identifier,
                            lease.acquired_at_utc_ns,
                            lease.lease_expires_at_utc_ns,
                        )
                        for listing_identifier in lease.listing_identifiers
                    ),
                )
            await self._record_review_mutation_response(
                connection,
                action=action,
                request_identifier=request_identifier,
                request_sha256=request_sha256,
                response=acquisition.model_dump(mode="json"),
                operation_identifier=operation_identifier,
                created_at_utc_ns=acquired_at_utc_ns,
            )
            return acquisition

    async def renew_review_claim(
        self,
        *,
        claim_token: str,
        owner_identifier: str,
        utc_now_ns: Callable[[], int],
        lease_duration_ns: int,
        action: str,
        request_identifier: str,
        request_sha256: str,
        component: Component,
        operation_identifier: str,
        provenance: CodeProvenance,
        invocation: dict[str, JsonValue],
        started_at_utc: str,
        ended_at_utc: str,
        duration_ns: int,
    ) -> ReviewClaimLease | None:
        """Renew one active claim group with ownership and token fencing."""

        if lease_duration_ns <= 0:
            raise ValueError("Review claim duration must be positive")
        async with self._connections.writer() as connection:
            replay = await self._review_mutation_replay(
                connection,
                action=action,
                request_identifier=request_identifier,
                request_sha256=request_sha256,
            )
            if replay is not None:
                return ReviewClaimLease.model_validate_json(encode_json(replay))
            now_utc_ns = utc_now_ns()
            cursor = await connection.execute(
                """
                SELECT workspace_record_id, batch_record_id, acquired_at_utc_ns,
                       lease_expires_at_utc_ns, listing_identifier
                FROM review_listing_claims
                WHERE claim_token = ? AND owner_identifier = ?
                  AND lease_expires_at_utc_ns > ?
                ORDER BY listing_identifier
                """,
                (claim_token, owner_identifier, now_utc_ns),
            )
            rows = await cursor.fetchall()
            if not rows:
                return None
            claim_identity = {
                (_text(row[0]), _text(row[1]), _integer(row[2]), _integer(row[3])) for row in rows
            }
            if len(claim_identity) != 1:
                raise RuntimeError("A review claim token spans inconsistent claim groups")
            workspace_identifier, batch_identifier, acquired_at, previous_expiry = next(
                iter(claim_identity)
            )
            new_expiry = max(previous_expiry + 1, now_utc_ns + lease_duration_ns)
            lease = ReviewClaimLease(
                claim_token=claim_token,
                owner_identifier=owner_identifier,
                workspace_record_identifier=workspace_identifier,
                batch_record_identifier=batch_identifier,
                acquired_at_utc_ns=acquired_at,
                lease_expires_at_utc_ns=new_expiry,
                listing_identifiers=tuple(_text(row[4]) for row in rows),
            )
            await self._begin_operation(
                connection,
                operation_id=operation_identifier,
                component=component,
                provenance=provenance,
                invocation=invocation,
                configuration={},
                started_at_utc=started_at_utc,
            )
            await connection.execute(
                """
                UPDATE review_listing_claims
                SET lease_expires_at_utc_ns = ?
                WHERE claim_token = ? AND owner_identifier = ?
                  AND lease_expires_at_utc_ns = ?
                """,
                (new_expiry, claim_token, owner_identifier, previous_expiry),
            )
            if await connection.changes() != len(rows):
                raise RuntimeError("Review claim changed during renewal")
            await self._complete_operation(
                connection,
                operation_id=operation_identifier,
                records=(),
                artifacts=(),
                inputs=(
                    NamedInput(name=("workspace",), object_identifier=workspace_identifier),
                    NamedInput(name=("review_batch",), object_identifier=batch_identifier),
                ),
                outputs=(),
                result={
                    "state": "completed",
                    "previous_expires_at_utc_ns": previous_expiry,
                    "lease_expires_at_utc_ns": new_expiry,
                },
                ended_at_utc=ended_at_utc,
                duration_ns=duration_ns,
            )
            await self._record_review_mutation_response(
                connection,
                action=action,
                request_identifier=request_identifier,
                request_sha256=request_sha256,
                response=lease.model_dump(mode="json"),
                operation_identifier=operation_identifier,
                created_at_utc_ns=now_utc_ns,
            )
            return lease

    async def release_review_claim(
        self,
        *,
        claim_token: str,
        owner_identifier: str,
        utc_now_ns: Callable[[], int],
        action: str,
        request_identifier: str,
        request_sha256: str,
        component: Component,
        operation_identifier: str,
        provenance: CodeProvenance,
        invocation: dict[str, JsonValue],
        started_at_utc: str,
        ended_at_utc: str,
        duration_ns: int,
    ) -> ReleaseReviewClaimResult | None:
        """Release one active claim group with ownership and token fencing."""

        async with self._connections.writer() as connection:
            replay = await self._review_mutation_replay(
                connection,
                action=action,
                request_identifier=request_identifier,
                request_sha256=request_sha256,
            )
            if replay is not None:
                return ReleaseReviewClaimResult.model_validate_json(encode_json(replay))
            now_utc_ns = utc_now_ns()
            cursor = await connection.execute(
                """
                SELECT workspace_record_id, batch_record_id, listing_identifier
                FROM review_listing_claims
                WHERE claim_token = ? AND owner_identifier = ?
                  AND lease_expires_at_utc_ns > ?
                ORDER BY listing_identifier
                """,
                (claim_token, owner_identifier, now_utc_ns),
            )
            rows = await cursor.fetchall()
            if not rows:
                return None
            claim_identity = {(_text(row[0]), _text(row[1])) for row in rows}
            if len(claim_identity) != 1:
                raise RuntimeError("A review claim token spans inconsistent claim groups")
            workspace_identifier, batch_identifier = next(iter(claim_identity))
            result = ReleaseReviewClaimResult(
                claim_token=claim_token,
                released_listing_count=len(rows),
                released_at_utc_ns=now_utc_ns,
            )
            await self._begin_operation(
                connection,
                operation_id=operation_identifier,
                component=component,
                provenance=provenance,
                invocation=invocation,
                configuration={},
                started_at_utc=started_at_utc,
            )
            await connection.execute(
                """
                DELETE FROM review_listing_claims
                WHERE claim_token = ? AND owner_identifier = ?
                  AND lease_expires_at_utc_ns > ?
                """,
                (claim_token, owner_identifier, now_utc_ns),
            )
            if await connection.changes() != len(rows):
                raise RuntimeError("Review claim changed during release")
            await self._complete_operation(
                connection,
                operation_id=operation_identifier,
                records=(),
                artifacts=(),
                inputs=(
                    NamedInput(name=("workspace",), object_identifier=workspace_identifier),
                    NamedInput(name=("review_batch",), object_identifier=batch_identifier),
                ),
                outputs=(),
                result={"state": "completed", "released_listing_count": len(rows)},
                ended_at_utc=ended_at_utc,
                duration_ns=duration_ns,
            )
            await self._record_review_mutation_response(
                connection,
                action=action,
                request_identifier=request_identifier,
                request_sha256=request_sha256,
                response=result.model_dump(mode="json"),
                operation_identifier=operation_identifier,
                created_at_utc_ns=now_utc_ns,
            )
            return result

    async def publish_listing_review_records(
        self,
        *,
        action: str,
        request_identifier: str,
        request_sha256: str,
        workspace_record_identifier: str,
        batch_record_identifier: str | None,
        claim_token: str | None,
        claim_owner_identifier: str | None,
        utc_now_ns: Callable[[], int],
        component: Component,
        operation_identifier: str,
        records: Sequence[RecordDraft],
        outputs: Sequence[NamedOutput],
        provenance: CodeProvenance,
        invocation: dict[str, JsonValue],
        started_at_utc: str,
        ended_at_utc: str,
        duration_ns: int,
    ) -> RecordListingReviewsResult | None:
        """Fence review publication against active claims and release reviewed members."""

        response = RecordListingReviewsResult.model_validate(
            {"records": tuple(cast(dict[str, JsonValue], record.value) for record in records)}
        )
        listing_identifiers = tuple(
            str(cast(dict[str, JsonValue], record.value)["listing_identifier"])
            for record in records
        )
        async with self._connections.writer() as connection:
            replay = await self._review_mutation_replay(
                connection,
                action=action,
                request_identifier=request_identifier,
                request_sha256=request_sha256,
            )
            if replay is not None:
                return RecordListingReviewsResult.model_validate_json(encode_json(replay))

            now_utc_ns = utc_now_ns()
            acquired_batch = False
            if batch_record_identifier is not None:
                cursor = await connection.execute(
                    """
                    SELECT operations.component_parts_json
                    FROM objects
                    JOIN operations ON operations.id = objects.created_by_operation_id
                    WHERE objects.id = ?
                    """,
                    (batch_record_identifier,),
                )
                batch_row = await cursor.fetchone()
                if batch_row is None:
                    return None
                acquired_batch = _text(batch_row[0]) == _json(list(ACQUIRE_REVIEW_BATCH.parts))

            placeholders = ", ".join("?" for _ in listing_identifiers)
            cursor = await connection.execute(
                f"""
                SELECT listing_identifier, batch_record_id, claim_token, owner_identifier
                FROM review_listing_claims
                WHERE workspace_record_id = ?
                  AND lease_expires_at_utc_ns > ?
                  AND listing_identifier IN ({placeholders})
                """,
                (workspace_record_identifier, now_utc_ns, *listing_identifiers),
            )
            active_rows = await cursor.fetchall()
            if claim_token is None or claim_owner_identifier is None:
                if acquired_batch or active_rows:
                    return None
            else:
                if batch_record_identifier is None or len(active_rows) != len(listing_identifiers):
                    return None
                expected = {
                    (
                        listing_identifier,
                        batch_record_identifier,
                        claim_token,
                        claim_owner_identifier,
                    )
                    for listing_identifier in listing_identifiers
                }
                actual = {
                    (_text(row[0]), _text(row[1]), _text(row[2]), _text(row[3]))
                    for row in active_rows
                }
                if actual != expected:
                    return None

            await self._begin_operation(
                connection,
                operation_id=operation_identifier,
                component=component,
                provenance=provenance,
                invocation=invocation,
                configuration={},
                started_at_utc=started_at_utc,
            )
            inputs = [
                NamedInput(name=("workspace",), object_identifier=workspace_record_identifier)
            ]
            if batch_record_identifier is not None:
                inputs.append(
                    NamedInput(name=("review_batch",), object_identifier=batch_record_identifier)
                )
            await self._complete_operation(
                connection,
                operation_id=operation_identifier,
                records=records,
                artifacts=(),
                inputs=inputs,
                outputs=outputs,
                result={"state": "completed", "recorded_count": len(records)},
                ended_at_utc=ended_at_utc,
                duration_ns=duration_ns,
            )
            if claim_token is not None and claim_owner_identifier is not None:
                await connection.execute(
                    f"""
                    DELETE FROM review_listing_claims
                    WHERE workspace_record_id = ? AND batch_record_id = ?
                      AND claim_token = ? AND owner_identifier = ?
                      AND listing_identifier IN ({placeholders})
                    """,
                    (
                        workspace_record_identifier,
                        batch_record_identifier,
                        claim_token,
                        claim_owner_identifier,
                        *listing_identifiers,
                    ),
                )
                if await connection.changes() != len(listing_identifiers):
                    raise RuntimeError("Review claims changed while reviews were published")
            await self._record_review_mutation_response(
                connection,
                action=action,
                request_identifier=request_identifier,
                request_sha256=request_sha256,
                response=response.model_dump(mode="json"),
                operation_identifier=operation_identifier,
                created_at_utc_ns=now_utc_ns,
            )
            return response

    async def publish_workspace_bulk_review_records(
        self,
        *,
        request_identifier: str,
        request_sha256: str,
        workspace_record_identifier: str,
        expected_prior_review_identifiers: dict[str, str | None],
        component: Component,
        operation_identifier: str,
        records: Sequence[RecordDraft],
        source_input: NamedInput | None,
        response: RecordWorkspaceBulkReviewResult,
        provenance: CodeProvenance,
        invocation: dict[str, JsonValue],
        started_at_utc: str,
        ended_at_utc: str,
        duration_ns: int,
        utc_now_ns: Callable[[], int],
    ) -> RecordWorkspaceBulkReviewResult | None:
        """Atomically publish a bulk review after claim and review-head fences."""

        action = "record_workspace_bulk_review"
        listing_identifiers = tuple(expected_prior_review_identifiers)
        if len(records) != len(listing_identifiers) or len(records) > 10_000:
            raise ValueError("Bulk review publication bounds are invalid")
        async with self._connections.writer() as connection:
            replay = await self._review_mutation_replay(
                connection,
                action=action,
                request_identifier=request_identifier,
                request_sha256=request_sha256,
            )
            if replay is not None:
                return RecordWorkspaceBulkReviewResult.model_validate_json(encode_json(replay))

            now_utc_ns = utc_now_ns()
            if listing_identifiers:
                cursor = await connection.execute(
                    """
                    SELECT 1
                    FROM review_listing_claims
                    WHERE workspace_record_id = ?
                      AND lease_expires_at_utc_ns > ?
                      AND listing_identifier IN (SELECT value FROM json_each(?))
                    LIMIT 1
                    """,
                    (
                        workspace_record_identifier,
                        now_utc_ns,
                        _json(list(listing_identifiers)),
                    ),
                )
                if await cursor.fetchone() is not None:
                    return None
                current_reviews = await self._latest_listing_reviews(
                    connection,
                    workspace_record_identifier=workspace_record_identifier,
                    listing_identifiers=listing_identifiers,
                )
                actual = {
                    identifier: (
                        None
                        if current_reviews.get(identifier) is None
                        else current_reviews[identifier].record_identifier
                    )
                    for identifier in listing_identifiers
                }
                if actual != expected_prior_review_identifiers:
                    return None

            await self._begin_operation(
                connection,
                operation_id=operation_identifier,
                component=component,
                provenance=provenance,
                invocation=invocation,
                configuration={},
                started_at_utc=started_at_utc,
            )
            inputs = [
                NamedInput(name=("workspace",), object_identifier=workspace_record_identifier)
            ]
            if source_input is not None:
                inputs.append(source_input)
            await self._complete_operation(
                connection,
                operation_id=operation_identifier,
                records=records,
                artifacts=(),
                inputs=inputs,
                outputs=tuple(
                    NamedOutput(
                        name=("listing_review", str(index)),
                        object_identifier=record.identifier,
                    )
                    for index, record in enumerate(records)
                ),
                result={"state": "completed", "recorded_count": len(records)},
                ended_at_utc=ended_at_utc,
                duration_ns=duration_ns,
            )
            await self._record_review_mutation_response(
                connection,
                action=action,
                request_identifier=request_identifier,
                request_sha256=request_sha256,
                response=response.model_dump(mode="json"),
                operation_identifier=operation_identifier,
                created_at_utc_ns=now_utc_ns,
            )
            return response

    async def _require_workspace_track_version(
        self,
        connection: AsyncConnection,
        workspace_identifier: str,
        track_identifier: str,
        expected_version: int,
    ) -> None:
        cursor = await connection.execute(
            """
            SELECT coalesce(max(json_extract(record.value_json, '$.version')), 1)
            FROM records AS record JOIN objects AS object ON object.id = record.object_id
            WHERE object.kind_parts_json = ?
              AND json_extract(record.value_json, '$.workspace_record_identifier') = ?
              AND json_extract(record.value_json, '$.track_identifier') = ?
            """,
            (
                _json(list(WORKSPACE_SEARCH_TRACK_SPECIFICATION_KIND)),
                workspace_identifier,
                track_identifier,
            ),
        )
        rows = await cursor.fetchall()
        current_version = _integer(rows[0][0])
        if current_version != expected_version:
            raise ReviewInputError(
                f"Search track version conflict: expected {expected_version}, "
                f"current {current_version}; read the current track before revising or refreshing"
            )

    async def publish_records_operation(
        self,
        *,
        component: Component,
        operation_identifier: str,
        records: Sequence[RecordDraft],
        inputs: Sequence[NamedInput],
        outputs: Sequence[NamedOutput],
        provenance: CodeProvenance,
        invocation: dict[str, JsonValue],
        started_at_utc: str,
        ended_at_utc: str,
        duration_ns: int,
        result: JsonValue,
        marketplace_target_guard: MarketplaceSearchTargetRecord | None = None,
        workspace_track_guard: tuple[str, str, int] | None = None,
    ) -> None:
        """Atomically publish a small local mutation as a provenance-linked operation."""

        async with self._connections.writer() as connection:
            if workspace_track_guard is not None:
                await self._require_workspace_track_version(connection, *workspace_track_guard)
            if marketplace_target_guard is not None:
                cursor = await connection.execute(
                    """
                    SELECT records.value_json
                    FROM records JOIN objects ON objects.id = records.object_id
                    WHERE objects.kind_parts_json = ?
                      AND json_extract(records.value_json, '$.search_record_identifier') = ?
                    """,
                    (
                        _json(list(MARKETPLACE_SEARCH_TARGET_KIND)),
                        marketplace_target_guard.search_record_identifier,
                    ),
                )
                existing = tuple(
                    MarketplaceSearchTargetRecord.model_validate(decode_json(_text(row[0])))
                    for row in await cursor.fetchall()
                )
                if len(existing) >= 20:
                    raise ValueError("A marketplace search supports at most 20 targets")
                if any(
                    target.specification == marketplace_target_guard.specification
                    for target in existing
                ):
                    raise ValueError("The marketplace search already contains this target")
            await self._begin_operation(
                connection,
                operation_id=operation_identifier,
                component=component,
                provenance=provenance,
                invocation=invocation,
                configuration={},
                started_at_utc=started_at_utc,
            )
            await self._complete_operation(
                connection,
                operation_id=operation_identifier,
                records=records,
                artifacts=(),
                inputs=inputs,
                outputs=outputs,
                result=result,
                ended_at_utc=ended_at_utc,
                duration_ns=duration_ns,
            )

    async def workspace_search_track_specifications(
        self, workspace_identifier: str, track_identifier: str | None = None
    ) -> tuple[tuple[str, JsonValue], ...]:
        """Read immutable versions scoped to one workspace, in version order."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT record.object_id, record.value_json
                FROM records AS record JOIN objects AS object ON object.id = record.object_id
                WHERE object.kind_parts_json = ?
                  AND json_extract(record.value_json, '$.workspace_record_identifier') = ?
                  AND (? IS NULL OR json_extract(record.value_json, '$.track_identifier') = ?)
                ORDER BY json_extract(record.value_json, '$.version'), object.rowid
                """,
                (
                    _json(list(WORKSPACE_SEARCH_TRACK_SPECIFICATION_KIND)),
                    workspace_identifier,
                    track_identifier,
                    track_identifier,
                ),
            )
            rows = await cursor.fetchall()
        return tuple((_text(row[0]), decode_json(_text(row[1]))) for row in rows)

    async def publish_review_workset_revision(
        self,
        *,
        workset_identifier: str,
        workspace_record_identifier: str,
        name: str,
        expected_version: int | None,
        listing_identifiers: Sequence[str],
        component: Component,
        operation_identifier: str,
        record_identifier: str,
        provenance: CodeProvenance,
        invocation: dict[str, JsonValue],
        started_at_utc: str,
        ended_at_utc: str,
        duration_ns: int,
    ) -> tuple[int, str] | ReviewWorksetConflict:
        """Create or revise a workset with optimistic concurrency control."""

        if len(listing_identifiers) > MAXIMUM_WORKSET_LISTINGS:
            raise ValueError("Workset membership exceeds the supported limit")
        async with self._connections.writer() as connection:
            cursor = await connection.execute(
                """
                SELECT objects.id, records.value_json
                FROM objects
                JOIN records ON records.object_id = objects.id
                WHERE objects.kind_parts_json = ?
                  AND json_extract(records.value_json, '$.workset_identifier') = ?
                ORDER BY CAST(json_extract(records.value_json, '$.version') AS INTEGER) DESC,
                         objects.rowid DESC
                LIMIT 1
                """,
                (_json(list(REVIEW_WORKSET_KIND)), workset_identifier),
            )
            current = await cursor.fetchone()
            current_value = (
                None
                if current is None
                else cast(dict[str, JsonValue], decode_json(_text(current[1])))
            )
            current_version = 0 if current_value is None else _integer(current_value["version"])
            if expected_version is None:
                if current is not None:
                    return ReviewWorksetConflict(
                        workset_identifier=workset_identifier,
                        expected_version=0,
                        current_version=current_version,
                        current_record_identifier=_text(current[0]),
                    )
            elif current is None:
                raise KeyError(workset_identifier)
            elif current_version != expected_version:
                return ReviewWorksetConflict(
                    workset_identifier=workset_identifier,
                    expected_version=expected_version,
                    current_version=current_version,
                    current_record_identifier=_text(current[0]),
                )
            version = current_version + 1
            value: dict[str, JsonValue] = {
                "record_identifier": record_identifier,
                "workset_identifier": workset_identifier,
                "workspace_record_identifier": workspace_record_identifier,
                "name": name,
                "version": version,
                "listing_identifiers": list(listing_identifiers),
                "created_at_utc": ended_at_utc,
            }
            operation_inputs: tuple[tuple[tuple[str, ...], str], ...] = (
                (("workspace",), workspace_record_identifier),
            )
            if current is not None:
                operation_inputs += ((("previous_workset",), _text(current[0])),)
            await self._begin_operation(
                connection,
                operation_id=operation_identifier,
                component=component,
                provenance=provenance,
                invocation=invocation,
                configuration={},
                started_at_utc=started_at_utc,
                inputs=operation_inputs,
            )
            await self._complete_operation(
                connection,
                operation_id=operation_identifier,
                records=(
                    RecordDraft(
                        identifier=record_identifier,
                        kind=REVIEW_WORKSET_KIND,
                        schema_version=1,
                        value=value,
                    ),
                ),
                artifacts=(),
                inputs=(),
                outputs=(NamedOutput(name=("workset",), object_identifier=record_identifier),),
                result={"state": "completed", "version": version},
                ended_at_utc=ended_at_utc,
                duration_ns=duration_ns,
            )
        return version, record_identifier

    async def object_operation_relations(
        self, identifier: str, *, maximum_outputs: int | None = None
    ) -> tuple[str, tuple[NamedInput, ...], tuple[NamedOutput, ...]]:
        """Return the producing operation and its explicit provenance edges."""

        if maximum_outputs is not None and maximum_outputs < 0:
            raise ValueError("Maximum outputs must be nonnegative")

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT created_by_operation_id FROM objects WHERE id = ?",
                (identifier,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise KeyError(identifier)
            operation_identifier = _text(row[0])
            cursor = await connection.execute(
                """
                SELECT name_parts_json, object_id
                FROM operation_inputs
                WHERE operation_id = ?
                ORDER BY name_parts_json, object_id
                """,
                (operation_identifier,),
            )
            input_rows = await cursor.fetchall()
            output_limit = "" if maximum_outputs is None else "LIMIT ?"
            cursor = await connection.execute(
                f"""
                SELECT name_parts_json, object_id
                FROM operation_outputs
                WHERE operation_id = ?
                ORDER BY name_parts_json, object_id
                {output_limit}
                """,
                (
                    (operation_identifier,)
                    if maximum_outputs is None
                    else (operation_identifier, maximum_outputs)
                ),
            )
            output_rows = await cursor.fetchall()

        def name(value: apsw.SQLiteValue) -> tuple[str, ...]:
            decoded = decode_json(_text(value))
            if not isinstance(decoded, list) or not all(isinstance(part, str) for part in decoded):
                raise ValueError("Invalid stored relation name")
            return tuple(decoded)

        return (
            operation_identifier,
            tuple(
                NamedInput(name=name(row[0]), object_identifier=_text(row[1])) for row in input_rows
            ),
            tuple(
                NamedOutput(name=name(row[0]), object_identifier=_text(row[1]))
                for row in output_rows
            ),
        )

    async def operation_output_count(self, operation_identifier: str) -> int:
        """Count one operation's immutable direct output edges without loading them."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT count(*) FROM operation_outputs WHERE operation_id = ?",
                (operation_identifier,),
            )
            row = await cursor.fetchone()
        if row is None:
            raise AssertionError("Output count query returned no row")
        return _integer(row[0])

    async def facebook_analysis_descriptors(
        self,
        observation_identifiers: Sequence[str] | None = None,
        *,
        maximum_completion_sequence: int | None = None,
    ) -> tuple[AnalysisDescriptor, ...]:
        """Return completed item analyses linked to their exact evidence observations."""

        if maximum_completion_sequence is not None and maximum_completion_sequence < 0:
            raise ValueError("Maximum analysis completion sequence must not be negative")

        parameters: list[apsw.SQLiteValue] = [
            _json(["carl", "facebook", "item_analysis"]),
            _json(["carl", "facebook", "work", "analyze_item"]),
            _json(["listing_observation"]),
        ]
        observation_filter = ""
        if observation_identifiers is not None:
            if not observation_identifiers:
                return ()
            observation_filter = "AND evidence_input.object_id IN (SELECT value FROM json_each(?))"
            parameters.append(_json(list(dict.fromkeys(observation_identifiers))))
        completion_filter = ""
        if maximum_completion_sequence is not None:
            completion_filter = "AND completed.sequence <= ?"
            parameters.append(maximum_completion_sequence)
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                f"""
                SELECT analysis.id, analysis_record.value_json, work.payload_json,
                       evidence_input.object_id, completed.sequence, operation.ended_at_utc
                FROM objects AS analysis
                JOIN records AS analysis_record
                  ON analysis_record.object_id = analysis.id
                JOIN operations AS operation
                  ON operation.id = analysis.created_by_operation_id
                JOIN work_operations AS work_operation
                  ON work_operation.operation_id = analysis.created_by_operation_id
                JOIN work_items AS work
                  ON work.id = work_operation.work_item_id
                 AND work.kind_parts_json = ?
                 AND work.state = 'completed'
                JOIN work_events AS completed
                  ON completed.work_item_id = work.id
                 AND completed.event_kind = 'completed'
                JOIN objects AS evidence
                  ON evidence.id = json_extract(
                      work.payload_json, '$.evidence_set_record_identifier'
                  )
                JOIN operation_inputs AS evidence_input
                  ON evidence_input.operation_id = evidence.created_by_operation_id
                 AND evidence_input.name_parts_json = ?
                WHERE analysis.kind_parts_json = ?
                  AND operation.state = 'completed'
                  {observation_filter}
                  {completion_filter}
                ORDER BY completed.sequence, analysis.id
                """,
                (
                    parameters[1],
                    parameters[2],
                    parameters[0],
                    *parameters[3:],
                ),
            )
            rows = await cursor.fetchall()
        return tuple(_analysis_descriptor(row) for row in rows)

    async def facebook_analysis_descriptor(self, record_identifier: str) -> AnalysisDescriptor:
        """Return one analysis attempt, including a retained failed attempt."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT analysis.id, analysis_record.value_json, work.payload_json,
                       evidence_input.object_id, outcome.sequence, operation.ended_at_utc
                FROM objects AS analysis
                JOIN records AS analysis_record
                  ON analysis_record.object_id = analysis.id
                JOIN operations AS operation
                  ON operation.id = analysis.created_by_operation_id
                JOIN work_operations AS work_operation
                  ON work_operation.operation_id = analysis.created_by_operation_id
                JOIN work_items AS work
                  ON work.id = work_operation.work_item_id
                 AND work.kind_parts_json = ?
                JOIN work_events AS outcome
                  ON outcome.work_item_id = work.id
                 AND outcome.event_kind IN ('released', 'completed', 'terminal_failure')
                 AND json_extract(outcome.data_json, '$.operation_identifier') = operation.id
                JOIN objects AS evidence
                  ON evidence.id = json_extract(
                      work.payload_json, '$.evidence_set_record_identifier'
                  )
                JOIN operation_inputs AS evidence_input
                  ON evidence_input.operation_id = evidence.created_by_operation_id
                 AND evidence_input.name_parts_json = ?
                WHERE analysis.id = ?
                  AND analysis.kind_parts_json = ?
                ORDER BY outcome.sequence DESC
                LIMIT 1
                """,
                (
                    _json(["carl", "facebook", "work", "analyze_item"]),
                    _json(["listing_observation"]),
                    record_identifier,
                    _json(["carl", "facebook", "item_analysis"]),
                ),
            )
            row = await cursor.fetchone()
        if row is None:
            raise KeyError(record_identifier)
        return _analysis_descriptor(row)

    async def facebook_projection_analysis_descriptors(
        self,
        listing_identifiers: Sequence[str],
        *,
        product_guide_record_identifier: str | None,
        maximum_per_listing: int,
        as_of_completion_sequence: int,
    ) -> tuple[tuple[str, AnalysisDescriptor], ...]:
        """Return bounded latest completed analyses across all listing observations."""

        if not listing_identifiers:
            return ()
        if (
            len(listing_identifiers) > 10_000
            or any(not identifier.isdecimal() for identifier in listing_identifiers)
            or not 1 <= maximum_per_listing <= 21
            or as_of_completion_sequence < 0
        ):
            raise ValueError("Projection analysis bounds are invalid")
        guide_filter = ""
        parameters: list[apsw.SQLiteValue] = [
            _json(list(dict.fromkeys(listing_identifiers))),
            _json(["carl", "facebook", "work", "analyze_item"]),
            _json(["listing_observation"]),
            _json(["carl", "facebook", "listing_observation"]),
            _json(["carl", "facebook", "item_analysis"]),
            as_of_completion_sequence,
        ]
        if product_guide_record_identifier is not None:
            guide_filter = (
                "AND json_extract(work.payload_json, '$.product_guide_record_identifier') = ?"
            )
            parameters.append(product_guide_record_identifier)
        parameters.append(maximum_per_listing)
        async with self._connections.reader() as connection:
            # Start from completed analysis work once. Starting from requested listings
            # lets SQLite reorder this chain into a full analysis scan per listing;
            # indexed CROSS JOINs preserve the single-pass direction without restricting
            # historical payload schema versions to fit the analysis lookup index.
            cursor = await connection.execute(
                f"""
                WITH requested(listing_identifier) AS MATERIALIZED (
                    SELECT value FROM json_each(?)
                ),
                candidates AS MATERIALIZED (
                    SELECT
                        json_extract(
                            observation_record.value_json, '$.listing_id'
                        ) AS listing_identifier,
                        analysis.id,
                        analysis_record.value_json,
                        work.payload_json,
                        evidence_input.object_id AS observation_identifier,
                        completed.sequence,
                        operation.ended_at_utc,
                        row_number() OVER (
                            PARTITION BY json_extract(
                                             observation_record.value_json, '$.listing_id'
                                         ),
                                         json_extract(
                                             work.payload_json,
                                             '$.product_guide_record_identifier'
                                         )
                            ORDER BY completed.sequence DESC, analysis.id DESC
                        ) AS guide_rank
                    FROM work_items AS work
                        INDEXED BY work_items_analysis_lookup
                    CROSS JOIN work_operations AS work_operation
                        INDEXED BY sqlite_autoindex_work_operations_2
                      ON work_operation.work_item_id = work.id
                    CROSS JOIN objects AS analysis
                        INDEXED BY objects_operation_kind
                      ON analysis.created_by_operation_id = work_operation.operation_id
                     AND analysis.kind_parts_json = ?
                    CROSS JOIN records AS analysis_record
                        INDEXED BY sqlite_autoindex_records_1
                      ON analysis_record.object_id = analysis.id
                    CROSS JOIN operations AS operation
                        INDEXED BY sqlite_autoindex_operations_1
                      ON operation.id = analysis.created_by_operation_id
                     AND operation.state = 'completed'
                    CROSS JOIN work_events AS completed
                        INDEXED BY work_events_work_item
                      ON completed.work_item_id = work.id
                     AND completed.event_kind = 'completed'
                     AND completed.sequence <= ?
                    CROSS JOIN objects AS evidence
                        INDEXED BY sqlite_autoindex_objects_1
                      ON evidence.id = json_extract(
                             work.payload_json, '$.evidence_set_record_identifier'
                         )
                    CROSS JOIN operation_inputs AS evidence_input
                        INDEXED BY sqlite_autoindex_operation_inputs_1
                      ON evidence_input.operation_id = evidence.created_by_operation_id
                     AND evidence_input.name_parts_json = ?
                    CROSS JOIN objects AS observation
                        INDEXED BY sqlite_autoindex_objects_1
                      ON observation.id = evidence_input.object_id
                     AND observation.kind_parts_json = ?
                    CROSS JOIN records AS observation_record
                        INDEXED BY sqlite_autoindex_records_1
                      ON observation_record.object_id = observation.id
                    WHERE work.kind_parts_json = ? AND work.state = 'completed'
                      AND json_extract(
                              observation_record.value_json, '$.listing_id'
                          ) IS NOT NULL
                      AND json_extract(
                              observation_record.value_json, '$.listing_id'
                          ) IN (SELECT listing_identifier FROM requested)
                      {guide_filter}
                ),
                latest_per_guide AS MATERIALIZED (
                    SELECT *, row_number() OVER (
                        PARTITION BY listing_identifier
                        ORDER BY sequence DESC, id DESC
                    ) AS listing_rank
                    FROM candidates
                    WHERE guide_rank = 1
                )
                SELECT listing_identifier, id, value_json, payload_json,
                       observation_identifier, sequence, ended_at_utc
                FROM latest_per_guide
                WHERE listing_rank <= ?
                ORDER BY listing_identifier, sequence DESC, id DESC
                """,
                (
                    parameters[0],
                    parameters[4],
                    parameters[5],
                    parameters[2],
                    parameters[3],
                    parameters[1],
                    *parameters[6:],
                ),
            )
            rows = await cursor.fetchall()
        return tuple((_text(row[0]), _analysis_descriptor(row[1:])) for row in rows)

    async def current_completion_boundary(self) -> int:
        """Return the latest durable successful or saved partial publication sequence."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                f"""
                WITH latest_completed AS MATERIALIZED (
                    SELECT COALESCE(MAX(sequence), 0) AS sequence
                    FROM work_events WHERE event_kind = 'completed'
                )
                SELECT MAX(sequence) FROM (
                    SELECT sequence FROM latest_completed
                    UNION ALL
                    SELECT COALESCE(MAX(event.sequence), 0)
                    FROM latest_completed AS latest
                    CROSS JOIN work_events AS event ON event.sequence > latest.sequence
                    JOIN work_operations AS owner
                      ON owner.operation_id = json_extract(event.data_json, '$.operation_identifier')
                     AND owner.work_item_id = event.work_item_id
                    JOIN work_items AS work ON work.id = owner.work_item_id
                    JOIN operations AS operation ON operation.id = owner.operation_id
                    WHERE work.kind_parts_json = '["carl","facebook","work","collect_search"]'
                      AND operation.state = 'failed'
                      AND {_facebook_search_publication_event("operation", "event")}
                )
                """
            )
            row = await cursor.fetchone()
        if row is None:
            raise AssertionError("Completion-boundary query returned no row")
        return _integer(row[0])

    async def latest_travel_estimate_records(
        self,
        listing_identifiers: Sequence[str],
        *,
        origin_identity: str,
        destination_identities: dict[str, str],
        as_of_completion_sequence: int,
    ) -> dict[str, tuple[str, JsonValue]]:
        """Seek exact listings' newest completed route records at a frozen boundary."""

        if (
            len(listing_identifiers) > 1000
            or any(not is_listing_identifier(identifier) for identifier in listing_identifiers)
            or not origin_identity
            or as_of_completion_sequence < 0
            or any(not destination_identities.get(identifier) for identifier in listing_identifiers)
        ):
            raise ValueError("Travel estimate lookup bounds or identities are invalid")
        if not listing_identifiers:
            return {}
        requested = [
            {"listing_id": identifier, "destination_identity": destination_identities[identifier]}
            for identifier in dict.fromkeys(listing_identifiers)
        ]
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                WITH travel_requested AS MATERIALIZED (
                    SELECT json_extract(value, '$.listing_id') AS listing_identifier,
                           json_extract(value, '$.destination_identity') AS destination_identity
                    FROM json_each(?)
                ),
                travel_ranked AS MATERIALIZED (
                    SELECT requested.listing_identifier,
                           estimate.id AS record_identifier,
                           record.value_json,
                           row_number() OVER (
                               PARTITION BY requested.listing_identifier
                               ORDER BY completed.sequence DESC, estimate.rowid DESC
                           ) AS history_rank
                    FROM travel_requested AS requested
                    CROSS JOIN records AS record
                        INDEXED BY records_listing_observation_listing
                      ON json_extract(record.value_json, '$.listing_id') =
                         requested.listing_identifier
                    CROSS JOIN objects AS estimate
                        INDEXED BY sqlite_autoindex_objects_1
                      ON estimate.id = record.object_id
                     AND estimate.kind_parts_json = ?
                    CROSS JOIN operations AS operation
                        INDEXED BY sqlite_autoindex_operations_1
                      ON operation.id = estimate.created_by_operation_id
                     AND operation.state = 'completed'
                    CROSS JOIN work_operations AS work
                        INDEXED BY sqlite_autoindex_work_operations_1
                      ON work.operation_id = estimate.created_by_operation_id
                    CROSS JOIN work_events AS completed
                        INDEXED BY work_events_work_item
                      ON completed.work_item_id = work.work_item_id
                     AND completed.event_kind = 'completed'
                     AND completed.sequence <= ?
                    WHERE json_extract(record.value_json, '$.listing_id') IS NOT NULL
                      AND record.schema_version = 1
                      AND json_extract(record.value_json, '$.origin_identity') = ?
                      AND json_extract(record.value_json, '$.destination_identity') =
                          requested.destination_identity
                )
                SELECT listing_identifier, record_identifier, value_json
                FROM travel_ranked
                WHERE history_rank = 1
                """,
                (
                    _json(requested),
                    _json(["carl", "travel", "estimate"]),
                    as_of_completion_sequence,
                    origin_identity,
                ),
            )
            rows = await cursor.fetchall()
        return {_text(row[0]): (_text(row[1]), decode_json(_text(row[2]))) for row in rows}

    async def current_object_boundary(self) -> int:
        """Freeze immutable publications, including records without work events."""
        async with self._connections.reader() as connection:
            cursor = await connection.execute("SELECT COALESCE(MAX(rowid), 0) FROM objects")
            row = await cursor.fetchone()
        if row is None:
            raise AssertionError("Object-boundary query returned no row")
        return _integer(row[0])

    async def facebook_projection_search_run(
        self,
        record_identifier: str,
        *,
        as_of_completion_sequence: int,
    ) -> SearchRunCandidate:
        """Read one exact published search run and its explicit refresh parent."""

        if as_of_completion_sequence < 0:
            raise ValueError("Completion boundary must not be negative")
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                f"""
                SELECT search_run.id,
                       json_extract(record.value_json, '$.search_run_identifier'),
                       completed.sequence,
                       operation.started_at_utc,
                       operation.ended_at_utc,
                       (
                           SELECT json_extract(
                               refresh.payload_json,
                               '$.base_search_run_record_identifier'
                           )
                           FROM work_requests AS request
                           JOIN work_items AS refresh
                             ON refresh.id = request.requester_identifier
                            AND refresh.kind_parts_json = ?
                           WHERE request.work_item_id = search_work.work_item_id
                             AND request.requester_kind_parts_json = ?
                           ORDER BY request.requested_at_utc_ns, request.id
                           LIMIT 1
                       ),
                       json_extract(record.value_json, '$.traversal.stopping_reason'),
                       json_extract(record.value_json, '$.request')
                FROM objects AS search_run
                JOIN records AS record ON record.object_id = search_run.id
                JOIN operations AS operation
                  ON operation.id = search_run.created_by_operation_id
                JOIN work_operations AS search_work
                  ON search_work.operation_id = search_run.created_by_operation_id
                JOIN work_events AS completed
                  ON completed.work_item_id = search_work.work_item_id
                 AND {_facebook_search_publication_event("operation", "completed", run_identifier="search_run.id")}
                 AND completed.sequence <= ?
                WHERE search_run.id = ?
                  AND search_run.kind_parts_json = ?
                """,
                (
                    _json(["carl", "facebook", "work", "refresh_search"]),
                    _json(["carl", "facebook", "search_refresh", "search"]),
                    as_of_completion_sequence,
                    record_identifier,
                    _json(["carl", "facebook", "search_run"]),
                ),
            )
            row = await cursor.fetchone()
        if row is None:
            raise KeyError(record_identifier)
        internal_identifier = row[1]
        if not isinstance(internal_identifier, str) or not internal_identifier:
            raise ValueError("Stored Facebook search run has no internal identifier")
        refresh_source = row[5]
        stopping_reason = row[6]
        if refresh_source is not None and not isinstance(refresh_source, str):
            raise ValueError("Stored Facebook search run has an invalid refresh parent")
        if stopping_reason is not None and not isinstance(stopping_reason, str):
            raise ValueError("Stored Facebook search run has an invalid stopping reason")
        return SearchRunCandidate(
            record_identifier=_text(row[0]),
            internal_search_run_identifier=internal_identifier,
            completion_sequence=_integer(row[2]),
            started_at_utc=_text(row[3]),
            completed_at_utc=None if row[4] is None else _text(row[4]),
            refresh_source_run_record_identifier=refresh_source,
            stopping_reason=(
                None if stopping_reason is None else SearchStoppingReason(stopping_reason)
            ),
            search_scope_sha256=search_scope_sha256(
                "facebook", decode_json(_text(row[7])) if row[7] is not None else None
            ),
        )

    async def facebook_projection_membership_candidates(
        self,
        included_search_runs: Sequence[tuple[str, str]],
        *,
        as_of_completion_sequence: int,
        maximum_listings: int,
        after_position: tuple[int, str] | None = None,
    ) -> tuple[FacebookProjectionMembershipCandidate, ...]:
        """Return bounded distinct lineage members in stable newest-run order."""

        if not included_search_runs:
            return ()
        if len(included_search_runs) > 100:
            raise ValueError("At most 100 search runs may be included")
        if len(set(included_search_runs)) != len(included_search_runs):
            raise ValueError("Included search runs must be unique")
        if as_of_completion_sequence < 0 or not 1 <= maximum_listings <= 10_001:
            raise ValueError("Projection membership bounds are invalid")
        position = after_position or (2**63 - 1, "")
        requested = [
            {
                "record_identifier": record_identifier,
                "internal_run_identifier": internal_identifier,
            }
            for record_identifier, internal_identifier in included_search_runs
        ]
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                f"""
                WITH requested_runs AS MATERIALIZED (
                    SELECT CAST(key AS INTEGER) AS run_depth,
                           json_extract(value, '$.record_identifier') AS record_identifier,
                           json_extract(value, '$.internal_run_identifier') AS internal_identifier
                    FROM json_each(?)
                ),
                eligible_runs AS MATERIALIZED (
                    SELECT requested.run_depth, run.id AS record_identifier,
                           requested.internal_identifier,
                           completed.sequence AS completion_sequence,
                           operation.ended_at_utc AS completed_at_utc
                    FROM requested_runs AS requested
                    JOIN objects AS run ON run.id = requested.record_identifier
                    JOIN records AS run_record ON run_record.object_id = run.id
                    JOIN operations AS operation
                      ON operation.id = run.created_by_operation_id
                    JOIN work_operations AS run_work
                      ON run_work.operation_id = run.created_by_operation_id
                    JOIN work_events AS completed
                      ON completed.work_item_id = run_work.work_item_id
                     AND {_facebook_search_publication_event("operation", "completed", run_identifier="run.id")}
                     AND completed.sequence <= ?
                    WHERE run.kind_parts_json = ?
                      AND json_extract(
                          run_record.value_json, '$.search_run_identifier'
                      ) = requested.internal_identifier
                ),
                ranked AS MATERIALIZED (
                    SELECT
                        json_extract(occurrence.value_json, '$.listing_identifier')
                            AS listing_identifier,
                        occurrence.object_id AS occurrence_record_identifier,
                        run.record_identifier AS search_run_record_identifier,
                        run.internal_identifier,
                        run.completion_sequence,
                        run.completed_at_utc,
                        run.run_depth,
                        CAST(json_extract(occurrence.value_json, '$.page_ordinal') AS INTEGER)
                            AS page_ordinal,
                        CAST(json_extract(occurrence.value_json, '$.edge_index') AS INTEGER)
                            AS edge_index,
                        row_number() OVER (
                            PARTITION BY json_extract(
                                occurrence.value_json, '$.listing_identifier'
                            )
                            ORDER BY run.completion_sequence DESC,
                                     CAST(json_extract(
                                         occurrence.value_json, '$.page_ordinal'
                                     ) AS INTEGER),
                                     CAST(json_extract(
                                         occurrence.value_json, '$.edge_index'
                                     ) AS INTEGER),
                                     occurrence.object_id
                        ) AS listing_rank
                    FROM eligible_runs AS run
                    CROSS JOIN records AS occurrence
                        INDEXED BY records_search_occurrence_run_position
                      ON json_extract(
                          occurrence.value_json, '$.search_run_identifier'
                      ) = run.internal_identifier
                    JOIN objects AS occurrence_object
                      ON occurrence_object.id = occurrence.object_id
                     AND occurrence_object.kind_parts_json = ?
                    WHERE json_extract(
                              occurrence.value_json, '$.listing_identifier'
                          ) IS NOT NULL
                      AND json_extract(
                              occurrence.value_json, '$.search_run_identifier'
                          ) IS NOT NULL
                )
                SELECT listing_identifier, occurrence_record_identifier,
                       search_run_record_identifier, internal_identifier,
                       completion_sequence, completed_at_utc, run_depth,
                       page_ordinal, edge_index
                FROM ranked
                WHERE listing_rank = 1
                  AND (
                      completion_sequence < ?
                      OR (completion_sequence = ? AND listing_identifier > ?)
                  )
                ORDER BY completion_sequence DESC, listing_identifier
                LIMIT ?
                """,
                (
                    _json(requested),
                    as_of_completion_sequence,
                    _json(["carl", "facebook", "search_run"]),
                    _json(["carl", "facebook", "search_listing_occurrence"]),
                    position[0],
                    position[0],
                    position[1],
                    maximum_listings,
                ),
            )
            rows = await cursor.fetchall()
        return tuple(
            FacebookProjectionMembershipCandidate(
                candidate=SearchMembershipOccurrenceCandidate(
                    occurrence_record_identifier=_text(row[1]),
                    listing_identifier=_text(row[0]),
                    search_run_record_identifier=_text(row[2]),
                    search_run_completion_sequence=_integer(row[4]),
                    acquisition_completion_sequence=_integer(row[4]),
                    observed_at_utc=None,
                ),
                internal_run_identifier=_text(row[3]),
            )
            for row in rows
        )

    async def facebook_projection_membership_occurrences(
        self,
        listing_identifiers: Sequence[str],
        included_search_runs: Sequence[tuple[str, str]],
        *,
        as_of_completion_sequence: int,
    ) -> tuple[SearchMembershipOccurrenceCandidate, ...]:
        """Return one exact occurrence per requested listing and included run."""

        if not listing_identifiers or not included_search_runs:
            return ()
        if (
            len(listing_identifiers) > 100
            or any(not identifier.isdecimal() for identifier in listing_identifiers)
            or len(included_search_runs) > 100
            or as_of_completion_sequence < 0
        ):
            raise ValueError("Projection membership-occurrence bounds are invalid")
        requested_runs = [
            {
                "record_identifier": record_identifier,
                "internal_run_identifier": internal_identifier,
            }
            for record_identifier, internal_identifier in included_search_runs
        ]
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                f"""
                WITH requested_runs AS MATERIALIZED (
                    SELECT CAST(key AS INTEGER) AS run_depth,
                           json_extract(value, '$.record_identifier') AS record_identifier,
                           json_extract(value, '$.internal_run_identifier') AS internal_identifier
                    FROM json_each(?)
                ),
                requested_listings AS MATERIALIZED (
                    SELECT CAST(value AS TEXT) AS listing_identifier FROM json_each(?)
                ),
                eligible_runs AS MATERIALIZED (
                    SELECT requested.run_depth, run.id AS record_identifier,
                           requested.internal_identifier,
                           completed.sequence AS completion_sequence,
                           operation.ended_at_utc AS completed_at_utc
                    FROM requested_runs AS requested
                    JOIN objects AS run ON run.id = requested.record_identifier
                    JOIN records AS run_record ON run_record.object_id = run.id
                    JOIN operations AS operation
                      ON operation.id = run.created_by_operation_id
                    JOIN work_operations AS run_work
                      ON run_work.operation_id = run.created_by_operation_id
                    JOIN work_events AS completed
                      ON completed.work_item_id = run_work.work_item_id
                     AND {_facebook_search_publication_event("operation", "completed", run_identifier="run.id")}
                     AND completed.sequence <= ?
                    WHERE run.kind_parts_json = ?
                      AND json_extract(
                          run_record.value_json, '$.search_run_identifier'
                      ) = requested.internal_identifier
                ),
                ranked AS MATERIALIZED (
                    SELECT
                        json_extract(occurrence.value_json, '$.listing_identifier')
                            AS listing_identifier,
                        occurrence.object_id AS occurrence_record_identifier,
                        run.record_identifier AS search_run_record_identifier,
                        run.completion_sequence,
                        run.completed_at_utc,
                        row_number() OVER (
                            PARTITION BY run.record_identifier,
                                         json_extract(
                                             occurrence.value_json, '$.listing_identifier'
                                         )
                            ORDER BY CAST(json_extract(
                                         occurrence.value_json, '$.page_ordinal'
                                     ) AS INTEGER),
                                     CAST(json_extract(
                                         occurrence.value_json, '$.edge_index'
                                     ) AS INTEGER),
                                     occurrence.object_id
                        ) AS occurrence_rank
                    FROM eligible_runs AS run
                    CROSS JOIN records AS occurrence
                        INDEXED BY records_search_occurrence_run_position
                      ON json_extract(
                          occurrence.value_json, '$.search_run_identifier'
                      ) = run.internal_identifier
                    JOIN requested_listings AS listing
                      ON listing.listing_identifier = json_extract(
                          occurrence.value_json, '$.listing_identifier'
                      )
                    JOIN objects AS occurrence_object
                      ON occurrence_object.id = occurrence.object_id
                     AND occurrence_object.kind_parts_json = ?
                    WHERE json_extract(
                              occurrence.value_json, '$.listing_identifier'
                          ) IS NOT NULL
                      AND json_extract(
                              occurrence.value_json, '$.search_run_identifier'
                          ) IS NOT NULL
                )
                SELECT occurrence_record_identifier, listing_identifier,
                       search_run_record_identifier, completion_sequence,
                       completed_at_utc
                FROM ranked
                WHERE occurrence_rank = 1
                ORDER BY completion_sequence, search_run_record_identifier,
                         listing_identifier
                """,
                (
                    _json(requested_runs),
                    _json(list(dict.fromkeys(listing_identifiers))),
                    as_of_completion_sequence,
                    _json(["carl", "facebook", "search_run"]),
                    _json(["carl", "facebook", "search_listing_occurrence"]),
                ),
            )
            rows = await cursor.fetchall()
        return tuple(
            SearchMembershipOccurrenceCandidate(
                occurrence_record_identifier=_text(row[0]),
                listing_identifier=_text(row[1]),
                search_run_record_identifier=_text(row[2]),
                search_run_completion_sequence=_integer(row[3]),
                acquisition_completion_sequence=_integer(row[3]),
                observed_at_utc=None,
            )
            for row in rows
        )

    async def facebook_projection_item_observations(
        self,
        listing_identifiers: Sequence[str],
        *,
        as_of_completion_sequence: int,
        maximum_per_listing: int,
        status_only: bool = False,
    ) -> tuple[ListingObservationCandidate, ...]:
        """Return item observations only for a bounded exact listing-ID set."""

        if not listing_identifiers:
            return ()
        if len(listing_identifiers) > 10_000 or any(
            not identifier.isdecimal() for identifier in listing_identifiers
        ):
            raise ValueError("Projection listing identifiers are invalid")
        if as_of_completion_sequence < 0 or not 1 <= maximum_per_listing <= 101:
            raise ValueError("Projection observation bounds are invalid")
        observation_json = (
            """json_object(
                'acquisition_record_id', json_extract(observation_record.value_json, '$.acquisition_record_id'),
                'response_classification', json_extract(observation_record.value_json, '$.response_classification'),
                'fields', json_object(
                    'availability_sold', json_extract(observation_record.value_json, '$.fields.availability_sold'),
                    'availability_pending', json_extract(observation_record.value_json, '$.fields.availability_pending'),
                    'availability_live', json_extract(observation_record.value_json, '$.fields.availability_live')
                )
            )"""
            if status_only
            else "observation_record.value_json"
        )
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                f"""
                WITH requested(listing_identifier) AS MATERIALIZED (
                    SELECT value FROM json_each(?)
                ),
                matching_observations AS MATERIALIZED (
                    SELECT
                        requested.listing_identifier,
                        observation_record.object_id AS observation_identifier,
                        {observation_json} AS value_json
                    FROM requested
                    CROSS JOIN records AS observation_record
                        INDEXED BY records_listing_observation_listing
                      ON json_extract(observation_record.value_json, '$.listing_id') =
                         requested.listing_identifier
                    WHERE json_extract(
                              observation_record.value_json, '$.acquisition_record_id'
                          ) IS NOT NULL
                      AND json_extract(
                              observation_record.value_json,
                              '$.response_classification.kind'
                          ) IS NOT NULL
                ),
                ranked AS MATERIALIZED (
                    SELECT
                        matching.listing_identifier,
                        observation.id AS observation_identifier,
                        acquisition.id AS acquisition_identifier,
                        observation.created_by_operation_id AS operation_identifier,
                        acquisition_completed.sequence AS acquisition_sequence,
                        extraction_completed.sequence AS extraction_sequence,
                        acquisition_operation.ended_at_utc AS observed_at_utc,
                        extraction_operation.ended_at_utc AS completed_at_utc,
                        json_extract(
                            matching.value_json,
                            '$.response_classification.kind'
                        ) AS response_classification,
                        matching.value_json,
                        row_number() OVER (
                            PARTITION BY matching.listing_identifier
                            ORDER BY acquisition_completed.sequence DESC,
                                     extraction_completed.sequence DESC,
                                     observation.id DESC
                        ) AS history_rank
                    FROM matching_observations AS matching
                    CROSS JOIN objects AS observation
                        INDEXED BY sqlite_autoindex_objects_1
                      ON observation.id = matching.observation_identifier
                     AND observation.kind_parts_json = ?
                    CROSS JOIN operations AS extraction_operation
                        INDEXED BY sqlite_autoindex_operations_1
                      ON extraction_operation.id = observation.created_by_operation_id
                     AND extraction_operation.state = 'completed'
                    CROSS JOIN work_operations AS extraction_work
                        INDEXED BY sqlite_autoindex_work_operations_1
                      ON extraction_work.operation_id = observation.created_by_operation_id
                    CROSS JOIN work_events AS extraction_completed
                        INDEXED BY work_events_work_item
                      ON extraction_completed.work_item_id = extraction_work.work_item_id
                     AND extraction_completed.event_kind = 'completed'
                     AND extraction_completed.sequence <= ?
                    CROSS JOIN objects AS acquisition
                        INDEXED BY sqlite_autoindex_objects_1
                      ON acquisition.id = json_extract(
                          matching.value_json, '$.acquisition_record_id'
                      )
                     AND acquisition.kind_parts_json = ?
                    CROSS JOIN operations AS acquisition_operation
                        INDEXED BY sqlite_autoindex_operations_1
                      ON acquisition_operation.id = acquisition.created_by_operation_id
                     AND acquisition_operation.state = 'completed'
                    CROSS JOIN work_operations AS acquisition_work
                        INDEXED BY sqlite_autoindex_work_operations_1
                      ON acquisition_work.operation_id = acquisition.created_by_operation_id
                    CROSS JOIN work_events AS acquisition_completed
                        INDEXED BY work_events_work_item
                      ON acquisition_completed.work_item_id = acquisition_work.work_item_id
                     AND acquisition_completed.event_kind = 'completed'
                     AND acquisition_completed.sequence <= ?
                )
                SELECT listing_identifier, observation_identifier,
                       acquisition_identifier, operation_identifier,
                       acquisition_sequence, extraction_sequence,
                       observed_at_utc, completed_at_utc,
                       response_classification, value_json
                FROM ranked
                WHERE history_rank <= ?
                ORDER BY listing_identifier, acquisition_sequence,
                         extraction_sequence, observation_identifier
                """,
                (
                    _json(list(dict.fromkeys(listing_identifiers))),
                    _json(["carl", "facebook", "listing_observation"]),
                    as_of_completion_sequence,
                    _json(["carl", "http", "acquisition"]),
                    as_of_completion_sequence,
                    maximum_per_listing,
                ),
            )
            rows = await cursor.fetchall()
        return tuple(
            ListingObservationCandidate(
                listing_identifier=_text(row[0]),
                response_classification=_text(row[8]),
                observation=decode_json(_text(row[9])),
                evidence=ProjectionEvidence(
                    evidence_record_identifier=_text(row[1]),
                    observation_record_identifier=_text(row[1]),
                    acquisition_record_identifier=_text(row[2]),
                    producing_operation_identifier=_text(row[3]),
                    acquisition_completion_sequence=_integer(row[4]),
                    observation_completion_sequence=_integer(row[5]),
                    observed_at_utc=None if row[6] is None else _text(row[6]),
                    completed_at_utc=None if row[7] is None else _text(row[7]),
                    source_kind=ProjectionSourceKind.ITEM_PAGE,
                ),
            )
            for row in rows
        )

    async def facebook_projection_search_cards(
        self,
        listing_identifiers: Sequence[str],
        *,
        as_of_completion_sequence: int,
        maximum_per_listing: int,
    ) -> tuple[SearchCardCandidate, ...]:
        """Return bounded retained search-card history for exact listing IDs."""

        if not listing_identifiers:
            return ()
        if len(listing_identifiers) > 10_000 or any(
            not identifier.isdecimal() for identifier in listing_identifiers
        ):
            raise ValueError("Projection listing identifiers are invalid")
        if as_of_completion_sequence < 0 or not 1 <= maximum_per_listing <= 101:
            raise ValueError("Projection search-card bounds are invalid")
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                f"""
                WITH requested(listing_identifier) AS MATERIALIZED (
                    SELECT value FROM json_each(?)
                ),
                matching_occurrences AS MATERIALIZED (
                    SELECT
                        requested.listing_identifier,
                        occurrence.object_id,
                        occurrence.value_json
                    FROM requested
                    CROSS JOIN records AS occurrence
                        INDEXED BY records_search_occurrence_listing
                      ON json_extract(
                          occurrence.value_json, '$.listing_identifier'
                      ) = requested.listing_identifier
                    WHERE json_extract(
                              occurrence.value_json, '$.listing_identifier'
                          ) IS NOT NULL
                      AND json_extract(
                              occurrence.value_json, '$.search_run_identifier'
                          ) IS NOT NULL
                      AND json_type(occurrence.value_json, '$.original') = 'object'
                ),
                ranked AS MATERIALIZED (
                    SELECT
                        matching.listing_identifier,
                        matching.object_id,
                        json_extract(
                            matching.value_json, '$.acquisition_record_identifier'
                        ) AS acquisition_record_identifier,
                        occurrence_object.created_by_operation_id,
                        completed.sequence,
                        operation.ended_at_utc,
                        matching.value_json,
                        CAST(json_extract(
                            matching.value_json, '$.page_ordinal'
                        ) AS INTEGER) AS page_ordinal,
                        CAST(json_extract(
                            matching.value_json, '$.edge_index'
                        ) AS INTEGER) AS edge_index,
                        row_number() OVER (
                            PARTITION BY matching.listing_identifier
                            ORDER BY completed.sequence DESC,
                                     CAST(json_extract(
                                         matching.value_json, '$.page_ordinal'
                                     ) AS INTEGER) DESC,
                                     CAST(json_extract(
                                         matching.value_json, '$.edge_index'
                                     ) AS INTEGER) DESC,
                                     matching.object_id DESC
                        ) AS history_rank
                    FROM matching_occurrences AS matching
                    CROSS JOIN objects AS occurrence_object
                        INDEXED BY sqlite_autoindex_objects_1
                      ON occurrence_object.id = matching.object_id
                     AND occurrence_object.kind_parts_json = ?
                    CROSS JOIN operations AS operation
                        INDEXED BY sqlite_autoindex_operations_1
                      ON operation.id = occurrence_object.created_by_operation_id
                    CROSS JOIN work_operations AS search_work
                        INDEXED BY sqlite_autoindex_work_operations_1
                      ON search_work.operation_id = occurrence_object.created_by_operation_id
                    CROSS JOIN work_events AS completed
                        INDEXED BY work_events_work_item
                      ON completed.work_item_id = search_work.work_item_id
                     AND {_facebook_search_publication_event("operation", "completed", internal_identifier="json_extract(matching.value_json, '$.search_run_identifier')")}
                     AND completed.sequence <= ?
                )
                SELECT listing_identifier, object_id, acquisition_record_identifier,
                       created_by_operation_id, sequence, ended_at_utc, value_json,
                       page_ordinal, edge_index
                FROM ranked
                WHERE history_rank <= ?
                ORDER BY listing_identifier, sequence, page_ordinal, edge_index, object_id
                """,
                (
                    _json(list(dict.fromkeys(listing_identifiers))),
                    _json(["carl", "facebook", "search_listing_occurrence"]),
                    as_of_completion_sequence,
                    maximum_per_listing,
                ),
            )
            rows = await cursor.fetchall()
        candidates: list[SearchCardCandidate] = []
        for row in rows:
            occurrence = decode_json(_text(row[6]))
            if not isinstance(occurrence, dict) or not isinstance(
                (original := occurrence.get("original")), dict
            ):
                continue
            candidates.append(
                SearchCardCandidate(
                    listing_identifier=_text(row[0]),
                    original=cast(dict[str, JsonValue], original),
                    evidence=ProjectionEvidence(
                        evidence_record_identifier=_text(row[1]),
                        observation_record_identifier=_text(row[1]),
                        acquisition_record_identifier=(None if row[2] is None else _text(row[2])),
                        producing_operation_identifier=_text(row[3]),
                        acquisition_completion_sequence=_integer(row[4]),
                        observation_completion_sequence=_integer(row[4]),
                        observed_at_utc=None,
                        completed_at_utc=None if row[5] is None else _text(row[5]),
                        source_kind=ProjectionSourceKind.SEARCH_CARD,
                        warnings=("search_card_ordered_by_search_completion",),
                    ),
                )
            )
        return tuple(candidates)

    async def facebook_projection_search_occurrences(
        self,
        listing_identifiers: Sequence[str],
        *,
        as_of_completion_sequence: int,
    ) -> tuple[StatusObservationCandidate, ...]:
        """Return the newest published search-card status evidence per listing."""

        if not listing_identifiers:
            return ()
        if len(listing_identifiers) > 10_000 or any(
            not identifier.isdecimal() for identifier in listing_identifiers
        ):
            raise ValueError("Projection listing identifiers are invalid")
        if as_of_completion_sequence < 0:
            raise ValueError("Completion boundary must not be negative")
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                f"""
                WITH requested(listing_identifier) AS MATERIALIZED (
                    SELECT value FROM json_each(?)
                ),
                matching_occurrences AS MATERIALIZED (
                    SELECT
                        requested.listing_identifier,
                        occurrence.object_id,
                        occurrence.value_json
                    FROM requested
                    CROSS JOIN records AS occurrence
                        INDEXED BY records_search_occurrence_listing
                      ON json_extract(
                          occurrence.value_json, '$.listing_identifier'
                      ) = requested.listing_identifier
                    WHERE json_extract(
                              occurrence.value_json, '$.listing_identifier'
                          ) IS NOT NULL
                      AND json_extract(
                              occurrence.value_json, '$.search_run_identifier'
                          ) IS NOT NULL
                      AND (
                          json_type(occurrence.value_json, '$.original.is_sold')
                              IN ('true', 'false')
                          OR json_type(occurrence.value_json, '$.original.is_pending')
                              IN ('true', 'false')
                          OR json_type(occurrence.value_json, '$.original.is_live')
                              IN ('true', 'false')
                      )
                ),
                ranked AS MATERIALIZED (
                    SELECT
                        matching.listing_identifier,
                        matching.object_id,
                        search_run.id AS search_run_record_identifier,
                        json_extract(
                            matching.value_json, '$.search_run_identifier'
                        ) AS internal_run_identifier,
                        json_extract(
                            matching.value_json, '$.acquisition_record_identifier'
                        ) AS acquisition_record_identifier,
                        occurrence_object.created_by_operation_id,
                        completed.sequence,
                        operation.ended_at_utc,
                        matching.value_json,
                        row_number() OVER (
                            PARTITION BY matching.listing_identifier
                            ORDER BY completed.sequence DESC,
                                     CAST(json_extract(
                                         matching.value_json, '$.page_ordinal'
                                     ) AS INTEGER) DESC,
                                     CAST(json_extract(
                                         matching.value_json, '$.edge_index'
                                     ) AS INTEGER) DESC,
                                     matching.object_id DESC
                        ) AS evidence_rank
                    FROM matching_occurrences AS matching
                    CROSS JOIN objects AS occurrence_object
                        INDEXED BY sqlite_autoindex_objects_1
                      ON occurrence_object.id = matching.object_id
                     AND occurrence_object.kind_parts_json = ?
                    CROSS JOIN operations AS operation
                        INDEXED BY sqlite_autoindex_operations_1
                      ON operation.id = occurrence_object.created_by_operation_id
                    CROSS JOIN work_operations AS search_work
                        INDEXED BY sqlite_autoindex_work_operations_1
                      ON search_work.operation_id = occurrence_object.created_by_operation_id
                    CROSS JOIN work_events AS completed
                        INDEXED BY work_events_work_item
                      ON completed.work_item_id = search_work.work_item_id
                     AND {_facebook_search_publication_event("operation", "completed", internal_identifier="json_extract(matching.value_json, '$.search_run_identifier')")}
                     AND completed.sequence <= ?
                    CROSS JOIN objects AS search_run
                        INDEXED BY objects_operation_kind
                      ON search_run.created_by_operation_id =
                         occurrence_object.created_by_operation_id
                     AND search_run.kind_parts_json = ?
                     AND (operation.state = 'completed' OR search_run.id =
                         json_extract(operation.result_json, '$.search_run_record_identifier'))
                )
                SELECT listing_identifier, object_id, search_run_record_identifier,
                       internal_run_identifier, acquisition_record_identifier,
                       created_by_operation_id, sequence, ended_at_utc, value_json
                FROM ranked
                WHERE evidence_rank = 1
                ORDER BY listing_identifier
                """,
                (
                    _json(list(dict.fromkeys(listing_identifiers))),
                    _json(["carl", "facebook", "search_listing_occurrence"]),
                    as_of_completion_sequence,
                    _json(["carl", "facebook", "search_run"]),
                ),
            )
            rows = await cursor.fetchall()
        candidates: list[StatusObservationCandidate] = []
        for row in rows:
            occurrence = decode_json(_text(row[8]))
            original = occurrence.get("original") if isinstance(occurrence, dict) else None
            candidate = status_candidate_from_search_occurrence(
                listing_identifier=_text(row[0]),
                original=original,
                evidence=ProjectionEvidence(
                    evidence_record_identifier=_text(row[1]),
                    observation_record_identifier=_text(row[1]),
                    acquisition_record_identifier=_text(row[4]),
                    producing_operation_identifier=_text(row[5]),
                    acquisition_completion_sequence=_integer(row[6]),
                    observation_completion_sequence=_integer(row[6]),
                    observed_at_utc=None,
                    completed_at_utc=None if row[7] is None else _text(row[7]),
                    source_kind=ProjectionSourceKind.SEARCH_CARD,
                    warnings=("search_card_ordered_by_search_completion",),
                ),
            )
            if candidate is None:
                raise ValueError("Stored explicit search status could not be normalized")
            candidates.append(candidate)
        return tuple(candidates)

    async def facebook_review_candidate_sources(
        self, listing_identifiers: Sequence[str] | None = None
    ) -> tuple[CandidateSource, ...]:
        """Return usable observations, optionally scoped to exact listing IDs."""

        if listing_identifiers is not None:
            if not listing_identifiers:
                return ()
            if len(listing_identifiers) > 10_000 or any(
                not identifier.isdecimal() for identifier in listing_identifiers
            ):
                raise ValueError("Review candidate listing identifiers are invalid")
            async with self._connections.reader() as connection:
                cursor = await connection.execute(
                    """
                    WITH requested(listing_identifier) AS MATERIALIZED (
                        SELECT value FROM json_each(?)
                    ),
                    matching_observations AS MATERIALIZED (
                        SELECT
                            requested.listing_identifier,
                            observation_record.object_id AS observation_identifier,
                            observation_record.value_json
                        FROM requested
                        CROSS JOIN records AS observation_record
                            INDEXED BY records_listing_observation_listing
                          ON json_extract(observation_record.value_json, '$.listing_id') =
                             requested.listing_identifier
                        WHERE json_extract(
                                  observation_record.value_json,
                                  '$.response_classification.kind'
                              ) IN ('full_listing', 'listing_unavailable')
                    )
                    SELECT
                        matching.listing_identifier,
                        json_extract(matching.value_json, '$.acquisition_record_id'),
                        observation.id,
                        json_extract(
                            matching.value_json, '$.response_classification.kind'
                        ),
                        acquisition_completed.sequence,
                        extraction_completed.sequence,
                        matching.value_json
                    FROM matching_observations AS matching
                    CROSS JOIN objects AS observation
                        INDEXED BY sqlite_autoindex_objects_1
                      ON observation.id = matching.observation_identifier
                     AND observation.kind_parts_json = ?
                    CROSS JOIN work_operations AS extraction_work
                        INDEXED BY sqlite_autoindex_work_operations_1
                      ON extraction_work.operation_id = observation.created_by_operation_id
                    CROSS JOIN work_events AS extraction_completed
                        INDEXED BY work_events_work_item
                      ON extraction_completed.work_item_id = extraction_work.work_item_id
                     AND extraction_completed.event_kind = 'completed'
                    CROSS JOIN objects AS acquisition
                        INDEXED BY sqlite_autoindex_objects_1
                      ON acquisition.id = json_extract(
                          matching.value_json, '$.acquisition_record_id'
                      )
                     AND acquisition.kind_parts_json = ?
                    CROSS JOIN work_operations AS acquisition_work
                        INDEXED BY sqlite_autoindex_work_operations_1
                      ON acquisition_work.operation_id = acquisition.created_by_operation_id
                    CROSS JOIN work_events AS acquisition_completed
                        INDEXED BY work_events_work_item
                      ON acquisition_completed.work_item_id = acquisition_work.work_item_id
                     AND acquisition_completed.event_kind = 'completed'
                    ORDER BY acquisition_completed.sequence, observation.id
                    """,
                    (
                        _json(list(dict.fromkeys(listing_identifiers))),
                        _json(["carl", "facebook", "listing_observation"]),
                        _json(["carl", "http", "acquisition"]),
                    ),
                )
                rows = await cursor.fetchall()
        else:
            async with self._connections.reader() as connection:
                cursor = await connection.execute(
                    """
                    SELECT
                        json_extract(observation_record.value_json, '$.listing_id'),
                        json_extract(
                            observation_record.value_json, '$.acquisition_record_id'
                        ),
                        observation.id,
                        json_extract(
                            observation_record.value_json,
                            '$.response_classification.kind'
                        ),
                        acquisition_completed.sequence,
                        extraction_completed.sequence,
                        observation_record.value_json
                    FROM objects AS observation
                    JOIN records AS observation_record
                      ON observation_record.object_id = observation.id
                    JOIN work_operations AS extraction_work
                      ON extraction_work.operation_id = observation.created_by_operation_id
                    JOIN work_events AS extraction_completed
                      ON extraction_completed.work_item_id = extraction_work.work_item_id
                     AND extraction_completed.event_kind = 'completed'
                    JOIN objects AS acquisition
                      ON acquisition.id = json_extract(
                          observation_record.value_json, '$.acquisition_record_id'
                      )
                     AND acquisition.kind_parts_json = ?
                    JOIN work_operations AS acquisition_work
                      ON acquisition_work.operation_id = acquisition.created_by_operation_id
                    JOIN work_events AS acquisition_completed
                      ON acquisition_completed.work_item_id = acquisition_work.work_item_id
                     AND acquisition_completed.event_kind = 'completed'
                    WHERE observation.kind_parts_json = ?
                      AND json_extract(
                          observation_record.value_json,
                          '$.response_classification.kind'
                      ) IN ('full_listing', 'listing_unavailable')
                    ORDER BY acquisition_completed.sequence, observation.id
                    """,
                    (
                        _json(["carl", "http", "acquisition"]),
                        _json(["carl", "facebook", "listing_observation"]),
                    ),
                )
                rows = await cursor.fetchall()
        observation_identifiers = tuple(_text(row[2]) for row in rows)
        analyses = await self.facebook_analysis_descriptors(observation_identifiers)
        analyses_by_observation: dict[str, list[AnalysisDescriptor]] = {}
        for analysis in analyses:
            analyses_by_observation.setdefault(
                analysis.listing_observation_record_identifier, []
            ).append(analysis)
        sources: list[CandidateSource] = []
        for row in rows:
            observation = decode_json(_text(row[6]))
            sources.append(
                CandidateSource(
                    listing_identifier=_text(row[0]),
                    acquisition_record_identifier=_text(row[1]),
                    observation_record_identifier=_text(row[2]),
                    availability=candidate_availability(observation, _text(row[3])),
                    acquisition_completion_sequence=_integer(row[4]),
                    observation_completion_sequence=_integer(row[5]),
                    observation=observation,
                    analyses=tuple(analyses_by_observation.get(_text(row[2]), ())),
                )
            )
        return tuple(sources)

    async def facebook_search_run_records(
        self, *, query: str | None = None, limit: int = 50
    ) -> tuple[FacebookSearchRunRecord, ...]:
        """Return recent published search-run records for refresh selection."""

        if limit < 1:
            raise ValueError("Search-run limit must be positive")
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                f"""
                SELECT search_run.id, completed.sequence,
                       producing_operation.started_at_utc,
                       producing_operation.ended_at_utc,
                       origin_request.requester_kind_parts_json,
                       json_extract(
                           origin_refresh.payload_json,
                           '$.base_search_run_record_identifier'
                       ),
                       record.value_json
                FROM objects AS search_run
                JOIN records AS record ON record.object_id = search_run.id
                JOIN operations AS producing_operation
                  ON producing_operation.id = search_run.created_by_operation_id
                JOIN work_operations AS work_operation
                  ON work_operation.operation_id = search_run.created_by_operation_id
                JOIN work_events AS completed
                  ON completed.work_item_id = work_operation.work_item_id
                 AND {_facebook_search_publication_event("producing_operation", "completed", run_identifier="search_run.id")}
                LEFT JOIN work_events AS enqueued
                  ON enqueued.work_item_id = work_operation.work_item_id
                 AND enqueued.event_kind = 'enqueued'
                LEFT JOIN work_requests AS origin_request
                  ON origin_request.id = json_extract(enqueued.data_json, '$.request_identifier')
                 AND origin_request.work_item_id = work_operation.work_item_id
                LEFT JOIN work_items AS origin_refresh
                  ON origin_refresh.id = origin_request.requester_identifier
                 AND origin_refresh.kind_parts_json = ?
                WHERE search_run.kind_parts_json = ?
                  AND (? IS NULL OR json_extract(record.value_json, '$.request.query') = ?)
                ORDER BY completed.sequence DESC, search_run.id DESC
                LIMIT ?
                """,
                (
                    _json(["carl", "facebook", "work", "refresh_search"]),
                    _json(["carl", "facebook", "search_run"]),
                    query,
                    query,
                    limit,
                ),
            )
            rows = await cursor.fetchall()
        records: list[FacebookSearchRunRecord] = []
        for row in rows:
            requester_kind = None if row[4] is None else decode_json(_text(row[4]))
            refresh_source = row[5] if isinstance(row[5], str) else None
            if requester_kind == ["carl", "facebook", "search_refresh", "search"]:
                origin = (
                    SearchRunOrigin.REFRESH
                    if refresh_source is not None
                    else SearchRunOrigin.UNKNOWN
                )
            elif requester_kind in (
                ["carl", "cli", "search_request"],
                ["carl", "mcp", "create_search"],
            ):
                origin = SearchRunOrigin.FRESH
                refresh_source = None
            else:
                origin = SearchRunOrigin.UNKNOWN
                refresh_source = None
            records.append(
                FacebookSearchRunRecord(
                    record_identifier=_text(row[0]),
                    completion_sequence=_integer(row[1]),
                    started_at_utc=_text(row[2]),
                    ended_at_utc=None if row[3] is None else _text(row[3]),
                    origin=origin,
                    refresh_source_run_record_identifier=refresh_source,
                    value=decode_json(_text(row[6])),
                )
            )
        return tuple(records)

    async def ebay_search_run_records(
        self, *, query: str | None = None, limit: int = 50
    ) -> tuple[FacebookSearchRunRecord, ...]:
        """Retained eBay run summaries, including partial or failed attempts."""
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT object.id, record.value_json, operation.started_at_utc,
                       operation.ended_at_utc,
                       COALESCE((SELECT MAX(event.sequence)
                           FROM work_items AS work JOIN work_events AS event
                             ON event.work_item_id=work.id AND event.event_kind='completed'
                           WHERE json_extract(work.result_json,'$.search_run_record_identifier')=object.id
                       ),0),
                       (SELECT json_extract(refresh.payload_json,'$.base_search_run_record_identifier')
                        FROM work_items AS refresh
                        WHERE refresh.kind_parts_json=?
                          AND json_extract(refresh.result_json,'$.refreshed_search_run_record_identifier')=object.id
                        ORDER BY refresh.created_at_utc_ns DESC LIMIT 1)
                FROM objects AS object JOIN records AS record ON record.object_id=object.id
                JOIN operations AS operation ON operation.id=object.created_by_operation_id
                WHERE object.kind_parts_json=? AND operation.state='completed'
                  AND (? IS NULL OR json_extract(record.value_json,'$.request.query')=?)
                ORDER BY object.rowid DESC LIMIT ?
                """,
                (
                    _json(["carl", "ebay", "work", "refresh_search"]),
                    _json(["carl", "ebay", "search_run"]),
                    query,
                    query,
                    limit,
                ),
            )
            rows = await cursor.fetchall()
        return tuple(
            FacebookSearchRunRecord(
                record_identifier=_text(row[0]),
                value=decode_json(_text(row[1])),
                started_at_utc=_text(row[2]),
                ended_at_utc=None if row[3] is None else _text(row[3]),
                completion_sequence=_integer(row[4]),
                origin=SearchRunOrigin.FRESH if row[5] is None else SearchRunOrigin.REFRESH,
                refresh_source_run_record_identifier=None if row[5] is None else _text(row[5]),
            )
            for row in rows
        )

    async def facebook_search_run_refresh_work_identifier(
        self, search_run_record_identifier: str
    ) -> str | None:
        """Return a completed refresh coordinator whose result is this exact search run."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT refresh.id
                FROM work_items AS refresh
                JOIN work_events AS completed
                  ON completed.work_item_id = refresh.id
                 AND completed.event_kind = 'completed'
                WHERE refresh.kind_parts_json IN (?, ?)
                  AND json_extract(
                          refresh.result_json,
                          '$.refreshed_search_run_record_identifier'
                      ) = ?
                ORDER BY completed.sequence DESC, refresh.id DESC
                LIMIT 1
                """,
                (
                    _json(["carl", "facebook", "work", "refresh_search"]),
                    _json(["carl", "ebay", "work", "refresh_search"]),
                    search_run_record_identifier,
                ),
            )
            row = await cursor.fetchone()
        return None if row is None else _text(row[0])

    async def product_guides(self) -> tuple[ProductGuideDetails, ...]:
        """Return immutable product-guide versions with their exact retained text."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT guide.id, guide_record.value_json,
                       guide.created_by_operation_id, guide_text.object_id
                FROM objects AS guide
                JOIN records AS guide_record ON guide_record.object_id = guide.id
                JOIN operation_outputs AS guide_text
                  ON guide_text.operation_id = guide.created_by_operation_id
                 AND guide_text.name_parts_json = ?
                WHERE guide.kind_parts_json = ?
                ORDER BY guide_record.value_json, guide.rowid
                """,
                (
                    _json(["guide_text"]),
                    _json(["carl", "analysis", "product_guide"]),
                ),
            )
            rows = await cursor.fetchall()
            previous_by_operation: dict[str, str] = {}
            for row in rows:
                cursor = await connection.execute(
                    """
                    SELECT object_id FROM operation_inputs
                    WHERE operation_id = ? AND name_parts_json = ?
                    """,
                    (_text(row[2]), _json(["previous_product_guide"])),
                )
                previous = await cursor.fetchone()
                if previous is not None:
                    previous_by_operation[_text(row[2])] = _text(previous[0])
        guides: list[ProductGuideDetails] = []
        for row in rows:
            record = ProductGuideRecord.model_validate_json(_text(row[1]))
            metadata, content = await self.get_artifact(_text(row[3]))
            if metadata.get("media_type") != "text/plain; charset=utf-8":
                raise ValueError("Stored product guide text has an invalid media type")
            guides.append(
                ProductGuideDetails(
                    record_identifier=_text(row[0]),
                    identity=record.identity,
                    display_name=record.display_name,
                    version=record.version,
                    previous_record_identifier=previous_by_operation.get(_text(row[2])),
                    text=content.decode("utf-8"),
                    text_artifact_identifier=_text(row[3]),
                )
            )
        return tuple(sorted(guides, key=lambda guide: (guide.identity, guide.version)))

    async def product_guide_summaries(self) -> tuple[ProductGuideSummary, ...]:
        """Return guide identities and versions without reading their text artifacts."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT guide.id, guide_record.value_json,
                       (
                           SELECT previous.object_id
                           FROM operation_inputs AS previous
                           WHERE previous.operation_id = guide.created_by_operation_id
                             AND previous.name_parts_json = ?
                       )
                FROM objects AS guide
                JOIN records AS guide_record ON guide_record.object_id = guide.id
                WHERE guide.kind_parts_json = ?
                ORDER BY guide_record.value_json, guide.rowid
                """,
                (
                    _json(["previous_product_guide"]),
                    _json(["carl", "analysis", "product_guide"]),
                ),
            )
            rows = await cursor.fetchall()
        guides = []
        for row in rows:
            record = ProductGuideRecord.model_validate_json(_text(row[1]))
            guides.append(
                ProductGuideSummary(
                    record_identifier=_text(row[0]),
                    identity=record.identity,
                    display_name=record.display_name,
                    version=record.version,
                    previous_record_identifier=(None if row[2] is None else _text(row[2])),
                )
            )
        return tuple(sorted(guides, key=lambda guide: (guide.identity, guide.version)))

    async def product_guide(self, identifier: str) -> ProductGuideDetails:
        for guide in await self.product_guides():
            if guide.record_identifier == identifier:
                return guide
        raise KeyError(identifier)

    async def require_product_guide_record(self, identifier: str) -> None:
        """Validate one exact product-guide record without loading every guide."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT 1 FROM objects WHERE id = ? AND kind_parts_json = ?",
                (identifier, _json(["carl", "analysis", "product_guide"])),
            )
            row = await cursor.fetchone()
        if row is None:
            raise KeyError(identifier)

    async def author_product_guide(
        self,
        *,
        identity: tuple[str, ...],
        display_name: str,
        text: str,
        expected_base_record_identifier: str | None,
        component: Component,
        operation_identifier: str,
        record_identifier: str,
        text_identifier: str,
        provenance: CodeProvenance,
        invocation: dict[str, JsonValue],
        started_at_utc: str,
        ended_at_utc: str,
        duration_ns: int,
    ) -> ProductGuideDetails | ProductGuideConflict:
        """Atomically create or revise one immutable product-guide identity."""

        if not identity or any(not part for part in identity):
            raise ValueError("Product guide identity parts must be nonempty")
        if not display_name or display_name != display_name.strip():
            raise ValueError("Product guide display name must be nonempty and trimmed")
        if not text or text != text.strip():
            raise ValueError("Product guide text must be nonempty and trimmed")
        async with self._connections.writer() as connection:
            cursor = await connection.execute(
                """
                SELECT guide.id, guide_record.value_json
                FROM objects AS guide
                JOIN records AS guide_record ON guide_record.object_id = guide.id
                WHERE guide.kind_parts_json = ?
                  AND json_extract(guide_record.value_json, '$.identity') = ?
                ORDER BY json_extract(guide_record.value_json, '$.version'), guide.rowid
                """,
                (
                    _json(["carl", "analysis", "product_guide"]),
                    _json(list(identity)),
                ),
            )
            rows = await cursor.fetchall()
            current_identifier: str | None = None
            current_version = 0
            for row in rows:
                record = ProductGuideRecord.model_validate_json(_text(row[1]))
                if record.version > current_version:
                    current_identifier = _text(row[0])
                    current_version = record.version
            if current_identifier is not None and (
                expected_base_record_identifier != current_identifier
            ):
                return ProductGuideConflict(
                    identity=identity,
                    expected_base_record_identifier=expected_base_record_identifier,
                    current_record_identifier=current_identifier,
                    current_version=current_version,
                )
            if current_identifier is None and expected_base_record_identifier is not None:
                raise KeyError(expected_base_record_identifier)
            operation_inputs = (
                ()
                if current_identifier is None
                else ((("previous_product_guide",), current_identifier),)
            )
            await self._begin_operation(
                connection,
                operation_id=operation_identifier,
                component=component,
                provenance=provenance,
                invocation=invocation,
                configuration={},
                started_at_utc=started_at_utc,
                inputs=operation_inputs,
            )
            version = current_version + 1
            await self._complete_operation(
                connection,
                operation_id=operation_identifier,
                records=(
                    RecordDraft(
                        identifier=record_identifier,
                        kind=("carl", "analysis", "product_guide"),
                        schema_version=2,
                        value=ProductGuideRecord(
                            identity=identity,
                            version=version,
                            display_name=display_name,
                        ).model_dump(mode="json"),
                    ),
                ),
                artifacts=(
                    BytesDraft(
                        identifier=text_identifier,
                        kind=("carl", "analysis", "product_guide_text"),
                        media_type="text/plain; charset=utf-8",
                        representation={"exact_product_guide": True},
                        content=text.encode("utf-8"),
                    ),
                ),
                inputs=(),
                outputs=(
                    NamedOutput(name=("product_guide",), object_identifier=record_identifier),
                    NamedOutput(name=("guide_text",), object_identifier=text_identifier),
                ),
                result={"state": "completed", "version": version},
                ended_at_utc=ended_at_utc,
                duration_ns=duration_ns,
            )
        return ProductGuideDetails(
            record_identifier=record_identifier,
            identity=identity,
            display_name=display_name,
            version=version,
            previous_record_identifier=current_identifier,
            text=text,
            text_artifact_identifier=text_identifier,
        )

    async def successful_facebook_item_page_results(
        self, listing_identifiers: Sequence[str]
    ) -> tuple[SuccessfulItemPageResult, ...]:
        """Return every usable result for the requested exact listing IDs."""

        if not listing_identifiers:
            return ()
        if any(not identifier.isdecimal() for identifier in listing_identifiers):
            raise ValueError("Listing identifiers must be decimal strings")
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                WITH requested(listing_id) AS (
                    SELECT value FROM json_each(?)
                ),
                observations AS MATERIALIZED (
                    SELECT records.object_id, records.value_json,
                           objects.created_by_operation_id
                    FROM objects
                    JOIN records ON records.object_id = objects.id
                    WHERE objects.kind_parts_json = ?
                      AND json_extract(records.value_json, '$.listing_id')
                          IN (SELECT listing_id FROM requested)
                )
                SELECT
                    json_extract(observation.value_json, '$.listing_id'),
                    json_extract(observation.value_json, '$.acquisition_record_id'),
                    observation.object_id,
                    json_extract(
                        observation.value_json,
                        '$.response_classification.kind'
                    ),
                    acquisition_completed.sequence,
                    extraction_completed.sequence
                FROM observations AS observation
                JOIN work_operations AS extraction_work
                  ON extraction_work.operation_id = observation.created_by_operation_id
                JOIN work_events AS extraction_completed
                  ON extraction_completed.work_item_id = extraction_work.work_item_id
                 AND extraction_completed.event_kind = 'completed'
                JOIN objects AS acquisition_object
                  ON acquisition_object.id = json_extract(
                      observation.value_json,
                      '$.acquisition_record_id'
                  )
                 AND acquisition_object.kind_parts_json = ?
                JOIN work_operations AS acquisition_work
                  ON acquisition_work.operation_id = acquisition_object.created_by_operation_id
                JOIN work_events AS acquisition_completed
                  ON acquisition_completed.work_item_id = acquisition_work.work_item_id
                 AND acquisition_completed.event_kind = 'completed'
                WHERE json_extract(
                    observation.value_json,
                    '$.response_classification.kind'
                ) IN ('full_listing', 'listing_unavailable')
                ORDER BY
                    acquisition_completed.sequence,
                    extraction_completed.sequence,
                    observation.object_id
                """,
                (
                    _json(list(dict.fromkeys(listing_identifiers))),
                    _json(["carl", "facebook", "listing_observation"]),
                    _json(["carl", "http", "acquisition"]),
                ),
            )
            rows = await cursor.fetchall()
        return tuple(
            SuccessfulItemPageResult(
                listing_id=_text(row[0]),
                acquisition_record_identifier=_text(row[1]),
                observation_record_identifier=_text(row[2]),
                response_classification=FacebookItemResponseKind(_text(row[3])),
                acquisition_completion_sequence=_integer(row[4]),
                extraction_completion_sequence=_integer(row[5]),
            )
            for row in rows
        )

    async def full_facebook_listing_identifiers(self) -> tuple[str, ...]:
        """Find IDs with a retained full item-page observation."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT DISTINCT json_extract(records.value_json, '$.listing_id')
                FROM objects
                JOIN records ON records.object_id = objects.id
                WHERE objects.kind_parts_json = ?
                  AND json_extract(
                      records.value_json, '$.response_classification.kind'
                  ) = 'full_listing'
                ORDER BY objects.rowid
                """,
                (_json(["carl", "facebook", "listing_observation"]),),
            )
            rows = await cursor.fetchall()
        return tuple(_text(row[0]) for row in rows)

    async def saved_facebook_image_results(
        self,
    ) -> tuple[tuple[str, dict[str, JsonValue]], ...]:
        """Return saved image records for exact rendition matching."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT records.object_id, records.value_json
                FROM objects
                JOIN records ON records.object_id = objects.id
                WHERE objects.kind_parts_json = ?
                  AND json_extract(records.value_json, '$.state') = 'saved'
                ORDER BY objects.rowid
                """,
                (_json(["carl", "facebook", "image_result"]),),
            )
            rows = await cursor.fetchall()
        results: list[tuple[str, dict[str, JsonValue]]] = []
        for row in rows:
            value = decode_json(_text(row[1]))
            if not isinstance(value, dict):
                raise ValueError("Saved image result is malformed")
            results.append((_text(row[0]), cast(dict[str, JsonValue], value)))
        return tuple(results)

    async def saved_ebay_image_candidates(
        self, urls: tuple[str, ...]
    ) -> tuple[tuple[str, dict[str, JsonValue]], ...]:
        """Seek possible saved renditions; callers still check canonical equality."""

        if len(urls) > 1000:
            raise ValueError("Image candidate URL scope exceeds its bound")
        if not urls:
            return ()
        requested: set[tuple[str, str]] = set()
        for url in dict.fromkeys(urls):
            identity = ebay_image_identity(url)
            requested.add(("url", url))
            if identity[2] == "rendition":
                requested.add(("path", identity[3]))
        return await self._saved_image_candidates("ebay", requested)

    async def saved_facebook_image_candidates_for_references(
        self,
        references: tuple[GalleryImageReference, ...],
        *,
        published_before_record_identifier: str | None = None,
    ) -> tuple[tuple[str, dict[str, JsonValue]], ...]:
        """Seek photo/URL/path candidates; the planner checks rendition equivalence."""

        if len(references) > 1000:
            raise ValueError("Image candidate reference scope exceeds its bound")
        if not references:
            return ()
        requested: set[tuple[str, str]] = set()
        for reference in references:
            identity = facebook_image_identity(reference.original_url, reference.photo_id)
            requested.add(("url", reference.original_url))
            if reference.photo_id is not None:
                requested.add(("photo_id", reference.photo_id))
            if identity[2] == "rendition":
                requested.add(("path", identity[4]))
        return await self._saved_image_candidates(
            "facebook",
            requested,
            published_before_record_identifier=published_before_record_identifier,
        )

    async def _saved_image_candidates(
        self,
        marketplace: str,
        requested: set[tuple[str, str]],
        *,
        published_before_record_identifier: str | None = None,
    ) -> tuple[tuple[str, dict[str, JsonValue]], ...]:
        async with self._connections.reader() as connection:
            boundary_rowid: int | None = None
            if published_before_record_identifier is not None:
                cursor = await connection.execute(
                    "SELECT rowid FROM objects WHERE id = ?",
                    (published_before_record_identifier,),
                )
                boundary = await cursor.fetchone()
                if boundary is None:
                    raise KeyError(published_before_record_identifier)
                boundary_rowid = _integer(boundary[0])
            cursor = await connection.execute(
                """
                WITH candidates AS MATERIALIZED (
                    SELECT DISTINCT lookup.result_object_id
                    FROM json_each(?) AS requested
                    CROSS JOIN saved_image_lookup AS lookup
                      ON lookup.marketplace = ?
                     AND lookup.lookup_kind = json_extract(requested.value, '$[0]')
                     AND lookup.lookup_value = json_extract(requested.value, '$[1]')
                )
                SELECT record.object_id, record.value_json
                FROM candidates
                CROSS JOIN records AS record ON record.object_id = candidates.result_object_id
                CROSS JOIN objects AS object ON object.id = candidates.result_object_id
                CROSS JOIN operations AS operation ON operation.id = object.created_by_operation_id
                WHERE object.kind_parts_json = ?
                  AND operation.state = 'completed'
                  AND json_extract(record.value_json, '$.state') = 'saved'
                  AND (? != 'facebook'
                       OR json_extract(record.value_json, '$.original_url') IS NOT NULL)
                  AND (? IS NULL OR object.rowid < ?)
                ORDER BY object.rowid
                """,
                (
                    _json([list(key) for key in sorted(requested)]),
                    marketplace,
                    _json(["carl", marketplace, "image_result"]),
                    marketplace,
                    boundary_rowid,
                    boundary_rowid,
                ),
            )
            rows = await cursor.fetchall()
        return tuple(
            (_text(row[0]), cast(dict[str, JsonValue], decode_json(_text(row[1])))) for row in rows
        )

    async def saved_facebook_image_results_for_references(
        self, reference_identifiers: Sequence[str]
    ) -> tuple[tuple[str, dict[str, JsonValue]], ...]:
        """Return saved image results directly attached to selected references."""

        if not reference_identifiers:
            return ()
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT records.object_id, records.value_json
                FROM records
                JOIN objects ON objects.id = records.object_id
                WHERE json_extract(records.value_json, '$.state') = 'saved'
                  AND json_extract(
                      records.value_json, '$.image_reference_record_identifier'
                  ) IS NOT NULL
                  AND json_extract(
                      records.value_json, '$.image_reference_record_identifier'
                  ) IN (SELECT value FROM json_each(?))
                  AND objects.kind_parts_json = ?
                ORDER BY objects.rowid
                """,
                (
                    _json(list(dict.fromkeys(reference_identifiers))),
                    _json(["carl", "facebook", "image_result"]),
                ),
            )
            rows = await cursor.fetchall()
        return tuple(
            (_text(row[0]), cast(dict[str, JsonValue], decode_json(_text(row[1])))) for row in rows
        )

    async def saved_facebook_image_results_by_identifiers(
        self, result_identifiers: Sequence[str]
    ) -> tuple[tuple[str, dict[str, JsonValue]], ...]:
        """Return selected saved image-result records by their exact identifiers."""

        if not result_identifiers:
            return ()
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT records.object_id, records.value_json
                FROM records
                JOIN objects ON objects.id = records.object_id
                WHERE records.object_id IN (SELECT value FROM json_each(?))
                  AND objects.kind_parts_json = ?
                  AND json_extract(records.value_json, '$.state') = 'saved'
                ORDER BY objects.rowid
                """,
                (
                    _json(list(dict.fromkeys(result_identifiers))),
                    _json(["carl", "facebook", "image_result"]),
                ),
            )
            rows = await cursor.fetchall()
        return tuple(
            (_text(row[0]), cast(dict[str, JsonValue], decode_json(_text(row[1])))) for row in rows
        )

    async def saved_facebook_image_results_for_renditions(
        self, renditions: Sequence[tuple[str | None, str]]
    ) -> tuple[tuple[str, dict[str, JsonValue]], ...]:
        """Return saved image results matching a bounded set of exact renditions."""

        if not renditions:
            return ()
        requested = [
            {"source_photo_id": photo_identifier, "original_url": original_url}
            for photo_identifier, original_url in dict.fromkeys(renditions)
        ]
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT records.object_id, records.value_json
                FROM json_each(?) AS requested
                CROSS JOIN records INDEXED BY records_saved_image_rendition
                  ON json_extract(records.value_json, '$.original_url') =
                     json_extract(requested.value, '$.original_url')
                 AND json_extract(records.value_json, '$.source_photo_id') IS
                     json_extract(requested.value, '$.source_photo_id')
                JOIN objects ON objects.id = records.object_id
                WHERE json_extract(records.value_json, '$.state') = 'saved'
                  AND json_extract(records.value_json, '$.original_url') IS NOT NULL
                  AND objects.kind_parts_json = ?
                ORDER BY objects.rowid
                """,
                (
                    _json(requested),
                    _json(["carl", "facebook", "image_result"]),
                ),
            )
            rows = await cursor.fetchall()
        return tuple(
            (_text(row[0]), cast(dict[str, JsonValue], decode_json(_text(row[1])))) for row in rows
        )

    async def facebook_image_reuse_resolutions(
        self, reference_identifiers: Sequence[str] | None = None
    ) -> tuple[ImageReuseResolution, ...]:
        """Return recorded reference-to-result reuse edges from their operation graph."""

        if reference_identifiers is not None and not reference_identifiers:
            return ()
        async with self._connections.reader() as connection:
            if reference_identifiers is None:
                cursor = await connection.execute(
                    """
                    SELECT reuse.id, reference_input.object_id, result_input.object_id,
                           reuse_record.value_json
                    FROM objects AS reuse
                    JOIN records AS reuse_record ON reuse_record.object_id = reuse.id
                    JOIN operation_outputs AS reuse_output
                      ON reuse_output.object_id = reuse.id
                     AND reuse_output.operation_id = reuse.created_by_operation_id
                    JOIN operation_inputs AS reference_input
                      ON reference_input.operation_id = reuse.created_by_operation_id
                     AND json_extract(reference_input.name_parts_json, '$[0]') =
                         'gallery_image_reference'
                     AND json_extract(reference_input.name_parts_json, '$[1]') =
                         json_extract(reuse_output.name_parts_json, '$[1]')
                    JOIN operation_inputs AS result_input
                      ON result_input.operation_id = reuse.created_by_operation_id
                     AND json_extract(result_input.name_parts_json, '$[0]') =
                         'source_image_result'
                     AND json_extract(result_input.name_parts_json, '$[1]') =
                         json_extract(reuse_output.name_parts_json, '$[1]')
                    WHERE reuse.kind_parts_json = ?
                      AND json_array_length(reuse_output.name_parts_json) = 2
                      AND json_extract(reuse_output.name_parts_json, '$[0]') = 'image_reuse'
                    ORDER BY reuse.rowid
                    """,
                    (_json(["carl", "facebook", "image_reuse"]),),
                )
            else:
                cursor = await connection.execute(
                    """
                    WITH requested_references AS MATERIALIZED (
                        SELECT value AS object_id FROM json_each(?)
                    )
                    SELECT reuse.id, reference_input.object_id, result_input.object_id,
                           reuse_record.value_json
                    FROM requested_references
                    CROSS JOIN operation_inputs AS reference_input
                        INDEXED BY operation_inputs_object_name_operation
                      ON reference_input.object_id = requested_references.object_id
                    CROSS JOIN operation_outputs AS reuse_output
                      ON reuse_output.operation_id = reference_input.operation_id
                     AND reuse_output.name_parts_json = json_array(
                             'image_reuse',
                             json_extract(reference_input.name_parts_json, '$[1]')
                         )
                    CROSS JOIN objects AS reuse
                      ON reuse.id = reuse_output.object_id
                     AND reuse.created_by_operation_id = reuse_output.operation_id
                     AND reuse.kind_parts_json = ?
                    CROSS JOIN records AS reuse_record ON reuse_record.object_id = reuse.id
                    CROSS JOIN operation_inputs AS result_input
                      ON result_input.operation_id = reference_input.operation_id
                     AND result_input.name_parts_json = json_array(
                             'source_image_result',
                             json_extract(reference_input.name_parts_json, '$[1]')
                         )
                    WHERE json_array_length(reference_input.name_parts_json) = 2
                      AND json_extract(reference_input.name_parts_json, '$[0]') =
                          'gallery_image_reference'
                    ORDER BY reuse.rowid
                    """,
                    (
                        _json(list(dict.fromkeys(reference_identifiers))),
                        _json(["carl", "facebook", "image_reuse"]),
                    ),
                )
            rows = await cursor.fetchall()
        resolutions: list[ImageReuseResolution] = []
        for row in rows:
            value = ImageReuseRecord.model_validate_json(_text(row[3]))
            resolutions.append(
                ImageReuseResolution(
                    image_reuse_record_identifier=_text(row[0]),
                    gallery_image_reference_record_identifier=_text(row[1]),
                    source_image_result_record_identifier=_text(row[2]),
                    match_kind=value.match_kind,
                )
            )
        return tuple(resolutions)

    async def resolved_facebook_image_results_by_reference(
        self, reference_identifiers: Sequence[str] | None = None
    ) -> dict[str, tuple[str, dict[str, JsonValue]]]:
        """Resolve direct acquisitions and recorded reuse decisions by gallery edge."""

        results = (
            await self.saved_facebook_image_results()
            if reference_identifiers is None
            else await self.saved_facebook_image_results_for_references(reference_identifiers)
        )
        by_identifier = dict(results)
        reuses = await self.facebook_image_reuse_resolutions(reference_identifiers)
        if reference_identifiers is not None:
            source_results = await self.saved_facebook_image_results_by_identifiers(
                tuple(reuse.source_image_result_record_identifier for reuse in reuses)
            )
            by_identifier.update(source_results)
        resolved: dict[str, tuple[str, dict[str, JsonValue]]] = {}
        for result_identifier, value in results:
            reference_identifier = value.get("image_reference_record_identifier")
            if isinstance(reference_identifier, str):
                resolved[reference_identifier] = (result_identifier, value)
        for reuse in reuses:
            source = by_identifier.get(reuse.source_image_result_record_identifier)
            if source is not None:
                resolved[reuse.gallery_image_reference_record_identifier] = (
                    reuse.source_image_result_record_identifier,
                    source,
                )
        return resolved

    async def facebook_listing_analysis_evidence_sets(
        self, observation_identifiers: Sequence[str] | None = None
    ) -> dict[ListingAnalysisEvidenceSet, str]:
        """Return previously recorded selections of immutable listing evidence."""

        if observation_identifiers is not None and not observation_identifiers:
            return {}
        candidate_cte = ""
        candidate_from = "objects AS evidence"
        parameters: list[apsw.SQLiteValue] = [
            _json(["carl", "facebook", "listing_analysis_evidence"])
        ]
        if observation_identifiers is not None:
            candidate_cte = """
                WITH requested_observations AS MATERIALIZED (
                    SELECT value AS object_id FROM json_each(?)
                ),
                candidate_operations AS MATERIALIZED (
                    SELECT DISTINCT operation_inputs.operation_id
                    FROM requested_observations
                    CROSS JOIN operation_inputs
                        INDEXED BY operation_inputs_object_name_operation
                      ON operation_inputs.object_id = requested_observations.object_id
                    WHERE operation_inputs.name_parts_json = ?
                )
            """
            candidate_from = """
                candidate_operations
                CROSS JOIN objects AS evidence INDEXED BY objects_operation_kind
                  ON evidence.created_by_operation_id = candidate_operations.operation_id
            """
            parameters.extend(
                (
                    _json(list(dict.fromkeys(observation_identifiers))),
                    _json(["listing_observation"]),
                )
            )

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                f"""
                {candidate_cte}
                SELECT evidence.id, evidence_record.value_json,
                       operation_inputs.name_parts_json, operation_inputs.object_id
                FROM {candidate_from}
                JOIN records AS evidence_record
                  ON evidence_record.object_id = evidence.id
                JOIN operation_inputs
                  ON operation_inputs.operation_id = evidence.created_by_operation_id
                WHERE evidence.kind_parts_json = ?
                ORDER BY evidence.rowid, operation_inputs.name_parts_json
                """,
                (
                    *parameters[1:],
                    parameters[0],
                ),
            )
            rows = await cursor.fetchall()
        by_identifier: dict[str, list[NamedInput]] = {}
        values: dict[str, JsonValue] = {}
        for row in rows:
            identifier = _text(row[0])
            values[identifier] = decode_json(_text(row[1]))
            name = decode_json(_text(row[2]))
            if not isinstance(name, list) or not all(isinstance(part, str) for part in name):
                raise ValueError("Stored listing-analysis evidence edge has an invalid name")
            name_parts = cast(list[str], name)
            by_identifier.setdefault(identifier, []).append(
                NamedInput(name=tuple(name_parts), object_identifier=_text(row[3]))
            )
        evidence_sets: dict[ListingAnalysisEvidenceSet, str] = {}
        for identifier, inputs in by_identifier.items():
            observations = [
                input_value.object_identifier
                for input_value in inputs
                if input_value.name == ("listing_observation",)
            ]
            references = {
                input_value.name[1]: input_value.object_identifier
                for input_value in inputs
                if len(input_value.name) == 2 and input_value.name[0] == "gallery_image_reference"
            }
            image_results = {
                input_value.name[1]: input_value.object_identifier
                for input_value in inputs
                if len(input_value.name) == 2 and input_value.name[0] == "image_result"
            }
            raw_value = values.get(identifier)
            if not isinstance(raw_value, dict):
                raise ValueError("Stored listing-analysis evidence value is malformed")
            raw_unavailable = raw_value.get("unavailable_gallery_images", [])
            if not isinstance(raw_unavailable, list):
                raise ValueError("Stored unavailable gallery selection is malformed")
            gallery_absence_reason = raw_value.get("gallery_absence_reason")
            if gallery_absence_reason is not None and not isinstance(gallery_absence_reason, str):
                raise ValueError("Stored gallery absence reason is malformed")
            unavailable = tuple(
                UnavailableAnalysisImageSelection.model_validate(item) for item in raw_unavailable
            )
            unavailable_reference_identifiers = {
                image.gallery_image_reference_record_identifier for image in unavailable
            }
            if (
                len(observations) != 1
                or not image_results.keys() <= references.keys()
                or set(references.values())
                != {
                    *(references[index] for index in image_results),
                    *unavailable_reference_identifiers,
                }
            ):
                raise ValueError("Stored listing-analysis evidence edges are incomplete")
            evidence = ListingAnalysisEvidenceSet(
                listing_observation_record_identifier=observations[0],
                gallery_images=tuple(
                    AnalysisImageSelection(
                        gallery_image_reference_record_identifier=references[index],
                        image_result_record_identifier=image_results[index],
                    )
                    for index in sorted(image_results)
                ),
                unavailable_gallery_images=unavailable,
                gallery_absence_reason=gallery_absence_reason,
            )
            evidence_sets.setdefault(evidence, identifier)
        return evidence_sets

    async def completed_facebook_item_analysis_payloads(
        self,
    ) -> frozenset[AnalyzeItemPayload]:
        """Return typed configurations of completed compatible analysis work."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT payload_json
                FROM work_items
                WHERE kind_parts_json = ?
                  AND payload_schema_version IN (?, ?)
                  AND state = 'completed'
                """,
                (
                    _json(["carl", "facebook", "work", "analyze_item"]),
                    LEGACY_ANALYZE_ITEM_WORK_SCHEMA_VERSION,
                    ANALYZE_ITEM_WORK_SCHEMA_VERSION,
                ),
            )
            rows = await cursor.fetchall()
        return frozenset(
            AnalyzeItemPayload.model_validate_json(_text(row[0])).model_copy(
                update={"claude_version": None}
            )
            for row in rows
        )

    async def facebook_item_analysis_work_identifier(
        self, payload: AnalyzeItemPayload
    ) -> str | None:
        """Find current work or a completed compatible legacy analysis."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT id, payload_json FROM work_items
                WHERE kind_parts_json = ?
                  AND (
                      (
                          payload_schema_version = ?
                          AND state IN ('pending', 'leased', 'completed')
                      )
                      OR (
                          payload_schema_version = ?
                          AND state = 'completed'
                      )
                  )
                  AND json_extract(payload_json, '$.evidence_set_record_identifier') = ?
                  AND json_extract(payload_json, '$.product_guide_record_identifier') = ?
                ORDER BY created_at_utc_ns DESC, id
                """,
                (
                    _json(["carl", "facebook", "work", "analyze_item"]),
                    ANALYZE_ITEM_WORK_SCHEMA_VERSION,
                    LEGACY_ANALYZE_ITEM_WORK_SCHEMA_VERSION,
                    payload.evidence_set_record_identifier,
                    payload.product_guide_record_identifier,
                ),
            )
            rows = await cursor.fetchall()
        expected = payload.model_dump(mode="json", exclude={"claude_version"})
        for row in rows:
            stored = AnalyzeItemPayload.model_validate_json(_text(row[1]))
            if stored.model_dump(mode="json", exclude={"claude_version"}) == expected:
                return _text(row[0])
        return None

    async def ebay_item_analysis_work_identifier(self, payload: AnalyzeItemPayload) -> str | None:
        """Reuse only exact active or successful eBay analysis configurations."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT id, payload_json FROM work_items
                WHERE kind_parts_json = ? AND payload_schema_version = 1
                  AND state IN ('pending', 'leased', 'completed')
                  AND json_extract(payload_json, '$.evidence_set_record_identifier') = ?
                  AND json_extract(payload_json, '$.product_guide_record_identifier') = ?
                ORDER BY created_at_utc_ns DESC, id
                """,
                (
                    _json(["carl", "ebay", "work", "analyze_item"]),
                    payload.evidence_set_record_identifier,
                    payload.product_guide_record_identifier,
                ),
            )
            rows = await cursor.fetchall()
        expected = payload.model_dump(mode="json", exclude={"claude_version"})
        for row in rows:
            stored = EbayAnalyzeItemPayload.model_validate_json(_text(row[1]))
            if stored.model_dump(mode="json", exclude={"claude_version"}) == expected:
                return _text(row[0])
        return None

    async def product_guide_record_identifiers(
        self, *, identity: tuple[str, ...], version: int
    ) -> tuple[str, ...]:
        """Return registrations for one structured guide identity and version."""

        if not identity or any(not part for part in identity) or version < 1:
            raise ValueError("Product guide identity and version must be valid")
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT objects.id
                FROM objects
                JOIN records ON records.object_id = objects.id
                WHERE objects.kind_parts_json = ?
                  AND json_extract(records.value_json, '$.identity') = ?
                  AND json_extract(records.value_json, '$.version') = ?
                ORDER BY objects.rowid
                """,
                (
                    _json(["carl", "analysis", "product_guide"]),
                    _json(list(identity)),
                    version,
                ),
            )
            rows = await cursor.fetchall()
        return tuple(_text(row[0]) for row in rows)

    async def ensure_product_guide_registration(
        self,
        *,
        definition: ProductGuideDefinition,
        component: Component,
        operation_identifier: str,
        record_identifier: str,
        text_identifier: str,
        provenance: CodeProvenance,
        invocation: dict[str, JsonValue],
        started_at_utc: str,
        ended_at_utc: str,
        duration_ns: int,
    ) -> str:
        """Atomically reuse or register one immutable exact product guide."""

        expected_value = ProductGuideRecord(
            identity=definition.identity, version=definition.version
        ).model_dump(mode="json", exclude_none=True)
        expected_bytes = definition.text.encode("utf-8")
        async with self._connections.writer() as connection:
            cursor = await connection.execute(
                """
                SELECT objects.id, objects.created_by_operation_id
                FROM objects
                JOIN records ON records.object_id = objects.id
                WHERE objects.kind_parts_json = ?
                  AND json_extract(records.value_json, '$.identity') = ?
                  AND json_extract(records.value_json, '$.version') = ?
                ORDER BY objects.rowid
                """,
                (
                    _json(["carl", "analysis", "product_guide"]),
                    _json(list(definition.identity)),
                    definition.version,
                ),
            )
            registrations = await cursor.fetchall()
            reusable_identifier: str | None = None
            for row in registrations:
                existing_identifier = _text(row[0])
                existing_operation_identifier = _text(row[1])
                cursor = await connection.execute(
                    """
                    SELECT component_parts_json, output_schema_version, state
                    FROM operations WHERE id = ?
                    """,
                    (existing_operation_identifier,),
                )
                operation_row = await cursor.fetchone()
                if operation_row is None or (
                    decode_json(_text(operation_row[0])),
                    _integer(operation_row[1]),
                    _text(operation_row[2]),
                ) != (
                    list(component.identifier.parts),
                    component.output_schema_version,
                    "completed",
                ):
                    raise ValueError("Stored product guide provenance is malformed")
                cursor = await connection.execute(
                    """
                    SELECT name_parts_json, object_id
                    FROM operation_outputs WHERE operation_id = ?
                    ORDER BY name_parts_json
                    """,
                    (existing_operation_identifier,),
                )
                outputs: dict[tuple[str, ...], str] = {}
                for output_row in await cursor.fetchall():
                    raw_name = decode_json(_text(output_row[0]))
                    if not isinstance(raw_name, list) or not all(
                        isinstance(part, str) and part for part in raw_name
                    ):
                        raise ValueError("Stored product guide output name is malformed")
                    outputs[tuple(cast(list[str], raw_name))] = _text(output_row[1])
                if outputs.get(("product_guide",)) != existing_identifier or set(outputs) != {
                    ("product_guide",),
                    ("guide_text",),
                }:
                    raise ValueError("Stored product guide outputs are malformed")
                cursor = await connection.execute(
                    """
                    SELECT artifacts.media_type, artifacts.representation_json,
                           content.inline_bytes
                    FROM artifacts
                    JOIN content
                      ON content.sha256 = artifacts.sha256 AND content.size = artifacts.size
                    WHERE artifacts.object_id = ?
                    """,
                    (outputs[("guide_text",)],),
                )
                text_row = await cursor.fetchone()
                if text_row is None or (
                    _text(text_row[0]),
                    decode_json(_text(text_row[1])),
                    bytes(text_row[2]),
                ) != (
                    "text/plain; charset=utf-8",
                    {"exact_product_guide": True},
                    expected_bytes,
                ):
                    raise ValueError(
                        "Product guide text changed without a structured identity or version change"
                    )
                reusable_identifier = reusable_identifier or existing_identifier
            if reusable_identifier is not None:
                return reusable_identifier

            await self._begin_operation(
                connection,
                operation_id=operation_identifier,
                component=component,
                provenance=provenance,
                invocation=invocation,
                configuration={},
                started_at_utc=started_at_utc,
                inputs=(),
            )
            await self._complete_operation(
                connection,
                operation_id=operation_identifier,
                records=(
                    RecordDraft(
                        identifier=record_identifier,
                        kind=("carl", "analysis", "product_guide"),
                        schema_version=1,
                        value=expected_value,
                    ),
                ),
                artifacts=(
                    BytesDraft(
                        identifier=text_identifier,
                        kind=("carl", "analysis", "product_guide_text"),
                        media_type="text/plain; charset=utf-8",
                        representation={"exact_product_guide": True},
                        content=expected_bytes,
                    ),
                ),
                inputs=(),
                outputs=(
                    NamedOutput(name=("product_guide",), object_identifier=record_identifier),
                    NamedOutput(name=("guide_text",), object_identifier=text_identifier),
                ),
                result={"state": "completed"},
                ended_at_utc=ended_at_utc,
                duration_ns=duration_ns,
            )
        return record_identifier

    async def outstanding_facebook_item_analysis_work(
        self,
        *,
        listing_identifiers: Sequence[str] | None,
        maximum_items: int,
        recipe_version: int | None = None,
        product_guide_record_identifier: str | None = None,
    ) -> tuple[str, ...]:
        """Resume older queued analysis before planning from newer observations."""

        if maximum_items < 1:
            raise ValueError("Maximum items must be positive")
        parameters: list[str | int] = [
            _json(["carl", "facebook", "work", "analyze_item"]),
            ANALYZE_ITEM_WORK_SCHEMA_VERSION,
        ]
        listing_filter = ""
        if listing_identifiers is not None:
            listing_filter = """
                AND json_extract(observation.value_json, '$.listing_id')
                    IN (SELECT value FROM json_each(?))
            """
            parameters.append(_json(list(listing_identifiers)))
        recipe_filter = ""
        if recipe_version is not None:
            recipe_filter += " AND json_extract(work_items.payload_json, '$.recipe_version') = ?"
            parameters.append(recipe_version)
        guide_filter = ""
        if product_guide_record_identifier is not None:
            guide_filter = """
                AND json_extract(
                    work_items.payload_json, '$.product_guide_record_identifier'
                ) = ?
            """
            parameters.append(product_guide_record_identifier)
        parameters.append(maximum_items)
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                f"""
                SELECT work_items.id
                FROM work_items
                JOIN objects AS evidence
                  ON evidence.id = json_extract(
                      work_items.payload_json, '$.evidence_set_record_identifier'
                  )
                JOIN operation_inputs AS evidence_observation
                  ON evidence_observation.operation_id = evidence.created_by_operation_id
                 AND evidence_observation.name_parts_json = ?
                JOIN records AS observation
                  ON observation.object_id = evidence_observation.object_id
                WHERE work_items.kind_parts_json = ?
                  AND work_items.payload_schema_version = ?
                  AND work_items.state IN ('pending', 'leased')
                  {listing_filter}
                  {recipe_filter}
                  {guide_filter}
                ORDER BY work_items.created_at_utc_ns, work_items.id
                LIMIT ?
                """,
                tuple([_json(["listing_observation"]), *parameters]),
            )
            rows = await cursor.fetchall()
        return tuple(_text(row[0]) for row in rows)

    async def saved_facebook_image_renditions(self) -> frozenset[tuple[str | None, str]]:
        """Return only verified renditions; failed attempts never displace a saved file."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT json_extract(records.value_json, '$.source_photo_id'),
                       json_extract(records.value_json, '$.original_url')
                FROM objects
                JOIN records ON records.object_id = objects.id
                WHERE objects.kind_parts_json = ?
                  AND json_extract(records.value_json, '$.state') = 'saved'
                """,
                (_json(["carl", "facebook", "image_result"]),),
            )
            rows = await cursor.fetchall()
        return frozenset((None if row[0] is None else _text(row[0]), _text(row[1])) for row in rows)

    async def pending_facebook_image_extractions(
        self,
    ) -> dict[tuple[str | None, str], tuple[str, ...]]:
        """Find retained image bodies whose follow-on validation has not finished."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT json_extract(collection.payload_json, '$.reference.photo_id'),
                       json_extract(collection.payload_json, '$.reference.original_url'),
                       extraction.id
                FROM work_items AS collection
                JOIN work_items AS extraction
                  ON extraction.id = json_extract(
                      collection.result_json, '$.extraction_work_identifier'
                  )
                WHERE collection.kind_parts_json = ?
                  AND collection.state = 'completed'
                  AND extraction.kind_parts_json = ?
                  AND extraction.state IN ('pending', 'leased')
                ORDER BY collection.created_at_utc_ns, collection.id
                """,
                (
                    _json(["carl", "facebook", "work", "collect_image"]),
                    _json(["carl", "facebook", "work", "extract_image"]),
                ),
            )
            rows = await cursor.fetchall()
        pending: dict[tuple[str | None, str], list[str]] = {}
        for row in rows:
            key = (None if row[0] is None else _text(row[0]), _text(row[1]))
            pending.setdefault(key, []).append(_text(row[2]))
        return {key: tuple(identifiers) for key, identifiers in pending.items()}

    async def facebook_gallery_reference_identifiers(
        self, observation_identifiers: Sequence[str] | None = None
    ) -> dict[GalleryImageReference, str]:
        """Find retained per-observation image edges without rewriting old evidence."""

        if observation_identifiers is not None and not observation_identifiers:
            return {}
        observation_filter = ""
        parameters: list[apsw.SQLiteValue] = [
            _json(["carl", "facebook", "gallery_image_reference"])
        ]
        if observation_identifiers is not None:
            observation_filter = """
                  AND json_extract(
                      records.value_json, '$.listing_observation_record_identifier'
                  ) IS NOT NULL
                  AND json_extract(
                      records.value_json, '$.listing_observation_record_identifier'
                  ) IN (SELECT value FROM json_each(?))
            """
            parameters.append(_json(list(dict.fromkeys(observation_identifiers))))

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                f"""
                SELECT records.object_id, records.value_json
                FROM objects
                JOIN records ON records.object_id = objects.id
                WHERE objects.kind_parts_json = ?
                  {observation_filter}
                ORDER BY objects.rowid
                """,
                parameters,
            )
            rows = await cursor.fetchall()
        identifiers: dict[GalleryImageReference, str] = {}
        for row in rows:
            value = decode_json(_text(row[1]))
            if not isinstance(value, dict):
                raise ValueError("Stored gallery image reference is malformed")
            reference = GalleryImageReference.model_validate_json(
                _json({field: value[field] for field in GalleryImageReference.model_fields})
            )
            identifiers.setdefault(reference, _text(row[0]))
        return identifiers

    async def facebook_projection_gallery_reference_identifiers(
        self,
        observation_identifiers: Sequence[str],
        *,
        as_of_completion_sequence: int,
    ) -> dict[GalleryImageReference, str]:
        """Find bounded gallery edges published by completed work at a boundary."""

        if not observation_identifiers:
            return {}
        if len(observation_identifiers) > 10_000 or as_of_completion_sequence < 0:
            raise ValueError("Projection gallery-reference bounds are invalid")
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                WITH ranked AS MATERIALIZED (
                    SELECT
                        requested.key AS requested_key,
                        reference_record.object_id,
                        reference_record.value_json,
                        row_number() OVER (
                            PARTITION BY requested.key
                            ORDER BY CAST(json_extract(
                                         reference_record.value_json, '$.gallery_order'
                                     ) AS INTEGER),
                                     reference.id
                        ) AS reference_rank
                    FROM json_each(?) AS requested
                    CROSS JOIN records AS reference_record
                        INDEXED BY records_gallery_observation
                      ON json_extract(
                          reference_record.value_json,
                          '$.listing_observation_record_identifier'
                      ) = requested.value
                    -- Preserve the requested-ID-driven expression-index lookup.  An ordinary
                    -- JOIN lets SQLite scan every object of this kind before applying the
                    -- small requested observation set.
                    CROSS JOIN objects AS reference
                      ON reference.id = reference_record.object_id
                     AND reference.kind_parts_json = ?
                    CROSS JOIN work_operations AS producing_work
                      ON producing_work.operation_id = reference.created_by_operation_id
                    CROSS JOIN work_events AS completed
                      ON completed.work_item_id = producing_work.work_item_id
                     AND completed.event_kind = 'completed'
                     AND completed.sequence <= ?
                    WHERE json_extract(
                              reference_record.value_json,
                              '$.listing_observation_record_identifier'
                          ) IS NOT NULL
                )
                SELECT object_id, value_json
                FROM ranked
                WHERE reference_rank <= 100
                ORDER BY requested_key, reference_rank
                """,
                (
                    _json(list(dict.fromkeys(observation_identifiers))),
                    _json(["carl", "facebook", "gallery_image_reference"]),
                    as_of_completion_sequence,
                ),
            )
            rows = await cursor.fetchall()
        identifiers: dict[GalleryImageReference, str] = {}
        for row in rows:
            value = decode_json(_text(row[1]))
            if not isinstance(value, dict):
                raise ValueError("Stored gallery image reference is malformed")
            reference = GalleryImageReference.model_validate_json(
                _json({field: value[field] for field in GalleryImageReference.model_fields})
            )
            identifiers.setdefault(reference, _text(row[0]))
        return identifiers

    async def facebook_projection_saved_image_results_for_references(
        self,
        reference_identifiers: Sequence[str],
        *,
        as_of_completion_sequence: int,
    ) -> tuple[SavedImageProjectionCandidate, ...]:
        """Return direct saved image results completed by a projection boundary."""

        if not reference_identifiers:
            return ()
        if len(reference_identifiers) > 10_000 or as_of_completion_sequence < 0:
            raise ValueError("Projection image-result bounds are invalid")
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT result_record.object_id, result_record.value_json,
                       result.created_by_operation_id, completed.sequence,
                       operation.ended_at_utc
                FROM json_each(?) AS requested
                CROSS JOIN records AS result_record
                    INDEXED BY records_saved_image_reference
                  ON json_extract(
                      result_record.value_json, '$.image_reference_record_identifier'
                  ) = requested.value
                -- Keep requested references first so SQLite uses the expression index before
                -- joining the comparatively large image-result object population.
                CROSS JOIN objects AS result
                  ON result.id = result_record.object_id
                 AND result.kind_parts_json = ?
                CROSS JOIN work_operations AS producing_work
                  ON producing_work.operation_id = result.created_by_operation_id
                CROSS JOIN operations AS operation
                  ON operation.id = result.created_by_operation_id
                 AND operation.state = 'completed'
                CROSS JOIN work_events AS completed
                  ON completed.work_item_id = producing_work.work_item_id
                 AND completed.event_kind = 'completed'
                 AND completed.sequence <= ?
                WHERE json_extract(result_record.value_json, '$.state') = 'saved'
                  AND json_extract(
                      result_record.value_json, '$.image_reference_record_identifier'
                  ) IS NOT NULL
                  AND json_extract(result_record.value_json, '$.original_url') IS NOT NULL
                ORDER BY requested.key, completed.sequence, result.id
                """,
                (
                    _json(list(dict.fromkeys(reference_identifiers))),
                    _json(["carl", "facebook", "image_result"]),
                    as_of_completion_sequence,
                ),
            )
            rows = await cursor.fetchall()
        candidates: list[SavedImageProjectionCandidate] = []
        for row in rows:
            result = cast(dict[str, JsonValue], decode_json(_text(row[1])))
            candidates.append(
                SavedImageProjectionCandidate(
                    result_record_identifier=_text(row[0]),
                    image_reference_record_identifier=_text(
                        result["image_reference_record_identifier"]
                    ),
                    source_photo_identifier=(
                        None
                        if result.get("source_photo_id") is None
                        else _text(result["source_photo_id"])
                    ),
                    original_url=_text(result["original_url"]),
                    result=result,
                    evidence=ProjectionEvidence(
                        evidence_record_identifier=_text(row[0]),
                        producing_operation_identifier=_text(row[2]),
                        acquisition_completion_sequence=_integer(row[3]),
                        observation_completion_sequence=_integer(row[3]),
                        observed_at_utc=None if row[4] is None else _text(row[4]),
                        completed_at_utc=None if row[4] is None else _text(row[4]),
                        source_kind=ProjectionSourceKind.IMAGE_DOWNLOAD,
                    ),
                )
            )
        return tuple(candidates)

    async def facebook_projection_saved_image_results_for_renditions(
        self,
        renditions: Sequence[tuple[str | None, str]],
        *,
        as_of_completion_sequence: int,
    ) -> tuple[SavedImageProjectionCandidate, ...]:
        """Return exact saved renditions completed by a projection boundary."""

        if not renditions:
            return ()
        if len(renditions) > 10_000 or as_of_completion_sequence < 0:
            raise ValueError("Projection rendition bounds are invalid")
        requested = [
            {"source_photo_id": photo_identifier, "original_url": original_url}
            for photo_identifier, original_url in dict.fromkeys(renditions)
        ]
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT result_record.object_id, result_record.value_json,
                       result.created_by_operation_id, completed.sequence,
                       operation.ended_at_utc
                FROM json_each(?) AS requested
                CROSS JOIN records AS result_record
                    INDEXED BY records_saved_image_rendition
                  ON json_extract(result_record.value_json, '$.original_url') =
                     json_extract(requested.value, '$.original_url')
                 AND json_extract(result_record.value_json, '$.source_photo_id') IS
                     json_extract(requested.value, '$.source_photo_id')
                -- Keep requested renditions first so SQLite uses the expression index before
                -- joining the comparatively large image-result object population.
                CROSS JOIN objects AS result
                  ON result.id = result_record.object_id
                 AND result.kind_parts_json = ?
                CROSS JOIN work_operations AS producing_work
                  ON producing_work.operation_id = result.created_by_operation_id
                CROSS JOIN operations AS operation
                  ON operation.id = result.created_by_operation_id
                 AND operation.state = 'completed'
                CROSS JOIN work_events AS completed
                  ON completed.work_item_id = producing_work.work_item_id
                 AND completed.event_kind = 'completed'
                 AND completed.sequence <= ?
                WHERE json_extract(result_record.value_json, '$.state') = 'saved'
                  AND json_extract(result_record.value_json, '$.original_url') IS NOT NULL
                ORDER BY requested.key, completed.sequence, result.id
                """,
                (
                    _json(requested),
                    _json(["carl", "facebook", "image_result"]),
                    as_of_completion_sequence,
                ),
            )
            rows = await cursor.fetchall()
        candidates: list[SavedImageProjectionCandidate] = []
        for row in rows:
            result = cast(dict[str, JsonValue], decode_json(_text(row[1])))
            reference_identifier = result.get("image_reference_record_identifier")
            source_photo_identifier = result.get("source_photo_id")
            candidates.append(
                SavedImageProjectionCandidate(
                    result_record_identifier=_text(row[0]),
                    image_reference_record_identifier=(
                        reference_identifier if isinstance(reference_identifier, str) else None
                    ),
                    source_photo_identifier=(
                        source_photo_identifier
                        if isinstance(source_photo_identifier, str)
                        else None
                    ),
                    original_url=_text(result["original_url"]),
                    result=result,
                    evidence=ProjectionEvidence(
                        evidence_record_identifier=_text(row[0]),
                        producing_operation_identifier=_text(row[2]),
                        acquisition_completion_sequence=_integer(row[3]),
                        observation_completion_sequence=_integer(row[3]),
                        observed_at_utc=None if row[4] is None else _text(row[4]),
                        completed_at_utc=None if row[4] is None else _text(row[4]),
                        source_kind=ProjectionSourceKind.IMAGE_DOWNLOAD,
                    ),
                )
            )
        return tuple(candidates)

    async def get_artifact_metadata(self, identifier: str) -> dict[str, JsonValue]:
        """Read inline or external artifact metadata without materializing content."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT sha256, size, media_type, representation_json, NULL
                FROM artifacts WHERE object_id = ?
                UNION ALL
                SELECT sha256, size, media_type, representation_json, locator
                FROM external_artifacts WHERE object_id = ?
                """,
                (identifier, identifier),
            )
            rows = await cursor.fetchall()
        if not rows:
            raise KeyError(identifier)
        row = rows[0]
        storage: dict[str, JsonValue] = (
            {"backend": "sqlite"}
            if row[4] is None
            else {"backend": "filesystem", "locator": _text(row[4])}
        )
        return {
            "sha256": row[0],
            "size": row[1],
            "media_type": row[2],
            "representation": decode_json(_text(row[3])),
            "storage": storage,
        }

    async def get_artifact(
        self, identifier: str, *, maximum_bytes: int | None = None
    ) -> tuple[dict[str, JsonValue], bytes]:
        """Verify artifact content, optionally rejecting oversized data before loading."""

        if maximum_bytes is not None and (type(maximum_bytes) is not int or maximum_bytes < 0):
            raise ValueError("maximum_bytes must be a nonnegative integer or None")
        async with self._connections.reader() as connection:
            if maximum_bytes is not None:
                cursor = await connection.execute(
                    """
                    SELECT artifacts.size, length(content.inline_bytes)
                    FROM artifacts JOIN content USING (sha256, size)
                    WHERE artifacts.object_id = ?
                    """,
                    (identifier,),
                )
                sizes = await cursor.fetchall()
                if sizes and any(_integer(size) > maximum_bytes for size in sizes[0]):
                    raise ValueError("Artifact exceeds maximum_bytes")
            cursor = await connection.execute(
                """
                SELECT artifacts.sha256, artifacts.size, artifacts.media_type,
                       artifacts.representation_json, content.inline_bytes
                FROM artifacts
                JOIN content USING (sha256, size)
                WHERE artifacts.object_id = ?
                """,
                (identifier,),
            )
            # Exhaust these unique-row queries before releasing the pooled
            # reader. A later file error retains this frame in its traceback;
            # an unstepped cursor would retain the reader's old WAL snapshot.
            rows = await cursor.fetchall()
            row = rows[0] if rows else None
            if row is None:
                cursor = await connection.execute(
                    """
                    SELECT sha256, size, media_type, representation_json, locator
                    FROM external_artifacts
                    WHERE object_id = ?
                    """,
                    (identifier,),
                )
                external_rows = await cursor.fetchall()
                external_row = external_rows[0] if external_rows else None
            else:
                external_row = None
        if row is not None:
            inline_bytes = row[4]
            if not isinstance(inline_bytes, bytes):
                raise ValueError("Stored artifact content is not bytes")
            value = inline_bytes
            if len(value) != row[1] or hashlib.sha256(value).hexdigest() != row[0]:
                raise ValueError("Stored artifact failed integrity verification")
            storage: dict[str, JsonValue] = {"backend": "sqlite"}
        elif external_row is not None:
            _, value = await anyio.to_thread.run_sync(
                _read_external_artifact,
                self.path,
                _text(external_row[4]),
                _text(external_row[0]),
                _integer(external_row[1]),
                maximum_bytes,
            )
            row = external_row
            storage = {"backend": "filesystem", "locator": _text(external_row[4])}
        else:
            raise KeyError(identifier)
        metadata: dict[str, JsonValue] = {
            "sha256": row[0],
            "size": row[1],
            "media_type": row[2],
            "representation": decode_json(_text(row[3])),
            "storage": storage,
        }
        return metadata, value

    async def get_external_artifact_metadata(self, identifier: str) -> dict[str, JsonValue] | None:
        """Read external metadata before bounded I/O, without materializing bytes."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT sha256, size, media_type, representation_json, locator
                FROM external_artifacts WHERE object_id = ?
                """,
                (identifier,),
            )
            rows = await cursor.fetchall()
        if not rows:
            return None
        row = rows[0]
        return {
            "sha256": row[0],
            "size": row[1],
            "media_type": row[2],
            "representation": decode_json(_text(row[3])),
            "storage": {"backend": "filesystem", "locator": _text(row[4])},
        }

    async def get_external_artifact_path(
        self, identifier: str, *, maximum_bytes: int | None = None
    ) -> tuple[dict[str, JsonValue], Path] | None:
        """Return a verified external path, or None for an inline artifact."""

        if maximum_bytes is not None and (type(maximum_bytes) is not int or maximum_bytes < 0):
            raise ValueError("maximum_bytes must be a nonnegative integer or None")
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT sha256, size, media_type, representation_json, locator
                FROM external_artifacts
                WHERE object_id = ?
                """,
                (identifier,),
            )
            rows = await cursor.fetchall()
            row = rows[0] if rows else None
        if row is None:
            return None
        path, _ = await anyio.to_thread.run_sync(
            _read_external_artifact,
            self.path,
            _text(row[4]),
            _text(row[0]),
            _integer(row[1]),
            maximum_bytes,
        )
        return (
            {
                "sha256": row[0],
                "size": row[1],
                "media_type": row[2],
                "representation": decode_json(_text(row[3])),
                "storage": {"backend": "filesystem", "locator": _text(row[4])},
            },
            path,
        )

    async def externalize_artifact(
        self,
        *,
        identifier: str,
        locator: str,
        sha256: str,
        size: int,
        migration_operation_identifier: str,
    ) -> bool:
        """Move one existing inline artifact to an already verified external file."""

        draft = ExternalFileDraft(
            identifier=identifier,
            kind=("validation",),
            media_type=None,
            representation={},
            sha256=sha256,
            size=size,
            locator=locator,
        )
        async with self._connections.writer() as connection:
            cursor = await connection.execute(
                """
                SELECT sha256, size, media_type, representation_json, locator
                FROM external_artifacts WHERE object_id = ?
                """,
                (identifier,),
            )
            external = await cursor.fetchone()
            if external is not None:
                if (
                    _text(external[0]),
                    _integer(external[1]),
                    _text(external[4]),
                ) != (sha256, size, draft.locator):
                    raise ValueError("External artifact identity does not match requested content")
                return False
            cursor = await connection.execute(
                """
                SELECT sha256, size, media_type, representation_json
                FROM artifacts WHERE object_id = ?
                """,
                (identifier,),
            )
            inline = await cursor.fetchone()
            if inline is None:
                raise KeyError(identifier)
            if (_text(inline[0]), _integer(inline[1])) != (sha256, size):
                raise ValueError("Inline artifact does not match externalized content")
            representation = decode_json(_text(inline[3]))
            if not isinstance(representation, dict):
                raise ValueError("Artifact representation is malformed")
            representation = {
                **representation,
                "externalized_by_operation_identifier": migration_operation_identifier,
            }
            await connection.execute(
                "INSERT INTO external_artifacts VALUES (?, ?, ?, ?, ?, ?)",
                (
                    identifier,
                    sha256,
                    size,
                    inline[2],
                    _json(representation),
                    draft.locator,
                ),
            )
            await connection.execute("DELETE FROM artifacts WHERE object_id = ?", (identifier,))
            await connection.execute(
                """
                DELETE FROM content
                WHERE sha256 = ? AND size = ?
                  AND NOT EXISTS (
                      SELECT 1 FROM artifacts
                      WHERE artifacts.sha256 = content.sha256
                        AND artifacts.size = content.size
                  )
                """,
                (sha256, size),
            )
            return True

    async def replace_record_value(
        self,
        *,
        identifier: str,
        expected_kind: tuple[str, ...],
        value: JsonValue,
    ) -> None:
        """Replace a record during an explicit, provenance-recorded data migration."""

        async with self._connections.writer() as connection:
            cursor = await connection.execute(
                """
                SELECT objects.kind_parts_json
                FROM objects JOIN records ON records.object_id = objects.id
                WHERE objects.id = ?
                """,
                (identifier,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise KeyError(identifier)
            if decode_json(_text(row[0])) != list(expected_kind):
                raise ValueError("Record kind does not match migration expectation")
            await connection.execute(
                "UPDATE records SET value_json = ? WHERE object_id = ?",
                (_json(value), identifier),
            )

    async def operation(self, identifier: str) -> dict[str, JsonValue]:
        columns = (
            "id",
            "component_parts_json",
            "output_schema_version",
            "code_provenance_json",
            "invocation_json",
            "configuration_json",
            "state",
            "started_at_utc",
            "ended_at_utc",
            "duration_ns",
            "result_json",
            "error_json",
            "code_state_id",
        )
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                f"""
                SELECT {", ".join("operation." + column for column in columns)},
                       code_state.commit_hash, code_state.worktree_state
                FROM operations AS operation
                LEFT JOIN code_states AS code_state ON code_state.id = operation.code_state_id
                WHERE operation.id = ?
                """,
                (identifier,),
            )
            row = await cursor.fetchone()
        if row is None:
            raise KeyError(identifier)
        result: dict[str, JsonValue] = dict(zip(columns, row[: len(columns)], strict=True))
        for key in (
            "component_parts_json",
            "code_provenance_json",
            "invocation_json",
            "configuration_json",
            "result_json",
            "error_json",
        ):
            output_key = key.removesuffix("_json")
            raw = result.pop(key)
            result[output_key] = decode_json(raw) if isinstance(raw, str) else None
        provenance = result["code_provenance"]
        if result["code_state_id"] is not None:
            if not isinstance(provenance, dict):
                raise ValueError("Operation code provenance is not an object")
            commit_hash, worktree_state = row[-2:]
            if commit_hash is not None and not isinstance(commit_hash, bytes):
                raise ValueError("Stored code state commit hash is not binary")
            provenance["commit_hash"] = None if commit_hash is None else commit_hash.hex()
            provenance["worktree_state"] = _text(worktree_state)
        return result
