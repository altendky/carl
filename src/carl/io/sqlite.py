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
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import anyio
import apsw

from carl.core.activity import (
    ActiveNetworkActivity,
    ActivitySnapshot,
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
from carl.core.facebook_work import COLLECT_SEARCH_WORK_KIND, SuccessfulItemPageResult
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
from carl.core.review import (
    AnalysisDescriptor,
    CandidateSource,
    ProductGuideConflict,
    ProductGuideDetails,
    ProductGuideSummary,
    candidate_availability,
)
from carl.core.review_workspace import (
    ACQUIRE_REVIEW_BATCH,
    LISTING_REVIEW_KIND,
    MAXIMUM_WORKSET_LISTINGS,
    REVIEW_BATCH_KIND,
    REVIEW_WORKSET_KIND,
    ListingReviewRecord,
    RecordListingReviewsResult,
    RecordWorkspaceBulkReviewResult,
    ReleaseReviewClaimResult,
    ReviewBatch,
    ReviewBatchAcquisition,
    ReviewClaimLease,
    ReviewWorksetConflict,
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
    SchedulingSubjectKind,
    SlidingWindowRateConstraint,
    UniformHoldoffConstraint,
    WorkCapability,
    WorkDefinition,
    WorkEventKind,
    WorkLease,
    WorkRequester,
    WorkState,
)
from carl.core.worker import FollowOnWork
from carl.io.db import DatabaseConnections

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
    version=11,
)

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

_SCHEMA = """
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

CREATE INDEX IF NOT EXISTS records_gallery_observation
ON records(json_extract(value_json, '$.listing_observation_record_identifier'), object_id)
WHERE json_extract(value_json, '$.listing_observation_record_identifier') IS NOT NULL;

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
    deduplication_identity_json TEXT NOT NULL CHECK (json_valid(deduplication_identity_json)),
    state TEXT NOT NULL CHECK (
        state IN ('pending', 'leased', 'completed', 'terminal_failure')
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
        (state = 'leased' AND lease_token IS NOT NULL AND lease_owner IS NOT NULL
         AND lease_expires_at_utc_ns IS NOT NULL)
        OR
        (state != 'leased' AND lease_token IS NULL AND lease_owner IS NULL
         AND lease_expires_at_utc_ns IS NULL)
    )
) STRICT;

CREATE INDEX IF NOT EXISTS work_items_claim
ON work_items(state, eligible_at_utc_ns, priority DESC, created_at_utc_ns);

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
            'lease_renewed', 'released', 'completed', 'terminal_failure'
        )
    ),
    recorded_at_utc_ns INTEGER NOT NULL CHECK (recorded_at_utc_ns >= 0),
    data_json TEXT NOT NULL CHECK (json_valid(data_json)),
    FOREIGN KEY (work_item_id) REFERENCES work_items(id)
) STRICT;

CREATE INDEX IF NOT EXISTS work_events_work_item
ON work_events(work_item_id, sequence);

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

_SCHEMA_DEFINITION_SHA256 = hashlib.sha256(_SCHEMA.encode()).hexdigest()


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
    database_path: Path, locator: str, expected_sha256: str, expected_size: int
) -> tuple[Path, bytes]:
    path = _external_artifact_path(database_path, locator)
    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        chunks = bytearray()
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
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


class _SuccessfulItemPageAvailable(Exception):
    def __init__(self, result: SuccessfulItemPageResult):
        super().__init__(result.listing_id)
        self.result = result


class Database:
    def __init__(self, path: Path, connections: DatabaseConnections):
        self.path = path
        self._connections = connections

    @classmethod
    @asynccontextmanager
    async def managed(
        cls,
        path: Path,
        *,
        initialize: bool = False,
        reader_count: int = 4,
        prepare_schema: bool = True,
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
            yield database

    @staticmethod
    def _expected_metadata() -> dict[str, str]:
        return {
            "schema_identity_json": encode_json(DATABASE_SCHEMA.model_dump(mode="json")),
            "schema_version": str(DATABASE_SCHEMA.version),
            "schema_definition_sha256": _SCHEMA_DEFINITION_SHA256,
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
                for key, value in self._expected_metadata().items():
                    await connection.execute(
                        "UPDATE schema_metadata SET value = ? WHERE key = ?", (value, key)
                    )
            if not await self._metadata_matches(connection, self._expected_metadata()):
                raise RuntimeError("Unsupported database schema metadata")
            await self._validate_schema(connection)

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

            await connection.execute(_SCHEMA)
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

    async def supersede_constraints(
        self,
        *,
        retired_identifiers: tuple[tuple[str, ...], ...],
        replacements: tuple[Constraint, ...],
        operation_identifier: str,
        at_utc_ns: int,
        reason: str,
    ) -> None:
        """Atomically retire known constraints and register immutable replacements.

        Retired definitions and rate reservations remain available for audit.
        Repeating a retirement leaves its original operation, reason, and timestamp
        intact. A missing old constraint needs no retirement row.
        """

        if at_utc_ns < 0 or not reason or not operation_identifier:
            raise ValueError("A constraint supersession needs time, reason, and operation")
        if len(set(retired_identifiers)) != len(retired_identifiers):
            raise ValueError("Duplicate retired constraint identifier")
        replacement_identifiers = [constraint.identifier for constraint in replacements]
        if len(set(replacement_identifiers)) != len(replacement_identifiers):
            raise ValueError("Duplicate replacement constraint identifier")
        if set(retired_identifiers) & set(replacement_identifiers):
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
            for identifier in retired_identifiers:
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
                    "SELECT 1 FROM scheduling_constraints WHERE identifier_parts_json = ?",
                    (identity_json,),
                )
                if await cursor.fetchone() is None:
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
                cursor = await connection.execute(
                    """
                    SELECT count(*), min(network_activities.permit_expires_at_utc_ns)
                    FROM network_activities
                    JOIN network_activity_scopes
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
                SELECT state, eligible_at_utc_ns
                FROM network_activities
                WHERE id = ?
                """,
                (network_activity_identifier,),
            )
            row = await cursor.fetchone()
            if row is None:
                raise KeyError(network_activity_identifier)
            if _text(row[0]) != NetworkActivityState.PENDING.value:
                raise RuntimeError("Only a pending network activity can be admitted")
            eligible_at = _integer(row[1])
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
            eligible_at = max(definition.not_before_utc_ns, enqueued_at_utc_ns)
            if definition.kind == COLLECT_EBAY_SEARCH_WORK_KIND:
                stack = _ebay_search_stack_identifier(definition.payload)
                cooldowns = await self._ebay_search_stack_cooldowns(connection, enqueued_at_utc_ns)
                eligible_at = max(eligible_at, cooldowns.get(stack, 0) if stack is not None else 0)
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
                        deduplication_identity_json, state, priority,
                        eligible_at_utc_ns, created_at_utc_ns
                    ) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                    """,
                    (
                        work_item_identifier,
                        kind_json,
                        definition.payload_schema_version,
                        _json(definition.payload),
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
                    requester_identifier, requested_at_utc_ns, context_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    requester.request_identifier,
                    work_item_identifier,
                    _json(list(requester.kind)),
                    requester.identifier,
                    enqueued_at_utc_ns,
                    _json(requester.context),
                ),
            )
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
                    requester_identifier, requested_at_utc_ns, context_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    requester.request_identifier,
                    work_item_identifier,
                    _json(list(requester.kind)),
                    requester.identifier,
                    requested_at_utc_ns,
                    _json(requester.context),
                ),
            )
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
    ) -> tuple[bool, int | None, tuple[Constraint, ...]]:
        constraints = await self._constraints_for_work(connection, work_item_identifier)
        next_eligible: int | None = None
        available = True
        for constraint in constraints:
            if isinstance(constraint, ConcurrencyConstraint):
                cursor = await connection.execute(
                    """
                    SELECT count(*), min(work_items.lease_expires_at_utc_ns)
                    FROM work_items
                    JOIN work_scopes ON work_scopes.work_item_id = work_items.id
                    WHERE work_scopes.scope_kind = ?
                      AND work_scopes.scope_identity_json = ?
                      AND work_items.state = 'leased'
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
        identifier_predicate = (
            "AND id IN (SELECT value FROM json_each(?))" if eligible_identifiers is not None else ""
        )
        identifier_bindings: tuple[apsw.SQLiteValue, ...] = (
            (_json(list(eligible_identifiers)),) if eligible_identifiers is not None else ()
        )
        async with self._connections.writer() as connection:
            now_utc_ns = utc_now_ns()
            ebay_cooldowns = (
                await self._ebay_search_stack_cooldowns(connection, now_utc_ns)
                if any(
                    capability.kind == COLLECT_EBAY_SEARCH_WORK_KIND
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
                ),
                representatives AS (
                    SELECT *, row_number() OVER (
                        PARTITION BY scope_profile,
                            CASE WHEN kind_parts_json='["carl","ebay","collect","search"]'
                                 THEN COALESCE(json_extract(payload_json,'$.request.stack_identifier'),
                                               'ebay_anonymous') END
                        ORDER BY priority DESC, eligible_at_utc_ns, created_at_utc_ns, id
                    ) AS profile_rank
                    FROM eligible
                )
                SELECT id, kind_parts_json, payload_schema_version, payload_json,
                       state, lease_token, lease_owner, lease_expires_at_utc_ns,
                       attempt
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
                    and network_work_kind(tuple(decode_json(_text(row[1]))))
                ):
                    next_checks.append(max(now_utc_ns + 1_000_000_000, _integer(connectivity[1])))
                    continue
                if _text(row[1]) == _json(list(COLLECT_EBAY_SEARCH_WORK_KIND)):
                    stack = _ebay_search_stack_identifier(decode_json(_text(row[3])))
                    cooldown_until = ebay_cooldowns.get(stack, 0) if stack is not None else 0
                    if cooldown_until > now_utc_ns:
                        next_checks.append(cooldown_until)
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
        return {
            "identifier": work_item_identifier,
            "kind": decode_json(_text(row[0])),
            "payload_schema_version": _integer(row[1]),
            "payload": decode_json(_text(row[2])),
            "state": _text(row[3]),
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
    ) -> ActivitySnapshot:
        """Read one consistent, bounded view of durable work and network activity."""

        if captured_at_utc_ns < 0 or recent_window_ns <= 0 or maximum_rows < 1:
            raise ValueError("Activity snapshot bounds are invalid")
        recent_since_utc_ns = max(0, captured_at_utc_ns - recent_window_ns)
        async with self._connections.reader() as connection:
            work_count_cursor = await connection.execute(
                """
                SELECT kind_parts_json, state, count(*)
                FROM work_items
                GROUP BY kind_parts_json, state
                ORDER BY kind_parts_json, state
                """
            )
            work_count_rows = await work_count_cursor.fetchall()
            recent_work_cursor = await connection.execute(
                """
                SELECT work.kind_parts_json, event.event_kind, count(*)
                FROM work_events AS event
                JOIN work_items AS work ON work.id = event.work_item_id
                WHERE event.recorded_at_utc_ns >= ?
                  AND event.event_kind IN ('completed', 'terminal_failure')
                GROUP BY work.kind_parts_json, event.event_kind
                ORDER BY work.kind_parts_json, event.event_kind
                """,
                (recent_since_utc_ns,),
            )
            recent_work_rows = await recent_work_cursor.fetchall()
            active_work_cursor = await connection.execute(
                """
                SELECT id, kind_parts_json, state, attempt, created_at_utc_ns,
                       eligible_at_utc_ns, lease_expires_at_utc_ns, lease_owner,
                       payload_json, result_json, error_json
                FROM work_items
                WHERE state IN ('pending', 'leased')
                ORDER BY state = 'leased' DESC, priority DESC, created_at_utc_ns, id
                LIMIT ?
                """,
                (maximum_rows,),
            )
            active_work_rows = await active_work_cursor.fetchall()
            terminal_work_cursor = await connection.execute(
                """
                SELECT work.id, work.kind_parts_json, work.state, work.attempt,
                       work.created_at_utc_ns, work.eligible_at_utc_ns,
                       work.payload_json, work.result_json, work.error_json,
                       event.recorded_at_utc_ns
                FROM work_events AS event
                JOIN work_items AS work ON work.id = event.work_item_id
                WHERE event.event_kind IN ('completed', 'terminal_failure')
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
                GROUP BY scope.scope_identity_json, activity.state
                ORDER BY scope.scope_identity_json, activity.state
                """,
                (recent_since_utc_ns,),
            )
            network_path_rows = await network_path_cursor.fetchall()
            active_network_cursor = await connection.execute(
                """
                SELECT activity.id, activity.kind_parts_json,
                       scope.scope_identity_json, activity.state,
                       activity.network_session_identifier, activity.ordinal,
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
                """
                SELECT paused, next_probe_utc_ns, probe_token, probe_expires_utc_ns,
                       checked_at_utc_ns, result_json
                FROM network_connectivity WHERE singleton = 1
                """
            )
            connectivity_row = await connectivity_cursor.fetchone()

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
                kind=kind,
                pending=counts.get(WorkState.PENDING, 0),
                leased=counts.get(WorkState.LEASED, 0),
                completed=counts.get(WorkState.COMPLETED, 0),
                terminal_failure=counts.get(WorkState.TERMINAL_FAILURE, 0),
                recent_completed=recent_work_counts.get(kind, {}).get(WorkState.COMPLETED, 0),
                recent_terminal_failure=recent_work_counts.get(kind, {}).get(
                    WorkState.TERMINAL_FAILURE, 0
                ),
            )
            for kind, counts in sorted(work_counts.items())
        )

        def activity_from_row(row: Sequence[apsw.SQLiteValue], *, terminal: bool) -> WorkActivity:
            kind = tuple(str(part) for part in decode_json(_text(row[1])))
            payload = decode_json(_text(row[8] if not terminal else row[6]))
            result_value = row[9] if not terminal else row[7]
            error_value = row[10] if not terminal else row[8]
            result = None if result_value is None else decode_json(_text(result_value))
            error = None if error_value is None else decode_json(_text(error_value))
            return WorkActivity(
                identifier=_text(row[0]),
                kind=kind,
                state=WorkState(_text(row[2])),
                attempt=_integer(row[3]),
                created_at_utc_ns=_integer(row[4]),
                eligible_at_utc_ns=_integer(row[5]),
                lease_expires_at_utc_ns=(None if terminal or row[6] is None else _integer(row[6])),
                worker_identifier=(None if terminal or row[7] is None else _text(row[7])),
                subject=work_subject(kind, payload),
                stage=work_stage(result),
                error_kind=work_error_kind(error),
                terminal_at_utc_ns=(None if not terminal else _integer(row[9])),
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
                    path=path,
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
                )
                for path, (all_counts, recent_counts) in sorted(path_counts.items())
            ),
            active_network=tuple(
                ActiveNetworkActivity(
                    identifier=_text(row[0]),
                    kind=tuple(str(part) for part in decode_json(_text(row[1]))),
                    path=tuple(str(part) for part in decode_json(_text(row[2]))),
                    state=NetworkActivityState(_text(row[3])),
                    session_identifier=_text(row[4]),
                    ordinal=_integer(row[5]),
                    attempt=_integer(row[6]),
                    created_at_utc_ns=_integer(row[7]),
                    eligible_at_utc_ns=_integer(row[8]),
                    admitted_at_utc_ns=None if row[9] is None else _integer(row[9]),
                    permit_expires_at_utc_ns=(None if row[10] is None else _integer(row[10])),
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
                    count(*) FILTER (WHERE work.state = 'leased'),
                    count(*) FILTER (WHERE work.state = 'terminal_failure')
                FROM requested
                JOIN work_items AS work ON work.id = requested.work_item_id
                WHERE work.state IN ('pending', 'leased', 'terminal_failure')
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
                WHERE work.state IN ('pending', 'leased')
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
                    result_json = NULL, error_json = NULL,
                    lease_token = NULL, lease_owner = NULL,
                    lease_expires_at_utc_ns = NULL
                WHERE id = ? AND state = 'terminal_failure'
                """,
                (retried_at_utc_ns, payload_schema_version, work_item_identifier),
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
                    result_json=NULL,error_json=NULL,lease_token=NULL,lease_owner=NULL,
                    lease_expires_at_utc_ns=NULL
                WHERE id=? AND state='terminal_failure'
                """,
                (
                    eligible_at,
                    payload_schema_version,
                    _json(payload.as_json()),
                    _json(list(definition.deduplication_identity)),
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

    async def _ebay_search_stack_cooldowns(
        self, connection: AsyncConnection, now_utc_ns: int
    ) -> dict[str, int]:
        """Read cooldowns from fenced attempt events, including terminal attempts."""
        cursor = await connection.execute(
            """
            WITH cooldowns AS (
                SELECT COALESCE(json_extract(event.data_json,'$.reason.cooldown.stack_identifier'),
                                json_extract(event.data_json,'$.error.cooldown.stack_identifier')) stack,
                       COALESCE(json_extract(event.data_json,'$.reason.cooldown.until_utc_ns'),
                                json_extract(event.data_json,'$.error.cooldown.until_utc_ns')) until_ns
                FROM work_items AS work JOIN work_events AS event ON event.work_item_id=work.id
                WHERE work.kind_parts_json=? AND event.event_kind IN ('released','terminal_failure')
            ) SELECT stack,MAX(until_ns) FROM cooldowns
              WHERE typeof(stack)='text' AND typeof(until_ns)='integer' AND until_ns>?
              GROUP BY stack
            """,
            (_json(list(COLLECT_EBAY_SEARCH_WORK_KIND)), now_utc_ns),
        )
        return {_text(row[0]): _integer(row[1]) for row in await cursor.fetchall()}

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
                             AND event.event_kind IN ('completed', 'terminal_failure')
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
        return _integer(row[0]), _integer(row[1])

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
            await self._defer_ebay_search_siblings(
                connection, work_item_identifier, reason, now_utc_ns, event_identifier
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
            await self._defer_ebay_search_siblings(
                connection, work_item_identifier, error, now_utc_ns, event_identifier
            )

    async def fail_operation(
        self,
        *,
        operation_id: str,
        error: JsonValue,
        result: JsonValue,
        ended_at_utc: str,
        duration_ns: int,
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

    async def search_listing_occurrences_for_runs(
        self,
        marketplace: str,
        run_identifiers: Sequence[str],
        *,
        as_of_completion_sequence: int,
        maximum_object_rowid: int | None = None,
    ) -> tuple[tuple[str, JsonValue], ...]:
        """Read completed cards for exact source runs, without other-source scans."""
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
                  AND operation.state = 'completed'
                  {partial_predicate}
                  AND COALESCE((SELECT MAX(event.sequence)
                    FROM work_operations AS work
                    JOIN work_events AS event ON event.work_item_id = work.work_item_id
                    WHERE work.operation_id = object.created_by_operation_id
                      AND event.event_kind = 'completed'
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
    ) -> None:
        """Atomically publish a small local mutation as a provenance-linked operation."""

        async with self._connections.writer() as connection:
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
        """Return the latest durable successful-work publication sequence."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT coalesce(max(sequence), 0) FROM work_events WHERE event_kind = 'completed'"
            )
            row = await cursor.fetchone()
        if row is None:
            raise AssertionError("Completion-boundary query returned no row")
        return _integer(row[0])

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
        """Read one exact completed search run and its explicit refresh parent."""

        if as_of_completion_sequence < 0:
            raise ValueError("Completion boundary must not be negative")
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
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
                       json_extract(record.value_json, '$.traversal.stopping_reason')
                FROM objects AS search_run
                JOIN records AS record ON record.object_id = search_run.id
                JOIN operations AS operation
                  ON operation.id = search_run.created_by_operation_id
                 AND operation.state = 'completed'
                JOIN work_operations AS search_work
                  ON search_work.operation_id = search_run.created_by_operation_id
                JOIN work_events AS completed
                  ON completed.work_item_id = search_work.work_item_id
                 AND completed.event_kind = 'completed'
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
                """
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
                     AND operation.state = 'completed'
                    JOIN work_operations AS run_work
                      ON run_work.operation_id = run.created_by_operation_id
                    JOIN work_events AS completed
                      ON completed.work_item_id = run_work.work_item_id
                     AND completed.event_kind = 'completed'
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
                """
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
                     AND operation.state = 'completed'
                    JOIN work_operations AS run_work
                      ON run_work.operation_id = run.created_by_operation_id
                    JOIN work_events AS completed
                      ON completed.work_item_id = run_work.work_item_id
                     AND completed.event_kind = 'completed'
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
                """
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
                     AND operation.state = 'completed'
                    CROSS JOIN work_operations AS search_work
                        INDEXED BY sqlite_autoindex_work_operations_1
                      ON search_work.operation_id = occurrence_object.created_by_operation_id
                    CROSS JOIN work_events AS completed
                        INDEXED BY work_events_work_item
                      ON completed.work_item_id = search_work.work_item_id
                     AND completed.event_kind = 'completed'
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
        """Return the newest completed search-card status evidence per listing."""

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
                """
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
                     AND operation.state = 'completed'
                    CROSS JOIN work_operations AS search_work
                        INDEXED BY sqlite_autoindex_work_operations_1
                      ON search_work.operation_id = occurrence_object.created_by_operation_id
                    CROSS JOIN work_events AS completed
                        INDEXED BY work_events_work_item
                      ON completed.work_item_id = search_work.work_item_id
                     AND completed.event_kind = 'completed'
                     AND completed.sequence <= ?
                    CROSS JOIN objects AS search_run
                        INDEXED BY objects_operation_kind
                      ON search_run.created_by_operation_id =
                         occurrence_object.created_by_operation_id
                     AND search_run.kind_parts_json = ?
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
        """Return recent completed search-run records for refresh selection."""

        if limit < 1:
            raise ValueError("Search-run limit must be positive")
        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
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
                 AND completed.event_kind = 'completed'
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

    async def get_artifact(self, identifier: str) -> tuple[dict[str, JsonValue], bytes]:
        async with self._connections.reader() as connection:
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
            row = await cursor.fetchone()
            if row is None:
                cursor = await connection.execute(
                    """
                    SELECT sha256, size, media_type, representation_json, locator
                    FROM external_artifacts
                    WHERE object_id = ?
                    """,
                    (identifier,),
                )
                external_row = await cursor.fetchone()
            else:
                external_row = None
        if row is not None:
            value = bytes(row[4])
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

    async def get_external_artifact_path(
        self, identifier: str
    ) -> tuple[dict[str, JsonValue], Path] | None:
        """Return a verified external path, or None for an inline artifact."""

        async with self._connections.reader() as connection:
            cursor = await connection.execute(
                """
                SELECT sha256, size, media_type, representation_json, locator
                FROM external_artifacts
                WHERE object_id = ?
                """,
                (identifier,),
            )
            row = await cursor.fetchone()
        if row is None:
            return None
        path, _ = await anyio.to_thread.run_sync(
            _read_external_artifact,
            self.path,
            _text(row[4]),
            _text(row[0]),
            _integer(row[1]),
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
