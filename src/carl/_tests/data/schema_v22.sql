
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

CREATE TABLE IF NOT EXISTS fair_initial_resources (
    kind_parts_json TEXT NOT NULL,payload_schema_version INTEGER NOT NULL,
    lane TEXT NOT NULL,resources_json TEXT NOT NULL,resident INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(kind_parts_json,payload_schema_version)
) STRICT;
CREATE TABLE IF NOT EXISTS fair_roots (
    root_request_identifier TEXT PRIMARY KEY,
    workspace_key TEXT NOT NULL,
    service_class TEXT NOT NULL CHECK(service_class IN ('interactive','background')),
    scheduling_json TEXT NOT NULL CHECK(json_valid(scheduling_json))
) STRICT;
CREATE TABLE IF NOT EXISTS work_scheduling (
    work_item_id TEXT PRIMARY KEY REFERENCES work_items(id),
    root_request_identifier TEXT NOT NULL REFERENCES fair_roots(root_request_identifier),
    workspace_key TEXT NOT NULL,
    service_class TEXT NOT NULL CHECK(service_class IN ('interactive','background')),
    scheduling_json TEXT NOT NULL CHECK(json_valid(scheduling_json))
) STRICT;
CREATE INDEX IF NOT EXISTS work_scheduling_root ON work_scheduling(root_request_identifier,work_item_id);
CREATE TABLE IF NOT EXISTS fair_unowned(work_item_id TEXT PRIMARY KEY REFERENCES work_items(id)) STRICT;
CREATE TABLE IF NOT EXISTS fair_scopes(scope TEXT PRIMARY KEY,enabled_at_utc_ns INTEGER NOT NULL,enabled INTEGER NOT NULL DEFAULT 0) STRICT;
CREATE TABLE IF NOT EXISTS fair_lanes (
    scope TEXT NOT NULL REFERENCES fair_scopes(scope), lane TEXT NOT NULL,
    sequence INTEGER NOT NULL DEFAULT 0,interactive_streak INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(scope,lane)
) STRICT;
CREATE TABLE IF NOT EXISTS fair_workspace_service (
    scope TEXT NOT NULL,lane TEXT NOT NULL,service_class TEXT NOT NULL,
    workspace_key TEXT NOT NULL,sequence INTEGER NOT NULL,
    ready_count INTEGER NOT NULL DEFAULT 0 CHECK(ready_count>=0),
    PRIMARY KEY(scope,lane,service_class,workspace_key)
) STRICT;
CREATE INDEX IF NOT EXISTS fair_workspace_turn ON fair_workspace_service(scope,lane,service_class,sequence,workspace_key);
CREATE INDEX IF NOT EXISTS fair_workspace_ready_turn ON fair_workspace_service(scope,lane,service_class,sequence,workspace_key) WHERE ready_count>0;
CREATE INDEX IF NOT EXISTS fair_workspace_count_group ON fair_workspace_service(workspace_key,service_class,lane);
CREATE TABLE IF NOT EXISTS fair_root_service (
    scope TEXT NOT NULL,lane TEXT NOT NULL,service_class TEXT NOT NULL,
    workspace_key TEXT NOT NULL,root_request_identifier TEXT NOT NULL,sequence INTEGER NOT NULL,
    ready_count INTEGER NOT NULL DEFAULT 0 CHECK(ready_count>=0),
    PRIMARY KEY(scope,lane,root_request_identifier)
) STRICT;
CREATE INDEX IF NOT EXISTS fair_root_turn ON fair_root_service(scope,lane,service_class,workspace_key,sequence,root_request_identifier);
CREATE INDEX IF NOT EXISTS fair_root_ready_turn ON fair_root_service(scope,lane,service_class,workspace_key,sequence,root_request_identifier) WHERE ready_count>0;
CREATE INDEX IF NOT EXISTS fair_root_count_group ON fair_root_service(root_request_identifier,lane);
CREATE TABLE IF NOT EXISTS fair_ready (
    work_item_id TEXT PRIMARY KEY REFERENCES work_items(id),root_request_identifier TEXT NOT NULL,
    workspace_key TEXT NOT NULL,service_class TEXT NOT NULL,lane TEXT NOT NULL,stage_id TEXT,
    kind_parts_json TEXT NOT NULL,payload_schema_version INTEGER NOT NULL,priority INTEGER NOT NULL,
    available_at_utc_ns INTEGER NOT NULL,ready_since_utc_ns INTEGER NOT NULL,
    created_at_utc_ns INTEGER NOT NULL,active INTEGER NOT NULL,
    work_eligible_at_utc_ns INTEGER NOT NULL DEFAULT 0,
    legacy_lease_expires_at_utc_ns INTEGER
) STRICT;
CREATE INDEX IF NOT EXISTS fair_ready_group ON fair_ready(root_request_identifier,lane,active,available_at_utc_ns,work_item_id);
CREATE INDEX IF NOT EXISTS fair_ready_workspace ON fair_ready(workspace_key,service_class,lane,active,available_at_utc_ns,root_request_identifier,work_item_id);
CREATE INDEX IF NOT EXISTS fair_ready_priority ON fair_ready(root_request_identifier,lane,active,priority DESC,available_at_utc_ns,created_at_utc_ns,work_item_id);
CREATE INDEX IF NOT EXISTS fair_ready_age ON fair_ready(root_request_identifier,lane,active,ready_since_utc_ns,work_item_id);
CREATE INDEX IF NOT EXISTS fair_ready_initial ON fair_ready(kind_parts_json,payload_schema_version,work_item_id) WHERE active=1 AND stage_id IS NULL;
CREATE INDEX IF NOT EXISTS fair_ready_dispatch ON fair_ready(priority DESC,work_eligible_at_utc_ns,created_at_utc_ns,work_item_id) WHERE active=1;
CREATE INDEX IF NOT EXISTS fair_ready_legacy_expiry ON fair_ready(legacy_lease_expires_at_utc_ns,work_item_id) WHERE legacy_lease_expires_at_utc_ns IS NOT NULL;
CREATE TABLE IF NOT EXISTS fair_offers (
    scope TEXT NOT NULL,lane TEXT NOT NULL,work_item_id TEXT NOT NULL REFERENCES work_items(id),
    stage_id TEXT,signature TEXT NOT NULL,resources_json TEXT NOT NULL CHECK(json_valid(resources_json)),
    ready_since_utc_ns INTEGER NOT NULL,expires_at_utc_ns INTEGER NOT NULL,
    PRIMARY KEY(scope,lane,work_item_id)
) STRICT;
CREATE INDEX IF NOT EXISTS fair_offers_live ON fair_offers(scope,lane,expires_at_utc_ns,work_item_id);
CREATE INDEX IF NOT EXISTS fair_offers_work ON fair_offers(work_item_id,stage_id,lane);
CREATE TABLE IF NOT EXISTS fair_admissions (
    token TEXT PRIMARY KEY,scope TEXT NOT NULL,lane TEXT NOT NULL,sequence INTEGER NOT NULL,
    work_item_id TEXT NOT NULL REFERENCES work_items(id),scheduling_json TEXT NOT NULL,
    admitted_at_utc_ns INTEGER NOT NULL,UNIQUE(scope,lane,sequence)
) STRICT;
CREATE TABLE IF NOT EXISTS fair_cpu_drains (
    scope TEXT PRIMARY KEY,intent_json TEXT,retry_not_before_utc_ns INTEGER NOT NULL DEFAULT 0
) STRICT;
INSERT OR IGNORE INTO fair_roots VALUES ('["legacy"]','["legacy"]','background',
    '{"root_request_identifier":"legacy","workspace_identifier":null,"service_class":"background"}');
CREATE TRIGGER IF NOT EXISTS fair_refresh_work_insert AFTER INSERT ON work_items BEGIN 
    INSERT INTO fair_ready(work_item_id,root_request_identifier,workspace_key,service_class,
        lane,stage_id,kind_parts_json,payload_schema_version,priority,available_at_utc_ns,
        ready_since_utc_ns,created_at_utc_ns,active,work_eligible_at_utc_ns,
        legacy_lease_expires_at_utc_ns)
    SELECT w.id,coalesce(owner.root_request_identifier,'["legacy"]'),
        coalesce(owner.workspace_key,'["legacy"]'),
        coalesce(owner.service_class,'background'),CASE WHEN s.id IS NULL THEN
        coalesce((SELECT CASE WHEN initial.resident=1 THEN 'resident' ELSE initial.lane END
            FROM fair_initial_resources initial WHERE initial.kind_parts_json=w.kind_parts_json
                AND initial.payload_schema_version=w.payload_schema_version),'cpu')
        WHEN json_extract(s.definition_json,'$.resident_session_identifier') IS NOT NULL
             AND NOT EXISTS(SELECT 1 FROM resident_sessions resident
                WHERE resident.id=json_extract(s.definition_json,'$.resident_session_identifier')
                  AND resident.state IN ('opening','ready','closing','quarantined')) THEN 'resident'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='analysis') THEN 'analysis'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='http') THEN 'http'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='cpu') THEN 'cpu'
        ELSE 'metadata' END,s.id,w.kind_parts_json,
        w.payload_schema_version,w.priority,max(w.eligible_at_utc_ns,coalesce(s.eligible_at_utc_ns,0)),
        max(w.created_at_utc_ns,w.eligible_at_utc_ns,
            CASE WHEN EXISTS(SELECT 1 FROM stage_attempts a WHERE a.operation_id=s.operation_id AND a.state='pending')
                 THEN 0 ELSE coalesce(s.created_at_utc_ns,0) END,
            coalesce(json_extract(s.definition_json,'$.not_before_utc_ns'),0)),w.created_at_utc_ns,
        CASE WHEN w.state IN ('pending','leased') AND
            ((w.state='pending' AND s.id IS NULL AND NOT EXISTS(
                SELECT 1 FROM stage_attempts a WHERE a.work_item_id=w.id AND a.state IN ('pending','active')))
             OR (s.state='pending' AND s.progress_waiting=0)) THEN 1 ELSE 0 END,w.eligible_at_utc_ns,
        CASE WHEN w.state IN ('leased','cancelling') AND s.id IS NULL AND NOT EXISTS(
            SELECT 1 FROM stage_attempts a WHERE a.work_item_id=w.id AND a.state IN ('pending','active'))
            THEN w.lease_expires_at_utc_ns END
    FROM work_items w LEFT JOIN work_scheduling owner ON owner.work_item_id=w.id
    LEFT JOIN work_stages s ON s.work_item_id=w.id AND s.state IN ('pending','running')
    WHERE w.id=NEW.id
    ON CONFLICT(work_item_id) DO UPDATE SET
        root_request_identifier=excluded.root_request_identifier,
        workspace_key=excluded.workspace_key,service_class=excluded.service_class,
        lane=excluded.lane,stage_id=excluded.stage_id,kind_parts_json=excluded.kind_parts_json,
        payload_schema_version=excluded.payload_schema_version,priority=excluded.priority,
        available_at_utc_ns=excluded.available_at_utc_ns,
        work_eligible_at_utc_ns=excluded.work_eligible_at_utc_ns,
        legacy_lease_expires_at_utc_ns=excluded.legacy_lease_expires_at_utc_ns,
        ready_since_utc_ns=CASE WHEN fair_ready.stage_id IS excluded.stage_id
            THEN fair_ready.ready_since_utc_ns ELSE excluded.ready_since_utc_ns END,
        active=excluded.active;
     END;
CREATE TRIGGER IF NOT EXISTS fair_refresh_work_update AFTER UPDATE ON work_items BEGIN 
    INSERT INTO fair_ready(work_item_id,root_request_identifier,workspace_key,service_class,
        lane,stage_id,kind_parts_json,payload_schema_version,priority,available_at_utc_ns,
        ready_since_utc_ns,created_at_utc_ns,active,work_eligible_at_utc_ns,
        legacy_lease_expires_at_utc_ns)
    SELECT w.id,coalesce(owner.root_request_identifier,'["legacy"]'),
        coalesce(owner.workspace_key,'["legacy"]'),
        coalesce(owner.service_class,'background'),CASE WHEN s.id IS NULL THEN
        coalesce((SELECT CASE WHEN initial.resident=1 THEN 'resident' ELSE initial.lane END
            FROM fair_initial_resources initial WHERE initial.kind_parts_json=w.kind_parts_json
                AND initial.payload_schema_version=w.payload_schema_version),'cpu')
        WHEN json_extract(s.definition_json,'$.resident_session_identifier') IS NOT NULL
             AND NOT EXISTS(SELECT 1 FROM resident_sessions resident
                WHERE resident.id=json_extract(s.definition_json,'$.resident_session_identifier')
                  AND resident.state IN ('opening','ready','closing','quarantined')) THEN 'resident'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='analysis') THEN 'analysis'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='http') THEN 'http'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='cpu') THEN 'cpu'
        ELSE 'metadata' END,s.id,w.kind_parts_json,
        w.payload_schema_version,w.priority,max(w.eligible_at_utc_ns,coalesce(s.eligible_at_utc_ns,0)),
        max(w.created_at_utc_ns,w.eligible_at_utc_ns,
            CASE WHEN EXISTS(SELECT 1 FROM stage_attempts a WHERE a.operation_id=s.operation_id AND a.state='pending')
                 THEN 0 ELSE coalesce(s.created_at_utc_ns,0) END,
            coalesce(json_extract(s.definition_json,'$.not_before_utc_ns'),0)),w.created_at_utc_ns,
        CASE WHEN w.state IN ('pending','leased') AND
            ((w.state='pending' AND s.id IS NULL AND NOT EXISTS(
                SELECT 1 FROM stage_attempts a WHERE a.work_item_id=w.id AND a.state IN ('pending','active')))
             OR (s.state='pending' AND s.progress_waiting=0)) THEN 1 ELSE 0 END,w.eligible_at_utc_ns,
        CASE WHEN w.state IN ('leased','cancelling') AND s.id IS NULL AND NOT EXISTS(
            SELECT 1 FROM stage_attempts a WHERE a.work_item_id=w.id AND a.state IN ('pending','active'))
            THEN w.lease_expires_at_utc_ns END
    FROM work_items w LEFT JOIN work_scheduling owner ON owner.work_item_id=w.id
    LEFT JOIN work_stages s ON s.work_item_id=w.id AND s.state IN ('pending','running')
    WHERE w.id=NEW.id
    ON CONFLICT(work_item_id) DO UPDATE SET
        root_request_identifier=excluded.root_request_identifier,
        workspace_key=excluded.workspace_key,service_class=excluded.service_class,
        lane=excluded.lane,stage_id=excluded.stage_id,kind_parts_json=excluded.kind_parts_json,
        payload_schema_version=excluded.payload_schema_version,priority=excluded.priority,
        available_at_utc_ns=excluded.available_at_utc_ns,
        work_eligible_at_utc_ns=excluded.work_eligible_at_utc_ns,
        legacy_lease_expires_at_utc_ns=excluded.legacy_lease_expires_at_utc_ns,
        ready_since_utc_ns=CASE WHEN fair_ready.stage_id IS excluded.stage_id
            THEN fair_ready.ready_since_utc_ns ELSE excluded.ready_since_utc_ns END,
        active=excluded.active;
     END;
CREATE TRIGGER IF NOT EXISTS fair_refresh_stage_insert AFTER INSERT ON work_stages BEGIN 
    INSERT INTO fair_ready(work_item_id,root_request_identifier,workspace_key,service_class,
        lane,stage_id,kind_parts_json,payload_schema_version,priority,available_at_utc_ns,
        ready_since_utc_ns,created_at_utc_ns,active,work_eligible_at_utc_ns,
        legacy_lease_expires_at_utc_ns)
    SELECT w.id,coalesce(owner.root_request_identifier,'["legacy"]'),
        coalesce(owner.workspace_key,'["legacy"]'),
        coalesce(owner.service_class,'background'),CASE WHEN s.id IS NULL THEN
        coalesce((SELECT CASE WHEN initial.resident=1 THEN 'resident' ELSE initial.lane END
            FROM fair_initial_resources initial WHERE initial.kind_parts_json=w.kind_parts_json
                AND initial.payload_schema_version=w.payload_schema_version),'cpu')
        WHEN json_extract(s.definition_json,'$.resident_session_identifier') IS NOT NULL
             AND NOT EXISTS(SELECT 1 FROM resident_sessions resident
                WHERE resident.id=json_extract(s.definition_json,'$.resident_session_identifier')
                  AND resident.state IN ('opening','ready','closing','quarantined')) THEN 'resident'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='analysis') THEN 'analysis'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='http') THEN 'http'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='cpu') THEN 'cpu'
        ELSE 'metadata' END,s.id,w.kind_parts_json,
        w.payload_schema_version,w.priority,max(w.eligible_at_utc_ns,coalesce(s.eligible_at_utc_ns,0)),
        max(w.created_at_utc_ns,w.eligible_at_utc_ns,
            CASE WHEN EXISTS(SELECT 1 FROM stage_attempts a WHERE a.operation_id=s.operation_id AND a.state='pending')
                 THEN 0 ELSE coalesce(s.created_at_utc_ns,0) END,
            coalesce(json_extract(s.definition_json,'$.not_before_utc_ns'),0)),w.created_at_utc_ns,
        CASE WHEN w.state IN ('pending','leased') AND
            ((w.state='pending' AND s.id IS NULL AND NOT EXISTS(
                SELECT 1 FROM stage_attempts a WHERE a.work_item_id=w.id AND a.state IN ('pending','active')))
             OR (s.state='pending' AND s.progress_waiting=0)) THEN 1 ELSE 0 END,w.eligible_at_utc_ns,
        CASE WHEN w.state IN ('leased','cancelling') AND s.id IS NULL AND NOT EXISTS(
            SELECT 1 FROM stage_attempts a WHERE a.work_item_id=w.id AND a.state IN ('pending','active'))
            THEN w.lease_expires_at_utc_ns END
    FROM work_items w LEFT JOIN work_scheduling owner ON owner.work_item_id=w.id
    LEFT JOIN work_stages s ON s.work_item_id=w.id AND s.state IN ('pending','running')
    WHERE w.id=NEW.work_item_id
    ON CONFLICT(work_item_id) DO UPDATE SET
        root_request_identifier=excluded.root_request_identifier,
        workspace_key=excluded.workspace_key,service_class=excluded.service_class,
        lane=excluded.lane,stage_id=excluded.stage_id,kind_parts_json=excluded.kind_parts_json,
        payload_schema_version=excluded.payload_schema_version,priority=excluded.priority,
        available_at_utc_ns=excluded.available_at_utc_ns,
        work_eligible_at_utc_ns=excluded.work_eligible_at_utc_ns,
        legacy_lease_expires_at_utc_ns=excluded.legacy_lease_expires_at_utc_ns,
        ready_since_utc_ns=CASE WHEN fair_ready.stage_id IS excluded.stage_id
            THEN fair_ready.ready_since_utc_ns ELSE excluded.ready_since_utc_ns END,
        active=excluded.active;
     END;
CREATE TRIGGER IF NOT EXISTS fair_refresh_stage_update AFTER UPDATE ON work_stages BEGIN 
    INSERT INTO fair_ready(work_item_id,root_request_identifier,workspace_key,service_class,
        lane,stage_id,kind_parts_json,payload_schema_version,priority,available_at_utc_ns,
        ready_since_utc_ns,created_at_utc_ns,active,work_eligible_at_utc_ns,
        legacy_lease_expires_at_utc_ns)
    SELECT w.id,coalesce(owner.root_request_identifier,'["legacy"]'),
        coalesce(owner.workspace_key,'["legacy"]'),
        coalesce(owner.service_class,'background'),CASE WHEN s.id IS NULL THEN
        coalesce((SELECT CASE WHEN initial.resident=1 THEN 'resident' ELSE initial.lane END
            FROM fair_initial_resources initial WHERE initial.kind_parts_json=w.kind_parts_json
                AND initial.payload_schema_version=w.payload_schema_version),'cpu')
        WHEN json_extract(s.definition_json,'$.resident_session_identifier') IS NOT NULL
             AND NOT EXISTS(SELECT 1 FROM resident_sessions resident
                WHERE resident.id=json_extract(s.definition_json,'$.resident_session_identifier')
                  AND resident.state IN ('opening','ready','closing','quarantined')) THEN 'resident'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='analysis') THEN 'analysis'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='http') THEN 'http'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='cpu') THEN 'cpu'
        ELSE 'metadata' END,s.id,w.kind_parts_json,
        w.payload_schema_version,w.priority,max(w.eligible_at_utc_ns,coalesce(s.eligible_at_utc_ns,0)),
        max(w.created_at_utc_ns,w.eligible_at_utc_ns,
            CASE WHEN EXISTS(SELECT 1 FROM stage_attempts a WHERE a.operation_id=s.operation_id AND a.state='pending')
                 THEN 0 ELSE coalesce(s.created_at_utc_ns,0) END,
            coalesce(json_extract(s.definition_json,'$.not_before_utc_ns'),0)),w.created_at_utc_ns,
        CASE WHEN w.state IN ('pending','leased') AND
            ((w.state='pending' AND s.id IS NULL AND NOT EXISTS(
                SELECT 1 FROM stage_attempts a WHERE a.work_item_id=w.id AND a.state IN ('pending','active')))
             OR (s.state='pending' AND s.progress_waiting=0)) THEN 1 ELSE 0 END,w.eligible_at_utc_ns,
        CASE WHEN w.state IN ('leased','cancelling') AND s.id IS NULL AND NOT EXISTS(
            SELECT 1 FROM stage_attempts a WHERE a.work_item_id=w.id AND a.state IN ('pending','active'))
            THEN w.lease_expires_at_utc_ns END
    FROM work_items w LEFT JOIN work_scheduling owner ON owner.work_item_id=w.id
    LEFT JOIN work_stages s ON s.work_item_id=w.id AND s.state IN ('pending','running')
    WHERE w.id=NEW.work_item_id
    ON CONFLICT(work_item_id) DO UPDATE SET
        root_request_identifier=excluded.root_request_identifier,
        workspace_key=excluded.workspace_key,service_class=excluded.service_class,
        lane=excluded.lane,stage_id=excluded.stage_id,kind_parts_json=excluded.kind_parts_json,
        payload_schema_version=excluded.payload_schema_version,priority=excluded.priority,
        available_at_utc_ns=excluded.available_at_utc_ns,
        work_eligible_at_utc_ns=excluded.work_eligible_at_utc_ns,
        legacy_lease_expires_at_utc_ns=excluded.legacy_lease_expires_at_utc_ns,
        ready_since_utc_ns=CASE WHEN fair_ready.stage_id IS excluded.stage_id
            THEN fair_ready.ready_since_utc_ns ELSE excluded.ready_since_utc_ns END,
        active=excluded.active;
     END;
CREATE TRIGGER IF NOT EXISTS fair_refresh_attempt_insert AFTER INSERT ON stage_attempts BEGIN 
    INSERT INTO fair_ready(work_item_id,root_request_identifier,workspace_key,service_class,
        lane,stage_id,kind_parts_json,payload_schema_version,priority,available_at_utc_ns,
        ready_since_utc_ns,created_at_utc_ns,active,work_eligible_at_utc_ns,
        legacy_lease_expires_at_utc_ns)
    SELECT w.id,coalesce(owner.root_request_identifier,'["legacy"]'),
        coalesce(owner.workspace_key,'["legacy"]'),
        coalesce(owner.service_class,'background'),CASE WHEN s.id IS NULL THEN
        coalesce((SELECT CASE WHEN initial.resident=1 THEN 'resident' ELSE initial.lane END
            FROM fair_initial_resources initial WHERE initial.kind_parts_json=w.kind_parts_json
                AND initial.payload_schema_version=w.payload_schema_version),'cpu')
        WHEN json_extract(s.definition_json,'$.resident_session_identifier') IS NOT NULL
             AND NOT EXISTS(SELECT 1 FROM resident_sessions resident
                WHERE resident.id=json_extract(s.definition_json,'$.resident_session_identifier')
                  AND resident.state IN ('opening','ready','closing','quarantined')) THEN 'resident'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='analysis') THEN 'analysis'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='http') THEN 'http'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='cpu') THEN 'cpu'
        ELSE 'metadata' END,s.id,w.kind_parts_json,
        w.payload_schema_version,w.priority,max(w.eligible_at_utc_ns,coalesce(s.eligible_at_utc_ns,0)),
        max(w.created_at_utc_ns,w.eligible_at_utc_ns,
            CASE WHEN EXISTS(SELECT 1 FROM stage_attempts a WHERE a.operation_id=s.operation_id AND a.state='pending')
                 THEN 0 ELSE coalesce(s.created_at_utc_ns,0) END,
            coalesce(json_extract(s.definition_json,'$.not_before_utc_ns'),0)),w.created_at_utc_ns,
        CASE WHEN w.state IN ('pending','leased') AND
            ((w.state='pending' AND s.id IS NULL AND NOT EXISTS(
                SELECT 1 FROM stage_attempts a WHERE a.work_item_id=w.id AND a.state IN ('pending','active')))
             OR (s.state='pending' AND s.progress_waiting=0)) THEN 1 ELSE 0 END,w.eligible_at_utc_ns,
        CASE WHEN w.state IN ('leased','cancelling') AND s.id IS NULL AND NOT EXISTS(
            SELECT 1 FROM stage_attempts a WHERE a.work_item_id=w.id AND a.state IN ('pending','active'))
            THEN w.lease_expires_at_utc_ns END
    FROM work_items w LEFT JOIN work_scheduling owner ON owner.work_item_id=w.id
    LEFT JOIN work_stages s ON s.work_item_id=w.id AND s.state IN ('pending','running')
    WHERE w.id=NEW.work_item_id
    ON CONFLICT(work_item_id) DO UPDATE SET
        root_request_identifier=excluded.root_request_identifier,
        workspace_key=excluded.workspace_key,service_class=excluded.service_class,
        lane=excluded.lane,stage_id=excluded.stage_id,kind_parts_json=excluded.kind_parts_json,
        payload_schema_version=excluded.payload_schema_version,priority=excluded.priority,
        available_at_utc_ns=excluded.available_at_utc_ns,
        work_eligible_at_utc_ns=excluded.work_eligible_at_utc_ns,
        legacy_lease_expires_at_utc_ns=excluded.legacy_lease_expires_at_utc_ns,
        ready_since_utc_ns=CASE WHEN fair_ready.stage_id IS excluded.stage_id
            THEN fair_ready.ready_since_utc_ns ELSE excluded.ready_since_utc_ns END,
        active=excluded.active;
     END;
CREATE TRIGGER IF NOT EXISTS fair_refresh_attempt_update AFTER UPDATE ON stage_attempts BEGIN 
    INSERT INTO fair_ready(work_item_id,root_request_identifier,workspace_key,service_class,
        lane,stage_id,kind_parts_json,payload_schema_version,priority,available_at_utc_ns,
        ready_since_utc_ns,created_at_utc_ns,active,work_eligible_at_utc_ns,
        legacy_lease_expires_at_utc_ns)
    SELECT w.id,coalesce(owner.root_request_identifier,'["legacy"]'),
        coalesce(owner.workspace_key,'["legacy"]'),
        coalesce(owner.service_class,'background'),CASE WHEN s.id IS NULL THEN
        coalesce((SELECT CASE WHEN initial.resident=1 THEN 'resident' ELSE initial.lane END
            FROM fair_initial_resources initial WHERE initial.kind_parts_json=w.kind_parts_json
                AND initial.payload_schema_version=w.payload_schema_version),'cpu')
        WHEN json_extract(s.definition_json,'$.resident_session_identifier') IS NOT NULL
             AND NOT EXISTS(SELECT 1 FROM resident_sessions resident
                WHERE resident.id=json_extract(s.definition_json,'$.resident_session_identifier')
                  AND resident.state IN ('opening','ready','closing','quarantined')) THEN 'resident'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='analysis') THEN 'analysis'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='http') THEN 'http'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='cpu') THEN 'cpu'
        ELSE 'metadata' END,s.id,w.kind_parts_json,
        w.payload_schema_version,w.priority,max(w.eligible_at_utc_ns,coalesce(s.eligible_at_utc_ns,0)),
        max(w.created_at_utc_ns,w.eligible_at_utc_ns,
            CASE WHEN EXISTS(SELECT 1 FROM stage_attempts a WHERE a.operation_id=s.operation_id AND a.state='pending')
                 THEN 0 ELSE coalesce(s.created_at_utc_ns,0) END,
            coalesce(json_extract(s.definition_json,'$.not_before_utc_ns'),0)),w.created_at_utc_ns,
        CASE WHEN w.state IN ('pending','leased') AND
            ((w.state='pending' AND s.id IS NULL AND NOT EXISTS(
                SELECT 1 FROM stage_attempts a WHERE a.work_item_id=w.id AND a.state IN ('pending','active')))
             OR (s.state='pending' AND s.progress_waiting=0)) THEN 1 ELSE 0 END,w.eligible_at_utc_ns,
        CASE WHEN w.state IN ('leased','cancelling') AND s.id IS NULL AND NOT EXISTS(
            SELECT 1 FROM stage_attempts a WHERE a.work_item_id=w.id AND a.state IN ('pending','active'))
            THEN w.lease_expires_at_utc_ns END
    FROM work_items w LEFT JOIN work_scheduling owner ON owner.work_item_id=w.id
    LEFT JOIN work_stages s ON s.work_item_id=w.id AND s.state IN ('pending','running')
    WHERE w.id=NEW.work_item_id
    ON CONFLICT(work_item_id) DO UPDATE SET
        root_request_identifier=excluded.root_request_identifier,
        workspace_key=excluded.workspace_key,service_class=excluded.service_class,
        lane=excluded.lane,stage_id=excluded.stage_id,kind_parts_json=excluded.kind_parts_json,
        payload_schema_version=excluded.payload_schema_version,priority=excluded.priority,
        available_at_utc_ns=excluded.available_at_utc_ns,
        work_eligible_at_utc_ns=excluded.work_eligible_at_utc_ns,
        legacy_lease_expires_at_utc_ns=excluded.legacy_lease_expires_at_utc_ns,
        ready_since_utc_ns=CASE WHEN fair_ready.stage_id IS excluded.stage_id
            THEN fair_ready.ready_since_utc_ns ELSE excluded.ready_since_utc_ns END,
        active=excluded.active;
     END;
CREATE TRIGGER IF NOT EXISTS fair_refresh_owner_insert AFTER INSERT ON work_scheduling BEGIN 
    INSERT INTO fair_ready(work_item_id,root_request_identifier,workspace_key,service_class,
        lane,stage_id,kind_parts_json,payload_schema_version,priority,available_at_utc_ns,
        ready_since_utc_ns,created_at_utc_ns,active,work_eligible_at_utc_ns,
        legacy_lease_expires_at_utc_ns)
    SELECT w.id,coalesce(owner.root_request_identifier,'["legacy"]'),
        coalesce(owner.workspace_key,'["legacy"]'),
        coalesce(owner.service_class,'background'),CASE WHEN s.id IS NULL THEN
        coalesce((SELECT CASE WHEN initial.resident=1 THEN 'resident' ELSE initial.lane END
            FROM fair_initial_resources initial WHERE initial.kind_parts_json=w.kind_parts_json
                AND initial.payload_schema_version=w.payload_schema_version),'cpu')
        WHEN json_extract(s.definition_json,'$.resident_session_identifier') IS NOT NULL
             AND NOT EXISTS(SELECT 1 FROM resident_sessions resident
                WHERE resident.id=json_extract(s.definition_json,'$.resident_session_identifier')
                  AND resident.state IN ('opening','ready','closing','quarantined')) THEN 'resident'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='analysis') THEN 'analysis'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='http') THEN 'http'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='cpu') THEN 'cpu'
        ELSE 'metadata' END,s.id,w.kind_parts_json,
        w.payload_schema_version,w.priority,max(w.eligible_at_utc_ns,coalesce(s.eligible_at_utc_ns,0)),
        max(w.created_at_utc_ns,w.eligible_at_utc_ns,
            CASE WHEN EXISTS(SELECT 1 FROM stage_attempts a WHERE a.operation_id=s.operation_id AND a.state='pending')
                 THEN 0 ELSE coalesce(s.created_at_utc_ns,0) END,
            coalesce(json_extract(s.definition_json,'$.not_before_utc_ns'),0)),w.created_at_utc_ns,
        CASE WHEN w.state IN ('pending','leased') AND
            ((w.state='pending' AND s.id IS NULL AND NOT EXISTS(
                SELECT 1 FROM stage_attempts a WHERE a.work_item_id=w.id AND a.state IN ('pending','active')))
             OR (s.state='pending' AND s.progress_waiting=0)) THEN 1 ELSE 0 END,w.eligible_at_utc_ns,
        CASE WHEN w.state IN ('leased','cancelling') AND s.id IS NULL AND NOT EXISTS(
            SELECT 1 FROM stage_attempts a WHERE a.work_item_id=w.id AND a.state IN ('pending','active'))
            THEN w.lease_expires_at_utc_ns END
    FROM work_items w LEFT JOIN work_scheduling owner ON owner.work_item_id=w.id
    LEFT JOIN work_stages s ON s.work_item_id=w.id AND s.state IN ('pending','running')
    WHERE w.id=NEW.work_item_id
    ON CONFLICT(work_item_id) DO UPDATE SET
        root_request_identifier=excluded.root_request_identifier,
        workspace_key=excluded.workspace_key,service_class=excluded.service_class,
        lane=excluded.lane,stage_id=excluded.stage_id,kind_parts_json=excluded.kind_parts_json,
        payload_schema_version=excluded.payload_schema_version,priority=excluded.priority,
        available_at_utc_ns=excluded.available_at_utc_ns,
        work_eligible_at_utc_ns=excluded.work_eligible_at_utc_ns,
        legacy_lease_expires_at_utc_ns=excluded.legacy_lease_expires_at_utc_ns,
        ready_since_utc_ns=CASE WHEN fair_ready.stage_id IS excluded.stage_id
            THEN fair_ready.ready_since_utc_ns ELSE excluded.ready_since_utc_ns END,
        active=excluded.active;
     END;
CREATE TRIGGER IF NOT EXISTS fair_refresh_resident_insert AFTER INSERT ON resident_sessions BEGIN 
    INSERT INTO fair_ready(work_item_id,root_request_identifier,workspace_key,service_class,
        lane,stage_id,kind_parts_json,payload_schema_version,priority,available_at_utc_ns,
        ready_since_utc_ns,created_at_utc_ns,active,work_eligible_at_utc_ns,
        legacy_lease_expires_at_utc_ns)
    SELECT w.id,coalesce(owner.root_request_identifier,'["legacy"]'),
        coalesce(owner.workspace_key,'["legacy"]'),
        coalesce(owner.service_class,'background'),CASE WHEN s.id IS NULL THEN
        coalesce((SELECT CASE WHEN initial.resident=1 THEN 'resident' ELSE initial.lane END
            FROM fair_initial_resources initial WHERE initial.kind_parts_json=w.kind_parts_json
                AND initial.payload_schema_version=w.payload_schema_version),'cpu')
        WHEN json_extract(s.definition_json,'$.resident_session_identifier') IS NOT NULL
             AND NOT EXISTS(SELECT 1 FROM resident_sessions resident
                WHERE resident.id=json_extract(s.definition_json,'$.resident_session_identifier')
                  AND resident.state IN ('opening','ready','closing','quarantined')) THEN 'resident'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='analysis') THEN 'analysis'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='http') THEN 'http'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='cpu') THEN 'cpu'
        ELSE 'metadata' END,s.id,w.kind_parts_json,
        w.payload_schema_version,w.priority,max(w.eligible_at_utc_ns,coalesce(s.eligible_at_utc_ns,0)),
        max(w.created_at_utc_ns,w.eligible_at_utc_ns,
            CASE WHEN EXISTS(SELECT 1 FROM stage_attempts a WHERE a.operation_id=s.operation_id AND a.state='pending')
                 THEN 0 ELSE coalesce(s.created_at_utc_ns,0) END,
            coalesce(json_extract(s.definition_json,'$.not_before_utc_ns'),0)),w.created_at_utc_ns,
        CASE WHEN w.state IN ('pending','leased') AND
            ((w.state='pending' AND s.id IS NULL AND NOT EXISTS(
                SELECT 1 FROM stage_attempts a WHERE a.work_item_id=w.id AND a.state IN ('pending','active')))
             OR (s.state='pending' AND s.progress_waiting=0)) THEN 1 ELSE 0 END,w.eligible_at_utc_ns,
        CASE WHEN w.state IN ('leased','cancelling') AND s.id IS NULL AND NOT EXISTS(
            SELECT 1 FROM stage_attempts a WHERE a.work_item_id=w.id AND a.state IN ('pending','active'))
            THEN w.lease_expires_at_utc_ns END
    FROM work_items w LEFT JOIN work_scheduling owner ON owner.work_item_id=w.id
    LEFT JOIN work_stages s ON s.work_item_id=w.id AND s.state IN ('pending','running')
    WHERE w.id=NEW.work_item_id
    ON CONFLICT(work_item_id) DO UPDATE SET
        root_request_identifier=excluded.root_request_identifier,
        workspace_key=excluded.workspace_key,service_class=excluded.service_class,
        lane=excluded.lane,stage_id=excluded.stage_id,kind_parts_json=excluded.kind_parts_json,
        payload_schema_version=excluded.payload_schema_version,priority=excluded.priority,
        available_at_utc_ns=excluded.available_at_utc_ns,
        work_eligible_at_utc_ns=excluded.work_eligible_at_utc_ns,
        legacy_lease_expires_at_utc_ns=excluded.legacy_lease_expires_at_utc_ns,
        ready_since_utc_ns=CASE WHEN fair_ready.stage_id IS excluded.stage_id
            THEN fair_ready.ready_since_utc_ns ELSE excluded.ready_since_utc_ns END,
        active=excluded.active;
     END;
CREATE TRIGGER IF NOT EXISTS fair_refresh_resident_update AFTER UPDATE ON resident_sessions BEGIN 
    INSERT INTO fair_ready(work_item_id,root_request_identifier,workspace_key,service_class,
        lane,stage_id,kind_parts_json,payload_schema_version,priority,available_at_utc_ns,
        ready_since_utc_ns,created_at_utc_ns,active,work_eligible_at_utc_ns,
        legacy_lease_expires_at_utc_ns)
    SELECT w.id,coalesce(owner.root_request_identifier,'["legacy"]'),
        coalesce(owner.workspace_key,'["legacy"]'),
        coalesce(owner.service_class,'background'),CASE WHEN s.id IS NULL THEN
        coalesce((SELECT CASE WHEN initial.resident=1 THEN 'resident' ELSE initial.lane END
            FROM fair_initial_resources initial WHERE initial.kind_parts_json=w.kind_parts_json
                AND initial.payload_schema_version=w.payload_schema_version),'cpu')
        WHEN json_extract(s.definition_json,'$.resident_session_identifier') IS NOT NULL
             AND NOT EXISTS(SELECT 1 FROM resident_sessions resident
                WHERE resident.id=json_extract(s.definition_json,'$.resident_session_identifier')
                  AND resident.state IN ('opening','ready','closing','quarantined')) THEN 'resident'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='analysis') THEN 'analysis'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='http') THEN 'http'
        WHEN EXISTS(SELECT 1 FROM json_each(s.definition_json,'$.resources') WHERE json_extract(value,'$.name')='cpu') THEN 'cpu'
        ELSE 'metadata' END,s.id,w.kind_parts_json,
        w.payload_schema_version,w.priority,max(w.eligible_at_utc_ns,coalesce(s.eligible_at_utc_ns,0)),
        max(w.created_at_utc_ns,w.eligible_at_utc_ns,
            CASE WHEN EXISTS(SELECT 1 FROM stage_attempts a WHERE a.operation_id=s.operation_id AND a.state='pending')
                 THEN 0 ELSE coalesce(s.created_at_utc_ns,0) END,
            coalesce(json_extract(s.definition_json,'$.not_before_utc_ns'),0)),w.created_at_utc_ns,
        CASE WHEN w.state IN ('pending','leased') AND
            ((w.state='pending' AND s.id IS NULL AND NOT EXISTS(
                SELECT 1 FROM stage_attempts a WHERE a.work_item_id=w.id AND a.state IN ('pending','active')))
             OR (s.state='pending' AND s.progress_waiting=0)) THEN 1 ELSE 0 END,w.eligible_at_utc_ns,
        CASE WHEN w.state IN ('leased','cancelling') AND s.id IS NULL AND NOT EXISTS(
            SELECT 1 FROM stage_attempts a WHERE a.work_item_id=w.id AND a.state IN ('pending','active'))
            THEN w.lease_expires_at_utc_ns END
    FROM work_items w LEFT JOIN work_scheduling owner ON owner.work_item_id=w.id
    LEFT JOIN work_stages s ON s.work_item_id=w.id AND s.state IN ('pending','running')
    WHERE w.id=NEW.work_item_id
    ON CONFLICT(work_item_id) DO UPDATE SET
        root_request_identifier=excluded.root_request_identifier,
        workspace_key=excluded.workspace_key,service_class=excluded.service_class,
        lane=excluded.lane,stage_id=excluded.stage_id,kind_parts_json=excluded.kind_parts_json,
        payload_schema_version=excluded.payload_schema_version,priority=excluded.priority,
        available_at_utc_ns=excluded.available_at_utc_ns,
        work_eligible_at_utc_ns=excluded.work_eligible_at_utc_ns,
        legacy_lease_expires_at_utc_ns=excluded.legacy_lease_expires_at_utc_ns,
        ready_since_utc_ns=CASE WHEN fair_ready.stage_id IS excluded.stage_id
            THEN fair_ready.ready_since_utc_ns ELSE excluded.ready_since_utc_ns END,
        active=excluded.active;
     END;

CREATE TRIGGER IF NOT EXISTS fair_ready_count_insert AFTER INSERT ON fair_ready
WHEN NEW.active=1 BEGIN
    UPDATE fair_workspace_service SET ready_count=ready_count+1
    WHERE workspace_key=NEW.workspace_key AND service_class=NEW.service_class AND lane=NEW.lane;
    UPDATE fair_root_service SET ready_count=ready_count+1
    WHERE root_request_identifier=NEW.root_request_identifier AND lane=NEW.lane;
END;
CREATE TRIGGER IF NOT EXISTS fair_ready_count_delete AFTER DELETE ON fair_ready
WHEN OLD.active=1 BEGIN
    UPDATE fair_workspace_service SET ready_count=ready_count-1
    WHERE workspace_key=OLD.workspace_key AND service_class=OLD.service_class AND lane=OLD.lane;
    UPDATE fair_root_service SET ready_count=ready_count-1
    WHERE root_request_identifier=OLD.root_request_identifier AND lane=OLD.lane;
END;
CREATE TRIGGER IF NOT EXISTS fair_ready_count_update AFTER UPDATE ON fair_ready
WHEN (OLD.active,OLD.workspace_key,OLD.service_class,OLD.root_request_identifier,OLD.lane)
    IS NOT (NEW.active,NEW.workspace_key,NEW.service_class,NEW.root_request_identifier,NEW.lane)
BEGIN
    UPDATE fair_workspace_service SET ready_count=ready_count-1
    WHERE OLD.active=1 AND workspace_key=OLD.workspace_key AND service_class=OLD.service_class AND lane=OLD.lane;
    UPDATE fair_root_service SET ready_count=ready_count-1
    WHERE OLD.active=1 AND root_request_identifier=OLD.root_request_identifier AND lane=OLD.lane;
    UPDATE fair_workspace_service SET ready_count=ready_count+1
    WHERE NEW.active=1 AND workspace_key=NEW.workspace_key AND service_class=NEW.service_class AND lane=NEW.lane;
    UPDATE fair_root_service SET ready_count=ready_count+1
    WHERE NEW.active=1 AND root_request_identifier=NEW.root_request_identifier AND lane=NEW.lane;
END;
CREATE TRIGGER IF NOT EXISTS fair_unowned_work AFTER INSERT ON work_items BEGIN
    INSERT OR IGNORE INTO fair_unowned VALUES (NEW.id);
END;
CREATE TRIGGER IF NOT EXISTS fair_owned_work AFTER INSERT ON work_scheduling BEGIN
    DELETE FROM fair_unowned WHERE work_item_id=NEW.work_item_id;
END;

CREATE TABLE IF NOT EXISTS work_progress_clock(singleton INTEGER PRIMARY KEY CHECK(singleton=1),revision INTEGER NOT NULL) STRICT;
INSERT OR IGNORE INTO work_progress_clock VALUES(1,0);
CREATE TABLE IF NOT EXISTS progress_notification_clock(singleton INTEGER PRIMARY KEY CHECK(singleton=1),ticket INTEGER NOT NULL) STRICT;
INSERT OR IGNORE INTO progress_notification_clock VALUES(1,0);
CREATE TABLE IF NOT EXISTS work_progress_revisions(work_id TEXT PRIMARY KEY REFERENCES work_items(id),revision INTEGER NOT NULL) STRICT;
CREATE TABLE IF NOT EXISTS work_consumer_edges(
    parent_work_id TEXT NOT NULL REFERENCES work_items(id),kind_json TEXT NOT NULL CHECK(json_valid(kind_json)),
    child_work_id TEXT NOT NULL REFERENCES work_items(id),PRIMARY KEY(parent_work_id,kind_json,child_work_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS work_consumer_edges_child ON work_consumer_edges(child_work_id,parent_work_id,kind_json);
CREATE TABLE IF NOT EXISTS consumer_group_progress(
    parent_work_id TEXT NOT NULL REFERENCES work_items(id),kind_json TEXT NOT NULL,revision INTEGER NOT NULL,
    PRIMARY KEY(parent_work_id,kind_json)
) STRICT, WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS progress_wait_builds(
    id TEXT PRIMARY KEY,work_id TEXT NOT NULL REFERENCES work_items(id),operation_id TEXT NOT NULL,
    source_stage_id TEXT NOT NULL REFERENCES work_stages(id),source_token TEXT NOT NULL,
    continuation_id TEXT NOT NULL,stage_id TEXT UNIQUE REFERENCES work_stages(id),
    baseline INTEGER NOT NULL,digest TEXT NOT NULL,groups_json TEXT NOT NULL CHECK(json_valid(groups_json)),
    state TEXT NOT NULL CHECK(state IN ('source','prepared','building','armed','inactive')),
    chunk_count INTEGER NOT NULL DEFAULT 0,next_chunk INTEGER NOT NULL DEFAULT 0,chunk_offset INTEGER NOT NULL DEFAULT 0,
    max_revision INTEGER NOT NULL DEFAULT 0,reconcile_after TEXT NOT NULL DEFAULT '',ticket INTEGER NOT NULL DEFAULT 0
) STRICT;
CREATE INDEX IF NOT EXISTS progress_wait_builds_ready ON progress_wait_builds(state,ticket,id);
CREATE INDEX IF NOT EXISTS progress_wait_builds_source ON progress_wait_builds(source_stage_id,source_token);
CREATE TABLE IF NOT EXISTS progress_wait_source_chunks(
    build_id TEXT NOT NULL REFERENCES progress_wait_builds(id),ordinal INTEGER NOT NULL,
    identifiers_json TEXT NOT NULL CHECK(json_valid(identifiers_json)),PRIMARY KEY(build_id,ordinal)
) STRICT, WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS progress_wait_members(
    build_id TEXT NOT NULL REFERENCES progress_wait_builds(id),child_work_id TEXT NOT NULL REFERENCES work_items(id),
    PRIMARY KEY(build_id,child_work_id)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS progress_wait_members_child ON progress_wait_members(child_work_id,build_id);
CREATE TABLE IF NOT EXISTS progress_wait_groups(
    build_id TEXT NOT NULL REFERENCES progress_wait_builds(id),parent_work_id TEXT NOT NULL REFERENCES work_items(id),
    kind_json TEXT NOT NULL,PRIMARY KEY(build_id,kind_json)
) STRICT, WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS progress_wait_groups_parent ON progress_wait_groups(parent_work_id,kind_json,build_id);
CREATE TABLE IF NOT EXISTS progress_notifications(
    child_work_id TEXT PRIMARY KEY REFERENCES work_items(id),latest_revision INTEGER NOT NULL,
    pass_revision INTEGER NOT NULL DEFAULT 0,phase INTEGER NOT NULL DEFAULT 0,
    after_parent TEXT NOT NULL DEFAULT '',after_kind TEXT NOT NULL DEFAULT '',after_build TEXT NOT NULL DEFAULT '',
    ticket INTEGER NOT NULL
) STRICT;
CREATE INDEX IF NOT EXISTS progress_notifications_turn ON progress_notifications(ticket,child_work_id);
CREATE INDEX IF NOT EXISTS work_stages_progress_ready ON work_stages(state,eligible_at_utc_ns,created_at_utc_ns,id)
    WHERE progress_waiting=0;
CREATE INDEX IF NOT EXISTS work_stages_progress_sleep ON work_stages(id) WHERE state='pending' AND progress_waiting=1;
CREATE TRIGGER IF NOT EXISTS progress_work_changed AFTER UPDATE ON work_items
WHEN NEW.result_json IS NOT OLD.result_json OR NEW.error_json IS NOT OLD.error_json
    OR NEW.eligible_at_utc_ns!=OLD.eligible_at_utc_ns
    OR (NEW.state!=OLD.state AND (NEW.state IN ('completed','terminal_failure','cancelled','cancelling')
        OR (NEW.state='pending' AND OLD.state IN ('leased','completed','terminal_failure','cancelled','cancelling'))))
BEGIN

UPDATE work_progress_clock SET revision=revision+1 WHERE singleton=1;
INSERT INTO work_progress_revisions(work_id,revision)
VALUES (NEW.id,(SELECT revision FROM work_progress_clock WHERE singleton=1))
ON CONFLICT(work_id) DO UPDATE SET revision=excluded.revision;
UPDATE progress_notification_clock SET ticket=ticket+1 WHERE singleton=1
    AND NOT EXISTS(SELECT 1 FROM progress_notifications WHERE child_work_id=NEW.id);
INSERT INTO progress_notifications(child_work_id,latest_revision,ticket)
VALUES (NEW.id,(SELECT revision FROM work_progress_clock WHERE singleton=1),
    (SELECT ticket FROM progress_notification_clock WHERE singleton=1))
ON CONFLICT(child_work_id) DO UPDATE SET latest_revision=excluded.latest_revision;

END;
CREATE TRIGGER IF NOT EXISTS progress_stage_inactive AFTER UPDATE OF state ON work_stages
WHEN NEW.state!='pending'
BEGIN
    UPDATE progress_wait_builds SET state='inactive' WHERE stage_id=NEW.id AND state IN ('building','armed');
END;

CREATE TABLE IF NOT EXISTS scheduler_meta(key TEXT PRIMARY KEY,value INTEGER NOT NULL) STRICT;
INSERT OR IGNORE INTO scheduler_meta VALUES('initialized',0),('started',0),('rollup_rows',0),('sample_rows',0),('retained_since',0),('dropped_samples',0),('active_loss_at',0);
CREATE TABLE IF NOT EXISTS scheduler_gauges(subject TEXT NOT NULL,state TEXT NOT NULL,value INTEGER NOT NULL,PRIMARY KEY(subject,state)) STRICT,WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS scheduler_waits(
    scope TEXT NOT NULL,work_id TEXT NOT NULL REFERENCES work_items(id),stage_id TEXT,
    reason TEXT NOT NULL,resources_json TEXT NOT NULL,source TEXT NOT NULL,
    started_ns INTEGER NOT NULL,last_seen_ns INTEGER NOT NULL,expires_ns INTEGER NOT NULL,
    PRIMARY KEY(scope,work_id)
) STRICT,WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS scheduler_waits_expiry ON scheduler_waits(expires_ns,scope,work_id);
CREATE INDEX IF NOT EXISTS scheduler_waits_scope ON scheduler_waits(scope,started_ns,work_id);
CREATE INDEX IF NOT EXISTS scheduler_waits_work ON scheduler_waits(work_id,scope);
CREATE TABLE IF NOT EXISTS scheduler_executions(
    stage_id TEXT NOT NULL,execution INTEGER NOT NULL,scope TEXT NOT NULL,work_id TEXT NOT NULL,
    started_ns INTEGER NOT NULL,ended_ns INTEGER,outcome TEXT,
    PRIMARY KEY(stage_id,execution)
) STRICT,WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS scheduler_executions_time ON scheduler_executions(started_ns,stage_id,execution);
CREATE INDEX IF NOT EXISTS scheduler_executions_finished ON scheduler_executions(ended_ns,stage_id,execution) WHERE ended_ns IS NOT NULL;
CREATE TABLE IF NOT EXISTS scheduler_rollups(
    scope TEXT NOT NULL,minute_ns INTEGER NOT NULL,metric TEXT NOT NULL,method TEXT NOT NULL,bucket INTEGER NOT NULL,
    count INTEGER NOT NULL,total_ns INTEGER NOT NULL,maximum_ns INTEGER NOT NULL,censored INTEGER NOT NULL,
    PRIMARY KEY(scope,minute_ns,metric,method,bucket)
) STRICT,WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS scheduler_rollups_time ON scheduler_rollups(minute_ns,scope,metric,method,bucket);
CREATE TABLE IF NOT EXISTS scheduler_cooldowns(work_id TEXT PRIMARY KEY REFERENCES work_items(id),reason TEXT NOT NULL,until_ns INTEGER NOT NULL) STRICT;
CREATE INDEX IF NOT EXISTS scheduler_cooldowns_expiry ON scheduler_cooldowns(until_ns,work_id);
CREATE TRIGGER IF NOT EXISTS scheduler_rollup_insert AFTER INSERT ON scheduler_rollups BEGIN
    UPDATE scheduler_meta SET value=value+1 WHERE key='rollup_rows'; END;
CREATE TRIGGER IF NOT EXISTS scheduler_rollup_delete AFTER DELETE ON scheduler_rollups BEGIN
    UPDATE scheduler_meta SET value=value-1 WHERE key='rollup_rows'; END;
CREATE TRIGGER IF NOT EXISTS scheduler_sample_insert AFTER INSERT ON scheduler_executions BEGIN
    UPDATE scheduler_meta SET value=value+1 WHERE key='sample_rows'; END;
CREATE TRIGGER IF NOT EXISTS scheduler_sample_delete AFTER DELETE ON scheduler_executions BEGIN
    UPDATE scheduler_meta SET value=value-1 WHERE key='sample_rows'; END;
INSERT OR IGNORE INTO scheduler_gauges VALUES('stage','progress_waiting',0);
CREATE TRIGGER IF NOT EXISTS scheduler_progress_insert AFTER INSERT ON work_stages WHEN NEW.state='pending' AND NEW.progress_waiting=1 BEGIN
    UPDATE scheduler_gauges SET value=value+1 WHERE subject='stage' AND state='progress_waiting'; END;
CREATE TRIGGER IF NOT EXISTS scheduler_progress_state AFTER UPDATE OF state,progress_waiting ON work_stages BEGIN
    UPDATE scheduler_gauges SET value=value-(OLD.state='pending' AND OLD.progress_waiting=1)+(NEW.state='pending' AND NEW.progress_waiting=1)
    WHERE subject='stage' AND state='progress_waiting'; END;
CREATE TRIGGER IF NOT EXISTS scheduler_progress_delete AFTER DELETE ON work_stages WHEN OLD.state='pending' AND OLD.progress_waiting=1 BEGIN
    UPDATE scheduler_gauges SET value=value-1 WHERE subject='stage' AND state='progress_waiting'; END;
INSERT OR IGNORE INTO scheduler_gauges VALUES('work','pending',0);
INSERT OR IGNORE INTO scheduler_gauges VALUES('work','leased',0);
INSERT OR IGNORE INTO scheduler_gauges VALUES('work','completed',0);
INSERT OR IGNORE INTO scheduler_gauges VALUES('work','terminal_failure',0);
INSERT OR IGNORE INTO scheduler_gauges VALUES('work','cancelling',0);
INSERT OR IGNORE INTO scheduler_gauges VALUES('work','cancelled',0);

    CREATE TRIGGER IF NOT EXISTS scheduler_work_insert AFTER INSERT ON work_items BEGIN
        UPDATE scheduler_gauges SET value=value+1 WHERE subject='work' AND state=NEW.state;
    END;
    CREATE TRIGGER IF NOT EXISTS scheduler_work_state AFTER UPDATE OF state ON work_items WHEN NEW.state!=OLD.state BEGIN
        UPDATE scheduler_gauges SET value=value-1 WHERE subject='work' AND state=OLD.state;
        UPDATE scheduler_gauges SET value=value+1 WHERE subject='work' AND state=NEW.state;
    END;
    CREATE TRIGGER IF NOT EXISTS scheduler_work_delete AFTER DELETE ON work_items BEGIN
        UPDATE scheduler_gauges SET value=value-1 WHERE subject='work' AND state=OLD.state;
    END;
    INSERT OR IGNORE INTO scheduler_gauges VALUES('stage','pending',0);
INSERT OR IGNORE INTO scheduler_gauges VALUES('stage','running',0);
INSERT OR IGNORE INTO scheduler_gauges VALUES('stage','completed',0);
INSERT OR IGNORE INTO scheduler_gauges VALUES('stage','cancelled',0);

    CREATE TRIGGER IF NOT EXISTS scheduler_stage_insert AFTER INSERT ON work_stages BEGIN
        UPDATE scheduler_gauges SET value=value+1 WHERE subject='stage' AND state=NEW.state;
    END;
    CREATE TRIGGER IF NOT EXISTS scheduler_stage_state AFTER UPDATE OF state ON work_stages WHEN NEW.state!=OLD.state BEGIN
        UPDATE scheduler_gauges SET value=value-1 WHERE subject='stage' AND state=OLD.state;
        UPDATE scheduler_gauges SET value=value+1 WHERE subject='stage' AND state=NEW.state;
    END;
    CREATE TRIGGER IF NOT EXISTS scheduler_stage_delete AFTER DELETE ON work_stages BEGIN
        UPDATE scheduler_gauges SET value=value-1 WHERE subject='stage' AND state=OLD.state;
    END;
    