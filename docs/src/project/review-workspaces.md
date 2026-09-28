# Review workspaces and agent-owned groupings

## Purpose

The composed projection answers “what does Carl currently know about this
listing?” A review workspace adds the durable coordination state needed to
answer “what has this agent examined, what did it decide, and which exact items
does a later operation mean?” Conversation memory is not the authority for any
of those questions.

The first interface is MCP-first and persists ordinary immutable records in the
same provenance graph as collected and derived evidence. It does not introduce
a process-local cache or materialize the composed listing projection itself.

## Objects

### Workspace

A workspace starts from one exact retained search run, an optional exact
product-guide version, and a component-level staleness policy. The initial run
becomes its first search track. Additional complete search intents can be queued
through `create_workspace_search`; the resulting root work ID is the stable
track ID. `request_workspace_refresh` advances one track without replacing the
workspace. Thus the workspace remains the durable scope for review history,
batches, worksets, selection snapshots, and search evolution.
Search acquisition admits at most one active job per proxy route. If a track's
initial search exhausts a transient transport or session failure,
`retry_workspace_search_track` requeues that same durable track with a fresh
retry budget rather than creating another track. Retried legacy work gains the
current per-route scheduler scope. Transport failures invalidate the shared
Proton transport and defer queued searches on that route before replacement.
The stable workspace record can be renamed through an immutable identity-state
record. It can also be archived or restored without deleting any retained
objects. Ordinary workspace listing hides archived workspaces by default;
explicit lookup and `list_review_workspaces(include_archived=true)` retain access
to them.

A workspace can hold several product-guide bindings. Each binding has a
workspace-local alias and is either pinned to an exact immutable guide record or
follows the newest version of one guide identity. One enabled binding may be the
workspace default; a selection-analysis request can instead name another binding or the exact
record of a guide resolved by an enabled binding.
Resolution happens when the request is previewed or queued, and the exact guide
record is retained in the analysis work and result. Revising a guide therefore
changes later `follow_latest` requests without changing the meaning of existing
analysis. Disabling a binding removes it from ordinary use without deleting its
history. Existing single-guide workspaces expose that guide as a legacy pinned
default binding.

Guide creation accepts only the identity suffix; Carl owns the
`["carl", "product_guide"]` namespace prefix. Already-prefixed requests are
normalized to prevent duplicated namespaces. An unused identity can be retired
after all of its workspace bindings are disabled. Retirement hides every
version from ordinary guide listings without deleting exact records and can be
reversed.

The `missing_for_selected_guide` analysis policy reuses a completed analysis under the exact
resolved guide across retained observations of the listing. Its preview separates candidates that
would create new analysis work from reusable analyses and excluded observations, so agents can
verify the scope before queueing.
`set_workspace_search_track_enabled` publishes an immutable state record that
removes a track from or restores it to the active union without deleting its
search or refresh history.

`list_workspace_listings` composes the deduplicated union of every completed
track and its bounded refresh ancestry. An item present in several phrases is
returned once. Both old-only and new-only results remain candidates; absence
from a newer run is recorded as membership history rather than interpreted as
sold or gone. Normalized availability evidence still controls the default
available-only filter. `get_workspace_listing` applies the same active-track
scope and workspace guide when retrieving one exact listing.

A newer usable search-card price supersedes an older item-page price, while a
newer item-page price likewise supersedes an older card price. This applies only
to listings actually observed by a refresh. A missed listing retains its last
observed price, including that evidence's original source and timestamp; search
absence does not make the price current or prove that it is unchanged.

### Projection revision

Every composed listing carries a deterministic revision with separate hashes
for status, scalar fields, preview image, gallery, analyses, and search
membership. The aggregate and component hashes exclude the unrelated global
completion boundary, response truncation, and record-identifier churn caused by
an identical refetch. A changed description or source image set changes the
relevant component; simply reading the same data through a smaller response
limit does not.

The default workspace policy treats status, scalar fields, preview image,
gallery, and analyses as review-relevant. Search membership is independently
visible but does not make a prior review stale by default. A workspace can
choose another nonempty component set.

### Review batch

A batch is an immutable, bounded set of composed listings issued for review.
It retains the exact projections and revisions shown to the agent, the prior
review state, relevant changed components, and the cursor for the next batch.
Issuing a batch does not imply that any item was delivered, inspected, or
decided.

After a refresh, the default batch states (`unreviewed` and `stale`) are the
ordinary agent review queue. Newly discovered listings are unreviewed. A
previously reviewed listing becomes stale when a component selected by the
workspace policy, including its scalar fields, materially changes. Merely being
absent from an incomplete or later search does not by itself make the listing
stale.

An agent explicitly records inspection, an optional disposition
(`promising`, `rejected`, `waiting_for_data`, or `deferred`), and an optional
note. A bulk review write validates every listing and revision against its
source batch and commits all records atomically.

For concurrent review, `acquire_review_batch` combines batch publication with a
per-listing lease in one SQLite writer transaction. Another agent can receive a
different batch, but cannot acquire a currently claimed listing in the same
workspace. Claims have an owner, opaque token, and finite expiration; renewal
and explicit release are ownership-fenced. Recording claimed reviews validates
the owner, token, batch, listing IDs, and expiration in the same transaction as
the review records, then releases only the submitted members. The lower-level
unclaimed batch and direct-review paths remain application internals and are
not exposed through MCP.

Acquire, renew, release, and review-recording requests carry a caller-generated
request ID. An exact retry returns the stored response, including original
record IDs or the original claim token; reusing that request ID with different
content is rejected. This makes recovery safe when an MCP response is lost
after the database commit.

### Workset

A workset is a named static set of listing IDs. Worksets may overlap. Each
mutation publishes a complete immutable revision and requires the caller's
expected version, so concurrent agents receive a conflict instead of silently
overwriting one another. The initial implementation intentionally does not make
worksets dynamic saved queries.

### Selection snapshot

A selection snapshot freezes an ordered, bounded set of listing IDs together
with their projection revisions. It can be created from a review batch, a
workset, or an explicit ID list. Later bulk operations use the snapshot record
identifier rather than re-evaluating an informal group after the underlying
evidence changes. Workspace-native analysis preview and request operations can
use a snapshot directly, as well as a whole workspace, review batch, workset,
or explicit bounded ID list.

## Agent workflow

1. Create a workspace for the exact search run and optional guide.
2. Acquire a review batch with a stable request ID and owner ID. Available
   listings and `unreviewed` or `stale` review states are the defaults; both are
   explicitly overrideable.
3. Renew the claim if necessary. Inspect selected listings and atomically record
   only what was actually inspected or decided, supplying the claim token and
   owner. Release unfinished members explicitly.
4. Put useful overlapping groups into named worksets using the returned version
   for each update.
5. Freeze a batch, workset, or explicit bounded list as a selection snapshot
   before requesting a later bulk operation.
6. Add or refresh search tracks as needed, then wait for workspace work. Every
   completed track must have a completed refresh before workspace-wide analysis.
7. Preview and request analysis from the workspace-native selection. The
   workspace supplies all current search runs, their refresh coordinators, and
   the exact product guide. Preview and request use a bounded indexed status
   scan rather than full listing composition. They examine 2,500 candidate
   listings by default, report whether that bound was reached, and allow an
   explicit bound up to 10,000.
8. Check all queued, in-progress, or failed workspace-root work with
   `get_workspace_work_status`, or wait for the workspace to become idle with
   `wait_for_workspace_work`. The wait reports progress every five seconds when
   supported, defaults to 30 seconds, is capped at five minutes, and returns the
   still-active status when that timeout expires. Idle does not imply success:
   inspect `successful`, `terminal_failure_count`, and `failed_work`, and retry
   transient initial-track failures with `retry_workspace_search_track`.
9. Resume with exact workspace, track, batch, workset, and snapshot record identifiers,
   not prose descriptions of prior state.

`get_review_workspace_activity` provides the bounded recovery index for this
workflow: recent issued batches, explicit review records, current workset
summaries, recent selection snapshots, and active claim summaries. Claim tokens
are omitted from activity output; recover a lost acquisition response by
replaying its request ID. Exact batch and snapshot getters then recover the full
bounded objects.

Workspace work discovery follows durable requester edges whose requester
identifier is the workspace record ID. Replayed requests are deduplicated by
work ID. Only root work is returned: coordinator work remains queued or leased
while its children run, so one root accurately represents the whole operation
without returning every child analysis. Waiting polls this small indexed view
asynchronously. It does not claim work or keep a worker runtime alive, and an
idle result means no workspace-root work was queued or leased at the instant of
the returned snapshot.

## Storage and ordering

Workspace records, batch records, listing-review records, workset revisions,
and selection snapshots are immutable. Current claims are a small mutable,
indexed coordination table; completed mutation responses are durably retained
for idempotent replay. Each mutation is one short SQLite writer transaction with
registered component identity, code provenance, input edges, and named outputs.
Workset optimistic concurrency is checked inside the same transaction that
publishes the next revision.

Review ordering is local publication order, not the worker completion-sequence
boundary. The completion boundary exists to freeze collected evidence; local
review writes do not pretend to be worker events.

## Deferred mechanisms

- Dynamic worksets, predicates, and set algebra over worksets.
- Idempotency keys for the older local mutation tools.
- Larger selection snapshots using a set-oriented projection query rather than
  bounded exact-listing composition.
- Product-guide declarations that refine which projection changes invalidate a
  particular analysis or review.
- Policy for concluding that an unobserved listing is “gone.” Search absence
  remains membership history, not status evidence.
