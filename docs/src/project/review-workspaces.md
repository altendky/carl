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

The track ID identifies a watch, not one immutable set of search parameters.
Read `get_review_workspace` for its complete current `search_specification` and
`search_specification_version`, then call `revise_workspace_search_track` with
that version as `expected_version` and the complete replacement search spec.
The compare-and-swap version check rejects a stale editor; the marketplace must
remain unchanged. Both legacy Facebook search input and source-neutral,
discriminated Facebook/eBay specifications are accepted, just as for
`create_workspace_search`. For example, changing a price cap from $3,000 to $300
revises the same track rather than adding a watch and disabling the original.

`list_workspace_search_track_versions` reads immutable specification history.
Legacy tracks initially expose version 1 from their original search; the first
revision persists that original version and its successor. Revision itself
does not queue acquisition. A subsequent `request_workspace_refresh` captures
the current version, while refreshes already queued retain their original
specification. Prior search and refresh ancestry, listing first-seen/last-seen,
and review history stay attached to the same stable track.
Wait for a prior-version refresh to settle before queueing the revised one. The
enqueue transaction checks the captured version again, so a concurrent edit
cannot queue an outdated specification after the edit has committed. If initial
acquisition failed before any retained run exists, `retry_workspace_search_track`
acquires the revised initial spec under the same track ID without rewriting the
old failed work. A retained failed baseline instead uses `request_workspace_refresh`.

Changing the scope does not turn listings excluded by the new scope into
missing, gone, or sold listings. Absence comparisons are conservative across
different search scopes, while historical listings remain in the track's
retained union. Create a new track for a genuinely separate watch; revise the
existing one when its search parameters evolve.

Facebook search acquisition admits at most one active job per effective Proton
route. New Facebook searches and both sources' images default directly to
`["decodo", "personal", "datacenter"]`, without Proton aliases. Use Facebook
search `network_path`, refresh `search_network_path`, and `image_network_path`
for explicit provider-qualified routes. Decodo searches run concurrently within
worker-pool and request-rate limits. eBay retains its separate
search scheduling policy. If a track's
initial search exhausts a transient transport or session failure,
`retry_workspace_search_track` requeues that same durable track with a fresh
retry budget rather than creating another track. Retried legacy work gains the
current per-route scheduler scope. Transport failures invalidate the shared
Proton transport and defer queued searches on that route before replacement.
For active eBay searches, the optional `acquisition_stack` retry field selects another
existing configured Decodo/wreq stack while retaining the track and attempt history.
Sold/completed acquisitions are disabled and cannot be retried through a stack override;
their retained targets and results remain readable. The override changes the retried work,
not the original search-group target specification. Refreshes whose search acquisition
failed require a new `request_workspace_refresh` for the same stable track; an
acquisition-stack override is optional. `retry_refresh_finalization` requires a
completed search and downstream checkpoint, so it cannot resume a refresh whose
search never completed. Search and item pages already
use the configured Decodo browser stack; a new name for the same route does not
provide a different acquisition path.
Each eBay search attempt shares one sticky Decodo session and cookie-preserving
HTTP client across its pages. Independent searches and retries use fresh
sessions; expiry stops traversal rather than silently changing the exit during
pagination. Page acquisitions retain the shared safe session record identifier
and request counters as observed at acquisition time, before session cleanup.

eBay search challenges and HTTP 403/429 have a five-attempt retry budget. Successive
failures impose stack-wide cooldowns of 10, 20, 40, then 60 minutes, with the
60-minute cap also applied after terminal failure. Each retry opens a fresh
provider session and cookie jar.
Search-fetch timeouts use the same budget and cooldown. A timeout of Carl's own
network-activity finalization deadline also retries under this policy, retaining
any completed HTTP response as evidence and reporting a local finalization
failure rather than a site error. Cancellation and unrelated database or
programming errors retain their separate classifications.
Pending siblings, new work, and same-stack manual retries respect these durable
deadlines across restarts without consuming attempts while waiting. Other
response failures retain short bounded backoff. Blocked eBay item pages use
the same five-attempt, 10-to-60-minute backoff policy. Refresh parents defer
until pending children are eligible, while runnable or leased work retains
ordinary progress polling. Retrying or refreshing makes
real provider calls; inspect retained evidence and choose a justified route
change before requeueing rather than repeatedly probing paid acquisitions.

Transport/session failures also trigger a shared basic-Internet DNS/TCP probe.
If neither independent endpoint is reachable, marketplace acquisitions pause
across worker processes, retry every 30 seconds, and do not consume their
bounded retry budgets. Offline extraction remains runnable. Physical attempt
ordinals and evidence remain immutable; outage attempts are excluded from the
retry budget. Connectivity pause and probe diagnostics are exposed through
`get_activity_snapshot.connectivity`. Acquisition activity is recorded before
opening eBay sessions and before Facebook proxy bootstrap, so connection
failures appear in the activity ledger. Historical missing rows are not invented.

A refresh with at least half its selected items failing ends in
`terminal_failure` (`mostly_failed_refresh`); smaller losses remain completed
with partial failures. `get_work_status.outcome` and `refresh_failure_summary`
also identify historical mostly-failed refreshes without rewriting their history.
`get_work_status.search_refresh_progress.image_failure_reason_counts` groups failed
image work by retained cause, including site HTTP status, transport timeout, and
explicit proxy quota exhaustion. HTTP 403/429 alone does not establish a quota
failure. Older acquisitions are consulted when their work errors were generic.
When a reused Facebook item page contains expired, unsaved signed gallery URLs,
refresh requests one replacement page per listing. Remaining expired renditions
are skipped and the gallery is marked stale; matching saved images remain reusable.
The progress view reports `gallery_state`, `expired_image_renditions_skipped`,
and bounded stale listing IDs. Image work presented an expired URL stops with
`expired_url` before acquisition, rather than retrying the same signed URL.
`retry_item_failures` accepts an exact settled Facebook/eBay refresh work ID
and a bounded `maximum_items` (default 1,000; maximum 10,000). It atomically
requeues linked terminal collection jobs with transient transport/session errors,
gives them fresh retry budgets, and resumes the same coordinator after its search.
It excludes permanent authentication/configuration and extraction/challenge
failures. The original image budget is retained conservatively, including reuse
jobs in the carried count. Recovery retains old operations and evidence and
creates generation-specific image plans/results to avoid immutable-ID collisions.
The response distinguishes matching, retryable, retried, and remaining terminal
item-page jobs. Wait until the resumed refresh settles before retrying another
bounded chunk. CLI equivalent: `carl retry-item-failures REFRESH_WORK_ID`.
The stable workspace record can be renamed through an immutable identity-state
record. It can also be archived or restored without deleting any retained
objects. Ordinary workspace listing hides archived workspaces by default;
explicit lookup and `list_review_workspaces(include_archived=true)` retain access
to them. Prefer `list_review_workspace_summaries` for discovery. It returns
workspace identities, names, guide references, and track counts without full
track rows or specifications. The request accepts `include_archived`, `page_size`
(twenty by default, at most one hundred), `cursor`, and `response_byte_budget`
(1,500 through 9,000 UTF-8 bytes; 9,000 by default). A page reports its exact
`response_bytes`, `response_byte_budget`, `has_more`, and `next_cursor`; byte
limits may produce fewer rows than requested. Continue until `has_more` is false.
Page size and budget may change between requests while the archive scope stays
fixed. Workspace IDs sort lexically; this discovery cursor does not freeze later
workspace edits or creations. Use the exact workspace ID with the activity
recovery index, or a known track ID with the track-version tool for specifications.

The older `list_review_workspaces` response remains available. Set `compact=true`
to retain concise track identities, queries, marketplace, enabled state,
specification version, and latest refresh state. Per-track work/run IDs and
specification records are available through `get_review_workspace(record_identifier)`.
Omit `compact` to retrieve complete workspace definitions; the full response
remains the default.

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

`list_workspace_listings` returns a compact, cursor-paged index over the
deduplicated union of every completed track and its bounded refresh ancestry.
Its rows contain browsing fields, availability, image and analysis presence,
and one aggregate revision token without repeating field-level evidence.
`get_workspace_listing` is the drill-down for the full composed projection and
its evidence. An item present in several phrases is returned once. Both
old-only and new-only results remain candidates; absence from a newer run is
recorded as membership history rather than interpreted as sold or gone.
Normalized availability evidence still controls the default available-only
filter. The exact-listing operation applies the same active-track scope and
workspace guide.

Set `compact: true` on `acquire_review_batch` to receive the first byte-bounded
page of frozen listing summaries and the lease for the complete claimed batch.
Summaries retain only `projection_revision.aggregate_sha256` and browsing fields, while omitting
descriptions, seller profiles, component revision hashes, field evidence, and prior scalar snapshots.
Fetch the full frozen revision with `get_review_batch_item` before recording a review.
Follow `batch.next_cursor` with `get_review_batch_page`, passing
`batch_record_identifier` and the cursor. The limit defaults to five and is
capped at ten; byte limits can reduce it further. `total_items`, `returned_items`,
`has_more`, and response size metadata distinguish a page from the whole batch.
Paging does not change claims. After the frozen batch is exhausted,
`workspace_next_cursor` continues workspace acquisition under a new request ID.
It is a short durable reference to the issued batch; Carl resolves the stored
workspace cursor when acquiring the next batch. Keep the token opaque. Legacy
workspace cursors remain accepted, and continuation survives server restarts.

`get_workspace_listing(compact=true)` provides scalar values and a shared
`sellers` table keyed by `seller_reference`; old/new seller changes use that
table. Its prior scalar snapshots and repeated field evidence are omitted.
This exact-listing lookup returns current evidence. Compare its revision with
the frozen batch revision before relying on its details for that review.
For a claimed batch member, use `get_review_batch_item` with the exact
`batch_record_identifier` and `listing_identifier` to read its full frozen
projection and review comparison. This preserves the evidence and revision
actually issued with the claim. The full `get_review_batch` also retains all
original members and uses generic continuation when large.
Batch item evidence is emitted once in `evidence_table`. Each typed evidence
location contains an `evidence_reference` key into that table, including scalar
fields, status, preview images, gallery images, and gallery reference sets.
The values, comparison details, and projection revision remain exact.
Stored batches retain full projections, so recording reviews and replaying
claims keep their original evidence and revisions. Full responses remain the
default when `compact` is omitted.

`get_listing_analysis` returns the retained report text, descriptor, limit
observations, and concise Claude metadata. Its prompt-bearing `argv` and
`output_metadata` usage diagnostics (including `modelUsage`) require
`include_details=true`. The stored analysis record is unchanged.

Check size and continuation metadata on every response. Follow native
`next_cursor` or `next_offset` values and inspect `has_more` in the page or
`carl/response` result metadata before treating a collection as complete.
If a response instead has
`kind="carl_tool_response_page"`, its original tool has already executed.
`original_is_error` and `critical_metadata` preserve immediate outcome details.
Read `json_fragment` directly from `structuredContent`, which contains the complete page.
Text contains only a short notice, so the fragment is sent once and remains visible to
clients that exclusively surface structured content, including Claude Code. Call
`get_tool_response_page` with each `next_cursor` until `has_more` is false,
concatenate the `json_fragment` strings in `offset` order, and parse the compact
JSON. `payload_format=structured_json` contains the original structured payload;
`mcp_result_json` preserves the complete MCP result for genuine text, image, or
mixed content. Offsets count Unicode characters; `total_json_bytes` and `sha256`
describe this single canonical representation. After
retaining all fragments and reconstructing the result, acknowledge the final
page with `get_tool_response_page(cursor=final_page.cursor, acknowledge=true)`.
This returns a small receipt without repeating the fragment. Its cached result
stays pinned until acknowledgment so a lost final response can be retried.
Do not repeat
the original tool to fetch the rest of its response. These cached continuation
pages must be read before the MCP server process restarts.

A newer usable search-card price supersedes an older item-page price, while a
newer item-page price likewise supersedes an older card price. This applies only
to listings actually observed by a refresh. A missed listing retains its last
observed price, including that evidence's original source and timestamp; search
absence does not make the price current or prove that it is unchanged.

### Incremental processing

`request_search_pipeline` can process enabled current workspace tracks, optionally restricted to
explicit track IDs, without rerunning search. It freezes their exact search work/run scope and uses
one supplied exact product guide for analysis. An actively refreshing selected track is rejected;
wait for that refresh or select its exact search child with the `search_work` source.
Alternatively, a `new_search` source starts a mixed Facebook/eBay search and its processing intent
atomically. Search-only tools remain unchanged.

Each listing independently acquires details, descriptions, and a bounded gallery before analysis;
one listing's slow download does not prevent a ready listing's analysis. Set `stop_after` to
`details`, `images`, or `analysis`, and choose global item/image/analysis limits before requesting
paid work. Incomplete galleries require an explicit override. Replay the unchanged caller request
ID after interruption; later targets and guide versions do not alter its accepted meaning.
Use `get_search_pipeline` for counts, conservative budget reservations, source-run IDs, and bounded
listing progress. A budget flag means a caller bound was reached, not exhaustive coverage.
Workspace work status includes the pipeline root and its propagated failures. Processing does not
advance refresh tracks or remove the completed-refresh requirement from fixed-selection previews.

### Projection revision

Every composed listing carries a deterministic revision with separate hashes
for status, scalar fields, preview image, gallery, analyses, and search
membership. The aggregate and component hashes exclude the unrelated global
completion boundary, response truncation, and record-identifier churn caused by
an identical refetch. A changed description or source image set changes the
relevant component; simply reading the same data through a smaller response
limit does not.

Scalar recipe version 2 compares normalized values rather than source-specific price envelopes.
Equivalent decimal spellings and redundant price display formatting do not change a revision.
When a new price omits currency, composition carries forward the newest retained known currency;
explicit currencies still take precedence, and no currency is assumed from a dollar symbol.
New reviews retain normalized scalar snapshots. Existing batch-backed reviews use their retained
projections, while legacy Facebook bulk reviews reconstruct values at the mutation's recorded
completion boundary and verify the old scalar hash before using that baseline. Historical records
are not rewritten. Unknown baselines remain conservative rather than suppressing potential changes.
Recipe versions alone do not invalidate reviews. With a verified scalar baseline,
comparison uses facts populated when the review was recorded: new fields and new
profile members do not make it stale, while changes or removal of previously
reviewed values do. Historical nulls represent unavailable facts. Unknown
baselines retain conservative hash comparison.

Review batches and `get_workspace_listing` report `scalar_field_changes`, with each field's
`previous_value` and `current_value`, plus `scalar_comparison_available`. An unavailable comparison
is distinct from a verified empty change list. The single-listing view also reports review state,
prior review, and policy-relevant changed components without acquiring a claim.

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
note. Ordinary per-listing writes validate every listing and revision against
their source batch and commit the submitted records atomically.

After explicit triage, `record_workspace_bulk_review` can apply one disposition
and note to the remaining members of a whole workspace, workset, or frozen
selection snapshot. It defaults to available, unreviewed listings and accepts
explicit listing exclusions, so promising, deferred, or waiting-for-data
decisions remain untouched. Carl composes one bounded current snapshot and
records its revision hashes server-side; the caller does not need to claim or
send thousands of revisions. Mixed Facebook/eBay selections expand their scope
once and read evidence in bounded batches within one read snapshot, including
gallery and analysis metadata needed for unchanged revision semantics. The write
is all-or-none and aborts if any target
has an active claim or receives another review before commit. Later changes to
status, price or other scalar fields, preview image, or gallery make the bulk
review stale under the same workspace policy as an ordinary review, causing the
listing to resurface.

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
   Bounded listing, batch, workset, and snapshot selections require freshness only
   for enabled tracks containing their listings, including retained ancestry.
   Freshness errors identify the track, refresh work, and retained failure reason.
7. Preview and request analysis from the workspace-native selection. The
   workspace supplies the relevant current search runs, their refresh coordinators, and
   the exact product guide. Explicit listing IDs and batch, workset, or snapshot
   members are looked up directly; unrelated members do not consume the
   `maximum_candidate_listings_examined` bound. Whole-workspace selections use
   a bounded status scan: 2,500 candidates by default, an explicit limit up to
   10,000, and `candidate_examination_limit_reached` when the bound is reached.
   Both preview and request return `skipped` entries with listing IDs and reasons
   for exclusions, missing observations, reused analyses, selection-policy
   filtering, and `maximum_items`.
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

Analysis-batch progress reports the runtime skip count. To inspect skipped
listing IDs, observation IDs, and reasons encountered after queueing, request
`get_work_status(include_details=true)` and read `details.result.skipped`.

Workspace work discovery follows durable requester edges whose requester
identifier is the workspace record ID. Replayed requests are deduplicated by
work ID. Only root work is returned: coordinator work remains queued or leased
while its children run, so one root accurately represents the whole operation
without returning every child analysis. Waiting polls this small indexed view
asynchronously. It does not claim work or keep a worker runtime alive, and an
idle result means no workspace-root work was queued or leased at the instant of
the returned snapshot.

## Sold and completed searches; cheap track cards

The eBay request/history schema accepts
`search.listing_state: "active" | "sold" | "completed"`. Carl does not presently
support new sold/completed searches: the configured eBay acquisition is anonymous
and does not support the sign-in/challenge gating encountered by closed-listing
requests. Creation, group reruns, retries, and refreshes reject them before
queuing; already queued closed-search work stops without acquisition calls.
Use `listing_state: "active"` for new searches. Existing sold/completed requests,
cards, and listings remain readable; no historical evidence is removed.
Selecting another stack does not bypass this restriction.

Historical sold mode uses `LH_Sold=1&LH_Complete=1`; completed mode uses
`LH_Complete=1`, including unsold endings. Results are ordinary items,
not reference-sale entities, and retain the same IDs as prior active results.

Card states describe observed outcomes rather than the requested search mode:
`sold` requires sale evidence, `completed` requires an explicit Ended marker
without sale evidence, and missing markers remain unknown. Their composed
statuses are `sold`, `unavailable`, and `unknown`. Use
`filters.statuses: ["sold", "unavailable"]` for known closed items. A displayed
price on an unsold or unclassified card is not a known sale price.

`list_workspace_search_results` reads one workspace track's latest retained
source run without composing item, image, or AI evidence and without making
network requests. Supply `workspace_record_identifier`, `track_identifier`, and
optionally `listing_state: "sold"` or `"completed"` (the latter includes both
sold and other ended cards). Omit this filter to read unknown cards too.
Workspace tracks report their marketplace
and active/sold/completed mode. The result includes condition, shipping text, source-card
identities, and `last_sale` with the displayed sold price, normalized amount and
currency when unambiguous, date, and price certainty. Accepted Best Offers do
not establish a known sale amount.

This is a card view, not the union of a track's refresh ancestry. Pagination
freezes the source run and evidence boundary, including if a refresh completes
between pages; page size may change but track and filters may not. Check
`collection_succeeded`, `response_classification`, `stopping_reason`, and
`warnings`: zero cards after a challenge or HTTP error is not evidence of an
empty market.

Read-time `required_title_keywords` requires all case-insensitive substrings;
`excluded_title_keywords` excludes any matching substring. These inexpensive
filters do not persist track settings or change the source acquisition query.

The full composed view and compact workspace index also expose condition,
shipping, and last-sale fields. Single eBay amounts use the same
`amount_decimal`/`currency` price structure as Facebook, with the original
`formatted_amount` retained for display. Ambiguous ranges remain display-only.
Unqualified dollar prices from ebay.com follow its USD display convention;
this convention is not applied to Facebook.

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
