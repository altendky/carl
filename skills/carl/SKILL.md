---
name: carl
description: Refresh and review retained Carl marketplace evidence, inspect exact listing images and analyses, maintain product guides, and request provenance-linked listing analysis through Carl's MCP tools.
---

# Carl Review

When calling `create_product_guide`, pass only the caller-owned identity suffix, such as
`["binoculars"]`; Carl supplies the leading `["carl", "product_guide"]` namespace. An already
prefixed request is normalized for compatibility. Use `set_product_guide_identity_retired` to hide
every version of an unused identity from ordinary `list_product_guides` results. Disable all of the
identity's workspace bindings first. Retirement preserves exact records and is reversible;
`include_retired=true` lists retired versions.

Use Carl's MCP tools as the source of record. Keep listing IDs, observation record IDs, guide record
IDs, analysis record IDs, and image artifact IDs distinct.

Facebook search and both marketplaces' image workers resolve explicit configuration
`route_overrides` before acquisition, including queued/retried work. A requested Proton route may
therefore use a separately credentialed Decodo datacenter route. Legacy `proton_route` options name
the requested path; inspect network activity and acquisition provenance for the effective provider.
Item pages and eBay search keep their separate mobile/residential route unless explicitly changed.
There is no automatic fallback. Datacenter Facebook bootstrap and pagination share one client and
cookie jar; use a static datacenter port to keep the exit stable across the search.

When diagnosing server behavior, call `get_server_info` first and report its instance ID, start time,
repository, database, source-tree hash, and capability versions. If the tool is absent, the client is
connected to an older server or is not connected to Carl. Do not substitute shell database queries
for an unavailable Carl MCP server without clearly reporting that the MCP connection is unavailable.

Call `get_activity_snapshot` to discover current queued and leased work, recent completions and
failures, and local processes holding the same database files open. Use the reported process IDs and
working directories to identify stale MCP or worker processes. It returns full durable work
identifiers; pass a relevant identifier to `get_work_status` for compact progress. Use
`include_details=true` only when the raw payload, checkpoint result, error, or recent operation IDs
are required. Do not assume a shortened identifier from a terminal display is callable.

Transport failures trigger a shared basic-Internet check using direct DNS/TCP, not a paid proxy
request. During an outage, marketplace acquisitions pause across workers without spending retry
budgets; offline extraction continues. Probes run every 30 seconds until connectivity returns.
Inspect `get_activity_snapshot.connectivity` for the pause and probe result, and its network activity
for failures including proxy-session startup. Healthy Internet with a blocked site or broken proxy
still uses ordinary bounded retries. Missing historical activity rows are not backfilled.

For the common current-view workflow, start with `list_composed_search` for an exact retained search
run or `get_composed_listing` for one returned listing identity. eBay review identities are
`ebay:<item ID>`; bare numeric identities remain Facebook IDs for compatibility. Copy identities
unchanged into claims, reviews, worksets, snapshots, and analysis selections. These tools make no network requests. They
read a fixed database completion boundary and compose status, scalar details, one coherent gallery,
older completed analyses, and optional refresh-lineage membership from independently timestamped
evidence. The collection view defaults to normalized status `available`; pass an explicit nonempty
`filters.statuses` array to include `pending`, `sold`, `unavailable`, or `unknown`. Follow
`next_cursor` without changing the request scope. Older analyses use the initial permissive reuse
policy and are labeled `assumed`, with their actual input observation retained. Search absence is
membership history, not a listing status or proof that an item is gone.
For a listing known only from search results, scalar details fall back to the retained search card
and `preview_image` exposes its primary thumbnail. Price is selected differently from descriptive
fields: the newest usable actual price observation from either a search card or an item page wins.
A search refresh updates prices only for listings it actually observes; it does not guarantee
coverage of older listings, and absence neither clears a price nor makes its evidence newer. Check
the selected price's evidence source and observation time before describing it as current. Card
prices omit currency in the observed source shape, so do not infer a currency when the returned
price has none. A preview image is not a complete gallery; `gallery` remains null until item-page
gallery evidence exists.

For review, call `create_review_workspace` with one exact Facebook/eBay search-run record or a
marketplace search-group record and, when relevant, one exact guide. A run becomes the first track;
a search group contributes one track per target. A workspace may mix Facebook and eBay. Refresh
each enabled track using the same `request_workspace_refresh` call; retained source provenance
selects collection, description, gallery, and analysis behavior. Call
`rename_review_workspace` when its scope becomes clearer. Archive inactive workspaces with
`set_review_workspace_archived`; `list_review_workspaces` hides them by default, while
`include_archived=true` reveals them for restoration or exact access. Archiving retains all review
history and does not invalidate exact workspace IDs. Call
`add_workspace_product_guide` to make additional guides available. Use `pinned` when future work
must keep one exact version and `follow_latest` when future requests should resolve the newest
version of that guide identity. The returned binding always names the exact currently resolved guide
record. Use `update_workspace_product_guide_binding` to rename, enable, disable, pin, or advance a
binding, and `set_workspace_default_product_guide` for ordinary workspace views. For analysis,
provide either `product_guide_binding_identifier` or an exact `product_guide_record_identifier`
when the desired guide is not the default; retain the preview's exact resolved guide record. Use
`missing_for_selected_guide` to treat a completed analysis under that exact guide as reusable even
when it came from an older retained observation. Inspect the preview's new, reused, and excluded
counts before queueing. Existing analyses remain tied to the exact guide version that produced
them. Call
`create_workspace_search` to add another complete search intent/phrase; its returned work ID is also
the stable track ID. Call `request_workspace_refresh` to advance a track, specifying its track ID
when more than one completed track exists. Prefer `list_workspace_listings` over
`list_composed_search` for review: it deduplicates the available-only union of every completed track
and each track's refresh ancestry. A listing missed by a later refresh remains in the union unless
separate retained evidence changes its status. Use `set_workspace_search_track_enabled` to remove a
track from or restore it to that union without deleting its history. Use `get_workspace_listing` for
an exact listing with the same active-track scope and the workspace's guide. Search acquisitions are
serialized per proxy route. If initial track creation exhausts a transient transport or session
failure, call `retry_workspace_search_track` with the stable track ID; it preserves the track and its
history while giving it a fresh retry budget. Retrying legacy work also adds the current route
scheduler scope. For eBay initial-track failures, optional `acquisition_stack` selects another
existing configured Decodo/wreq stack for active searches. Sold/completed acquisitions are disabled;
do not retry them or suggest another stack to bypass the restriction. eBay search already uses the
same configured Decodo browser transport as item pages; a renamed stack on the same provider route
is not an alternative acquisition path. The override applies to the retried work, not the original
search-group target specification. When a refresh's search acquisition failed, request a new
`request_workspace_refresh` with `acquisition_stack` instead; item-only transport recovery uses
`retry_item_failures` below. Challenges and HTTP 403/429 defer searches on the same stack for
two, four, then eight minutes after successive failures, including the terminal third failure.
The durable cooldown also applies to new work and operator retries, without consuming attempts
while waiting. Retries and refreshes spend provider calls: inspect retained evidence and select a
justified change before queueing; do not make repeated paid probe calls.
A Facebook transport failure, when Proton is selected, invalidates the shared Proton transport and backs off other
queued searches on that route before replacement. Use `acquire_review_batch` with a stable caller-generated
request ID and owner ID. It atomically issues bounded available-only work and
claims only listings not leased to another agent. Replay the same unchanged
request ID to recover the exact response after an interruption. Renew longer
work with `renew_review_claim`. After actually inspecting items, call
`record_listing_reviews` with the batch ID, claim token and owner, each exact
listing projection revision, and any disposition or note; successful recording
releases only those submitted listings. Give review recording its own stable
request ID and replay it unchanged if the response is lost. Release unfinished members with
`release_review_claim`. The MCP surface intentionally does not expose unclaimed
batches or direct, unclaimed review writes. Acquiring a batch does not mark its listings
inspected. Resume the exact retained batch with `get_review_batch`. A later
batch defaults to `unreviewed` and `stale` items. After refresh work finishes, acquiring
that default batch is the ordinary way to review newly discovered and materially changed listings;
search absence alone does not classify a retained listing as changed. Component revisions ignore
response limits, unrelated database activity, and identical refetch record IDs;
the workspace policy says which actual component changes make a review stale.
Use `get_review_workspace_activity` to rediscover bounded recent batches,
explicit review records, current workset summaries, and recent selection
snapshots plus active claim summaries when resuming work. Claim tokens are not
included there; retain the acquisition response or replay its request ID. Do not
depend on a previous conversation to retain IDs.

Use `create_review_workset` and version-checked `update_review_workset` for
reusable static groups such as shortlists, rejected alternatives, or items
needing measurements. Worksets may overlap. Before a bulk follow-up, call
`create_selection_snapshot` on a review batch, workset, or explicit bounded ID
list and retain the returned snapshot record ID. The snapshot freezes both IDs
and projection revisions. Pass it as a `selection_snapshot` selection to the
workspace-native analysis preview and request tools.

For a new search, call `create_search` with a `targets` array. Each target has a `marketplace`
discriminator and a `search` object: Facebook uses its complete search intent and bounded traversal
policy; eBay uses `query`, `stack_identifier`, and `maximum_pages` (default five, maximum twenty).
The returned search group contains each target's executions and work IDs. Poll those IDs with
`get_work_status`, then page through `list_search_results` using the group's record ID and returned
cursors. Results deduplicate within each marketplace and retain all source occurrences at a fixed
completion boundary. Use `add_search_target` to add and initially execute another target,
`set_search_target_enabled` to control future scheduling, and `run_search` to execute all enabled
targets. `get_search` retains execution history even for disabled targets.

For eBay item details, call `request_listing_details` with `marketplace="ebay"`, the exact
`external_identifier`, and optionally `stack_identifier`, `maximum_images` (default twenty, cap
fifty), or `refresh=true`. Zero images skips downloads but still retains gallery references.
The request reprocesses a usable retained item page by default; refresh explicitly fetches a new one.
Item and seller-description pages use the configured Decodo browser stack; gallery images resolve
the configured Proton-to-datacenter cutover when present. Poll the root work, its returned `extraction_work_identifier` when present, and the
extraction result's `image_work_identifiers` and `description_work_identifiers`. Completion of item
acquisition alone does not mean those follow-ups succeeded. Keep `carl work` active.
Read either marketplace with `get_listing_details`. Check `classification`, `description_state`,
and each image's state before describing the result as complete. Older usable detail remains visible
alongside a newer unavailable/challenge observation; reprocessing does not advance acquisition age.
Pass saved `artifact_identifier` values to `get_listing_image`. Search-card image URLs alone do not
mean image bytes have been saved. The detail-request tool accepts either marketplace; Facebook uses route options, eBay its stack.
The same source-dispatched search-refresh workflow handles bulk follow-up. Review workspaces,
listing projections, analysis previews, and analysis requests support either source and mixed
workspaces. eBay analysis includes exact retained seller-description and gallery evidence.

New eBay searches support only `search.listing_state="active"` (the default). Carl does not presently
support sold/completed search acquisitions: its configured acquisition is anonymous and cannot handle
the sign-in/challenge gating encountered by closed-listing requests. Create, rerun, retry, and refresh
reject these modes before acquisition. Retained sold/completed targets, results, and ordinary item
identities remain readable. To rerun a historical group containing closed targets, disable those
targets first rather than deleting their history. For retained sale comparables, use
`list_workspace_search_results` with the workspace and track IDs and optionally
`listing_state="sold"` or `"completed"`; completed includes both sold and other ended cards.
Historical sold mode adds both `LH_Sold=1` and `LH_Complete=1`; completed adds only `LH_Complete=1`.
`list_search_results` retains each card's
`sold_price`, `sold_date`, `sold_date_text`, and `sold_price_status` with its acquisition/run
provenance. Summary sale fields come from the single `sold_occurrence_record_identifier`, never
from independently merged price/date fields. Composed listing views expose `last_sale` separately
from ordinary price evidence. Use `statuses=["sold"]` to view sold cards; available-only remains
the default. Keep condition and shipping in comparisons. Missing years are not inferred, and
recognized Best Offer/crossed-out prices are not reported as known sale amounts. Public sold-card
prices are evidence, not verified transaction totals; do not promise hidden accepted offer amounts.

For an existing source run from either marketplace, call `list_search_runs`, optionally filter by
the exact query text, and select the exact baseline search-run record. Each summary includes the
producing attempt's `started_at_utc` and `ended_at_utc`, whether the originating request was a fresh
search or refresh, and the direct source run ID for a refresh. Use these summary fields to judge age
and lineage without fetching provenance. Use `get_search_run_listings` with the exact search-run
record ID to page through its immutable returned listing IDs; follow `next_offset` until null when a
complete membership comparison is required.
For Facebook or active-mode eBay, call `request_search_refresh` with its exact source-run record ID.
`maximum_images` is a global refresh budget, not multiplied per listing. eBay inherits its retained
stack and page bounds; optional `acquisition_stack` and `maximum_pages` override them. Facebook
traversal overrides and nondefault proxy-route overrides are rejected for eBay, not silently ignored.
A refresh with at least half its selected items failing now ends in `terminal_failure` with
`mostly_failed_refresh`; smaller losses remain `completed_with_failures`. Inspect
`get_work_status.outcome` and `refresh_failure_summary`, which also flag historical mostly-failed
refreshes whose stored state is `completed`. Use `retry_item_failures` with the exact settled
`refresh_work_identifier` and bounded `maximum_items` to requeue linked transport/session item-page
failures with fresh retry budgets. It resumes extraction, descriptions, and images from the retained
search, without rerunning search or expanding the original image budget. The carried image count is
conservative and can include reuse jobs. It does not retry extraction/challenge failures or permanent
authentication/configuration failures. Wait for the refresh to settle before another recovery call;
the operation reports matching, retryable, retried, and remaining terminal item-page jobs.
For Facebook, omit `traversal` and `traversal_strategy` to reuse
the retained settings. To change bounds, provide a complete traversal object. To use price buckets,
provide an `overlapping_price_partitions` strategy with decimal `width`, `overlap`, and either
`balanced` or `ascending` order. Carl searches through the effective configured search route, reuses the latest semantically usable
retained item page for each exact baseline or newly returned listing ID, and spends Decodo only on
IDs without one. It then downloads missing images through the effective configured image route while reusing validated matching
image evidence. Poll the returned work ID with `get_work_status`; the result
separates the last durable `checkpoint_stage` from `active_phase` and reports counts for item-page,
item-extraction, image, and image-extraction child work. A `search_complete` checkpoint can remain
visible while item children are actively completing; use the child counts to judge progress. Current
image collection validates and publishes the image file itself. Image-extraction work represents
legacy/offline reprocessing, so zero image-extraction children is normal when current image children
completed successfully. Do not use the listing-observation count to measure page collection because
observations are created during the later extraction phase. Do not enqueue another refresh merely
because the current one is pending or leased. Keep a separate long-running `carl work` or
`carl monitor --work` process active while queued work should progress. MCP servers never run
workers implicitly, so concurrent or leaked clients do not multiply worker pools.

Transient search and image session failures retry automatically with exponential backoff.
When Proton is selected, a failed request invalidates the shared transport so the next attempt opens and probes a fresh
WireProxy child instead of reusing a dead local endpoint. Search refreshes resume retryable search
children rather than immediately failing the whole refresh. If a refresh contains older terminal
image failures, call `retry_image_failures` with
the exact refresh work ID; the exact search-run record ID is also accepted. The result reports the
matching, requeued, and still-terminal counts. Keep `carl work` active and poll activity or refresh
status after requeueing, or use `carl monitor --work` for both. The call performs one bounded bulk transaction and does not wait for
collection. Requeued image jobs use payload schema v2 so stale pre-fix schema-v1 workers cannot claim
them; current workers can still consume unrelated legacy v1 work. Do not requeue terminal failures
blindly when their retained code identifies a permanent configuration problem; correct that problem
first.

For every work item, use `runtime.latest_event_kind`, `runtime.latest_event_age_ns`,
`runtime.lease_remaining_ns`, and `runtime.lease_activity_age_ns` to distinguish a renewing lease
from an abandoned one. Analysis-batch progress combines its last checkpoint with child work observed
through durable requester edges, so child counts can advance while
`checkpoint_processed_observations` remains unchanged. Use `remaining_observations`, bounded active
child identifiers, and grouped failure reasons for routine monitoring rather than fetching every
child status.

After every completed workspace track has a completed refresh, call
`preview_selection_analyses` with that workspace and select the whole
workspace, a review batch, a workset, a selection snapshot, or an explicit bounded ID list. Use
`selection_policy=missing_for_current_evidence` to analyze fresher evidence again, or the top-level
`selection_policy=never_analyzed_listing` to select only listing IDs with no completed analysis from
any earlier observation. Available listings are the default; override `statuses` explicitly when
needed. The preview reports matching, excluded, eligible, and selected counts without queueing work.
It uses bounded retained-evidence selection across both marketplaces. The request examines at
most `maximum_candidate_listings_examined` workspace members (2,500 by default) and reports
`candidate_examination_limit_reached`; raise the bound, up to 10,000, when an exact larger-workspace
plan is needed. Use the same bound in the subsequent request so preview and queueing describe the
same selection.
If those counts are acceptable, pass the same request to `request_selection_analyses`. Carl freezes
the matching latest-observation snapshot,
revisits each selected observation against its current saved images and the chosen guide, reuses
compatible completed or queued analysis work, and enqueues only what is missing. `maximum_items`
narrows that snapshot. Use `get_workspace_work_status` for a bounded view of every queued,
in-progress, or terminally failed root operation requested by the workspace. `idle=true` only says no
work is active; `successful=false`, `terminal_failure_count`, `completed_with_failures_count`,
and `failed_work` expose settled
failures. Use `wait_for_workspace_work` for a short wait until the workspace is idle. It reports
progress every five seconds when the MCP client accepts progress notifications, defaults to a
30-second timeout, and accepts at most 300 seconds. A timeout returns the still-active status so the
caller can poll again; the wait is asynchronous and does not run workers.
The workspace work view includes searches, per-track refreshes, and analysis batches requested
through the workspace. Analysis responses identify every contributing current search run and refresh
coordinator; compatible item analyses may still be shared with other workspaces.
Use `get_work_status` with the returned batch work ID when detailed diagnostics are needed;
`analysis_batch_progress` reports observations processed, jobs created or reused,
skipped observations, and child analysis states. The batch is complete only after its child analyses
are terminal. A later batch may create new work when its exact evidence or analysis configuration
differs.

Use `get_workspace_listing` for a selected workspace listing's current normalized fields, gallery, and completed
analysis descriptors. Each analysis descriptor names its own
`listing_observation_record_identifier`; do not assume it analyzed the projection's newest evidence.
Treat approximate location as approximate. Treat a later search absence as unknown rather than
evidence that a listing sold.

Fetch complete reports with `get_listing_analysis`. When a conclusion matters, retain the exact
analysis, observation, and product-guide record identifiers it depends on. Use `get_provenance` one
hop at a time when the origin or tooling needs examination; follow returned object identifiers for
deeper traversal. It reports the total `output_edge_count` but returns no sibling outputs by default.
Set `maximum_output_edges` from 1 through 100 for a deterministic bounded prefix, or explicitly pass
null only when every output edge is truly required.

Use `get_listing_image` only for image artifact IDs returned by a composed listing or
`get_listing_details`. Each call returns the
exact validated stored image and may put its bytes into model context, so request only images useful
to the current judgment.

Before requesting analysis, choose an exact guide record from `list_product_guides` or inspect it
with `get_product_guide`. Create a guide with a stable structured identity and human-readable name
when none fits. Revise a guide using its current record as the expected base. If Carl reports a
conflict, read the current version and apply the intended change to that version; never overwrite or
delete history.

Request analysis through `preview_selection_analyses` and `request_selection_analyses`; direct
observation-level requests are intentionally not exposed through MCP. By default, let Carl fail when
any gallery image is unavailable. Set `allow_incomplete_gallery` only when text-only or partial image
analysis is useful, and report that limitation in later comparisons. Prefer the workspace status or
wait tools for routine coordination; do not enqueue duplicates merely because analysis is still
pending. A Claude
deadline automatically retries twice with bounded backoff before becoming terminal, and each failed
attempt retains its own record, exact process output, and observed tool calls. New analysis work uses
schema v4 so stale schema-v3 workers cannot claim it. `get_listing_analysis` accepts an exact failed
attempt record ID for diagnosis, while composed projections show only completed analyses.

Carl's first-pass reports identify the offered item, condition, included or missing parts, and
researchable specifications. Use the retained listing evidence and report together for later value
or preference assessment. Distinguish seller claims, visible facts, external research reported by
the analysis agent, and later interpretation.
