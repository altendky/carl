# Composed listing projection and status monitoring plan

## Status

The initial read-only composed projection is implemented as indexed database
queries plus deterministic application-layer composition. It is exposed through
MCP and the CLI for one exact listing and for a bounded retained-search lineage.
It is computed on demand at a fixed completion boundary: there is no
process-local cache and no materialized projection table. Status monitoring,
cheap PDP probes, automated freshness policy, and evidence fingerprints remain
planned work. Facebook's private protocol is not guaranteed to remain stable.

## Motivation

Carl's immutable acquisitions, observations, artifacts, and provenance graph
are the source of truth. That model is valuable for audit and reprocessing, but
it is more detail than an agent or person should need for common questions such
as:

- What is the latest information Carl has about this listing?
- Which results from a saved search are currently known to be available,
  pending, sold, unavailable, or of unknown status?
- When and how was that status last observed?
- Which listings need a cheap status check or a more expensive detail refresh?

The feature will add a bounded, higher-level composed projection derived from
retained evidence. It will not replace, rewrite, or discard that evidence.
Every component will retain identifiers that let a caller drill down to its
contributing records and operations.

## Terminology and architecture

The public result is a **composed projection**. The two words name different
parts of the mechanism:

- A projection transforms retained evidence into a task-oriented read shape.
- Composition independently selects status, detail, gallery, analysis, and
  search-membership components that may have different observation times.

The design separates three layers:

1. **Facts** are immutable acquisitions, observations, artifacts, analyses, and
   exact search-run membership.
2. **Composition** deterministically selects the best eligible evidence for
   each component at one database completion boundary.
3. **Policy** decides which composed listings to display, when evidence is too
   old, what to refresh, and eventually when a listing should be considered
   gone.

Composition does not make the selected components contemporaneous. Each
component retains its own evidence identity and time. A recent search-card
status may therefore be composed with older item details, gallery artifacts,
and analyses without suggesting that those older components were re-observed.

“Current listing view” remains useful explanatory language, but the typed model
and implementation should use “composed projection.” “Aggregate” would imply a
transactional consistency boundary, and “snapshot” would incorrectly imply
that every component was observed together.

## Agreed initial behavior

- Collection queries show only listings whose selected explicit status is
  `available` by default. Callers can select any nonempty combination of
  normalized statuses for granular control.
- An older completed analysis remains usable even when a newer listing
  observation exists. The initial implementation labels this applicability as
  `assumed` and preserves the analysis's actual input observation.
- Exact search-run membership is factual. A listing's absence from a later run
  is derived only by comparing exact membership and does not create a listing
  state.
- Search-context projections retain listings seen in earlier runs of the
  selected refresh lineage and expose first/last-seen and selected-run
  membership facts.
- One global listing projection supplies status, detail, and gallery
  components. Search membership and product-guide-specific analysis selection
  are contextual overlays.
- Responses expose component observation and completion times. Server-evaluated
  age, freshness thresholds, automated refresh policy, analysis invalidation,
  and the policy for deciding that a listing is gone are deferred.

## Goals

- Make the latest usable composed listing data available with one bounded
  lookup.
- Combine independently fresh status, detail, gallery, and analysis evidence
  without pretending they were observed together.
- Monitor saved search results for status changes while avoiding full item-page
  downloads when cheaper evidence is sufficient.
- State observation time, source, and uncertainty explicitly.
- Keep routine queries index-backed and avoid unbounded provenance traversal or
  a long-lived in-process cache.
- Preserve Carl's rule that absence from later search results is exact
  membership history, not evidence that a listing sold or a distinct listing
  status.

## Non-goals

- Mutating old observations to make them look current.
- Treating a projection as stronger evidence than its inputs.
- Performing network work as a side effect of reading a projection.
- Inferring `sold` from disappearance, rank changes, or failure to complete a
  search traversal.
- Refreshing description, seller, gallery, or other rich detail merely to
  answer a status-only question.
- Publishing Facebook query identifiers, cookies, cursors, or other
  session-scoped protocol material as configuration.

## Proposed evidence and read models

### Listing status observation

A `listing_status_observation` will be immutable and identify:

- the exact listing ID;
- the observation time;
- its source scope, initially `search_card`, `pdp_probe`, or `item_page`;
- the raw source flags when present: `is_sold`, `is_pending`, and `is_live`;
- a normalized status: `available`, `pending`, `sold`, `unavailable`, or
  `unknown`;
- the acquisition, search occurrence, extraction operation, and extractor
  version that support the observation; and
- structured warnings when the source is incomplete or ambiguous.

Normalization will use explicit precedence. A positive sold flag wins over a
pending flag, and pending wins over an otherwise active/live indication.
`is_live` must not be treated as synonymous with available: retained item data
contains live listings that are also sold or pending. An explicit unavailable
item response is distinct from a listing merely not appearing in a search.

Exact membership in each completed search run is retained. Absence requires no
separate record: it can be determined relative to a particular run from that
run's membership. It is not a listing status. If no explicit status evidence
exists, the composed projection reports `unknown` instead of manufacturing an
available or terminal state.

### Composed listing projection

The composed listing projection will initially be computed by indexed queries
over immutable records. It should expose, when present:

| Area | Composed fields |
| --- | --- |
| Identity | Listing ID and canonical source URL |
| Status | Normalized status, raw flags, observation time, source scope, and warnings |
| Detail | Item-page title, price, description, location, and seller fields, with search-card fallbacks and field provenance |
| Preview | Latest usable search-card primary image and its card provenance |
| Gallery | Latest usable gallery summary, saved/expected counts, and observation identity |
| Analysis | Selected completed analysis summaries, guide/configuration identity, input observation, applicability, and completion time |
| Search context | Exact first/last-seen and selected-run membership facts when a search lineage is requested |
| Provenance | Record and operation identifiers needed to inspect each selected value |

The parts may have different observation times. The response must show those
times rather than presenting a synthetic single snapshot. “Latest” means the
latest semantically usable retained evidence for that part, selected by a
documented stable rule; it does not mean that every field was re-fetched.

The read path should support both one exact listing and a stable, paginated set
of results from one retained search or refresh lineage. Collection queries
default to the normalized status `available`; callers can explicitly select any
nonempty combination of `available`, `pending`, `sold`, `unavailable`, and
`unknown`. Filters should eventually include component age and whether a usable
analysis exists. The initial page contract does not promise exact counts over
an arbitrarily large lineage; a later count projection must use the same
consistent database boundary and filters as the returned page.

The existing review model's `full_listing` value describes response
completeness, not marketplace availability. The composed projection must not
reuse it as a normalized status. A complete item response can still describe a
pending or sold listing.

### Composition mechanics

One read establishes an immutable `as_of_completion_sequence` boundary. Every
component selector sees only evidence published at or before that boundary.
Selectors run independently and use stable record identifiers as their final
tie-breakers. Reading never schedules acquisition or analysis work.

“Newest” uses a total repository ordering, not nullable source timestamps. For
listing fields and status, compare the source acquisition completion sequence,
then the extraction/observation completion sequence, then the evidence record
identifier. Re-extracting an old acquisition therefore cannot outrank a later
acquisition. Image-result selection uses the image acquisition completion
sequence and then its result record identifier. Human-facing UTC timestamps are
reported when retained but do not control ordering.

Current search-card occurrences are published atomically with the completed
search and do not have an independent per-page publication sequence. Their
evidence therefore uses the whole search's completion sequence and reports
`search_card_ordered_by_search_completion`. A later persisted status-observation
write path should assign page-level durable ordering; until then, an unrelated
item collection interleaved with a long search can make source-time ordering
ambiguous even though database publication ordering remains deterministic.

The initial status selector uses the newest eligible **explicit** normalized
status observation. A source that contains no status, a failed acquisition,
and absence from a search run do not supersede earlier explicit status
evidence. Older status transitions remain inspectable but are not inherently
warnings. Presentation of genuinely contradictory near-contemporaneous sources
is deferred until a conflict-window policy is defined.

Scalar detail selectors independently choose a usable present value for each
field at the completion boundary. Item-page evidence takes precedence for
descriptive fields; retained search cards fill missing title, location,
description, or seller values. Price instead uses the newest actual usable
observation from either an item page or a search card, because a newer card can
report a price change without another item-page fetch. Missing, malformed, or
explicitly unsupported fields do not erase older usable values. Nor does a
listing's absence from a later search create a price observation: the old price
remains selected with its original evidence and age, without being described as
refreshed. This is deliberately not the current latest-whole-observation
behavior. A selected field always points back to the observation or search
occurrence that supplied it. Search-card prices are normalized to a decimal
amount and retain the formatted amount, but do not invent a currency when
Facebook omits it.

The public request explicitly bounds both item-observation and search-card
history examined per listing (100 by default for an exact listing and 25 for a
collection page). If older evidence is omitted by that bound, the listing
reports `item_observation_history_truncated` or
`search_card_history_truncated`, respectively. Analysis selection is queried
independently across all retained observations, so this detail-history bound
does not hide an older completed analysis.

The latest usable `primary_listing_photo` from retained search cards is exposed
as `preview_image`. It is not represented as a gallery: one search thumbnail
does not establish the listing's complete gallery membership or download
completeness.

Gallery membership remains coherent: the selector chooses one eligible item
observation's ordered gallery-reference set rather than unioning references
from several observations. It then selects the newest validated saved result
for each exact reference. Download completeness means only that every retained
reference in that selected set has a saved result; it does not claim that the
source exposed every real-world image. At most 100 retained references from a
selected gallery are inspected; `reference_set_truncated` and
`images_truncated` disclose a larger retained set, and completeness remains
false when that bound is hit.

Search context is defined by an exact selected run and a caller-bounded portion
of its refresh ancestry. The ancestry is obtained by following explicit
refresh-source edges toward the originating fresh run, up to the requested
maximum. Its composed membership is the union of exact run memberships in that
bounded path; independent runs with equivalent query parameters and sibling
refresh branches are not silently merged. The response identifies the oldest
included run and whether older ancestry was truncated. For each listing the
overlay reports exact first/last-seen runs within that disclosed scope and
whether it appeared in the selected run. A later rebuildable lineage projection
or saved-search identity may support complete long-lived history with bounded
read cost.

The search overlay also reports the selected run's stopping reason and a
coverage classification. `seen_in_selected_run=false` is always an exact set
membership answer, but `absence_comparison_valid` is true only when the
selected traversal completed with sufficient coverage for the requested
comparison. Result limits, failed partitions, cancellation, challenges, and
similar truncation remain visible instead of turning absence into evidence.

Coverage is derived from retained work outcome and `SearchStoppingReason`, not
accepted independently from the caller:

- `complete`: `no_next_page` or `price_partitions_exhausted` on successfully
  completed work;
- `bounded`: a configured pages, results, duration, bytes, or no-new-results
  limit stopped otherwise successful work;
- `incomplete`: failed/cancelled work, `missing_cursor`, `repeated_cursor`, or
  unavailable transfer accounting; and
- `unknown`: legacy evidence cannot support the classification.

`absence_comparison_valid` is a derived convenience value and is true exactly
when coverage is `complete`; model validation rejects any contradictory pair.
First/last-seen UTC values come from the completion time of the search-page
acquisition that supplied the occurrence. Legacy records without that time
return null. Completion-sequence ordering remains authoritative even when a UTC
value is absent.

Analyses are selected across all retained observations of a listing, not only
the observation supplying current details. `ListingAnalysisEvidenceSet`
already records the exact listing observation and gallery records consumed by
an analysis. Initially, a completed analysis selected for the requested product
guide has applicability `assumed`. Later comparison can use per-component
fingerprints and the retained evidence set to distinguish:

- `exact`: every relevant current input is demonstrably unchanged;
- `assumed`: the permissive initial reuse policy applies;
- `stale`: at least one relevant input demonstrably changed; and
- `indeterminate`: retained evidence is insufficient for a safe comparison.

Missing fields in a partial source are not changes. A search card without a
description does not remove the description, and an incomplete gallery does
not prove image removal. Gallery comparison should prefer retained source media
identities and artifact hashes over temporary delivery URLs, and should
distinguish additions, removals, replacements, and order changes.

### Initial typed contract

The following excerpt reflects the implemented public model. The source remains
authoritative, but the semantic distinctions and boundedness are part of the
contract.

```python
class ListingStatus(JsonStringEnumeration):
    AVAILABLE = "available"
    PENDING = "pending"
    SOLD = "sold"
    UNAVAILABLE = "unavailable"
    UNKNOWN = "unknown"


class ProjectionSourceKind(JsonStringEnumeration):
    SEARCH_CARD = "search_card"
    PDP_PROBE = "pdp_probe"
    ITEM_PAGE = "item_page"
    IMAGE_DOWNLOAD = "image_download"


class ProjectionEvidence(StrictModel):
    evidence_record_identifier: str = Field(min_length=1)
    observation_record_identifier: str | None = Field(default=None, min_length=1)
    acquisition_record_identifier: str | None = Field(default=None, min_length=1)
    producing_operation_identifier: str | None = Field(default=None, min_length=1)
    acquisition_completion_sequence: int = Field(ge=0)
    observation_completion_sequence: int = Field(ge=0)
    observed_at_utc: str | None
    completed_at_utc: str | None
    source_kind: ProjectionSourceKind
    warnings: tuple[str, ...] = Field(max_length=20)


class ComposedField(StrictModel):
    value: JsonValue
    evidence: ProjectionEvidence


class ComposedStatus(StrictModel):
    value: ListingStatus
    raw_flags: JsonValue
    evidence: ProjectionEvidence | None


class ComposedGalleryImage(StrictModel):
    descriptor: ProjectionGalleryImageDescriptor
    evidence: ProjectionEvidence | None


class ComposedGallery(StrictModel):
    referenced_image_count: int = Field(ge=0)
    saved_image_count: int = Field(ge=0)
    all_referenced_images_saved: bool
    images: tuple[ComposedGalleryImage, ...] = Field(max_length=100)
    images_truncated: bool
    reference_set_truncated: bool
    reference_set_evidence: ProjectionEvidence


class ComposedPreviewImage(StrictModel):
    descriptor: ProjectionGalleryImageDescriptor
    evidence: ProjectionEvidence


class AnalysisApplicability(JsonStringEnumeration):
    EXACT = "exact"
    ASSUMED = "assumed"
    STALE = "stale"
    INDETERMINATE = "indeterminate"


class ComposedAnalysis(StrictModel):
    descriptor: AnalysisDescriptor
    applicability: AnalysisApplicability
    applicability_reasons: tuple[str, ...] = Field(max_length=10)


class AnalysisPresenceFilter(JsonStringEnumeration):
    ANY = "any"
    PRESENT = "present"
    ABSENT = "absent"


class SearchComparisonCoverage(JsonStringEnumeration):
    COMPLETE = "complete"
    BOUNDED = "bounded"
    INCOMPLETE = "incomplete"
    UNKNOWN = "unknown"


class SearchMembershipProjection(StrictModel):
    lineage_root_search_run_record_identifier: str | None
    selected_search_run_record_identifier: str
    oldest_included_search_run_record_identifier: str
    included_ancestry_run_count: int = Field(ge=1)
    older_ancestry_truncated: bool
    first_seen_search_run_record_identifier: str
    last_seen_search_run_record_identifier: str
    first_seen_at_utc: str | None
    last_seen_at_utc: str | None
    seen_in_selected_run: bool
    seen_run_count: int = Field(ge=1)
    selected_run_stopping_reason: str | None
    comparison_coverage: SearchComparisonCoverage
    absence_comparison_valid: bool
    warnings: tuple[str, ...] = Field(max_length=20)


class ComposedListingProjection(StrictModel):
    listing_identifier: str = Field(min_length=1)
    canonical_source_url: str = Field(min_length=1)
    as_of_completion_sequence: int = Field(ge=0)
    status: ComposedStatus
    title: ComposedField | None
    price: ComposedField | None
    location: ComposedField | None
    description: ComposedField | None
    seller: ComposedField | None
    preview_image: ComposedPreviewImage | None
    gallery: ComposedGallery | None
    analyses: tuple[ComposedAnalysis, ...] = Field(max_length=20)
    analyses_truncated: bool
    search_membership: SearchMembershipProjection | None
    warnings: tuple[str, ...] = Field(max_length=20)


class ComposedListingFilters(StrictModel):
    statuses: Sequence[ListingStatus] = Field(
        default=(ListingStatus.AVAILABLE,), min_length=1, max_length=5
    )
    product_guide_record_identifier: str | None = Field(default=None, min_length=1)
    analysis: AnalysisPresenceFilter = AnalysisPresenceFilter.ANY

    @field_validator("statuses")
    @classmethod
    def validate_unique_statuses(cls, value: Sequence[ListingStatus]) -> Sequence[ListingStatus]:
        if len(set(value)) != len(value):
            raise ValueError("Listing status filters must be unique")
        return value


class GetComposedListingRequest(StrictModel):
    listing_identifier: str = Field(min_length=1)
    search_run_record_identifier: str | None = Field(default=None, min_length=1)
    product_guide_record_identifier: str | None = Field(default=None, min_length=1)
    maximum_ancestry_runs: int = Field(default=100, ge=1, le=100)
    maximum_gallery_images: int = Field(default=20, ge=0, le=100)
    maximum_analyses: int = Field(default=10, ge=0, le=20)


class ListComposedSearchRequest(StrictModel):
    search_run_record_identifier: str = Field(min_length=1)
    filters: ComposedListingFilters = ComposedListingFilters()
    maximum_ancestry_runs: int = Field(default=100, ge=1, le=100)
    maximum_gallery_images_per_listing: int = Field(default=0, ge=0, le=10)
    maximum_analyses_per_listing: int = Field(default=1, ge=0, le=5)
    maximum_candidate_listings_examined: int = Field(default=2_500, ge=1, le=10_000)
    page_size: int = Field(default=25, ge=1, le=100)
    cursor: str | None = None


class ComposedListingPage(StrictModel):
    as_of_completion_sequence: int = Field(ge=0)
    selected_search_run_record_identifier: str
    included_ancestry_run_count: int = Field(ge=1)
    older_ancestry_truncated: bool
    examined_candidate_listing_count: int = Field(ge=0, le=10_000)
    candidate_examination_limit_reached: bool
    listings: tuple[ComposedListingProjection, ...] = Field(max_length=100)
    next_cursor: str | None
```

MCP request collections use `Sequence[...]` so ordinary JSON arrays validate
under strict models. Returned collections remain immutable tuples. Exact model
fields should use the repository's established timestamp and identifier types
when implemented rather than introducing multiple encodings for the same
concept.

The request's lineage, gallery, analysis, page, warning, and reason limits are
hard bounds, not hints. Cursors retain the completion boundary, lineage bound,
filter identity, and nested limits so later pages cannot silently change the
scope. When ancestry is truncated, first/last-seen values are explicitly scoped
to the included runs and the lineage-root identifier remains null unless the
root is actually included.

Candidate examination is also bounded per call. The indexed query considers at
most `maximum_candidate_listings_examined` distinct listing IDs after the
cursor, applies composition and filters, and returns up to `page_size` matches.
The next cursor advances past every examined candidate, not merely returned
matches. `candidate_examination_limit_reached` distinguishes an underfilled
page caused by the work bound from exhaustion of the selected lineage. Callers
requesting another status set traverse it explicitly; the initial contract
does not hide an unbounded whole-lineage count behind a summary field.

When no product-guide identifier is supplied, analysis presence means any
completed analysis and output selects the newest completed analysis per guide,
subject to the request's total analysis limit. When a guide is supplied, both
filtering and output consider only that exact guide. A zero output limit does
not change analysis-presence filtering.

Exact-listing and collection reads both bound gallery and analysis output.
`referenced_image_count` and `images_truncated` describe omitted gallery
descriptors, while `all_referenced_images_saved` refers only to the selected
observation's retained gallery-reference set. It does not claim that Facebook
returned a complete real-world gallery. Analysis selection remains explicitly
bounded and reports truncation.

## Status collection strategy

### Tier 0: search-card observations

Every search refresh already downloads result cards for the listings it
actually encounters; it does not guarantee coverage of every older listing.
Their retained original
objects include `is_sold`, `is_pending`, and `is_live`, so extraction can emit a
fresh status observation for each occurrence without another Facebook request.
They also supply fallback title, location, seller, and preview-image evidence
for listings whose item pages have not been collected, and a newly observed
card price can supersede an older item-page price. This is the default and
cheapest monitoring path, but omitted listings retain their older evidence and
its original observation age.

A local measurement on 2026-09-25 found those three flags on all 6,195 retained
search occurrences. Every observed card in that data was active, however, so
the measurement establishes that appearance can provide useful status evidence;
it does not establish that sold or pending listings remain searchable. The
extractor must tolerate missing or changed fields and preserve warnings instead
of assuming that this private response shape is permanent.

### Tier 1: lightweight exact-listing probe

For a monitored listing that is absent from a completed refresh or whose status
has exceeded its freshness policy, Carl should try an exact-ID status probe
before downloading the full item HTML. The candidate approach is to establish
the existing anonymous Facebook session, obtain the route definition for
`/marketplace/item/{id}/`, and replay the session's
`MarketplacePDPContainerQuery`. Gallery media is requested separately in the
observed protocol, so the container query may provide status at substantially
lower cost than the complete document.

This tier remains an experiment until live measurements confirm its response
size, reliability, required variables, challenge behavior, and ability to
distinguish active, pending, sold, unavailable, and inaccessible listings.
Query identifiers and session material remain ephemeral implementation detail.

### Tier 2: full item-page fallback

Use the existing full item-page acquisition when the lightweight probe is
ambiguous or blocked, when its schema has drifted, or when rich detail itself
needs refreshing. Login and challenge responses are failures, not status
observations.

The expected savings are material. In a 2026-09-25 local sample, 1,457 item
HTML responses with transfer measurements used approximately 148–187 kB each
after content encoding, with a mean of about 164 kB; decoded bodies averaged
about 485 kB over a larger set. Search responses amortize their transfer over
many cards, making status extraction from an already-requested search
effectively free. These measurements guide the design but are not performance
guarantees.

## Monitoring saved search results

A monitoring pass over a retained search will:

1. Run the existing bounded refresh and retain its search acquisitions and
   occurrences.
2. Emit status observations for every result card that supplies status fields.
3. Compare the completed refreshed result set with the explicitly bounded
   monitored ancestry.
4. Report which prior IDs are absent from this exact traversal through factual
   run-membership comparison, without creating a new listing state or changing
   their last evidence-backed status.
5. Schedule a lightweight exact-listing probe only when the monitoring policy
   requires fresher status evidence.
6. Fall back to a full page only under the Tier 2 conditions above.
7. Publish a composed-projection summary with changed, stale, unknown, and
   probe-needed counts, plus bounded examples and drill-down identifiers.

Status age is independent from detail age. A recent search card may update an
old detail observation's status without causing another detail-page request.
Conversely, a rich detail observation may remain useful while its status is
old. A later policy may define separate time-to-recheck values for currently
available, pending, sold, unavailable, and unknown listings. Terminal states
may be checked less often, but remain reversible when later source evidence
disagrees.

Overlapping or incomplete search traversals need explicit coverage metadata.
Only a successfully completed scope supports a meaningful membership
comparison, and even then absence cannot become `sold`. Rate limits,
challenges, cancelled work, truncated result bounds, and failed partitions make
the comparison incomplete and must be reported as such.

## Proposed user-facing operations

The implemented MCP tools are `get_composed_listing` and
`list_composed_search`; the CLI exposes the corresponding hyphenated commands.
The complete high-level surface should support:

- get the composed projection for one exact listing ID;
- list composed results for one retained search or refresh lineage with stable
  pagination and bounded filters;
- preview and request a status-monitoring pass;
- inspect monitoring progress and a compact change summary; and
- follow returned record identifiers into the existing evidence/provenance
  interfaces when deeper investigation is needed.

Routine operations should return compact typed fields, not raw retained source
objects. Mutating operations remain durable queued work and should expose their
work identifiers immediately. Preview and request operations must share the
same selection policy so a caller can understand request cost before enqueueing
network work.

These are high-level operations rather than silent semantic changes to the
older `list_candidates` or `get_listing_dossier` application methods. Those
evidence-oriented methods are no longer exposed through MCP; the agent-facing
contract uses normalized status and component-level provenance deliberately.

## Storage and query considerations

- Store status observations as ordinary immutable records with typed operation
  inputs and outputs.
- Add narrow indexes for listing ID, source search/refresh, observation time,
  normalized status, and the relationships needed by the composed projection.
- Push exact-listing selection, search-lineage membership, filtering, bounded
  summaries, and pagination into SQLite. The current review path loads the
  global usable observation set into memory and is not an acceptable
  implementation for this bounded interface.
- Keep eligibility, precedence, and tie-breaking policy as pure `carl.core`
  functions. The SQLite adapter performs indexed bounded retrieval and applies
  or compiles those policies; storage layout does not become the authority for
  composition semantics.
- Use the documented completion-sequence ordering and stable record-identifier
  tie-break when two usable observations share the same completion keys.
- Materialize a projection only if measured query cost justifies it. If one is
  materialized, make it reproducible from retained evidence and update it in the
  same durable publication boundary as its source observation.
- Do not hide correctness behind a process-local cache. Small request-scoped
  memoization is acceptable after indexed lookups are bounded and measured.
- Keep result limits and pagination mandatory for collection views.

## Delivery stages

1. **Initial read path delivered:** derive typed status candidates from retained
   search cards and full item observations, including normalization and
   precedence. Dedicated persisted status-observation records remain part of
   the monitoring write path.
2. **Delivered:** indexed component selectors, explicit refresh-lineage
   membership queries, and read-only MCP/CLI composed projections for exact
   listings and retained search results.
3. Add monitoring summaries and durable scheduling based on independent status
   freshness policies.
4. Prototype and measure the exact-listing PDP probe against active, pending,
   sold, unavailable, login, challenge, and schema-drift fixtures; enable it
   only if it is reliably cheaper than full HTML.
5. Add the full-page fallback and operational metrics for request count, bytes,
   classifications, ambiguous results, and avoided item-page acquisitions.
6. **Partly delivered:** composed listings expose stable per-component
   revisions for review staleness. Add product-guide dependency declarations,
   then replace permissive `assumed` analysis applicability where the retained
   inputs support an exact comparison.

## Acceptance criteria

- A search result with status flags produces a traceable status observation
  without an item-page request.
- Status precedence is tested for conflicting raw flag combinations.
- A listing missing from a later search retains its last explicit status and
  exact run-membership history; it never becomes sold solely because it
  disappeared.
- The composed projection can combine a new status observation with older
  usable detail, gallery, and analysis components while showing the independent
  timestamps and provenance of each.
- Collection views default to `available` and accept explicit nonempty status
  sets that can include pending, sold, unavailable, and unknown listings.
- An older completed analysis is returned with applicability `assumed` and its
  actual input observation and evidence-set identifiers.
- Old reusable detail does not prevent an independently stale status from being
  scheduled for a check.
- Failed, partial, challenged, or bounded search traversal cannot create false
  absence evidence.
- Search overlays report traversal coverage and distinguish exact set
  non-membership from whether that absence is meaningful for comparison.
- Exact-listing, paginated composed-search, preview, and monitoring-status
  responses are bounded and do not load all raw evidence or all usable listing
  observations into application memory.
- Long refresh ancestry and nested gallery, analysis, warning, and reason
  collections have explicit limits and truncation metadata.
- Status monitoring reports item-page requests and transferred bytes avoided,
  so the intended bandwidth and proxy-usage benefit can be verified.

## Open decisions

- The precise freshness policy and whether it is configured per saved search,
  listing state, or both.
- The evidence and policy required to conclude that a listing is gone, and how
  that reversible policy conclusion is represented separately from explicit
  source status.
- The minimum trustworthy PDP-probe response and fallback threshold.
- Whether measured query cost eventually justifies a rebuildable materialized
  projection; the first implementation is computed directly with indexes.
- Retention and presentation policy for contradictory status observations from
  different source scopes at nearly the same time.
- Whether a saved-search identity should explicitly group branches or repeated
  fresh runs beyond one selected run's refresh ancestry.
- Whether a later guide-context projection should expose bounded alternate
  analyses in addition to the initial newest-per-guide selection.
