# Carl

Carl is a local evidence store and work queue for scraping and reviewing marketplace listings.
It currently has Facebook Marketplace and eBay adapters. It retains acquisitions, extracted
observations, images, and analysis reports, and assembles them into source-linked listing views.

The main interface is MCP. A client can request searches, queue follow-up work, inspect retained
evidence, and maintain review workspaces. A separate worker process executes queued collection,
extraction, and analysis. The CLI also provides acquisition, reprocessing, inspection, and monitoring
commands.

## Evidence and derivation

Carl separates acquiring evidence from interpreting it:

1. **Acquisition** sends HTTP requests through a configured transport and network route. It retains
   response metadata and collected content, including failed or challenged responses where available.
   A search result, an item page, and an image request are separate acquisitions.
2. **Extraction** turns retained content into versioned records: search occurrences, listing
   observations, gallery references, and other source-specific fields. Extraction can run again on
   saved evidence without fetching the source again.
3. **Composition** selects usable evidence for a listing's current read model. It does not rewrite
   the acquisitions or observations that supplied the values.
4. **Analysis**, when requested, gives Claude an exact retained evidence selection and a versioned
   product guide. Its report identifies the item, compares seller claims with visible evidence and
   researched specifications, describes condition and missing parts, and records uncertainty.

Records and artifacts are linked by named operation inputs and outputs. Operations retain the
component and output-schema version, code and runtime provenance, configuration, and execution
status. Later observations and derivations add history rather than replace earlier evidence.
Identical content may share storage without collapsing the separate acquisition histories.

Metadata, JSON records, and ordinary response bodies live in SQLite. Validated images use
database-relative, content-addressed files. Retained response bodies are collected, decoded content,
not packet captures; transport redaction, response limits, and incomplete downloads affect what is
available. The database and image files together form the evidence store.

## Composed listing views

Carl calls the final read model a **composed projection**. Its components are selected independently
from layered evidence; they need not have been observed at the same time.

- Descriptive fields prefer usable item-page evidence, with search cards filling gaps.
- Price uses the newest usable price observation from a search card or item page. A newly observed
  card price can therefore coexist with an older description.
- Status comes from explicit retained status evidence. Absence from a search is not evidence that an
  item sold or became unavailable.
- The gallery uses a coherent retained reference set, not an arbitrary union of photos from
  different observations. A search thumbnail is not a collected gallery.
- Completed analyses retain their own input observation and guide version. An older analysis can
  remain visible with an applicability label; that does not mean it analyzed the newest evidence.
- Search membership adds first/last-seen and refresh-lineage information for the selected context.

Selected components carry evidence references and observation/completion times. Missing values do
not erase older usable evidence, and re-extracting an old response does not make its acquisition
fresh. Reading a projection performs no acquisition or AI analysis.

Workspaces combine related search tracks, including tracks from both marketplaces, into a
deduplicated listing set. Tracks have stable identities and versioned search specifications;
revising a watch preserves its history. Review state compares normalized, review-relevant component
values, so an identical re-observation does not by itself make a review stale. Worksets and selection
snapshots preserve explicit groups for later review or analysis.

## Scraping and follow-up work

Acquisition uses HTTP transports, including wreq browser-profile emulation; it does not launch a
Chrome browser. Network routes and acquisition stacks select the configured proxy/VPN provider and
transport. Routes can differ between search, item pages, and images. The currently supported setup
and route defaults are documented in the [setup and usage guide](docs/src/project/setup-and-usage.md).

Search-only requests retain search results. They do not automatically fetch every item page or
download every photo. An explicit search pipeline can request bounded follow-up through details,
images, or analysis. Work progresses per listing: one listing can become ready for analysis while
another is still collecting images. Fixed-selection analysis can also operate on retained workspace
evidence after previewing the selection.

Overlapping requests can share acquisition work, and follow-ups reuse usable retained item pages
and validated matching images. This is evidence reuse, not remote unchanged-page detection. An
ordinary search refresh may reuse item details without checking whether the remote item page changed;
explicit item-detail refresh is a separate request. Reused evidence keeps its original age.

## Running Carl

From a checkout, with uv and Python 3.13 or newer:

```console
uv sync --locked --all-groups
uv run --locked carl locations
uv run --locked carl init
```

`locations` reports the per-user configuration, database, image, and diagnostic paths. `init` creates
or migrates the evidence database. It does not generate network configuration or install credentials.
Configure the required routes before requesting live collection; install and authenticate Claude CLI
in the worker's environment before requesting analysis. See the
[setup and usage guide](docs/src/project/setup-and-usage.md) for configuration, credentials, database
selection, MCP client launch, and upgrade instructions.

The processes have separate roles:

| Command | Role |
| --- | --- |
| `uv run --locked carl mcp` | MCP stdio server: handles client requests and reads retained state; does not run workers. |
| `uv run --locked carl work` | Processes the durable work queue until interrupted. |
| `uv run --locked carl monitor` | Displays activity without processing the queue. |
| `uv run --locked carl monitor --work` | Runs workers and displays activity in the same process. |

Normally, keep one worker process running and let the MCP client launch its server separately.
Both must use the same evidence database and per-user configuration. Multiple MCP connections do
not create additional worker pools. A queued work ID acknowledges a request, not completion; inspect
root and child outcomes before treating collection or analysis as successful.

Workers lease persisted work, apply scheduling constraints, and retain checkpoints and attempt
history. Temporary failures have bounded retries; detected Internet outages pause acquisition
without consuming outage retry budgets. Retained reads remain useful without a worker. Recovering
terminal work is explicit, and downstream recovery can reuse a completed search rather than rerun it.

The [agent skill](skills/carl/SKILL.md) describes the MCP workflow and evidence/identifier conventions.
The server also publishes tool schemas, instructions, identity, and capability versions for connected
clients. Unexpected MCP failures return a redacted diagnostic and are logged locally; handler errors
retain structured validation details and the latest durable checkpoint.

## Limits and interpretation

Searches are bounded observations, not exhaustive marketplace inventories. Anonymous acquisition
can encounter challenges, rate limits, layout changes, or incomplete responses. Facebook's search
radius is not a reliable hard geographic filter. New eBay sold/completed searches are currently
disabled because the acquisition path does not support their sign-in/challenge gating; retained
closed-listing evidence remains readable.

An absent listing, an empty result, an idle queue, and successful collection are different states.
Read the response classifications, coverage warnings, budgets, and child failure counts. Incomplete
galleries require an explicit analysis override and remain marked incomplete.

Product guides currently contain identification instructions, not typed field schemas. Analysis
reports are prose, and external research references are reported by the analysis agent rather than
independently archived source pages. Seller claims, visible facts, researched specifications, and
later judgments about value should remain distinguishable. General SQL queries, typed analysis
fields, and saved queries are [planned work](https://github.com/altendky/carl/issues/39), not current
interfaces.

## Development and design

```console
mise run check
mise run pre-commit
```

The implementation separates sans-I/O parsing and models in `carl.core`, effect adapters in
`carl.io`, and application orchestration. Async work uses AnyIO with Trio. See the
[architecture notes](docs/src/project/index.md),
[composed projection design](docs/src/project/current-listing-view-plan.md), and
[review workspace design](docs/src/project/review-workspaces.md) for implementation details and
explicitly marked future work.

Carl is under active development and is available under the MIT or Apache-2.0 license.
