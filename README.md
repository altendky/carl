# Carl

Carl collects web evidence and derives useful, traceable information from it.
Facebook Marketplace is its first source, but collection, extraction, and later
assessment are separate components.

Async code uses AnyIO with Trio as the selected backend.

The first vertical slice runs a Marketplace item request through the durable
work queue and an injected HTTPX transport, stores the complete synthetic
response in SQLite, atomically queues extraction, and can repeat extraction
without another network request. The live item command uses the same route-aware
durable worker path.

Marketplace search intent also has a typed durable input: query, requested
location, explicit radius and unit, optional currency and decimal price bounds,
exact-match choice, bounded traversal policy, and network path. The pure
traversal reducer records page, unique-listing, elapsed-duration, byte, repeated
cursor, and empty-page stopping decisions. Pure session-bootstrap,
route-definition, and pagination protocol extraction is implemented against
synthetic fixtures and retained proof-of-concept responses. The search worker
keeps the Marketplace root request, route-definition request, and GraphQL
pagination requests in one Proton tunnel and HTTP cookie jar, checkpoints every
complete response and its extraction before continuing, and stops at the
configured bounds.

Mullvad and Proton routes run a verified `wireproxy` process feeding a generic
local SOCKS5h HTTPX transport. They use private temporary configurations rebuilt
from allowlisted WireGuard fields, lock a device identity across processes,
probe egress through the proxy, and record safe route observations. Each adapter
executes only its assigned route. Future flow policies may explicitly choose
among eligible routes or start another attempt using an ordered fallback.
Proton consumes a dedicated exported WireGuard configuration; it does not
control the desktop VPN application or alter host routing.

Decodo residential and mobile proxy routes use the provider's authenticated
HTTP gateway. Carl requests a fresh sticky session in the configured country
for every independent item-page acquisition, while redirects and cookies inside
that acquisition stay in one HTTPX client. Decodo is the default item-page
route; Mullvad remains an explicit override. Search and image collection
continue to use Proton.

Bright Data native proxy routes use an explicit product, account, zone, gateway,
port, and base proxy username. A managed session holds one sticky constant peer
and one in-memory cookie jar for a related bootstrap and pagination sequence.
The proxy password, provider session token, cookies, and `Set-Cookie` values do
not enter ordinary evidence. The safe account, username, and credential
references do. Web Unlocker,
Scraping Browser, and scraper APIs are separate future adapters rather than
values of the native proxy configuration.

The provider adapters have deterministic transport, failure, cancellation, and
secret-redaction tests. A bounded initial Marketplace search page has been
verified through Proton. Cursor pagination received an explicit Facebook
rate-limit error in its first bounded live attempt, so end-to-end pagination
remains unverified. Bright Data detail retrieval has not yet been verified by
Carl.

Every request in a search session is now a separate durable network activity
admitted by the same SQLite-backed constraint engine used for queued work. The
flow retains ownership of its HTTPX client, cookies, and Proton tunnel while it
waits. The initial search policy used a random 2–5 second holdoff and a
12-per-minute limit. Future search and item-page collections activate a
versioned policy of at most three starts per second and 180 per minute on both
the route and `www.facebook.com`, with a 0–100 ms holdoff for follow-up and
item-page requests. Admission, sampled delay, rate reservation,
completion, failure, and cancellation are retained. Facebook GraphQL error code
1675004 is classified explicitly as rate limiting. A bounded live retest
confirmed the delays were applied, but the tested Proton exit still received
1675004 on its first pagination request.

Carl uses standard per-user directories selected by `platformdirs`. Inspect the
effective locations, including the database and image directory, with:

```console
uv run carl locations
```

Watch the durable queue, active leases, recent completions and failures, and
network activity grouped by its ordered Proton, Decodo, or other configured
path with:

```console
uv run carl monitor
```

The display refreshes every five seconds. Use `--once` for a static snapshot,
or adjust `--refresh-interval-seconds`, `--recent-window-minutes`, and
`--maximum-rows`. Add `--work` to run the bounded worker pool for exactly the
monitor process's lifetime. The monitor reports durable active leases, network
admissions, and processes sharing the database.

Process the durable queue independently of any interactive client with:

```console
uv run carl work
```

Keep this foreground process running while collection or analysis is expected to progress. It owns
a bounded worker pool and cleanly releases process resources on cancellation. `carl monitor --work`
combines the same pool with the live dashboard. MCP servers queue and inspect work but never start
workers implicitly, so leaked or intentionally concurrent MCP clients cannot multiply worker pools.
Concurrency limits are registered in SQLite across explicit worker processes. Pending work and
expired leases remain in SQLite and are recovered when a worker process runs again.

Safe route configuration lives in `config.toml` under the reported
configuration directory. Proxy passwords remain outside ordinary configuration
and provenance. Configure a Decodo password through a private, non-echoing
prompt with:

```console
uv run carl configure-decodo-credential carl
```

Import a dedicated Proton WireGuard
configuration into Carl's private configuration area with:

```console
uv run carl import-proton-config \
  ~/Downloads/carl.conf \
  carl
```

The import validates the configuration, never changes the source, refuses to
overwrite an existing logical ID, and does not print private file contents or
locations.

Import a Mullvad all-relays WireGuard bundle in the same way:

```console
uv run carl import-mullvad-config \
  ~/Downloads/mullvad_wireguard_linux_all_all.zip \
  carl
```

This repository is under active design. See
[`docs/src/project/index.md`](docs/src/project/index.md) for the current
architecture and settled decisions.

## Development

Install the locked environment and run the checks:

```console
uv sync --all-groups
mise run check
mise run pre-commit
```

The default pre-commit suite includes blocking secret, repository-file,
workflow, Markdown, and internal-link checks. Run the bounded external-link check
separately with:

```console
pre-commit run lychee-external --all-files --hook-stage manual
```

Initialize the default local database:

```console
uv run carl init
```

Set `FACEBOOK_LOCATION_ID` to the Marketplace location identifier to search,
then run a bounded search using the configured Proton route:

```console
uv run carl search telescope \
  --facebook-location "$FACEBOOK_LOCATION_ID" \
  --radius 60 \
  --radius-unit miles \
  --maximum-price 600 \
  --maximum-results 50 \
  --maximum-pages 4 \
  --proton-route united-states-free-54-ipv4
```

Use `--proton-route IDENTIFIER` to select another explicitly configured Proton
route. The default route identifier is `carl`; route selection remains a flow
choice rather than fallback behavior inside a network transport.

The command creates durable queue work and immediately prints its work identifier and pending
state. Run `carl work` or `carl monitor --work` separately to process it; the command never opens a
competing Proton session or private worker. The search session first loads the Marketplace
root through the same route and HTTP client, preserving that acquisition,
embedded JSON, response classification, and anonymous cookies. It then submits
the relative search URL to Facebook's route-definition endpoint and correlates
the returned preloader metadata with the matching framed result. Each result
page is stored as its own HTTP
acquisition with the raw body, decoded body, embedded JSON or GraphQL response,
listing occurrences, extractor identity, and links to the originating work
operation. Pagination is cursor based and sequential; `--maximum-pages` counts
the initial result page, while `--maximum-results` counts unique listing IDs.
Either limit may be used alone, and pagination stops when the first configured
limit is reached. Carl preserves the whole final response page, so its retained
unique count may exceed the requested result limit if Facebook returns more
items than requested. Repeated listing IDs contribute separate observations but
only once to the result count. There is no offset-based random access.

Anonymous cursor pagination is currently rejected by Facebook on the tested
Proton exits. A bounded alternative issues independent first-page searches over
fixed, overlapping price intervals in one session:

```console
uv run carl search telescope \
  --facebook-location "$FACEBOOK_LOCATION_ID" \
  --radius 60 \
  --maximum-price 600 \
  --maximum-results 50 \
  --maximum-pages 8 \
  --price-partition-width 100 \
  --price-partition-overlap 5 \
  --proton-route united-states-free-54-ipv4
```

The width and overlap use the requested currency. Partition traversal requires
a finite maximum price; an omitted minimum becomes zero. Each interval request,
its requested bounds, Facebook's applied configuration, and every duplicate
listing occurrence are retained. Results are deduplicated only by exact listing
ID for limit accounting. An interval reporting another page is marked
saturated, so completion of all planned intervals does not claim exhaustive
coverage. Intervals use deterministic balanced ordering by default so a result
limit samples across the requested price span; use
`--price-partition-order ascending` only when lower prices should be visited
first.

The internal direct HTTPX adapter exists for deterministic fixtures and bounded
development experiments. It is not an allowed production route. Once an
acquisition record exists, repeat extraction without network access using its
record identifier:

Collect and immediately extract one item page through the default configured
Decodo route:

```console
uv run carl collect \
  https://www.facebook.com/marketplace/item/1234567890123456/ \
  --network-route carl
```

The command exits unsuccessfully when the retained response is a login page,
challenge, generic error, malformed response, or lacks the requested listing.
The acquisition and diagnostic extraction outputs remain stored in those
cases.

Follow up one or more retained searches with item-page collection:

```console
uv run carl collect-search-items \
  SEARCH_RUN_RECORD_IDENTIFIER_ONE \
  SEARCH_RUN_RECORD_IDENTIFIER_TWO \
  --network-route carl
```

The command unions candidates by exact Facebook listing ID and reuses the
latest semantically usable retained item-page result. It sends a new request
only for an ID without such a result. Repeated search observations and the
requester relationship from every contributing search run remain retained.
Failures such as login pages do not replace an older usable result and remain
eligible for a later retry when no usable result exists.

Collect a bounded number of missing gallery-image renditions from saved item
pages through Proton:

```console
uv run carl collect-images SEARCH_RUN_RECORD_IDENTIFIER --maximum-images 10
```

The command chooses the latest usable item page per exact listing ID, retains
each selected gallery reference and its source path, and first reuses a
previously validated image with the same Facebook photo ID when its observed
dimensions meet or exceed the new declared dimensions. The reuse operation
links the new reference to the existing image result; it preserves the new
signed URL without representing it as fetched. Missing photo IDs or dimensions
require an exact signed-URL match. Only unresolved references enter the request
pool. The command never rewrites the signed URL or falls back to a direct
connection. The initial durable image policy permits one
active request and at most 12 starts per minute across Facebook CDN hosts,
with a separately recorded random 2–5 second holdoff. The current image policy
supersedes those retained limits: one durable image work item may open a Proton
session at a time across all Carl processes, preventing workers from racing for
the exclusive WireGuard device and port-allocation locks. Network admission also
caps starts at three per second and 180 starts per minute across CDN hosts and
records a 0–100 ms randomized holdoff. The 180/minute path budget is shared with
other traffic on that Proton route; Marketplace page requests use a separate
three-per-second, 180-per-minute `www.facebook.com` budget. Eight workers pull
from the durable queue, with progress
reported every 100 completed jobs. The maximum defaults to 10 new network requests
per invocation; repeated invocations reuse verified image
files and continue with the next missing renditions. Carl retains response
metadata for a failed response, discards its unvalidated bytes, and does not
count it as a saved image. A successful response is content-decoded, validated,
and stored once in its original image format under a database-relative,
content-addressed `images/sha256/` path. SQLite records the artifact, hash,
headers, dimensions, request, route, and provenance without storing another
copy of the image body. This happens within `collect_image`; `extract_image` is
retained for offline reprocessing of legacy saved response bodies.

Transient Proton session startup, cleanup, and lock-contention failures retry
automatically with backoff and retain their failure code, diagnostic, and exit
status. To requeue older terminal image failures linked to a retained search run
or refresh, use:

```console
uv run carl retry-image-failures SEARCH_RUN_OR_REFRESH_IDENTIFIER
```

The command reports how many matching failures were found, requeued, and left
terminal after the requested cap. Run `carl work` or `carl monitor --work` to process them.

Move legacy validated image BLOBs to that layout with:

```console
uv run carl migrate-image-files
```

The migration preserves old artifact identifiers and links both legacy image
representations to the same file. It removes unreferenced inline content rows,
but does not compact SQLite's freed pages.

Rerun image validation against one saved HTTP acquisition without network
access:

```console
uv run carl extract-image IMAGE_ACQUISITION_RECORD_IDENTIFIER
```

Identify saved listings with the installed Claude Code CLI, without making a
Facebook request:

```console
uv run carl analyze-items --product-guide telescope --maximum-items 1 --model claude-sonnet-5
uv run carl analyze-items LISTING_ID --product-guide telescope --maximum-items 1 --model claude-opus-5 --effort medium
uv run carl analyze-items --product-guide telescope --maximum-items 100 --worker-count 10
```

The command considers the latest full item page per exact listing ID and
requires every gallery image in that observation to have a verified saved file.
It stages the title, description, approximate location, asking price, source
attributes and availability in a private temporary directory and links its
ordered images into that directory with hard links, without copying their bytes. Claude receives
a common identification and product-research prompt plus an explicitly selected product guide,
read-only access to that directory, and the WebSearch and WebFetch tools. Research begins only when
listing evidence identifies a specific,
researchable item or narrow set of variants. It prefers manufacturer literature, checks variant
differences, establishes useful specifications and expected accessories, and compares them with
what the seller claims or the photos show. Research continues while additional sources narrow the
identity or add model-specific evidence, and stops when repeated attempts yield only generic,
duplicative, or conflicting material. The initial policy aims for roughly 90 seconds, permits up to
three combined search and fetch calls, and uses a 150-second process deadline so a nearly completed
report can finish. Reports are capped at 1,200 words to keep the first pass useful and fast. Carl
sizes the safety turn ceiling from the number of gallery images and records it as analysis
configuration. Claude output is captured as NDJSON so actual WebSearch and WebFetch tool-use
events can be counted. Every analysis records structured observations for the 90-second target,
150-second deadline, turn ceiling, web-tool-call budget, and report-word budget. A hard deadline or
turn exhaustion fails the attempt; advisory overruns retain the report with warnings. Carl retains
one listing-analysis evidence-set object whose producing operation has typed input edges to the
selected listing observation and ordered gallery-reference/image-result pairs. The evidence-set
record itself is empty, so it does not repeat those edges. The upstream records trace
to the original body entry, image acquisition, validated file, hash, MIME type, dimensions, and
gallery order without copying that metadata into the analysis. Carl separately retains the exact
input manifest and prompt presented at the model boundary, stdout, stderr, Claude version and argv,
timing, exit status, and response text. The requested model and effort are always
explicit in Claude's argv; Carl also retains the CLI version and Claude's exact
output metadata, including reported model usage. Each product guide is registered once as a
structured identity/version record with an exact UTF-8 text artifact. The analysis operation has a
typed input edge to that guide record. Re-registering identical text reuses the record; changing
text under the same identity and version fails instead of silently changing meaning. The exact
composed prompt remains separate model-boundary evidence. A completed analysis of the same evidence
set, guide, and analysis configuration is skipped on later
runs. The command defaults to one item so a broad
run requires an explicit `--maximum-items` value. `--product-guide` is required so guidance is not
silently applied to the wrong kind of item; `telescope` is the first guide. Use `--model` to select a
Claude model; it defaults explicitly to `claude-sonnet-5`. Use `--effort` to set reasoning effort,
`--timeout-seconds` to bound each invocation, and
`--worker-count` to choose how many queue-pulling analysis workers run concurrently. The general
`carl work` process owns the bounded pool used by MCP-requested analysis and refresh coordination.
Web claims and cited URLs are initially provenance reported by the agent; Carl does not yet acquire
and retain the referenced pages independently. Price comparison and preference assessment are
separate future stages.

## Interactive review through MCP

Serve the retained evidence and durable analysis queue to an interactive agent over stdio:

```console
uv run carl mcp
```

The read-only `get_server_info` tool identifies the exact live server instance, its start time,
process, repository, database, startup commit and worktree state, startup Python-source hash, and
structured capability versions. Use it to distinguish a stale development server from one that has
loaded current changes.
The read-only `get_activity_snapshot` tool discovers active and queued work, recent completions and
failures, and network-layer activity without requiring a previously saved work identifier. Its full
work identifiers can be passed to `get_work_status` for compact coordinator and child progress.
Set `include_details=true` only when raw payload, checkpoint result, error, or recent operation IDs
are needed.

`create_search` queues a new, fully specified bounded search and returns immediately. Search
session-open and HTTP transport failures retry with exponential backoff; a transport failure
invalidates the shared WireProxy session so the next attempt uses a newly started and health-probed
child. `list_search_runs` includes each producing attempt's start and completion timestamps plus its
fresh-search or refresh origin and direct refresh source run. `get_search_run_listings` pages through
the exact immutable listing-ID membership of one run without inflating every search summary.
`get_provenance` reports the total
output-edge count while omitting sibling outputs by default; callers can request a bounded prefix or
explicitly request the complete set. `request_search_refresh` repeats the selected search, then reuses the latest semantically usable
retained item page for every exact listing ID in the baseline/refreshed union. It queues Decodo item
collection only for IDs without a retained `full_listing` or `listing_unavailable` observation;
login, challenge, error, and malformed responses remain eligible for retry. The durable result
reports reused pages separately from new item collections.

`retry_image_failures` requeues terminal image collection work linked to one exact search-run
record or refresh-work identifier. Transient session failures already retry automatically; use this
tool for failures retained before that behavior or after correcting an external configuration or
transport problem, then poll activity or refresh status while workers process the requeued jobs.
Selection, checkpoint restoration, requeueing, and audit-event insertion happen in one bounded
SQLite transaction rather than one transaction per image; the call reports counts without waiting
for collection. Requeued jobs use collect-image payload schema v2, which prevents pre-fix v1 workers
from claiming them, while current workers continue to accept ordinary legacy v1 jobs.

The activity snapshot also reports local processes that currently hold the database, WAL, or shared
memory file open. Use the process IDs, commands, and working directories there to spot stale MCP or
worker processes sharing the database. The current process includes its source-tree hash; an old
process that cannot identify itself is reported with an unknown source version.

Once a search refresh completes, `request_missing_listing_analyses` creates a durable batch for its
exact union of retained listing IDs. The batch freezes the matching latest-observation snapshot,
applies optional candidate filters and a maximum count, selects one exact product-guide record, and
uses each observation's current saved gallery. Compatible completed or queued analysis work is
reused; missing work is enqueued under the global analysis concurrency limit. Reused work retains a
requester edge to the batch so live progress does not depend on the next parent checkpoint. Its selection policy
can either revisit fresher evidence or restrict the batch to listing IDs that have never had a
completed analysis. `get_work_status`
reports batch selection, planning, skips, and child work states, and the batch completes only after
all child analyses are terminal. Compact status includes remaining observations, bounded active
child IDs, and grouped failure reasons.
`preview_missing_listing_analyses` applies the same selection logic read-only and reports the counts
before selection policy, after historical-analysis exclusion, and after any maximum-item cap.
During development, changing an MCP tool schema requires a new agent session. Reconnecting the MCP
transport alone is not evidence that the client replaced the tool definition already supplied to
the model.

Use `--database PATH` when reviewing a non-default database. The stdio process opens Carl's existing
database but does not run queue workers. Run `carl work` or `carl monitor --work` separately while
collection or analysis should progress. Work remains in SQLite across process restarts. The MCP surface can list
retained search runs and queue a refresh that repeats the search,
reuses usable detail pages for the union of prior and newly returned listing IDs, fetches only
missing usable pages, and downloads missing images using the existing reuse rules. A complete
traversal override exposes page/result/resource
bounds; a complete traversal-strategy override exposes overlapping price-partition width, overlap,
and order. Search and image traffic use the selected Proton route, while item details use the
selected Decodo route. The surface can also list a stable snapshot of current listing candidates; return listing dossiers, exact
analysis reports, one-hop provenance, and exact validated image artifacts; create and revise
immutable product guides; enqueue analysis for an exact observation and guide; and report work
status. Refresh status distinguishes the last durable checkpoint from the active child phase and
reports bounded recent operation history instead of returning every historical attempt. It does not
use listing-observation counts as page-collection progress because observations are produced during
the later extraction phase. It does not alter route configuration or delete evidence.

The repository-local [`carl` skill](skills/carl/SKILL.md) describes the intended review workflow.
It is optional guidance for an agent; the MCP server's tool descriptions and validation are complete
enough to use the server independently. Client-specific registration remains local configuration:
point the MCP client at `uv run --project /path/to/carl carl mcp` and, when supported, add
`skills/carl/SKILL.md` as a project skill.

```console
uv run carl extract 00000000-0000-0000-0000-000000000000
```

Carl uses `platformdirs` to place the default database in the operating system's
per-user application data directory. On a typical Linux host this is
`~/.local/share/carl/carl.sqlite3`. Use `--database PATH` on a command to select
another database.

Carl retains decoded HTML, embedded JSON blocks, normalized observations,
failures, and operation provenance. Validated image responses use the external
file policy above; failed or invalid image bodies are discarded. Real collection
databases do not belong in the source repository. Repository-local SQLite files
remain ignored as a safeguard; a small committed test fixture would require a
deliberate exception.
