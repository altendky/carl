# Carl project design

## Purpose

Carl preserves source evidence and turns it into versioned, traceable derived
information. Collection, extraction, and assessment are independent so saved
evidence can be reprocessed without fetching it again.

The [review workspace design](review-workspaces.md) describes durable agent
review batches, explicit decisions, reusable overlapping worksets, and frozen
selection snapshots layered over the composed listing projection.

Facebook Marketplace is the first source adapter. Source-neutral operation,
provenance, artifact, and storage interfaces must not depend on Facebook.

## Architectural boundaries

`carl.core` is sans-I/O in the literal sense: it performs no network,
filesystem, database, subprocess, clock, environment, or random-number I/O and
holds no mutable global state. It contains immutable data, parsing, state
transitions, validation, and extraction.

`carl.io` interprets effects and supplies external facts to the core. The
application layer wires registered components to those adapters.

Carl uses AnyIO APIs with Trio as the selected runtime backend. The active
backend is retained in operation provenance so library callers using a different
backend cannot be mistaken for Trio executions.

Carl relies on Trio/AnyIO level cancellation. Every asynchronous call and
context-manager boundary is treated as a possible repeated cancellation point.
Resources are owned through `contextlib.asynccontextmanager`, and the task that
enters a resource context is responsible for leaving it. Cancellation is caught
only where cleanup requires it and is then re-raised. Awaited rollback, close,
and pool-return operations use bounded shielded scopes; ordinary processing is
never shielded.

Durable effects remain potentially ambiguous when cancellation races completion.
A database commit may finish after the caller loses its result, and an HTTP
request may reach a remote service before local cancellation is observed.
Identifiers and transitions are therefore idempotent where practical, workers
must re-read durable state after an uncertain result, and a lease token fences
publication even when local cancellation has already been requested. Each
leased execution will run in its own cancel scope so lease loss can cancel that
work without cancelling unrelated workers.

Every retained derivation is an operation in a provenance graph:

```text
registered component + code provenance + configuration + input records
    -> operation
    -> output records and byte artifacts
```

Input links mean actual derivation. A later acquisition of the same URL is an
independent record and has no automatic relationship to an earlier acquisition.

Each operation references a shared `code_states` row identifying its Git commit
and `clean`, `dirty`, or `unknown` worktree state. Records and artifacts inherit
this reference through `objects.created_by_operation_id`; network activities
use their operation reference. Commit hashes are SQLite `BLOB` values containing
the raw 20-byte SHA-1 or 32-byte SHA-256 hash, not repeated hexadecimal strings.
Other execution-environment provenance remains on the operation. Provenance
reads return the familiar hexadecimal commit hash and include `code_state_id`.
Different local edits at the same commit share the same dirty state: this pair
does not identify the exact uncommitted source tree.

## Registered code

Every component that creates retained outputs has a structured identifier such
as `("carl", "facebook", "extract", "embedded_json")`. Closed Carl-owned
identity vocabularies use semantic `StrEnum` types internally; the initial
storage schema identity already does so. Pydantic serializes enum values as
strings only at a storage or interchange boundary. Component and record
identities will move to typed identifier models as their vocabularies are
defined. Open extension points must not be forced into an unrelated closed enum
merely to avoid strings. The parts remain separate in the core; delimiter
parsing is never an identifier convention.

Startup constructs a registry explicitly and rejects duplicate identifiers.
Import order cannot silently determine which implementation wins. Each
component also declares its output schema version.

An operation records the component identifier, schema version, repository
commit, clean/dirty/unknown working-copy state, relevant runtime and dependency
versions, effective configuration, input and output references, UTC start/end
timestamps, and a duration measured by a high-resolution monotonic clock.
Dirty source is not snapshotted. Dirty operations and their descendants may be
ignored by later processing, but records are retained.

## Storage

The first adapter stores metadata and JSON records in SQLite. Ordinary retained
response bodies and small derived artifacts also remain inline. Validated image
content uses database-relative, content-addressed files so models and other
tools can read the original-format image without exporting a second copy.
Artifacts have stable identities and content hashes independent of physical
placement.

Pydantic validates JSON recursively at the persistence boundary without
coercion, including rejection of non-finite numbers and non-JSON containers.
All retained record envelopes and payloads will use strict typed Pydantic models
and Pydantic serialization. Dynamic source JSON remains a native JSON value
inside typed evidence envelopes. Cyclopts supplies the typed command-line
surface; it does not participate in acquisition or extraction behavior.

Database and retained-record schemas have structured identity parts and
independent versions. A record's existing structured kind plus schema version
is its schema identity. The SQLite database records and validates its own
typed namespace, domain, and backend segments corresponding to
`("carl", "storage", "sqlite")`. Version 3 adds external artifact locations;
opening a recognized earlier schema upgrades it in a transaction without
rewriting retained evidence.
Version 11 normalizes commit/worktree states and backfills operations from their
retained provenance; unavailable historical state remains unknown.

Real databases live outside the source repository. The CLI uses `platformdirs`
to choose the operating system's per-user application data directory. A small
committed test database would be an explicit fixture, never the default output
of collection.

## Local configuration

Carl uses `platformdirs` as the single authority for user-local locations. On a
typical Linux system the application config, data, cache, state, and runtime
directories are respectively under `~/.config/carl`, `~/.local/share/carl`,
`~/.cache/carl`, `~/.local/state/carl`, and `$XDG_RUNTIME_DIR/carl`. The
`carl locations` command reports the effective paths rather than requiring a
caller to infer them. Tests inject an explicit directory value object.

`config.toml` is a strict, versioned document containing safe route identity and
runtime settings. Unknown fields and schema versions fail closed. Structured
identities use TOML arrays and become tuples in the core; no component parses a
delimiter-separated identifier. The configuration document's SHA-256 and schema
identity are available for operation provenance. A minimal two-route document
has this shape:

```toml
[schema]
namespace = "carl"
domain = "configuration"
version = 1

[wireproxy]
executable_path = "/opt/carl/bin/wireproxy"
version = "1.1.3"
binary_sha256 = "<verified 64-character lowercase SHA-256>"

[[http_transports]]
identifier = "browser_chrome_153"
implementation = "wreq"
emulation_profile = "chrome_153"

[[acquisition_stacks]]
identifier = "ebay_anonymous"
http_transport = "browser_chrome_153"
network_path = ["decodo", "personal", "carl"]

[[routes]]
provider = "bright_data"
network_path = ["bright_data", "personal", "marketplace_search"]
account_identifier = "personal"
proxy_username = "<safe base username without session options>"
credential_reference = ["one_password", "<vault>", "<item>", "<field>"]
zone_identifier = "marketplace-search"
product = "residential_proxy"
max_idle_seconds = 240.0

[routes.endpoint]
host = "brd.superproxy.io"
port = 33335

[[routes]]
provider = "decodo"
network_path = ["decodo", "personal", "carl"]
account_identifier = "personal"
proxy_username = "<raw proxy username without the user- prefix or targeting options>"
credential_id = "carl"
product = "residential_proxy"
country_code = "us"
session_duration_minutes = 10

[routes.endpoint]
host = "gate.decodo.com"
port = 7000

[[routes]]
provider = "proton"
network_path = ["proton", "personal", "carl"]
account_identifier = "personal"
configuration_id = "carl"
peer_endpoint = "server.example:51820"
internet_protocol_version = "dual_stack"

[[routes]]
provider = "mullvad"
network_path = ["mullvad", "personal", "carl"]
account_identifier = "personal"
configuration_id = "carl"
relay_hostname = "us-was-wg-001"
```

Facebook searches and Facebook/eBay gallery images default directly to the
separately credentialed Decodo datacenter route below. No Proton alias is needed.
The mobile/residential item-page and eBay search stack above is unchanged:

```toml
[[routes]]
provider = "decodo"
network_path = ["decodo", "personal", "datacenter"]
account_identifier = "personal"
proxy_username = "<base datacenter proxy username>"
credential_id = "datacenter"
product = "datacenter_proxy"
country_code = "us"

[routes.endpoint]
host = "dc.decodo.com"
port = 10001

```

Facebook search `network_path`, refresh `search_network_path`, and Facebook
`image_network_path` fields select complete provider-qualified routes. CLI
search and image collection default to `--network-provider decodo
--network-route datacenter`. Explicit Proton paths use real Proton; there is
no automatic provider fallback. Decodo searches run concurrently within the
worker pool and request-rate limits; only actual Proton routes are serialized.

For an installation with older Proton-to-Decodo overrides, stop workers and
run `carl cutover-route-overrides` before removing those configuration entries.
It audits unfinished/failed work's old and new payloads, rebuilds their
deduplication identities and scheduler scopes, and publishes new current
workspace-track specification versions. It rejects live affected leases and
deduplication conflicts. Completed work and immutable acquired evidence remain
unchanged. The command performs no provider requests and is safe to repeat;
remove the overrides only after it succeeds, then restart MCP and workers.

Datacenter authentication uses `user-<base>-country-<country>` without mobile
session or duration controls. [Decodo documents port 10000 as rotating on each
request and ports 10001-63000 as static ports](https://help.decodo.com/docs/datacenter-pay-per-gb-proxy-session-types).
Use a static port for Facebook bootstrap and pagination: they share one
cookie-preserving client and endpoint throughout the search. Datacenter
observations record the port and whether a static peer was requested; they do
not invent a mobile sticky-session lifetime. Import its separate password with
`carl configure-decodo-credential datacenter` before enabling the override.

Facebook and eBay image downloads share a database-enforced limit of 25
simultaneous image requests, across network paths and worker processes. Each
source's image work limit is also 25; these are not two additive pools of 25.
eBay seller descriptions permit ten concurrent jobs. The worker pool remains
30 per process, and the existing request-rate and holdoff limits are unchanged.
Worker startup retires the older immutable image/description constraints and
records the replacement policy with code provenance, including for work queued
before the upgrade.

Facebook's `overlapping_price_partitions` strategy requests the first result
page of each price bucket without cursor pagination. Cursor pagination is
still available and remains the default when no traversal strategy is supplied;
a cursor-pagination failure does not establish that bucketed searches fail.

The ordinary document contains credential identities, never proxy passwords.
HTTP transports and network routes are independently named configuration
components. An acquisition stack composes one of each without adding the HTTP
implementation to `network_path`. The `ebay_anonymous` example therefore means
wreq with its Chrome 153 profile over the configured Decodo route. The stack
model can represent other route providers, but the initial eBay command
deliberately resolves Decodo only and has no implicit fallback.
Carl currently requires `config.toml` to be owned by the current user with mode
0600. `carl configure-decodo-credential IDENTIFIER` reads the password from a
non-echoing prompt and creates a mode-0600 file in Carl's fixed private Decodo
directory without replacement. Proton uses only a logical `configuration_id`;
arbitrary secret-file paths are not accepted. `carl import-proton-config` validates an
exported configuration and publishes it without replacement under Carl's fixed
private configuration directory. Managed directories are mode 0700 and the
imported file is mode 0600. The source may be read-only to other users but may
not be writable by them, must be a regular file owned by the current user, and
is neither modified nor removed. A failure after atomic publication is reported
as an uncertain published state, rather than as if no destination could exist.

Runtime settings resolve the logical ID to that fixed private file. The file is
read once into an excluded in-memory snapshot; both the device lock and the
temporary wireproxy configuration are derived from those same bytes. The
WireGuard private key is used only to derive an in-memory device-lock identity;
neither the key nor that secret-derived identity is serialized, logged, or
stored in ordinary provenance. This makes aliases for the same device contend
on the same cross-process lock. Safe provenance retains the route, account and
configuration references, provider/product settings, verified wireproxy
identity, and configuration-document identity.

## eBay item and image follow-up

`request_listing_details` accepts an explicit eBay item ID and queues bounded durable follow-up.
It defaults to reprocessing an already retained usable item acquisition, with `refresh=true` for a
new page. Collection and offline extraction are separate work items. Extraction binds structured
data and targeted DOM fields to the requested item identity; challenges, unavailable pages,
mismatched items, and unrecognized responses remain explicit observations. A later failure does
not discard earlier usable details. Observation ordering follows acquisition publication, then
derivation publication, so reprocessing old evidence does not give it a new acquisition age.

Item and seller-description iframe pages use the configured Decodo/wreq acquisition stack.
Descriptions are separate provenance-linked follow-ups and must return complete HTML rather than
a challenge page. Gallery extraction excludes unrelated recommendation images and accepts only
credential-free HTTPS eBay image-host URLs. Downloads use the effective configured image route,
disable redirects, and require the exact requested HTTP 200 resource. The per-item image bound
defaults to twenty and is capped at fifty; zero retains gallery URLs without downloading images.

Images pass Carl's existing decoding, MIME, image-structure and pixel-limit checks before being
published as content-addressed external files. Acquisition metadata points at that validated image
artifact, not a duplicate response BLOB. Exact-URL reuse keeps a new listing/reference relationship
and links the original saved result as input rather than claiming to create its existing artifact.
Failed image validation retains diagnostics, not dangling references to discarded body bytes.
Transport failures retry with a bounded budget; permanent configuration or validation failures
remain terminal.

`get_listing_details` reads retained Facebook or eBay detail without collecting anything and returns
per-image outcomes and exact saved artifact IDs. `get_listing_image` authorizes saved image results
from either source. The same detail, refresh, workspace, and analysis calls dispatch from retained
source provenance. Workspaces may mix Facebook/eBay tracks, reviews, claims, worksets, and selections;
eBay review identities use `ebay:<item ID>` while legacy Facebook numeric IDs remain compatible.
Refresh coordinators wait for item, description, and gallery work; search-only calls do not implicitly
fetch every item. eBay AI inputs include exact saved description and gallery provenance and share
the global analysis concurrency limit. SQLite v9 migrates existing numeric claim rows intact and adds
marketplace evidence indexes. Restart old MCP and worker processes together after upgrading.
Offline fixtures cover these paths;
the eBay item parser has not yet been validated against live provider responses.

New eBay searches must use `listing_state="active"`. Sold/completed acquisitions are presently
unsupported: the anonymous configured acquisition does not support closed-listing sign-in/challenge
gating. Creation, reruns, retries, and refreshes reject these modes; retained evidence remains readable.
Historical `"sold"` mode adds `LH_Sold=1` and
`LH_Complete=1`, while `"completed"` adds only `LH_Complete=1` to search all closed listings,
including unsold endings. These retained request modes are
ordinary search modes; results retain the same item identities as active observations. Explicit
Sold evidence yields sold status; explicit Ended evidence without a sale yields unavailable;
unmarked cards stay unknown. A closed listing's asking price is not a sale price.
Each retained sold card records its displayed
sale price and date, original date text, and price availability; result summaries preserve that
price/date pair's exact occurrence provenance. Missing years are not guessed. Recognized accepted
Best Offers and crossed-out prices remain unknown sale amounts rather than false comparables.
Public card prices are not verified transaction totals. Composed views expose `last_sale`
separately from ordinary price evidence and classify sold cards as sold; available-only filtering
remains the default. This is retained evidence, not automatic matching or deal scoring.

## Storage implementation

SQLite uses WAL mode, foreign keys, a busy timeout, and short transactions.
Network activity never occurs inside a database transaction. SQLite permits
concurrent readers and serializes writers; this is adequate for the initial
local collector and will be measured rather than assumed at larger scale.

Carl's connection layer uses APSW's AnyIO-compatible asynchronous support so
SQLite lock waits and result iteration do not block Trio's event loop. It owns
one serialized writer connection and a bounded pool of query-only reader
connections. Writer contexts nest with savepoints in the owning task; snapshot
reader contexts reuse a task's current writer or reader. Transaction cleanup and
connection return are cancellation-shielded. This follows the useful transaction
and ownership semantics of Chia's `DBWrapper2` while avoiding its
`asyncio`/`aiosqlite` dependency. Query-only mode is set and verified for every
reader rather than assumed from pool membership. Evidence-store, durable-queue,
and worker operations use this layer and share the same managed lifetime.

Opening an existing database must validate its schema identity before running
any DDL. Initialization may create the current schema only in an empty database;
an unrecognized nonempty database must be rejected without modification.
Migrations accept only exact known schema identities and run as versioned
transactions. The current migrations are small enough to remain explicit SQL in
the database adapter; a migration framework is unnecessary so far. SQL
statement tracing is a
diagnostic facility and must not be enabled in ordinary collection logs because
expanded statements can contain retained or secret values.

HTTP bodies are buffered outside the database. A body is published only after
the complete response has been received within configured limits and its
declared content encoding has been successfully decoded. For ordinary
acquisitions, the decoded bytes are stored, with the response headers and
original content-encoding declaration retained separately. Image acquisitions
use the validated-file rule below. A failed or interrupted transfer, or an invalid or
unsupported content encoding,
records its failed operation and response metadata when available, but does not
store that response's bytes. If a later redirect hop
fails, complete bodies from earlier hops remain outputs of the failed attempt;
every retained available-body reference therefore resolves to an artifact.

Records are append-only in ordinary operation. There is no deletion workflow.

## Durable work scheduling

All collection, extraction, image processing, and later assessment work enters
a bounded pool. A fixed number of Trio workers claim work; Carl does not create
one task per pending item and leave those tasks waiting on database or network
limits. Different work kinds and network activities may have separate worker
limits.

SQLite is the durable source of pending work. A claim uses a short transaction
to assign a lease token, worker identity, and expiration time. Processing occurs
outside the transaction. Completion uses the lease token as a fencing value so
an expired worker cannot overwrite a newer claim. Expired leases become
claimable after restart. Current scheduling state may be updated, while every
enqueue, claim, renewal, release, completion, and terminal failure is retained
as an append-only work event.

Before handler dispatch, the worker atomically revalidates and renews its lease,
creates the operation, and binds that operation to the exact work item, attempt,
lease token, and worker. A replacement attempt cannot publish through an old
operation. Code-provenance collection occurs before this boundary, so a lease
that expires during setup never dispatches external work. Heartbeat, database,
or publication failures are worker-runtime failures: they fail visibly and
leave the lease recoverable instead of becoming terminal handler failures.

Work items have typed payload schemas, priorities, eligibility times, and
structured deduplication identities. Relationships from every requester to a
shared work item are retained separately so queue deduplication never erases
provenance. Deduplication applies only while matching work is pending or leased;
a completed or terminal occurrence does not prevent a later monitoring
occurrence for the same logical target. Workers claim individual items directly
from SQLite. Each registered handler declares an exact structured work kind and
payload schema version, and its strict Pydantic payload model is validated before
execution. A specialized worker cannot claim work it cannot decode.

Follow-up selection is a separate layer above queue deduplication. Search runs
remain immutable and keep every listing occurrence. Item-page follow-up accepts
one or more search-run records, unions their candidates only by exact listing
ID, and selects the latest semantically usable retained item-page result for
each ID. `full_listing` and positively identified `listing_unavailable` results
are usable; login, challenge, generic-error, malformed, and requested-ID-absent
responses are failures and do not displace an older usable result. Latest is
ordered by the database's append-only work-completion sequence rather than the
local wall clock, with extraction completion order selecting among repeated
offline extractions of one acquisition. Only IDs without a usable result enter
the collection queue. Every contributing search run remains attached as a
separate requester when several runs share one new item-page job. A future
monitoring policy may explicitly request a fresh observation; ordinary
follow-up does not turn repeated discovery into repeated traffic.

Successful publication may create typed follow-on work in the same fenced
transaction. This is the acquisition-to-extraction handoff: either the evidence
and extraction request both commit, or neither does. Named input edges supplied
by a handler are committed with its outcome, so derived records directly link
to the evidence objects they used.

Scheduling constraints are structured policies applied to work and network
activities, not sleeps or semaphores hidden inside a collector. A constraint
declares whether its subject is a work item or a network activity, then matches
a typed scope. Scope kinds include overall, network path, work kind, network
activity kind, and remote origin. A network-path scope refers to the complete
structured path identity; individual path-layer scopes may be added when
measurements show they are needed.

Concurrency, rate, and holdoff are distinct constraint kinds:

- A concurrency constraint caps active leased executions. Every applicable
  slot must be reserved atomically with the work claim. Expired execution
  leases recover those slots.
- A rate constraint caps starts over time. A reservation is retained even if a
  worker later crashes because the request may have reached the remote system.
- A randomized holdoff sets a minimum eligibility time for a particular work
  attempt. Its policy, sampled delay, decision timestamp, and resulting
  eligibility timestamp are retained. Sampling occurs once; restart does not
  draw a more favorable value.

Each applicable time constraint produces an earliest eligible timestamp. The
effective timestamp is their maximum, so overlapping overall, path, and kind
policies do not accidentally add their delays together. Persisted UTC
timestamps allow restart recovery. A running process converts the remaining
delay into an AnyIO cancellation-aware wait, whose duration is measured with a
monotonic clock, and rechecks persisted eligibility after waking. A queued work
item does not hold a work concurrency slot, and a pending network activity does
not hold a request concurrency permit, while waiting for future eligibility. A
running search remains one active work item because it owns the live network
session needed by its next request.

Network activities have their own durable lifecycle: created, admitted, and a
terminal completion, failure, or cancellation. Their definition records the
registered activity kind, owning operation, live network-session identity,
ordinal, attempt, and all applicable scopes. Work starts and request starts use
the same constraint model, admission rules, and rate-reservation table,
distinguished by a typed subject kind. A network activity admitted inside a
search does not become an independent worker task: the search flow keeps the
HTTPX cookie jar and managed tunnel alive while the shared scheduler waits.

Permit decisions and releases are append-only scheduling events containing the
policy schema identity and all evaluated scope identities. This gives a future
analysis enough evidence to distinguish server latency, queue delay, deliberate
holdoff, and rate limiting. Dynamic values such as path and work identifiers
remain structured values; only the closed scope and constraint vocabularies
are enums.

Libraries are selected per responsibility. Pydantic owns policy and event
validation, AnyIO owns task groups, cancellation, monotonic waiting, and small
in-process channels, and Trio remains the configured backend. SQLite owns
cross-process leases and durable arbitration. Its transactions must remain the
authority even if an in-memory limiter is used as an optimization. Before Carl
implements a rate algorithm, candidate limiter libraries will be evaluated for
Trio compatibility, SQLite-backed atomic reservations, composite scopes,
restart behavior, and inspectable decisions. A library that only blocks a large
set of already-created asyncio tasks does not meet these requirements. If no
library provides the durable portion cleanly, Carl will implement the small
SQLite scheduling transition itself and use libraries for the surrounding
typed models and asynchronous execution.

The initial durable start boundary is the successful work claim. The worker
starts its registered action immediately after that transaction, without a
local semaphore or scheduled wait between claim and execution. Charging the
rate reservation there is conservative: a cancelled worker may spend capacity
without reaching the remote service, but a request that did reach the service
cannot disappear from rate accounting. If later network setup makes the gap
material, Carl can add a separately fenced dispatch transition without changing
the meaning of retained claim events.

## HTTP evidence

The HTTPX adapter records the request plan and effective request, including
ordered duplicate headers, timeout and redirect settings, compression behavior,
routing selection, program argument arrays, library versions, UTC timestamps,
and monotonic duration. It records each redirect response separately, including
ordered duplicate headers, status, URL, protocol version, server `Date` values,
and complete body.

HTTPX's raw stream provides HTTP message content after protocol transfer
framing. Carl records the received content-byte count and decodes any declared
HTTP content encoding. For ordinary HTTP evidence it stores one decoded body
artifact per complete response hop. It does not describe those bytes as exact
wire bytes or retain the original compressed representation. Existing earlier
acquisitions marked as undecoded message content remain readable by offline
extractors. Transport chunks are reassembled; if the response uses a multipart
MIME type, its parts remain inside that one body unless a later extractor parses
them. Image collection has the narrower retention rule described below.

Storage compression is a future feature. Its encoding and parameters must be
recorded per stored artifact so readers can distinguish storage compression
from the source's HTTP Content-Encoding and decode either representation.

An HTTP error status can still be a complete acquisition. Transport failure,
size-limit failure, incomplete response, unavailable extraction, and parser
failure are distinct outcomes.

The direct acquisition plan accepts no cookies, authorization headers, or
inherited proxy/session environment. A provider adapter may add credentials or
session state after validating the plan. Its evidence policy must retain the
header's position and name while replacing protected values with an explicit
redaction record. The HTTPX request object inspected for evidence is the same
request object sent by the client.

The browser-profiled wreq adapter is a distinct HTTP evidence producer. It
records the installed wreq version, named emulation profile, profile platform
mode, default-header policy, cookie and redirect behavior, TLS verification,
HTTP-version policy, worker-thread execution mode, effective timeout mapping,
and compression behavior. Wreq generates part of the outbound header block
inside its native engine; Carl records caller-supplied headers separately and
marks the profile-generated portion as unobserved rather than claiming it is a
complete effective-request capture. Wreq also decodes HTTP content encoding
before exposing body bytes to Python. Those artifacts are explicitly labeled
as wreq-decoded, with raw received-content byte counts unavailable, and are
never described as exact wire bytes. HTTP statuses and semantic challenge pages
remain complete target responses; only native request, proxy, TLS, timeout,
stream, and cleanup failures are transport failures.

## eBay search

`carl ebay-search QUERY` performs a bounded anonymous eBay search traversal
through the configured `ebay_anonymous` stack. The initial stack composes the
wreq Chrome 153 transport with Decodo. Each page is a separately retained HTTP
acquisition; the traversal follows only a validated, sequential eBay next-page
link and defaults to at most five pages, with a hard request-model limit of
twenty. Follow-up pages send the preceding search page's URL as `Referer`, without
a deliberate inter-page delay, using the same sticky session and cookie-preserving client.
A missing next link stops normally. Challenge, empty, and unrecognized
pages are retained as the terminal page. Item-detail requests, retries, and
route fallback remain out of scope.

MCP agents use the single `create_search` tool with one or more discriminated
targets. Each target names `marketplace = "ebay"` or `marketplace = "facebook"`
and carries that source's request. One group can therefore search either source
or both. Both branches enqueue durable work for `carl work`; the MCP server
itself does not perform the network request.

Search groups, target definitions, target-state changes, and executions are
immutable records. `add_search_target` appends and initially runs a target;
`set_search_target_enabled` changes only whether later `run_search` calls
schedule it. Disabling a target never removes its earlier executions or search
runs. `get_search` returns all targets and executions, including disabled-target
history, and a deduplicated list of every completed source search-run record.

`list_search_results` is the marketplace-neutral read path after creation. It
takes the search-group record identifier, discovers each completed target
execution from retained provenance, and returns common search-card fields. The
caller does not select Facebook- or eBay-specific processing. Results are
deduplicated by marketplace plus the source's external item identifier; all
supporting occurrence, source-run, target, execution, acquisition, and
completion-sequence references remain attached. Opaque cursors preserve the
initial completion boundary, so later search completions appear only in a new
traversal rather than between pages of an existing one. Disabling a target does
not remove its historical results, and removing a marketplace implementation
later would not erase already retained records.

Check `get_search` execution `collection_succeeded`, `response_classification`,
and `stopping_reason`, and `list_search_results.execution_warnings`, before
interpreting zero cards as a successful empty search. Warnings include failed
collections (even legacy work marked completed) and pending or failed work.
Execution warnings are live; only listing-card pagination uses the fixed cursor
boundary. Older failed runs remain immutable and are flagged on read.

The acquisition uses the generic `carl/http/acquisition` record and
`carl/http/response_body` artifact kinds. A separate offline extraction
operation stores a UTF-8 text derivative, one `carl/ebay/search_extraction`
record per page, and one `carl/ebay/search_listing_occurrence` record per
page-local distinct item ID. A final `carl/ebay/search_run` manifest ties every
page, occurrence, classification, requested bound, and stopping reason together
while reporting both occurrence and cross-page unique-listing counts. The
extractor classifies usable results, legitimate empty results, eBay challenge
pages, error pages, non-success HTTP responses, and unrecognized page structures.
A challenge or error remains retained HTTP evidence but is not a successful
empty search. Durable work retries these response failures at most three total
attempts with exponential backoff, preserving each attempt's evidence, then
reports terminal failure. Explicit empty results remain successful.

## Network providers

Every production network route describes one concrete, ordered network path and
must match the path assigned to the request attempt. A route adapter executes
that path or reports failure; it does not select another route internally. Safe
route identity contains an account identifier and a structured credential or
device reference, never an authorizing secret.

Route selection belongs to the flow or activity policy. A future policy may
require one path, choose randomly from an eligible set, or try candidates in an
explicit fallback order. Selection may apply to complete paths or to an
alternative at one layer of a path. For example, Marketplace search and image
downloads default directly to Decodo datacenter, while item details use their separate Decodo
route. A later flow may
choose among Decodo, Mullvad, Bright Data, or other explicitly configured paths.
Anonymous eBay acquisition currently selects the named wreq/Chrome 153
transport composed with the Decodo route.

The policy must resolve a concrete path before each request attempt. A fallback
therefore creates another attempt with its own selected path and complete
evidence; it never rewrites the failed attempt or changes transport midway
through it. Direct networking is available only when the flow policy names it
as an eligible path. Selection provenance will retain the policy component and
version, candidate identities and order, eligibility or health inputs, random
decision when applicable, selected path, and the failure that caused an ordered
fallback. These selection policies are designed but not yet implemented.

Bright Data is implemented as a native HTTP proxy adapter. Product families are
the native datacenter, residential, ISP, and mobile proxy networks. Web
Unlocker, Scraping Browser, and scraper APIs have different behavior and will
use separate adapters if adopted. Carl requires the proxy gateway and port
explicitly instead of assuming a provider default. A session manager creates a
random provider session value, adds Bright Data's constant-peer directive, and
owns one HTTPX client and cookie jar for the related request sequence. The
provider session value and constructed username remain transient. An opaque
Carl session-record identifier links safe observations. The exact base proxy
username is retained as the non-secret provider account identifier; the password
is supplied separately and never serialized. Requests in one session
are serialized, and a configured idle guard below the provider's documented
five-minute context expiry rejects stale pagination rather than changing peers
silently. A transport or confirmed provider failure poisons the session so the
outer durable retry must restart the bootstrap chain. A 407, 429, or 502 response
is classified as Bright Data only when it carries Bright Data's error code;
ordinary Facebook responses with those statuses remain target evidence.
Descriptive provider headers, `Proxy-Status`, cookies, and `Set-Cookie` values
are redacted. Native proxy TLS uses system trust roots. A future product that
requires Bright Data's inspection certificate will have a separate adapter and
explicit certificate identity.

Decodo is also a native HTTP proxy adapter, with typed residential, mobile, and
shared datacenter products. A route records the safe base proxy username, account identity,
gateway, target country, credential reference, and requested sticky duration.
For each independent residential/mobile acquisition Carl generates a transient provider session
identifier and appends the country, session, and duration controls to the
username. The constructed username, provider session identifier, password,
cookies, and protected proxy headers are not retained. Redirects and cookies
within an acquisition share one client of the selected HTTP implementation.
Facebook item collection currently selects HTTPX, while the anonymous eBay
stack selects wreq with the configured browser profile. The next independent
acquisition opens a fresh provider session so a provider-selected bad exit is
scoped to that acquisition. Provenance says that a sticky peer was requested;
it does not claim that Decodo guaranteed the peer remained available. HTTPX and
wreq proxy-transport errors are classified as provider failures. HTTP statuses
remain target evidence unless a later adapter has provider-specific proof.
Provider selection, fallback, and semantic retry remain flow policy rather than
route behavior.

Proton is implemented as a provider-specific managed WireGuard transport over a
provider-neutral `wireproxy` process manager. It requires a dedicated exported
configuration owned by the current user with mode 0600. Carl never reads the
desktop application's cache or changes host routing. It rebuilds a temporary
configuration from validated interface, peer, address, DNS, MTU, key, endpoint,
and allowed-IP fields, dropping hooks and unknown fields, then adds a loopback
SOCKS5 listener. The source configuration is not modified. The temporary file
is mode 0600 and is removed during shielded cleanup. The configured wireproxy
executable is copied into the same private runtime directory, then that snapshot
is hashed, version-checked, executed, and removed. The recorded tool identity
therefore describes the bytes used by the session. A cross-process device lock
prevents concurrent use of the same identity. Lock contention waits for a
bounded interval and becomes a retryable managed-route failure. DNS is required,
must be covered by `AllowedIPs`, and the configured peer endpoint must match the
safe route identity. Listener readiness and successful egress validation have
separate timestamps. If child termination cannot be confirmed, Carl reports the
cleanup failure and intentionally retains both the device lock and private
runtime configuration rather than risking a second process on the identity. The
child inherits the lock descriptor, so the quarantine remains effective if Carl
exits while the child is still alive. Cleanup failure supersedes an active
cancellation as a structured managed-route failure while retaining the primary
exception type in its diagnostic. The `ip.me` health request proves
that a public address was observed through the SOCKS route; it does not by
itself prove who owns that address.
Each Proton route also records an explicit `internet_protocol_version` of
`dual_stack`, `version_4`, or `version_6`. A single-stack route filters the
interface addresses, DNS servers, and allowed networks written to wireproxy and
fails configuration validation if the imported configuration cannot support the
selection. This makes an address-family experiment a distinct, attributable
route rather than an implicit retry or fallback.

Mullvad retains its provider archive and Mullvad-specific exit verification.
The archive is imported once into Carl's fixed private directory and referenced
by logical ID. Carl selects one exact relay member into memory without extracting
the archive, derives the device lock from that member's private key, and passes
the snapshot to the same provider-neutral WireGuard and wireproxy lifecycle used
by Proton. The health response must identify the selected relay when it supplies
a hostname. Image and Facebook HTML work can select independent routes.

## Time and routing

UTC wall-clock timestamps and monotonic durations are separate. Remote clock
accuracy verification is deferred.

Routing is an explicit acquisition setting and may differ by network activity.
Registered actions declare structured activity identifiers and receive a
network executor rather than constructing clients or resolving credentials.
Each request attempt is bound to the concrete ordered network path resolved by
its activity's selection policy. Path layers are always ordered from the
application toward the network; this direction is part of the schema rather
than a per-record option.

Facebook searches and both marketplaces' images default directly to the
Decodo datacenter path, without Proton aliases.
eBay searches and item-page
collection defaults to Decodo, with Mullvad available as an explicit override
and Bright Data retained as another native proxy implementation. No adapter
silently changes its assigned path. A future flow policy may explicitly select
another eligible path for a later attempt.

An anonymous search session begins with a navigation request to the Marketplace
root. Carl preserves its response evidence and embedded JSON, then reuses the
same HTTPX client, cookie jar, browser identity, and network route for the search
sequence. The root response is a session bootstrap rather than a result page and
does not count toward the page or result limits. This models the observed browser
sequence and gives later client-side search work a preserved source for session
cookies and Relay metadata.

In the 2026-09-20 Proton Free 54 experiment, the Marketplace root loaded and
classified as `marketplace_landing`, but a subsequent full navigation to the
search URL still redirected to Facebook login. The logged-out browser likewise
redirected when the search URL was entered directly, while searching from within
the loaded Marketplace interface displayed results. Network inspection showed
that the initial result set came from a POST to `/ajax/route-definition/`, not a
navigation to the visible search URL. Its newline-delimited,
anti-hijacking-prefixed frames contain one search preloader description and a
separate result frame. Carl extracts the current query identifier and variables
from the former and requires its preloader identifier to match the latter.

A minimal HTTPX reproduction through the same Proton route succeeded. Carl
retained 15 listing observations and the server-applied search variables,
including the requested 60 miles converted to 97 kilometres and USD 600 encoded
as 60000 minor units. The page reported another cursor. One bounded GraphQL
pagination attempt then returned Facebook error code 1675004, `Rate limit
exceeded`; Carl stored the complete response and failed the work explicitly.
Initial-page collection is therefore live-verified, while cursor pagination
through this route is not yet verified.

The Mullvad capability has two layers. A generic HTTPX adapter sends one
request plan through an explicit loopback `socks5h` endpoint, keeping DNS
resolution on the proxy path. A Mullvad manager verifies a pinned `wireproxy`
executable, selects one relay configuration from the private ZIP, rebuilds a
minimal allowlisted configuration in a mode-0600 temporary file, holds an
interprocess lock for the opaque device identity, starts and probes the child,
and removes the file after shielded process shutdown. The manager suppresses
child output because it is not safe provenance. Configuration archives with
group or other permissions are rejected unless a bounded experiment explicitly
enables the unsafe-permissions override.

Proton uses the same generic SOCKS transport and the provider-neutral
WireGuard-to-`wireproxy` manager. Its provider identity, account and device
references, egress observation, and secret lifecycle remain separate from
Mullvad. Bright Data remains a remote credentialed proxy route rather than a
locally managed WireGuard process.

### Mullvad verification on 2026-09-20 UTC

The bounded verification used a selected Mullvad relay and a test listing;
their source identifiers are intentionally omitted. Carl first queried Mullvad's
JSON connection endpoint through the local SOCKS route, retained the complete
probe request and response, and required `mullvad_exit_ip: true`. The endpoint
identified the selected relay. Only then did Carl request Facebook. The item
response stayed on the requested URL, returned HTTP 200 without redirects,
contained the target listing object, and classified as `full_listing`.

The explicit direct control retained 161,787 bytes before content decoding and
decoded to 971,392 bytes. The Mullvad response retained 161,611 bytes and
decoded to 965,401 bytes. Both extracted the same normalized title, price,
description, location, sold/pending values, and four ordered photo
identifiers. Their HTML and embedded-block counts differed, so equality of
normalized fields does not imply byte-identical evidence.

The tested `wireproxy` executable reports version 1.1.3 and has SHA-256
`70ae5e52223dac7974af8d98a321f14a0e1689d2b14655ebc8dadfa1ec69466d`.
Its official Linux AMD64 release archive has SHA-256
`e88c1d090740373fc606c1bafd81d9a5eadc642cce5667616e20e9d7a444f51c`;
these hashes identify different artifacts and must not be interchanged. The
supplied Mullvad configuration archive was mode 0664, so the test used the
recorded experimental permission override. Normal use rejects that archive
mode. The configuration workflow now requires owner-only permissions and imports
the bundle into Carl's private directory as mode 0600; the runtime adapter no
longer exposes an override. The selected configuration was materialized as mode 0600 and removed,
the process terminated, and no secret configuration remained in the runtime
directory.

Proxy credentials and WireGuard private keys are secrets. Initially these are
the only network secret classes. Ordinary provenance records the route kind,
provider, non-secret endpoint properties, a stable account identifier, and an
opaque credential reference. It never records the authorizing secret or a
secret-derived hash. Configured routing, the effective path, and independently
observed egress identity remain separate facts. The current direct adapter is a
vertical-slice and test implementation; it is not an acceptable production path
for either Marketplace item pages or images.

## Attempts and retries

Every durable claim creates a separate operation bound to the work item, its
one-based attempt ordinal, the lease token, and the worker. The operation
configuration retains the exact typed work payload, including the request plan.
Failed acquisition results retain available response metadata and their
stopping condition. Retry or terminal decisions are retained in the operation
error, while work events retain the attempt transitions. The shared work item
and its ordered attempts provide the current retry grouping; Carl does not yet
create a separate parent collection operation.

The implemented item-page policy allows at most three total attempts for HTTP
transport failures, including interrupted transfers. Each retry advances the
work item's persisted eligibility time by one second, so restart does not erase
the delay. Complete HTTP responses, including error status responses and
challenge pages, are acquisitions and are not retried automatically. Item-page
extraction classifies full listings, login pages, challenges, generic errors,
unavailable listings, malformed or incomplete responses, and valid Marketplace
payloads missing the requested ID. This content classification is separate
from HTTP status and can later feed time-bounded relay-health observations.
Extraction failures never trigger network retries because retained evidence can
be reprocessed offline.

## Extraction outcomes

Extraction success, partial extraction, and unavailable source data are typed
states in listing observations. Embedded-JSON parse problems and field mapping
problems are retained as structured warnings with block or field provenance.
An extractor exception produces a failed operation with a structured safe error
kind and type; its known acquisition and body input edges are still retained.
These failures are discoverable by operation state and registered extractor
identity without searching logs.

Content decoding and HTML/JSON extraction run in AnyIO's bounded worker-thread
pool so CPU work cannot stop Trio lease heartbeats. The call is abandoned on
cancellation: the pure thread may finish computing, but it has no database or
network capability and cannot publish after its owning leased task is
cancelled. Durable publication remains fenced in the async worker.

Dedicated diagnostic records and indexed projections remain future work. They
will support grouping warning and failure codes across request policy, HTTP
outcome, and extractor version. A new extractor version can then enqueue
offline re-extraction work for selected acquisitions without another request.

A future collection-health policy should detect when scraping appears broken,
using bounded windows of failures such as invalid content encoding, login or
challenge responses, protocol/extraction failures, and rate limits. It should
stop new work in the affected source, route, and activity scope, leave queued
work durable, expose the triggering evidence, and require an explicit review
and resume decision. This is separate from the current per-attempt failure
outcomes; Carl does not yet infer a global outage from one response or silently
switch routes.

## Facebook semantics

Facebook listing identity is the listing ID string. No similarity matching or
cross-ID repost merging occurs. Each extraction is an observation linked to a
source acquisition and embedded JSON blocks. Search-card observations cannot
erase richer detail observations, and later search absence does not establish a
sale.

The initial [composed listing projection](current-listing-view-plan.md) provides
bounded, on-demand answers for common “latest known data” questions without
weakening that evidence model. It independently composes status, details,
galleries, analyses, and exact search membership, deriving status from retained
search cards where possible. Search cards also provide fallback scalar details
and a separately labeled preview image when no item-page observation exists.
Status-monitoring writes and cheaper PDP probes
remain planned; those will reserve item-page requests for cases where cheaper
evidence is insufficient or rich detail needs refreshing.

Seller claims, source-reported facts, observed facts, and Carl's interpretations
are distinct evidence kinds. Approximate source locations stay approximate.
Images require downloaded, verified content before being considered saved; URL
discovery alone is a separate state.

### Agent identification and later assessment

The first agent stage is durable queued work over a listing-analysis evidence set. The producing
operation's typed input edges identify one listing observation and pair each ordered gallery
reference with the saved image result chosen for it. The evidence-set record itself is empty;
following its operation inputs reaches the exact
embedded JSON path, item-page acquisition, image acquisition, validated file, content hash, MIME
type, dimensions, and source gallery order. The analysis queue and result do not copy that upstream
metadata. It gives Claude Code a private
temporary directory containing a small listing manifest and links to the
ordered, externally stored image files. Carl stages those as hard links in a
private temporary directory on the database filesystem, so the model sees
ordinary files without another copy of their content. The common prompt first asks what is being
sold, what appears included, and what can be said about condition, with local source citations and
verbal confidence. An explicitly selected, versioned product guide adds the comparison and
inspection priorities for the relevant product type. Once the listing identifies a sufficiently
specific, researchable item, the
same run uses web search and fetch to find manufacturer literature and reliable secondary sources,
resolve variants, summarize useful specifications, establish standard and optional accessories,
and compare those with the listing evidence. If the identity remains too broad, research stops
rather than borrowing facts from a merely similar product. Multiple sources are appropriate while
they narrow the identity or add model-specific evidence; repeated generic, duplicative, or
conflicting results trigger a partial report instead of continued broad searching. The prompt does
not request price research or value judgments. The initial policy aims for roughly 90 seconds,
permits up to three combined web searches and fetches, stops after two unproductive query
refinements, retains a 210-second process deadline as a cleanup backstop, and caps the report at
1,200 words. A deadline retries twice with 30- and
60-second durable backoff before becoming terminal, and every failed attempt retains its record,
artifacts, output edges, and parsed tool calls. New analysis work uses a schema version that stale
pre-retry workers cannot claim. The safety turn ceiling is sized from the gallery image count and retained as analysis
configuration. Claude output is captured as NDJSON so WebSearch and WebFetch calls
are measured from actual tool-use events. Each result records the configured bound, observed value,
unit, enforcement type, and within/exceeded/unavailable status for target duration, maximum
duration, turns, web calls, and report words. Hard deadline or turn exhaustion prevents completion;
advisory overruns retain the useful report and add warnings. The analysis text and execution
observations are new analysis output. The exact manifest and prompt, Claude stdout and stderr,
invocation and version, and timing are retained as operation outputs or metadata. The analysis
operation has typed input edges to the evidence set and product-guide record. The guide is
registered once as a structured identity/version record with an exact UTF-8 text artifact; its
registration operation retains normal code provenance. Exact identity/version/text matches reuse
the registration, while different text under an existing identity and version fails. The exact
composed prompt remains model-boundary evidence. The evidence-set and guide provenance supply the
rest of the source graph without copying upstream facts into the analysis result. The queue skips a
completed analysis of the same evidence set, guide, and analysis configuration. The CLI requires
an explicit product guide so telescope guidance cannot silently apply to tool storage or another
product family. The default model choice is
Claude Sonnet 5.5 with medium effort; both are explicit invocation settings, while
Claude's exact output metadata retains reported model usage. Carl uses the
exact `claude-sonnet-5-5` model ID for repeatability. Existing queued work retains its recorded
model choice. Exact analysis-record lookup includes retained
failed attempts for diagnosis, but candidate and dossier analysis summaries remain completed-only.

Web claims in this initial implementation have agent-reported provenance: the report retains page
titles, publishers, URLs, supported claims, exact prompt, and exact Claude invocation, but Carl
does not independently acquire or preserve the cited pages. A later research acquisition adapter
can turn selected references into retained source evidence before stronger downstream conclusions
depend on them.

Interactive assessment uses a listing-dossier application interface rather than querying SQLite
directly. `carl mcp` exposes it over stdio without requiring a resident service. Carl has one
installed executable and entry point; MCP serving and all other user-facing operations are
subcommands of that `carl` command rather than separately installed programs. The initial interface
lists compact candidates in a stable completion-sequence snapshot, derives sold and pending states
from normalized listing fields, and returns the latest retained observation and gallery descriptors
for an exact listing ID. Candidate and dossier analysis history spans every retained observation
while each descriptor labels the exact observation it analyzed. The interface retrieves complete first-pass reports,
walks provenance one hop per call, and returns one exact validated image artifact at a time as MCP
image content. Exact search-run membership is exposed through a stable offset-paginated lookup rather
than multiplying hundreds of listing IDs across broad search-summary responses. It does not
materialize a second dossier directory or duplicate image bytes.
The bounded `get_activity_snapshot` view exposes queued and leased work, recent terminal outcomes,
per-kind totals, and network-path activity with full identifiers. An agent can therefore discover
current work before using compact `get_work_status` progress. Raw durable payload, result, error, and
recent operation identifiers are opt-in diagnostics.

The mutations create and revise immutable product guides using optimistic concurrency and
enqueue analysis for one exact observation and guide record. A stale guide revision returns the
current record instead of overwriting it. Analysis requires a complete gallery by default; an
explicit override can retain and analyze a partial or empty gallery with the missing positions and
reasons in the evidence set. The MCP surface can queue a fully specified new search, lists recent
retained searches, and accepts one durable refresh request. New-search requests return pending work
immediately and rely on the explicit shared worker pool just like refreshes. The refresh request inherits the exact search intent, bounds, and traversal
strategy unless the caller supplies complete typed replacements. This makes overlapping price
partition width, overlap, and order available without reproducing CLI flags as loosely related
strings. A refresh repeats the search through the effective configured route, selects the union of the baseline and refreshed
listing IDs, and reuses the latest semantically usable retained item page for each exact ID. Only an
ID without a usable `full_listing` or `listing_unavailable` observation causes a new Decodo request.
The refresh then runs the existing image reuse and routed collection logic against those latest
usable observations. Including baseline-only IDs preserves their retained detail evidence without
treating absence from search as evidence of sale. Optional item and image limits bound experiments;
the first configured search traversal limit remains decisive.

For opt-in incremental processing, `request_search_pipeline` accepts a new mixed-source search,
an existing search group or exact run, exact search work IDs, or enabled current workspace tracks.
New-search acceptance publishes the search, its executions, an immutable pipeline intent, and the
root work atomically. Caller request IDs provide durable replay, including after completion;
different content under the same ID is rejected. Existing scope is frozen at acceptance and does
not launch another search or silently include later reruns. A selected workspace track with an
active refresh must settle first, or the caller can select its exact search child directly.

The pipeline consumes retained pages while searches are still running. Facebook checkpoints are
attributed through their owning work operation; eBay records an exact work/attempt/run binding
before acquisition. Source-specific publication cursors prevent delayed legacy eBay attribution
from being skipped behind newer Facebook publications. Each qualified listing gets one durable
coordinator, which pins its own item observation, waits for its description and planned gallery,
then seals analysis evidence under an exact guide. A ready listing can finish analysis while another
listing or search is pending. Requester edges recover children after enqueue/checkpoint interruption.
Ordinary search-only tools and fixed-selection analysis previews retain their existing behavior.

Global item/image/analysis bounds and an outstanding-listing window bound queue growth and spending.
Image and analysis reservations are conservative; unused reservations are not reassigned. Retained
analysis history excluded by the chosen policy does not reserve a new analysis. Title and known
card-status filters run before paid details work; exact item status is checked again afterward.
Missing, truncated, or empty galleries require an explicit incomplete-analysis override. Pipeline
status exposes counts, reservations, skips/failures, source-run IDs, and up to 100 listing progress
entries. A reached bound is reported rather than presented as exhaustive processing. Search failures
preserve partial downstream results; child partial failures propagate to the root and workspace
status. Coordinators yield their leases while dependencies run under the existing shared queue,
provider limits, and resource-owning runtime. No schema migration is required for this workflow.

Acquisition sharing is separate from caller evidence identity. The durable queue serializes
equivalent item/image/description resources across workers, including previously queued jobs,
while unrelated resources retain their normal concurrency. A waiting caller rechecks committed
evidence at dispatch and retains its own extraction budget, observation, and gallery-reference
relationships. Explicit item collection only shares pages actually acquired after its enqueue
boundary; a later reuse operation cannot make an old page appear fresh. Description reuse also
allows observations of the same retained parent item acquisition to share seller text.
Recognized eBay and Facebook CDN renditions can share across hosts; Facebook signatures/expiry
are excluded from identity, while sizes, formats, crops, and unknown parameters remain distinct.
Unknown URL layouts fall back to exact matching. Different signed fetch URLs retain separate jobs
so a failed expired URL does not suppress a caller's valid alternative. Cached image bytes are
validated before reuse; missing/corrupt storage is not reported as a successful cache hit.

The refresh itself is durable coordinator work and returns immediately. It enqueues ordinary search,
item, image, and extraction work items, then returns its lease while those children run in the shared
worker pool. It never starts a private nested worker pool. Phase results are saved on retry
boundaries. `get_work_status` reports
that last checkpoint separately from the inferred active phase and item/image child-state counts;
its operation history is bounded while retaining the total count. Exact image renditions and
adequate saved images with the same Facebook photo identity are reused through the existing
provenance records. Current image collection validates and publishes the image file itself;
image-extraction child counts describe legacy/offline reprocessing, so zero is normal for a refresh
whose image children completed successfully. Generic work status includes its observation time,
latest durable event, lease expiry, and last lease activity without exposing lease tokens. Analysis
batch status unions checkpointed child identifiers with live durable requester edges, so agents can
observe progress while a coordinator attempt is still open. Reused analysis work receives a durable
requester edge as well. Compact batch status reports remaining observations, a bounded active-child
sample, and grouped terminal-failure reasons. Coordinators process one analysis
request between checkpoints to keep cancellation and progress boundaries small. A separate
`carl work` foreground process can keep a bounded worker pool alive, so closing an MCP client cannot
stop queue processing. `carl monitor --work` runs that same bounded pool alongside the dashboard.
MCP servers and `carl search` never start workers implicitly; stale or concurrent clients therefore
cannot multiply worker pools. Search session and HTTP transport failures use bounded exponential
backoff. A transport failure invalidates the runtime-scoped WireProxy entry, whose child is always
waited and reaped; the next attempt starts and health-probes a fresh child. Refresh coordinators
resume retryable terminal search children before declaring the whole refresh failed. Work claims
and concurrency constraints are atomic SQLite state, so explicit worker
processes share global limits. Expired leases are recovered when a worker process restarts. Route
configuration changes, deletion, and preference assessments remain outside the MCP mutation
surface.

Analysis remains a separate agent-selected stage because a request must identify one exact product
guide. After a refresh completes, `request_missing_listing_analyses` freezes the filtered latest
observations from that refresh's baseline/refreshed listing union. Its durable coordinator plans in
bounded chunks, asks the existing exact-evidence analysis planner for each observation, and relies on
that planner's evidence/guide/configuration identity to reuse compatible completed or queued work.
The selection policy can revisit updated evidence or exclude every listing ID that has any completed
analysis, even when the refresh produced a newer observation for it.
The read-only `preview_missing_listing_analyses` tool uses that same planner to expose matching,
historically excluded, eligible, and finally selected counts before any analysis work is queued.
The coordinator checkpoints observation position and created, reused, and skipped counts, then waits
for all child analysis jobs to become terminal. A repeated batch is intentionally a new coordinator:
the child identity prevents duplicate analysis while permitting newly available image evidence or a
changed analysis configuration to create new work. SQLite constrains one planner coordinator and ten
analysis children globally across worker processes.

`carl monitor` reads a consistent SQLite snapshot and renders durable queue counts, active leases,
recent terminal work, and current network activities. Network counts are grouped by the full
ordered network-path tuple, preserving the distinction between Proton, Decodo, and any later
provider route. The polling loop uses Trio cancellation points and context-managed Rich and
database lifetimes. `--work` explicitly owns the bounded worker pool for the monitor lifetime.
Activity includes local processes sharing the database, while leases and admissions remain the
authoritative work state. A bounded `--once` form supports logs, tests, and noninteractive
inspection.

The MCP adapter must remain a thin boundary around that application interface. Carl-specific query
models, selection rules, provenance traversal, SQLite access, and artifact resolution do not belong
in generic MCP plumbing. Protocol transport, typed tool registration, argument/result adaptation,
error mapping, and server lifecycle should be isolated so experience from Carl can later support a
shared Python MCP library without requiring Carl to depend on such a library prematurely. The
official MCP SDK remains responsible for protocol implementation. MCP string tool names are
boundary values; Carl's registered component identities remain structured parts. Any future
extraction into a shared library should follow demonstrated needs from multiple applications rather
than designing a framework from Carl alone.

The review application follows the same dependency direction as Carl's other sans-I/O code. Strict
Pydantic request, result, cursor, filter, and projection models live in `carl.core` and contain no MCP
SDK, SQLite, or filesystem types. Latest-observation selection, filtering, and stable pagination are
pure functions. The application layer coordinates those rules with SQLite and artifact adapters.
The MCP module translates between official-SDK values and the application; it does not issue SQL,
traverse provenance itself, or construct storage paths. `carl.cli` selects Trio. MCP owns only its
database/application lifetime. `carl work` and `carl monitor --work` compose the isolated worker-pool
runtime with an independently owned database lifetime. Resource-owning context managers enclose
their users: the database outlives shared transports, and transports outlive the worker/watch task
group. Shutdown stops new work, cancels and joins that group, closes transports, then closes the
database. Borrowers never close their shared dependencies.

Resource finalizers use bounded shielding before their first cleanup await, including ownership
locks and successful context exits. Independent cleanup obligations are attempted even after a
failure; incomplete database closure remains retryable. Secondary cleanup failures are reported
using safe phase/type metadata without replacing an active error or cancellation. Transaction
rollback failures remain explicit rather than permitting reuse of uncertain transaction state.
Durable work interruption is reconciled atomically using the lease token and operation binding:
setup without an operation can release its lease, but committed outcomes and replacement owners
are never undone. Lease/permit expiry is crash recovery, not the normal shutdown mechanism.
Ending an operation also cancels any unfinished network activities in the same transaction.
These fallback records retain the previous state, whether dispatch was possible, and the owner's
terminal state; the request outcome remains explicitly unknown. Existing terminal activity results
and event history are preserved. Worker startup reconciles historical activities whose owners
already ended; an expired permit alone is not enough to cancel an activity with a live owner.

MCP tools are defined by an explicit immutable registry constructed during startup. Registration
rejects duplicate tool names and duplicate structured operation identities and retains generated
typed input schemas. Each advertised tool includes its structured Carl operation identity as MCP
metadata. A checked, reviewable snapshot covers every published tool's description,
annotations, input schema, and output schema. Contract tests compare that snapshot with schemas
returned by both in-process and stdio `tools/list`, including agreement between paired preview and
mutation tools. The MCP initialize version includes the startup source-tree hash, and
`get_server_info` exposes the full hash and structured capability versions for runtime diagnosis.
During development, a changed tool schema requires a new agent session. Reconnecting the MCP
transport alone does not prove that a client replaced the model-visible tool definition from the
existing session.
Expected argument, selection, incomplete-gallery, and not-found failures become
actionable tool errors. Protocol, database-integrity, and other infrastructure failures remain
server errors rather than being presented as ordinary user mistakes. The first version exposes tools
only. MCP resources and prompts remain deferred until a concrete client workflow benefits from them.

The initial companion skill lives at `skills/carl/SKILL.md`, the portable plugin layout understood by
the target agent ecosystems. It explains the efficient review workflow, evidence identities, image
context cost, guide conflict handling, and partial-gallery override. It is advisory and the server is
independently usable. Client registration remains outside repository data and may point Codex,
Claude Code, or OpenCode at the same `carl mcp` stdio command. Streamable HTTP can reuse the same
transport-independent server later; legacy SSE is not a target.

Pure tests cover review requests, projections, cursor snapshots, filtering, and latest-observation
selection without a database or MCP process. Adapter tests exercise SQLite guide versioning,
provenance, analysis discovery, and candidate queries in temporary databases. A process-level smoke
test starts `carl mcp` over stdio and exercises tool discovery through the official protocol
implementation.

External new and used price references, their acquisition provenance and
storage, and a separate agent assessment against the user's preferences remain
future features. Candidate assessment should retain every viable listing by its
exact listing ID, including several similar offers with similar prices and
conditions. Ranking and filtering may help the user compare them by location,
cost, condition, or another preference, but must not suppress similar good
options for variety: they are useful alternatives if another listing becomes
unavailable. No reference source or assessment schema is selected yet.

### Search request input

The version-one Marketplace search request retains the query text, requested
location, positive integer radius and explicit unit, optional currency and
exact decimal price bounds, and exact-match choice. Currency is currently
validated as three uppercase letters rather than against provider support.
Location is a discriminated value: either the human text still requiring
resolution or a Facebook location identifier with an optional human-readable
label. This preserves what the caller requested without pretending that a
label, source identifier, or coordinate is another one of those forms.

The collection payload separately requires a bounded traversal policy and the
structured network-path identity, then becomes durable work with overall,
network-path, and work-kind scopes. The traversal policy may bound unique
listing results, pages, monotonic elapsed duration, transferred bytes, decoded
body bytes, or consecutive pages without new listing IDs, and requires at least
one bound. The command requires a result limit, page limit, or both; pagination
stops when the first configured bound is reached. A requested page size is
retained as a hint rather than a promise about Facebook's response size. Its
deduplication identity hashes the exact typed request, traversal policy, and
path serialization;
numerically equivalent decimal spellings or different labels therefore remain
conservatively distinct request intents. The payload does not accept rendered
or server-resolved filters. Those will be extraction outputs linked to the
bootstrap acquisition, allowing a requested 60-mile radius to remain distinct
from an observed 97-kilometer filter. Likewise, `exact_match` records requested
behavior and does not assert that Facebook applied it.

The collection payload also carries a discriminated traversal strategy. Cursor
traversal follows the provider's opaque cursor chain. Overlapping price
partition traversal plans fixed intervals from a caller-selected width and
overlap, requires a finite maximum price, and treats an omitted minimum as
zero. Width and overlap remain decimal currency values rather than encoded
identity strings. The planner is an explicitly registered component, and its
structured identity and output schema version accompany the retained plan.
The default deterministic balanced order visits interval midpoints breadth
first across the price span, reducing the systematic low-price bias that an
early result limit would create with ascending traversal. Ascending order
remains an explicit enum choice and is retained in the strategy provenance.

The input, durable work kind, sans-I/O traversal reducer, protocol extractors,
Proton search session, and foreground worker are implemented. The reducer
accepts one completely acquired page at a time, retains observation and unique
listing counts separately, rejects repeated cursors, and produces a typed
continuation or stopping decision. Elapsed time is accumulated from a monotonic
clock across tunnel startup, requests, extraction, and page checkpoints, and is
rechecked before each pagination request. Traversal state carries its search-run
identifier, attempt, and immutable policy so restored state cannot silently
change bounds.

Price-partition traversal uses the same root bootstrap, Proton tunnel, HTTPX
cookie jar, SQLite-backed request constraints, route-definition extractor, and
page checkpoints. It sends one route-definition request per planned interval
and does not issue GraphQL cursor requests. Page and result limits apply across
the entire plan and retain the complete interval response that first crosses a
limit. Overlap may create duplicate observations; aggregate counting remains
strictly by listing ID. Every page retains its derived requested search and
partition descriptor separately from Facebook's applied configuration.

If an interval reports `has_next_page`, Carl marks that interval saturated and
continues with the next planned interval. A completed plan with saturated
intervals is incomplete evidence rather than an exhaustive inventory. Dense
groups at one price, listings without a numeric price, provider ranking, and
undocumented bound inclusivity can still cause omissions. The first version
therefore requires explicit width and overlap instead of claiming an adaptive
or universally safe partition size.

The first bounded live partition run completed successfully through Proton. It
used the initial ascending implementation with $100 USD intervals and $5
overlap, stopped after four complete pages when the 50-result limit reached 59
unique IDs, and made no cursor request. Every processed interval remained
saturated. Because ascending order stopped before the upper ranges, the planner
now defaults to balanced order. One returned asking price fell below its
requested and server-resolved lower bound, reinforcing that partitioning can
improve discovery while neither enforcing relevance nor proving coverage.
The follow-up live run verified balanced ordering across $285–385, $95–195,
and $475–575 before its result limit stopped at 52 unique IDs. All three pages
were again saturated; balanced ordering reduces systematic price bias but does
not make a bounded result a complete sample.

Route-definition extraction selects query metadata only from the named
preloader list, correlates it to the exact preloader result frame, and selects
result pages only through the structural
`data.marketplace_search.feed_units` chain. Each parseable frame is retained as
native JSON with its acquisition, frame position, and extractor identity;
malformed frames become structured issues. Pagination JSON allows Facebook's
anti-hijacking prefix. Arbitrary `page_info` objects, story identifiers,
recommendation cards, and advertisements cannot become listing identities;
listing IDs come only from `edge.node.listing.id`. Raw native listing objects,
duplicate occurrences, ignored edges, applied variables, source JSON paths,
and structured failures remain retained.

The first adapter requires a Facebook location identifier and USD when price
bounds are supplied. It converts an explicit requested mile radius to the
nearest whole kilometre for Facebook while retaining the original input;
kilometre inputs are passed through. Location-name resolution remains future
work. The worker sends the root GET, route-definition POST, and cursor-based
GraphQL POSTs through one managed Proton tunnel and one HTTPX cookie jar. It
extracts protected session material into non-serializable secret values, records
only redacted metadata for those values, and checkpoints each page's complete
acquisition and extraction before issuing the next request. Deterministic mock
coverage includes the full sequence, and the root plus first result page have
been verified live through Proton Free 54.

Each root, route-definition, and pagination request is a separately recorded
network activity. The initial policy allowed 12 starts per minute and sampled a
2–5 second holdoff for route-definition and pagination requests. The versioned
replacement allows three starts per second and 180 per minute on both the
network path and `www.facebook.com`, with a 0–100 ms follow-up holdoff. The
root request has no initial holdoff. Old definitions, reservations, and
admission events remain retained; policy retirement is linked to a code-
provenance operation. These values are trial limits, not an inferred Facebook
quota.
GraphQL error code 1675004 is retained as a typed `graphql_rate_limited` issue
and produces a `search_pagination_rate_limited` terminal outcome; Carl does not
silently retry it.

An image-reference record comes only from the target listing's gallery fragment
and retains listing ID, source acquisition and block path, original URL, role,
gallery order, source photo ID, and declared dimensions. The bounded
`collect-images` command selects references from the latest usable item-page
result per listing and consumes selected references using a mandatory Proton
activity. It retains the request, response status, headers, timing, routing, and
transfer outcome for every attempt. Response bytes remain transient until
content decoding and image validation succeed.

The initial image policy allowed one active image activity, 12 starts per
minute, and a persisted random 2–5 second holdoff. Its definitions and past
admissions remain retained after supersession. The current policy admits only
one image collection work item at a time across all Carl processes before it
opens an exclusive Proton session. This prevents independent worker processes
from racing on the WireGuard device and port-allocation locks. Network activity
has a separate upper bound of eight, caps starts at three per second and 180 per
minute across CDN hosts, and records a 0–100 ms random holdoff; the serialized
per-item session path currently makes the work limit authoritative. The per-path
limit is also 180 per minute and is shared with other traffic on that route. Earlier search
and item-page limits were scoped to the whole route and inadvertently slowed
CDN requests; they are retired. Marketplace pages now have a separate
three-per-second, 180-per-minute `www.facebook.com` origin limit and a 0–100 ms
holdoff for item pages and search follow-ups. The shared worker pool pulls image
work from the durable queue and reports progress every 100 completed jobs. The command
defaults to at most 10 missing network requests per invocation. Previously
validated images with the same Facebook photo ID are reusable when their
observed dimensions meet or exceed the new reference's declared dimensions. A
reuse does not consume the network-request maximum. Missing photo IDs or
dimensions require an exact signed-URL match. The command does not silently
follow a redirect to a different origin.

Managed Proton startup, shutdown, and lock-contention failures retain their
failure code, diagnostic object, and exit status. Codes classified as transient
release the durable work lease for a bounded backoff and retry up to three
attempts; permanent configuration and route failures remain terminal. The CLI
`retry-image-failures` command and MCP operation `retry_image_failures` requeue
terminal image work related to an exact search-run record or refresh work item.
The retry operation restores the failed attempt's result checkpoint, clears the
terminal error, retains the original operation and events, and adds a new enqueue
event in one bounded SQLite transaction; it reports counts without waiting for
image collection. Requeued jobs are upgraded to collect-image payload schema v2,
so pre-fix schema-v1 workers cannot lease them. Current workers also accept legacy
v1 work, avoiding a stranded old queue. This supports recovery of failures
recorded before automatic retry was available without discarding their history.

Activity snapshots list local processes holding the selected database, WAL, or
shared-memory file open. This makes stale MCP and worker processes visible even
when they predate process reporting. The current process reports its source-tree
hash; the source version of an older peer remains unknown.

On 2026-09-22, a Proton batch collected 10,339 missing renditions from six
saved search runs. Its network requests spanned 58.3 minutes at an average of
177.4 starts per minute, and planning plus offline validation brought the full
invocation to about 67 minutes. All 10,339 requests returned HTTP 200 and all
10,339 image validations succeeded. Together with 50 earlier saves, the
database held all 10,389 distinct renditions referenced by those runs. These
are measurements of that batch, not an assumed future success rate.

A successful image result requires a complete HTTP body, successful content
decoding, a cryptographic content hash, and format and dimension verification.
Carl then writes exactly one original-format, decoded image file beneath
`images/sha256/` next to the selected database and publishes one image artifact
that points to it. The acquisition's available-body reference points to that
same artifact; no generic response-body BLOB or second validated image copy is
stored. A transfer, decoding, or image-validation failure retains its metadata
and typed failure but discards the received bytes. Header MIME type, detected
format, declared dimensions, and observed dimensions are retained separately.
Pillow is the decoder and dimension verifier. Equal bytes share one
content-addressed file while gallery observations and their provenance remain
independent.

`migrate-image-files` revalidates legacy saved images, writes the
content-addressed files, and redirects both the old response-body and image-file
artifact identifiers to the same file. It preserves those identifiers and
operation edges, records the migration operation on changed metadata, and
removes inline content rows once no inline artifact references them. The
migration is resumable. SQLite may retain freed pages until a separately
controlled compaction; compaction is not part of this command.

Facebook source-photo ID is the source image identity when present. A listing
image observation is an edge from a listing observation to that source image
and carries role, gallery order, exact signed URL, declared dimensions, and
source provenance. Multiple listings may therefore reference one source image
without losing their individual observations. Different source-photo IDs are
never merged merely because their bytes match.

Each exact signed URL remains a rendition observation because one source photo
can have multiple transformations or sizes. Carl never normalizes or rewrites
that evidence. Before requesting it, Carl may satisfy the reference from a
previously validated image with the same Facebook photo ID whose observed width
and height meet or exceed the new declared dimensions. It records the new
reference and existing image result as separate inputs to a versioned reuse
operation, so it does not claim that the new URL was fetched. If the source ID
or required dimensions are unavailable, reuse requires the exact signed URL.
After download, identical content hashes share stored bytes while retaining
distinct source-image and listing relationships.

This relation uses the existing provenance graph and a new record kind; it does
not change the SQLite schema or rewrite prior image results. Existing exact-
rendition behavior remains readable without a data migration.

Image follow-up uses the same selection principle as item pages. Repeated
references first resolve to an exact successful acquisition or an explicitly
recorded adequate source-photo reuse. Only an unresolved reference enters the
image request pool;
an explicit future freshness policy can request another acquisition. Failed
attempts remain analyzable and never hide an older successful image artifact.

The item-page slice handles durable acquisition and extraction through an
explicit configured route, while tests may inject deterministic transports. A
retained HTTP 200 login response is an unusable item response and produces a
terminal extraction outcome rather than a successful detail result. Image
download is a separate durable acquisition and validation component. Playwright
is not part of the initial item-page or image collection. It remains a possible
later adapter for virtualized search discovery, logged-in-only fields, or
browser navigation fallback, where its browser behavior is itself retained as
distinct acquisition provenance. Browser fallback, assessment, and monitoring
are future components sharing the same provenance model.

Offline findings from the supplied search proof of concept are recorded in
[Facebook search reference evidence](facebook-search-reference.md). Decodo item
retrieval remains unverified until account credentials are configured and a
bounded live request is authorized.
