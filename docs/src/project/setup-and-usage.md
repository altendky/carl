# Setup and everyday usage

Carl retains Facebook Marketplace and eBay evidence for agent-led comparison. MCP
queues work and reads saved evidence; a separate worker performs collection and
analysis. Read-only review of an existing database needs neither proxy access nor
Claude. Live collection and analysis can incur provider and AI charges.

## Install and find the local state

Use Python 3.13 or newer and `uv`. From the checkout:

```console
uv sync --locked --all-groups
uv run --locked carl locations
uv run --locked carl init
```

`init` creates/migrates the database, not configuration, credentials, or workers.
There is no general `carl migrate` command. `migrate-image-files` moves legacy
inline images into files; it is not schema migration or database compaction.

`locations` reports the effective configuration, database, image, error-log,
cache, state, and runtime paths. Typical Linux locations are
`~/.config/carl/config.toml`, `~/.local/share/carl/carl.sqlite3`, and
`~/.local/state/carl/mcp-errors.jsonl`. Platform and XDG settings can change them.
Images live under `images/` in the database's parent directory. Databases in the
same directory share that content-addressed tree; a different parent directory
uses a different tree. Keep the database and its referenced files together for backups.

## Configure the collection routes

Create `config.toml` at the reported `configuration_file` path. It must be a
regular file owned by the current user with no group/other permissions (`chmod 600`).
Route names are exact identities, not automatic discovery. Do not store passwords here.

The baseline below supplies the current defaults: Decodo datacenter for Facebook
searches and both marketplaces' gallery images, and a separate mobile route for
Facebook item pages and the eBay search/item/description stack. Residential may
replace mobile if that is the purchased product; use its actual gateway and port.
Replace the two base usernames with the values from the corresponding products,
without `user-`, country, session, or duration suffixes; Carl constructs those.
Replace the Wireproxy metadata placeholders too: declare an absolute executable
path, its actual version, and its verified lowercase SHA-256. The current schema
requires that declaration even for proxy-only configuration; Decodo does not load
the binary. Use the endpoint and product supplied by your proxy account rather
than assuming a gateway works for every subscription.

```toml
[schema]
namespace = "carl"
domain = "configuration"
version = 1

# Required by the current schema even when all configured routes are proxies.
# Replace these placeholders with the binary's verified metadata.
[wireproxy]
executable_path = "/opt/carl/bin/wireproxy"
version = "REPLACE_WITH_WIREPROXY_VERSION"
binary_sha256 = "REPLACE_WITH_64_CHARACTER_LOWERCASE_SHA256"

[ebay_search]
maximum_page_attempts = 7
maximum_homepage_attempts = 2
minimum_holdoff_seconds = 0.0
maximum_holdoff_seconds = 0.0

[ebay_item]
maximum_page_attempts = 7
minimum_holdoff_seconds = 0.0
maximum_holdoff_seconds = 0.0

[[http_transports]]
identifier = "browser_chrome_154"
implementation = "wreq"
emulation_profile = "chrome_154"

[[acquisition_stacks]]
identifier = "ebay_anonymous"
http_transport = "browser_chrome_154"
network_path = ["decodo", "personal", "carl"]

[[routes]]
provider = "decodo"
network_path = ["decodo", "personal", "datacenter"]
account_identifier = "personal"
proxy_username = "DATACENTER_BASE_USERNAME"
credential_id = "datacenter"
product = "datacenter_proxy"
country_code = "us"

[routes.endpoint]
host = "dc.decodo.com"
port = 11000
port_end = 19000

[[routes]]
provider = "decodo"
network_path = ["decodo", "personal", "carl"]
account_identifier = "personal"
proxy_username = "MOBILE_BASE_USERNAME"
credential_id = "carl"
product = "mobile_proxy"
country_code = "us"
session_duration_minutes = 10

[routes.endpoint]
host = "gate.decodo.com"
port = 11000
port_end = 19000

# Optional driving estimates. Replace this example origin in local config only.
[travel]
provider = "openrouteservice"
origin_label = "Philadelphia, PA"
country_code = "USA"
cache_ttl_seconds = 604800
maximum_geocodes_per_batch = 20

[openrouteservice]
credential_id = "carl"

# Optional local backend; select it with travel.provider = "osrm" after preparation.
[osrm]
dataset_path = "/absolute/path/to/region.osrm"
dataset_identifier = "example-map-version"
maximum_snap_distance_meters = 5000
use_mmap = true

# Optional offline US locality/ZIP lookup; setup writes these paths automatically.
[geonames]
database_path = "/absolute/path/to/places.sqlite3"
dataset_identifier = "example-geocoder-version"
```

The mandatory Wireproxy declaration does not install or invoke Wireproxy for
Decodo routes. Do not substitute fabricated verification data for these placeholders.
VPN use requires the declared verified binary to be installed and privately imported
provider configuration; see [the provider configuration notes](index.md#local-configuration).
The mobile gateway above follows [Decodo's mobile quick start](https://help.decodo.com/docs/mobile-proxy-quick-start).
Use your account-generated HTTP endpoint if it differs; provider examples are not a promise
that every product or subscription uses the same gateway.
The pinned `wreq` Python dependency supplies Chrome 154/Linux emulation; this does not
require installing or automating a Chrome browser.
For any Decodo product, `endpoint.port_end` enables uniform random selection from
the inclusive range starting at `endpoint.port`. Choose a range available to your
account. Each new session chooses one port and retains it through requests,
pagination, same-session page retries and reconnects. Routing observations record
the selected port. A durable retry that opens a new session chooses again.
Omit `port_end` to use one fixed port, or specify an explicit `endpoint.ports` list
instead of a range. A new session makes a new selection, which can repeat a port.
Install this version and restart workers and MCP before adding the new settings;
older configuration parsers reject them.
Search pages retry challenges and HTTP 403/429 responses in the same session, up to
seven total attempts per page. A homepage 403 gets one retry before search continues,
even if the homepage remains blocked. Each attempt retains its acquisition evidence.
The optional `[ebay_search]` section above shows the defaults: no added waits between
homepage, search, pagination or retry requests. Set the holdoff bounds to `4.0` and
`8.0` to restore random 4–8 second waits. Transport failures still use the worker's
existing retry policy; blocked runs that exhaust page attempts retain the shared
cooldown and fresh-session retries.
Item pages use the separate optional `[ebay_item]` settings. Blocked responses retry
in the same session with no added wait by default, up to seven total HTTP responses
per item work item, including responses retained by earlier attempts. Exhausting
that budget stops the item job instead of scheduling another 40-minute recovery.
Each response retains its acquisition evidence. Set the item holdoff bounds above
zero to add waits between blocked-response retries. Workers reload these settings
between jobs; installing the new retry implementation requires restarting workers.
Existing transports explicitly selecting `chrome_153` retain that profile and its
default platform. After updating Carl and restarting its workers and MCP clients,
change their `emulation_profile` to `chrome_154` to select the new profile. The
transport identifier and acquisition-stack references can stay unchanged.

The optional `[travel]` section above enables driving distance and duration
estimates on listing and review reads. Keep the pickup origin in local config;
do not commit personal locations.
Alternatively, supply `origin = { latitude = 40.0, longitude = -75.0 }` instead
of `origin_label`. Coordinates take precedence when both are present. The API key
belongs in `private/openrouteservice/carl.api-key`, with owner-only permissions
(`0600`), following the same separation as Decodo credentials. Inline API keys
are rejected. Restart workers and MCP after updating Carl before adding this
section, so every process recognizes it. Legacy origin/cache settings in
`[openrouteservice]` still work when `[travel]` is absent.

Set `travel.provider` to `"osrm"` for local driving tables through the pinned
`osrm-bindings` dependency, or `"openrouteservice"` for HeiGIT's hosted Matrix API.
There is no automatic provider fallback. The local backend needs a prepared car
MLD dataset: run the packaged `python -m osrm extract`, `partition`, and `customize`
tools using the package's car profile. `osrm.dataset_path` is the absolute
`.osrm` prefix; the suffixed companion files must remain together. Give every
rebuilt dataset a new immutable directory and `dataset_identifier`. Never replace
files while mapped by a running process. Changing the dataset identity reloads
the engine and invalidates cached and retained route selection. Carl automatically
manages a local subprocess for the memory-mapped engine so native Python-lock
holds cannot stall MCP or lease renewal. Queries time out after 60 seconds and
cancel the busy subprocess. A distant endpoint snap is
reported unavailable rather than inventing a route to the edge of the map.
Keep a buffered map larger than the listing search area and retain its source
timestamps, car-profile hash, and build manifest outside the repository.

Local routes between known coordinates need no API key or network. Text-only
locations can use the full-US offline GeoNames database with
`travel.geocoding_provider = "geonames"`, or the separately cached ORS geocoder with its quota;
set `travel.geocoding_provider = "disabled"` to allow only coordinates and cached
geocodes. An explicit coordinate origin avoids a geocoding request at startup.

Listing reads return an approximate `route_estimate` using retained item or search
card coordinates where available, otherwise a geocoded locality. Distances are
road distances in meters; durations are seconds without live traffic. Missing,
ambiguous, conflicting, unreachable, and provider-failed locations remain explicit
unknowns; they never fall back to straight-line estimates. Older eBay evidence
without an item location remains unknown until new extraction captures one.
Routing and geocoding use a separate private cache and paced, batched requests.
The per-batch geocoding budget limits new hosted lookups; cached and local GeoNames
lookups do not consume it. GeoNames resolves exact city/state, aliases and ZIP codes,
including ZIP+4. It rejects ambiguity instead of choosing a nearby/populous namesake.
It supplies locality coordinates, not street-address precision. Data attribution:
[GeoNames](https://www.geonames.org/), Creative Commons Attribution 4.0.

Local dataset management is part of Carl. Neither worker startup nor listing reads
download or rebuild data. Run `carl travel status` for readiness, age and corrective
actions; `carl travel verify` also checks file hashes and GeoNames SQLite integrity.
Use `carl travel setup geonames` for the full US locality/alias/ZIP database, and
`carl travel setup osrm` for a buffered US road map around the coordinates in your
private `travel.origin` config. Both run in the foreground without a queue worker.
OSRM preparation requires the separate `osmium-tool` package (`osmium` on PATH);
the OSRM tools and car profile come from the pinned Python dependency. The measured
250-mile build took about 30 minutes, 23 GiB peak RAM and 8.84 GiB installed space.
Updates also need room for temporary files and both map versions. Choose
`--radius-miles` and `--threads` explicitly if needed; defaults are 250 and 4.

`carl travel update geonames` and `carl travel update osrm` explicitly rebuild from
current sources. Downloads are checksummed, road inputs share one dated snapshot,
and completed data is published under an immutable version. Activation atomically
updates only the corresponding provider settings in private config. A failed or
cancelled build leaves the selected version unchanged. `--no-activate` prepares a
version without selecting it. Use `carl travel datasets --limit 20` (and `--cursor`
for continuation) and `carl travel activate PROVIDER DATASET_IDENTIFIER` to select
an installed version or roll back. Old versions remain installed until you choose
to remove them; mapped files must never be overwritten. Data over 30 days old is
reported as outdated, without triggering automatic network access.

MCP equivalents are `get_travel_data_status`, `list_travel_datasets`,
`request_travel_data_setup` and `activate_travel_dataset`. Setup/update requests use
stable request IDs and return durable work IDs promptly; a running Carl worker is
required. Poll `get_work_status` and `get_travel_data_status` for outcome/current
build stage; ordinary work cancellation also stops a build. Builds are serialized
across providers and foreground CLI commands with a process-shared lock. MCP does
not start a worker. Full verification and activation hash entire datasets; use the
CLI for large maps when a client's MCP timeout would be restrictive.
If a foreground build owns the lock, queued setup waits for a minute before
checking again. Failed OSRM builds retain the failing stage's last 64 KiB in a
private `build-reports` log; status reports the stage, reason and log filename.

For composed/workspace listing filters, use `filters.travel` with
`maximum_driving_distance_miles`, `maximum_driving_duration_minutes`, and
`include_unknown` (default `true`). Review-batch acquisition accepts the same
limits in `travel`. Estimates and filters apply before listing pagination, with
cursors bound to the origin. Set `include_travel=false` to skip estimates when
no travel filter is active. Estimates do not invalidate completed reviews.
Automatic search-pipeline selection and explicit analysis selection remain
unchanged; they do not apply these travel filters.

To refresh only a listing's driving estimate, call `request_listing_route_refresh`
with an exact `listing_identifier`, a new `request_identifier`, and optionally a
`workspace_record_identifier` for ownership and cancellation. This queues a
standalone worker job without fetching marketplace pages, images, or analyses.
The worker bypasses cached route metrics and publishes an immutable
`["carl", "travel", "estimate"]` record in Carl's main database, with origin and
destination identities, the frozen destination, timestamp, and source-record links.
Poll `get_work_status` for `estimate_record_identifier`, then read it with
`get_listing_route_estimate`. Replaying a request ID returns the same work even
after completion; use a new ID for a new measurement. Composed listing reads
select the newest retained record matching the current origin and location.
An unavailable measurement remains explicit rather than reviving an older
successful result. Changed origin or location inputs cannot select the old record.
Expired results fall back to the normal routing service; their immutable records
remain available by ID. Successful estimates use the configured cache lifetime;
unavailable results expire after 60 seconds. These retained estimates are
independent of completed review revisions.

Store each product's separate password through a non-echoing prompt:

```console
uv run --locked carl configure-decodo-credential datacenter
uv run --locked carl configure-decodo-credential carl
```

These commands create private `private/decodo/` files and refuse replacement.
They do not import from 1Password. Never put passwords in arguments, examples,
provenance, or agent-visible output.

Decodo [documents static ports and rotating port 10000](https://help.decodo.com/docs/datacenter-pay-per-gb-proxy-session-types).
Use a static datacenter port for Facebook bootstrap and follow-ups; a session can
select it from a configured pool. Mobile/residential
searches use one requested sticky session per attempt and fresh sessions for
independent attempts. Neither policy guarantees the provider's exit remains
healthy. An explicit Proton route is real Proton; no automatic fallback changes
providers. Optional adapters are not all wired into every production workflow.

## Start workers and connect MCP

Run as the same user/environment with the same database. Start one persistent worker:

```console
uv run --locked carl monitor --work
```

| Process | Executes queued work? | Purpose |
| --- | --- | --- |
| `carl work` | Yes | Persistent worker without dashboard |
| `carl monitor --work` | Yes | Persistent worker and dashboard |
| `carl monitor` | No | Observe queue/activity only |
| `carl monitor --once` | No | Print one snapshot and exit |
| `carl mcp` | No | Queue mutations and read retained evidence over stdio |

Do not use `monitor --once --work` as a persistent worker: it exits after its
first snapshot. Additional worker processes increase the per-process worker
pool, although database-enforced constraints remain shared.

Configure the MCP client's executable as `uv` and its arguments as:

```text
--directory /absolute/path/to/carl run --locked carl mcp
```

The absolute checkout path avoids relying on the client's working directory.
For a custom store, append `--database /absolute/path/to/carl.sqlite3` to MCP,
`init`, and worker commands alike. A different database means a different queue.
Initialize it before starting MCP. MCP can migrate/validate an existing database
after its transport handshake; tools wait for readiness, not for background jobs.

After connecting, call `get_server_info`, then `get_activity_snapshot`. Confirm
the database, server instance, source-tree hash, capabilities, and worker activity.
Work IDs mean accepted, not completed. Pass full IDs to `get_work_status`, not display abbreviations.

## Choose a bounded workflow

`create_search` takes marketplace-discriminated targets and is search-only. For
example, its request object can contain:

```json
{
  "targets": [
    {"marketplace": "ebay", "search": {"query": "baader morpheus eyepiece", "maximum_pages": 2}}
  ]
}
```

Poll work IDs, inspect `get_search`, and page `list_search_results`. Failure or an
unrecognized response is not successful emptiness. eBay defaults to five pages,
capped at twenty. Anonymous sold/completed acquisition cannot handle encountered
sign-in/challenge gating and is unsupported. Retained closed results remain
readable; ordinary items can still later be observed as sold.

Facebook targets require explicit location, radius/unit, and finite traversal
bounds. A numeric Facebook location ID is preferable to ambiguous text. These
values express search intent; approximate location and server-applied radius do
not establish exact distance or complete geographic coverage. Cursor traversal is
the default. `overlapping_price_partitions` requests the first result page of each
bucket instead; it is not exhaustive pagination. Bound its price range and number
of partitions, and inspect stopping/coverage evidence before claiming completeness.

For authorized end-to-end acquisition, use `request_search_pipeline` with a
stable caller-generated request ID. Select `source.kind="new_search"` and targets,
or retained `search`, `search_work`, or `workspace` scope. Choose `stop_after`
(`details`, `images`, or `analysis`) and finite item, global image, per-listing
image, and analysis budgets. Analysis requires an exact product-guide record.
Replay an interrupted request ID only with unchanged intent. Inspect
`get_search_pipeline` stage counts, failures, skips, and exhausted budgets;
partial results are not exhaustive success.

For one item, use `request_listing_details` for either marketplace. Acquisition
completion does not mean extraction, descriptions, or images succeeded. Inspect
`get_listing_details` classification, description state, and individual image
states. Gallery URLs are not saved bytes; use artifact IDs with `get_listing_image`.

Use review workspaces to combine related searches, refresh stable tracks, retain
shortlists, and coordinate claims. Available-only views are the default; supply
explicit statuses for other retained items. See [review workspaces](review-workspaces.md)
and [the agent workflow](../../../skills/carl/SKILL.md) for bounded review and analysis.

## Enable AI analysis only when needed

Install and authenticate Claude Code separately, and ensure the worker's PATH
contains `claude`. Carl invokes it noninteractively with `Read`, `WebSearch`, and
`WebFetch`; Python dependency installation does not provide that executable or
its account access. Default analysis uses `claude-sonnet-5-5`, medium effort,
at least eight turns (sized to the selected gallery), and a 210-second deadline.
Existing queued jobs retain their captured
settings. Incomplete galleries fail by default; explicitly allow partial analysis
only when useful, and disclose missing evidence in comparisons.

## Diagnose and recover without unnecessary collection

Start with server info, activity, and the full work ID. `get_work_status` with
`include_details=true` exposes checkpoints, errors, and operation details.
Outages pause acquisition with direct connectivity probes every 30 seconds;
healthy Internet plus a blocked site or broken proxy still uses bounded retries.

Unexpected MCP errors include an error ID and traceback. The same diagnostics
appear in the private rotating `mcp-errors.jsonl` reported by `locations` (5 MiB,
three backups). Redaction is best-effort: do not publish logs without checking for
unlabeled secrets. Inspect retained evidence before spending on another attempt.

- `retry_workspace_search_track`: retry a failed initial track without creating
  another watch. It does not bypass the closed-search restriction.
- `retry_item_failures`: bounded recovery of a settled refresh's transient
  item-page failures without repeating search or expanding its image budget.
  CLI: `carl retry-item-failures REFRESH_WORK_ID`.
- `retry_image_failures`: bounded image recovery for an exact supported search run
  or refresh. CLI: `carl retry-image-failures SOURCE_ID`. Fix permanent
  configuration/authentication errors first.
- `retry_refresh_finalization`: after fixing a handler/programming failure,
  resume an exact terminal Facebook/eBay refresh from a safe retained checkpoint.
  It does not rerun search or reset terminal/completed child jobs; unsafe recovery
  is rejected. This recovery is MCP-only, not a general automatic retry.
  A refresh reporting `refresh_image_reuse_evidence_unavailable` can also resume
  after its missing or corrupt saved image files are restored. The retained image
  plan is revalidated before reuse is reported.

Wait for each resumed refresh to settle before another recovery call. A refresh
losing at least half its selected item pages is a failure; smaller losses remain
partial completion. Check outcomes and failure summaries rather than state alone.

## Cancel queued or running work

Use the MCP tool `cancel_work(work_identifier, reason)` for one exact root and
its descendants, or `cancel_workspace_work(workspace_record_identifier, reason)`
for all active roots in a workspace and their descendants. For a global stop,
call `cancel_all_work(reason, confirm=true)`. An exact terminal root is a no-op.
Set `include_descendants=false` on `cancel_work` to cancel only that item.

Queued work becomes `cancelled` immediately, even with no worker running.
Running work shows `cancelling` until its next operation or lease renewal stops
the attempt cooperatively. Completed evidence and checkpoints remain available.
Cancellation is separate from failure in work, activity and workspace status;
cancelled jobs do not appear in the workspace's failed list. A parent depending
on a cancelled child also cancels; this can include another parent sharing that
child. New children cannot escape a cancellation racing their parent.

Results report `cancelled_pending`, `cancelling_leased`, and `already_terminal`,
with their work IDs. Large results use the standard bounded response pages:
follow `get_tool_response_page` until complete and acknowledge the final page.
Each cancellation event retains the reason and calling MCP tool/server identity.
Use `request_workspace_refresh` normally to create fresh work after cancellation;
cancelled work does not occupy the active deduplication identity.

## Upgrade together

Stop old workers and MCP servers, update the checkout, run
`uv sync --locked --all-groups` and `uv run --locked carl init`, then restart workers and
MCP clients. If the client caches tool definitions, reload them or start a new agent
session after reconnecting. Compare server capability versions. Worker startup
updates immutable constraint policy and reconciles unfinished activities whose
owning operations already ended. Historical evidence and events remain retained.
The image-reuse schema upgrade builds indexed lookup keys from retained saved-image
metadata once, inside the migration transaction. All writers must run the upgraded
code so each new image publication also maintains these derived keys. Saved-image
records and artifact verification remain authoritative.
The cancellation schema upgrade preserves scheduling history while adding terminal
cancellation states and indexed dependency links. Stop all old workers and MCP
servers before migration so every process understands cancellation and records
new dependency links.
For legacy Proton-to-Decodo overrides, follow the separate audited
`cutover-route-overrides` procedure before removing those configuration entries.
