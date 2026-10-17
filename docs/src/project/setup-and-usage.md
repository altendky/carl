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
port = 10001

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
port = 7000
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

Store each product's separate password through a non-echoing prompt:

```console
uv run --locked carl configure-decodo-credential datacenter
uv run --locked carl configure-decodo-credential carl
```

These commands create private `private/decodo/` files and refuse replacement.
They do not import from 1Password. Never put passwords in arguments, examples,
provenance, or agent-visible output.

Decodo [documents static ports and rotating port 10000](https://help.decodo.com/docs/datacenter-pay-per-gb-proxy-session-types).
Use a static datacenter port for Facebook bootstrap and follow-ups. Mobile/residential
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
For legacy Proton-to-Decodo overrides, follow the separate audited
`cutover-route-overrides` procedure before removing those configuration entries.
