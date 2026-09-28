# Carl

Carl helps an AI agent search and compare used items on Facebook Marketplace. It saves the listings,
photos, and research behind the agent's conclusions so the work can be checked and continued later.

Carl is designed primarily for agents through MCP. Its CLI runs background work and shows activity.

## The flow

1. **Search** — Search Marketplace and save what it returned.
2. **Collect details** — Fetch listing pages and photos. Previously collected pages and images are
   reused instead of downloaded again.
3. **Organize the search** — Combine related searches, such as several product names or price ranges,
   into one workspace without duplicate listings.
4. **See what is known** — View the newest known price, availability, description, photos, and prior
   research for each listing. Available listings are shown by default.
5. **Evaluate promising listings** — A product guide is a reusable checklist of what the agent should
   determine. Carl gives Claude that guide together with the listing text and photos. Claude can
   identify the product, research published specifications, compare them with the seller's claims,
   assess visible condition and missing parts, and suggest questions or checks for pickup. The report
   says what is known, what is uncertain, and which listing evidence it used.
6. **Refresh and revisit** — Rerun the searches, see which listings are new or materially changed,
   and reuse earlier analysis when the evidence still applies.

## Features

- Search several phrases or price ranges and review their combined results.
- See a current listing view assembled from the newest useful search, listing-page, photo, and
  research data while retaining its sources.
- Available-only results by default, with explicit controls for pending, sold, unavailable, and
  unknown listings.
- Use different evaluation guides for different kinds of products in one workspace.
- Keep shortlists and decisions so another session or agent can continue the review.
- Refresh searches without discarding history, highlight meaningful changes, and avoid downloading
  unchanged pages and photos again.
- Continue long-running searches, downloads, and evaluations across restarts, with automatic retries
  for temporary failures.
- Build explicit, ordered network paths from VPN and proxy layers. Carl includes integrations for
  Proton and Mullvad through WireGuard, plus Decodo and Bright Data proxies, and records the exact
  path used for every request.

## Running Carl

Carl requires Python 3.13 or newer. From a checkout:

```console
uv sync --all-groups
uv run carl init
uv run carl locations
```

At least one network path must be configured before live collection. `carl locations` shows the
user-specific configuration, database, and image directories.

Keep one worker process running whenever queued collection or analysis should progress:

```console
uv run carl monitor --work
```

This runs background work and displays its progress. Use `uv run carl work` to run it without the
dashboard. MCP servers intentionally do not start background work themselves.

Configure an MCP client to launch:

```console
uv run carl mcp
```

The MCP interface guides agents through the workflow above and allows them to inspect the underlying
evidence when necessary.

## Development

```console
mise run check
mise run pre-commit
```

The project is under active development. See the
[architecture notes](docs/src/project/index.md) and
[review workspace design](docs/src/project/review-workspaces.md) for the detailed model.

Carl is available under the MIT or Apache-2.0 license.
