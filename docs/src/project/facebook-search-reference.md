# Facebook Marketplace search reference evidence

## Scope

The original findings came from offline inspection of temporary
proof-of-concept artifacts under `/tmp/agents/OGYrj2TBqy/`. They support design
work but are not Carl production captures. A later bounded browser inspection
and Carl HTTPX experiment on 2026-09-20 established the current initial-search
request described below.

The inspected evidence files were:

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| `page1.html` | 654711 | `8a5338b3e660cd107ea03d1523ae8bc1e9f2cf5cae30d64ba2addd9cc08bd960` |
| `page1.headers.txt` | 5935 | `191c891d0afae87dbca782be2d838fa19903bb0c46c5f606f933b46bc4085ef8` |
| `page2.json` | 66449 | `2e17490ad92de2fb16f2df9341a15f48c5afa53bd11131fa4f74cc55ee8d8927` |
| `page2.headers.txt` | 5570 | `113d6d24946dadc0067befe5e9de6f2b22bd42214cc1a57736d235ba3f98388e` |

The cookie jar was not inspected. No token, cursor, cookie value, signed image
URL, or persisted-query ID is reproduced here.

## Confirmed structure

The bootstrap file contains 59 `application/json` script elements, all of which
parse as JSON with Carl's current parser. This differs from the supplied count
of 54 and may reflect a different counting method. The discrepancy reinforces
recording parser version and block provenance rather than treating a block count
as a protocol invariant.

One object names `CometMarketplaceSearchContentContainerQuery` and provides a
decimal persisted-query ID plus variables. The persisted-query ID is ephemeral
and must be extracted for each bootstrap session. Relevant observed variables
include:

- `count` of 24;
- null initial cursor;
- saved search query `telescope`;
- a configured search center and radius;
- a Marketplace topic location selected for the experiment;
- lower price 0 and upper price 60000;
- radius 65 km.

The filter values are nested under
`variables.params.browse_request_params`. They are not direct children of the
variables object. Requested search inputs and these server-resolved values must
be retained separately.

The bootstrap has one `page_info` candidate with `has_next_page`, at the search
connection ending in `data.marketplace_search.feed_units`. The pagination body
also has exactly one candidate at `data.marketplace_search.feed_units`. Carl
should nevertheless bind page information to this connection path and expected
listing-edge structure instead of accepting the first recursively encountered
`page_info` object.

The bootstrap contains 16 listing objects and the pagination body contains 24.
Their listing ID sets do not overlap. Each page also contains a distinct story
key for every listing, confirming that story identity and listing identity are
separate. Across the 40 listing objects, 40 source primary-photo IDs were
observed and none was reused in this sample.

Both saved responses report HTTP/2 status 200 and `Content-Encoding: zstd`.
The bootstrap reports HTML with UTF-8. The pagination response also reports
`text/html` despite containing a JSON response body, so MIME type alone cannot
classify the GraphQL result. The saved files contain curl-decoded bodies. Their
headers contain no content length, so the claimed compressed transfer size and
transport bandwidth cost cannot be independently reconstructed from these
artifacts.

## Production implications

The implemented search work input retains requested query,
location, explicit distance value and unit, optional exact decimal price bounds
and currency, exact-match choice, and network path. It deliberately excludes
server-resolved filters. Root-page extraction produces protected anonymous
session material. Route-definition extraction produces a session-scoped
pagination plan: query ID, complete variables, the precisely identified search
connection, and cursor. Query IDs, cursors, cookies, and request tokens are
never static configuration.

The Marketplace root GET, route-definition POST, and pagination POSTs form one
search session and use one stable Proton tunnel, exit, request identity, and
cookie jar unless later evidence demonstrates otherwise. The route-definition
response consists of newline-delimited anti-hijacking-prefixed JSON frames. Carl
extracts the current search query plan from the first response and accepts the
listing connection only from the result frame with the same preloader ID. The
search worker processes the cursor chain sequentially under one lease rather
than enqueueing hundreds of cursor tasks. Every page remains an independent
HTTP acquisition and extraction checkpoint linked to the search session and
page ordinal.

Stopping conditions include no next page, missing cursor, repeated cursor,
configured page or listing limit, deadline, transport exhaustion, invalid
session material, persisted-query rejection, malformed response, and explicit
cancellation. Exact listing IDs deduplicate scheduling of detail work, while
every search-page observation and rank or position remains retained.

The proof script uses a broad recursive `page_info` search, regular expressions
for session tokens, a fixed browser version, curl, and a shared header list with
GraphQL additions. These demonstrate feasibility but are not production
interfaces. Carl uses typed root-bootstrap, route-definition, and pagination
extractors, separate navigation and GraphQL request models, current runtime
identity, retained attempt evidence, and the mandatory Proton network activity.

The bounded Proton Free 54 experiment retained 15 listings from the first page,
the current search query plan, a next-page cursor, and applied filters including
97 km for the requested 60 miles. A following pagination request received
Facebook error code 1675004 (`Rate limit exceeded`), which Carl retained as a
terminal protocol failure. Stable-tunnel initial collection is verified;
pagination reliability and bandwidth behavior across multiple sessions remain
open. Bright Data evaluation now belongs to the later item-detail route.

That pagination request started 47.962 milliseconds after the successful first
page response ended. Carl now submits every request through its durable network
activity scheduler while retaining the same live session. The initial policy
adds a recorded, uniformly sampled 2–5 second holdoff before route-definition
and pagination requests and limits the selected network path to 12 starts per
minute. These conservative values require measurement; they are not evidence of
Facebook's actual quota or proof that timing caused error 1675004.

A second bounded Proton Free 54 run verified this scheduler. It sampled
4.515808414 seconds before route-definition and 3.645782839 seconds before
pagination. The actual response-end-to-next-request-start gaps were 4.642192
seconds and 3.713430 seconds. Facebook still returned code 1675004 for the
pagination request. Pacing is therefore working and attributable, but this
particular policy did not restore pagination on that exit at that time.

## Authenticated browser experiment on 2026-09-20

A bounded inspection used an authenticated browser profile, with the Proton
browser extension on a newly selected exit. The shared VPN exit address is
intentionally omitted. Browser identity at collection time was Chromium on
Linux; locale and time-zone details are also omitted. These are observations,
not values Carl should hard-code.

The browser submitted searches through Marketplace's own search box. It made a
route-definition request and then used
`CometMarketplaceSearchContentPaginationQuery` for scrolling. Every observed
pagination request kept `count` at 24 and supplied a nonempty cursor. The
request sequence was shared by all requests belonging to the current document
and encoded in base 36. It did not start over at one for a route transition:

| Search | Route-definition `__req` | Pagination `__req` values |
| --- | --- | --- |
| `telescope` | `12` | `1a`, `1b`, `1c` |
| `binoculars` | `1q` | `1s`, `1u`, `1y`, `1z` |
| `microscope` after a new Marketplace document | `11` | `18`, `19` |

Full document navigation reset the page sequence. Client-side route changes
continued it, with unrelated GraphQL queries and mutations consuming
intermediate values. Carl currently controls every request in its anonymous
session, so it can advance the sequence deterministically, but it must not use
the same request number for route-definition and the following pagination
request.

The successful pagination gaps included approximately 1.815, 2.071, 2.552,
3.362, and 3.393 seconds. This does not establish a safe production rate, but
it shows that Carl's earlier failure after a 3.713-second gap cannot be
attributed to that delay alone.

After correcting Carl to advance the first pagination request to `__req=2`
and preserve the extracted `count=24`, one more bounded anonymous attempt used
a newly selected Proton route. The retained request
confirmed both corrected values and began approximately 3.685 seconds after the
route-definition response ended. Facebook still returned code 1675004 in a
113-byte response. Those two fidelity corrections are necessary, but neither
was sufficient. The authenticated browser experiment used a different Proton
exit, so exit reputation, authentication state, and query-operation choice are
still confounded.

Two additional corrected anonymous attempts sampled distinct Proton routes and
exit IPs. All three returned 14 first-page listing observations and the same
113-byte code-1675004 pagination response:

| Route | Exit address | Response-end to pagination-start gap |
| --- | --- | ---: |
| route A | omitted | 3.684632 seconds |
| route B | omitted | 4.930367 seconds |
| route C | omitted | 4.301797 seconds |

This small sample makes a single bad exit less likely, while remaining too
small to characterize Proton generally. The strongest remaining differences
are anonymous versus authenticated state, the container versus pagination
persisted operation, and the HTTP client's page-context fields or transport
fingerprint.

Current third-party reports disagree about anonymous cursor support. Some
commercial collectors claim to paginate anonymous Relay searches when using a
residential proxy and a browser-like TLS fingerprint. Another current collector
explicitly reports that anonymous cursor pagination is disabled and instead
subdivides the requested price range. Carl therefore supports overlapping,
fixed price partitions as an explicit search-flow strategy. It records every
interval and flags intervals whose first page still advertises a next page;
the strategy is an experiment in improving coverage, not evidence of complete
coverage.

A bounded live Carl run then used the initial ascending implementation with
$100 USD partitions and $5 overlap through Proton Free 54. It completed four
interval pages before the 50-result limit was first crossed, retaining 59
observations and 59 unique listing IDs. The requested intervals were $0–100,
$95–195, $190–290, and $285–385; Facebook's resolved variables reported the
corresponding cent bounds. The pages contained 13, 13, 16, and 17 observations.
All four advertised another page, so Carl retained all four as saturated and
did not claim complete coverage. No GraphQL pagination request was made. The
early stop also demonstrated systematic low-price bias, so the implemented
default is now a deterministic balanced order across the requested span;
ascending remains available explicitly.

One observed asking price was $264.59 in the $285–385 request despite the
resolved lower bound being 28,500 cents. This confirms that requested and
server-resolved filters still do not guarantee that every returned listing
satisfies the bound. Partition overlap produced no duplicate IDs in this small
sample, but duplicate occurrences remain valid evidence and are preserved.

A second bounded run verified balanced ordering. Its first three intervals were
$285–385, $95–195, and $475–575 and returned 18, 16, and 18 observations. The
50-result limit stopped the run at 52 unique IDs after those three complete
pages. All three intervals were saturated. This validates that an early limit
now samples separated parts of the price span, while still leaving unvisited
intervals and incomplete coverage explicit.

The authenticated browser's pagination plan differed materially from the
route preloader that Carl currently extracts. The preloader identified
`CometMarketplaceSearchContentContainerQuery`, while browser pagination used
`CometMarketplaceSearchContentPaginationQuery` with a different persisted-query
ID. Its variables were the pagination subset: `count`, `cursor`, `params`,
`scale`, and one Marketplace feature flag. The route response did not expose a
second `queryName` record for the pagination operation; the browser appears to
obtain that operation from loaded client code. The old anonymous proof used the
container query for successful pagination, so authentication state, deployment
changes, and operation selection remain separate hypotheses to test.

The authenticated form also contained more page-context fields than the old
anonymous proof: `__hs`, `dpr`, `__ccg`, `__rev`, `__s`, `__dyn`, `__csr`,
`__hsdp`, `__hblp`, `__sjsp`, `__crn`, and the authenticated `fb_dtsg` token,
in addition to the familiar session fields. Carl must not copy authenticated
tokens into anonymous requests. Field-name comparison is useful evidence, but
the old minimal anonymous request remains the more relevant baseline.

### Item navigation and media

Three item pages were inspected. Clicking a search card caused these current
GraphQL reads:

- `MarketplacePDPC2CMediaViewerWithImagesQuery`;
- `MarketplacePDPContainerQuery`;
- `MarketplacePDPRightColumnAdsQuery`.

It also caused telemetry mutations including
`CometMarketplaceSetProductItemSeenStateMutation`, `useCIXLogMutation`, and a
PDP dwell-time mutation. Full navigation to an item loaded the useful listing
payload in the HTML document, but still generated normal page telemetry after
load. This reinforces using browser navigation only as an explicit fallback.

Target gallery media were ordinary signed `fbcdn.net` resources referenced by
the page. Two image listings displayed duplicate large DOM `<img>` instances
of the same current gallery URL, without `srcset`. Observed source dimensions
were 720 by 960 for one listing and 444 by 960 for another. Advancing either
gallery changed both displayed elements to the next signed URL without adding a
resource-timing entry: the next photo was already present in the DOM and had
already been fetched. The first two observed JPEG pairs had encoded sizes of
59,059/60,379 bytes and 36,666/28,588 bytes respectively, with
`Cache-Control: max-age=1209600, no-transform`.

Another listing included an MP4 resource from a `video-*.fbcdn.net` host. A
large `<img>` element pointed at that URL and consequently reported zero natural
image dimensions, while the response correctly declared `video/mp4` and a
4,619,077-byte content length. Carl must classify downloaded media from the
response and decoded content rather than assuming every gallery URL is an
image based on its DOM placement.

The browser also loaded recommendation, advertisement, JavaScript, CSS, and
other `fbcdn.net` resources. A raw host match or a scan of all DOM images cannot
identify the target gallery. Association must begin with the exact listing
object and its media records, then correlate those signed URLs with network
responses. Signed URLs must remain byte-for-byte unchanged.

On a direct item reload, resource timing reported roughly 7.2 MB of encoded
`fbcdn` resources but only about 91 KB transferred, showing extensive browser
cache reuse. Carl's independent image collector cannot infer a successful saved
image from browser presence or cache state; it must retain its own acquisition,
bytes, hash, MIME type, dimensions, and status.

### Retained evidence

Raw authenticated observations are outside the repository under
`~/.local/share/carl/sensitive-browser-observations/`. Directories are mode
0700 and files are mode 0600. They contain authenticated session material and
must remain in the secret-handling boundary. The final corrected capture is
`20260920T235147Z`; it retains the full network index, complete available
request and response bodies for 28 Marketplace document, route-definition, and
GraphQL requests, response/request metadata for 272 `fbcdn` requests, and a
full item DOM/resource inspection. Earlier captures remain useful for the
no-reload sequence and gallery transitions, but some requested bodies were not
retained because Playwright requires body parts to be requested explicitly.

The repository records only this sanitized analysis. It excludes cookies,
authorization material, session tokens, cursors, signed media URLs, and current
persisted-query identifiers.

The corrected bounded anonymous runs are retained separately under
`~/.local/share/carl/experiments/facebook-search-20260921T0005Z/` with mode
0600 inside a mode-0700 directory. `carl.sqlite3`,
`proton-free-35.sqlite3`, and `proton-free-75.sqlite3` contain Carl's native
acquisition, extraction, scheduling, and provenance records for each run's
three HTTP attempts.

Carl's implemented protocol extractor was checked offline against these two
retained responses. It found 59 parseable bootstrap JSON blocks, the unique
structurally anchored query plan, 16 bootstrap listing occurrences, and 24
pagination listing occurrences without extraction issues. These are reference
artifact measurements, not promises about future responses. Small invented
fixtures cover decoy `page_info` objects, story/listing identity separation,
duplicate listing occurrences, ignored non-listing edges, missing cursors,
ambiguous connections, GraphQL errors, and malformed JSON.
