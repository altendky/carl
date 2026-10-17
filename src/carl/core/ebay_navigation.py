"""Response-derived eBay pagination requests for the Chrome154/Linux profile."""

import json
import re
from html.parser import HTMLParser
from typing import override
from urllib.parse import urlsplit

from carl.core.ebay import EbaySearchRequest, ebay_search_url
from carl.core.http import RequestPlan
from carl.core.models import Header

# Observed eBay wire protocol, including page-wide regions beyond the results
# container's wire-target. The configuration token is always taken from this run.
_WIRE_REGIONS = (
    "results,media-provider,river-tray,gh-update,page-config,tracking,showdiag,"
    "left-rail-ads,right-rail-ads,footer-ads,center-top-ads,rtm,sitespeed"
)
_TOKEN_PROPERTY = re.compile(r'"plConfigToken"\s*:\s*')
EBAY_FETCH_HEADER_ORDER = (
    "sec-ch-ua-platform",
    "x-pl-config",
    "accept-language",
    "sec-ch-ua",
    "sec-ch-ua-model",
    "sec-ch-ua-mobile",
    "x-wire-regions",
    "x-wire-request",
    "accept",
    "sec-ch-ua-full-version",
    "user-agent",
    "sec-ch-ua-platform-version",
    "sec-fetch-site",
    "sec-fetch-mode",
    "sec-fetch-dest",
    "referer",
    "accept-encoding",
    "cookie",
    "priority",
)


class _ConfigurationParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.in_script = False
        self.scripts: list[str] = []

    @override
    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "script":
            self.in_script = True

    @override
    def handle_endtag(self, tag: str) -> None:
        if tag == "script":
            self.in_script = False

    @override
    def handle_data(self, data: str) -> None:
        if self.in_script:
            self.scripts.append(data)


def ebay_pagination_token(html: str) -> str:
    """Require an unambiguous header-safe JSON string in current page scripts."""
    parser = _ConfigurationParser()
    parser.feed(html)
    parser.close()
    decoder = json.JSONDecoder()
    values: set[str] = set()
    for script in parser.scripts:
        for match in _TOKEN_PROPERTY.finditer(script):
            try:
                value, _ = decoder.raw_decode(script, match.end())
            except ValueError:
                raise ValueError("pagination_configuration_invalid") from None
            if (
                not isinstance(value, str)
                or not 1 <= len(value) <= 8192
                or not value.isascii()
                or any(ord(character) < 32 or ord(character) == 127 for character in value)
            ):
                raise ValueError("pagination_configuration_invalid")
            values.add(value)
    if not values:
        raise ValueError("pagination_configuration_missing")
    if len(values) != 1:
        raise ValueError("pagination_configuration_ambiguous")
    return values.pop()


def is_ebay_search_url(url: str) -> bool:
    try:
        parsed = urlsplit(url)
        return (
            parsed.scheme == "https"
            and parsed.hostname == "www.ebay.com"
            and parsed.port in (None, 443)
            and parsed.username is None
            and parsed.password is None
            and parsed.path == "/sch/i.html"
        )
    except ValueError:
        return False


def ebay_fetch_plan(
    request: EbaySearchRequest,
    *,
    network_path: tuple[str, ...],
    page_number: int,
    token: str,
    referer: str,
) -> RequestPlan:
    """Build Fetch pagination; callers must select Chrome154/Linux transport."""
    if page_number < 2 or not is_ebay_search_url(referer):
        raise ValueError("eBay Fetch pagination requires a preceding search page")
    # These reduced identity headers match the installed, pinned wreq154/Linux
    # profile. Disabling its document defaults also removes Sec-Fetch-User and
    # Upgrade-Insecure-Requests without replacing the native cookie jar.
    values = {
        "sec-ch-ua-platform": '"Linux"',
        "x-pl-config": token,
        "accept-language": "en-US,en;q=0.9",
        "sec-ch-ua": '"Chromium";v="154", "Google Chrome";v="154", "Not A(Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "x-wire-regions": _WIRE_REGIONS,
        "x-wire-request": "true",
        "accept": "text/html,image/webp",
        "user-agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/154.0.0.0 Safari/537.36"
        ),
        "sec-fetch-site": "same-origin",
        "sec-fetch-mode": "cors",
        "sec-fetch-dest": "empty",
        "referer": referer,
        "accept-encoding": "gzip, deflate, br, zstd",
        "priority": "u=1, i",
    }
    return RequestPlan(
        reconnect_on_broken_pipe=True,
        url=ebay_search_url(
            request.query, page_number=page_number, listing_state=request.listing_state
        ),
        headers=tuple(
            Header(name=name.encode("ascii"), value=value.encode("ascii"))
            for name, value in values.items()
        ),
        header_order=EBAY_FETCH_HEADER_ORDER,
        profile_default_headers=False,
        # A wire token belongs to this origin. Do not forward it to a challenge
        # redirect or another origin; the existing classifier handles the 3xx.
        follow_redirects=False,
        routing=network_path,
    )
