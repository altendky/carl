"""Coherent Fetch pagination from the current search's configuration."""

from pathlib import Path
from uuid import uuid4

import pytest

from carl._tests.test_ebay import _Acquirer, _PagingAcquirer, _provenance
from carl.core.ebay import (
    EbaySearchRequest,
    ebay_homepage_plan,
    ebay_search_plan,
    extract_ebay_search,
)
from carl.core.ebay_navigation import ebay_fetch_plan, ebay_pagination_token
from carl.core.http import RequestPlan
from carl.ebay import collect_ebay_search
from carl.io.httpx import Acquisition, IdentifierFactory
from carl.io.sqlite import Database

_FIRST = '<li class="s-card"><a href="/itm/256123456789">Scope</a></li><a class="pagination__next" href="/sch/i.html?_pgn=2">Next</a>'
_SECOND = '<li class="s-card"><a href="/itm/256123456790">Scope</a></li><a class="pagination__next" href="/sch/i.html?_pgn=3">Next</a>'
_THIRD = '<li class="s-card"><a href="/itm/256123456791">Scope</a></li>'


@pytest.mark.parametrize("status_code", (301, 302, 303, 307, 308))
def test_unfollowed_challenge_redirect_remains_a_blocked_response(status_code: int) -> None:
    extraction = extract_ebay_search(
        "",
        status_code=status_code,
        redirect_target="https://www.ebay.com/splashui/challenge?public=example",
    )
    assert extraction.response_classification.kind == "challenge"
    assert "ebay_challenge_redirect" in extraction.response_classification.evidence
    assert not extraction.listing_occurrences
    assert extraction.next_page_number is None


@pytest.mark.parametrize(
    "target",
    [
        "https://www.ebay.com.evil.example/splashui/challenge",
        "https://www.ebay.com:444/splashui/challenge",
        "https://www.ebay.com/sch/i.html?_pgn=2",
    ],
)
def test_other_redirects_are_not_classified_as_challenges(target: str) -> None:
    extraction = extract_ebay_search("", status_code=307, redirect_target=target)
    assert extraction.response_classification.kind == "http_error"


@pytest.mark.parametrize(
    "script,reason",
    [
        ('<div>{"plConfigToken":"not-a-script"}</div>', "missing"),
        ('<script>{"plConfigToken":"one","plConfigToken":"two"}</script>', "ambiguous"),
        ('<script>{"plConfigToken":null}</script>', "invalid"),
        ('<script>{"plConfigToken":"\\r\\n"}</script>', "invalid"),
        ('<script>{"plConfigToken":"\\u007f"}</script>', "invalid"),
        ('<script>{"plConfigToken":"\\u2603"}</script>', "invalid"),
        ('<script>{"plConfigToken":""}</script>', "invalid"),
        ('<script>{"plConfigToken":"unterminated}</script>', "invalid"),
        ('<script>{"plConfigToken":"' + "x" * 8193 + '"}</script>', "invalid"),
    ],
)
def test_reject_invalid_or_ambiguous_page_configuration(script: str, reason: str) -> None:
    with pytest.raises(ValueError, match=f"^pagination_configuration_{reason}$"):
        ebay_pagination_token(script)


def test_read_json_escaped_configuration_once_from_script() -> None:
    assert (
        ebay_pagination_token(
            '<script>window.config={"plConfigToken":"local\\u002dtoken"};</script>'
            '<script type="application/json">{"plConfigToken":"local-token"}</script>'
        )
        == "local-token"
    )


def test_fetch_plan_has_complete_profile_and_no_document_only_headers() -> None:
    plan = ebay_fetch_plan(
        EbaySearchRequest(query="scope"),
        network_path=("test",),
        page_number=2,
        token="local-token",
        referer="https://www.ebay.com/sch/i.html?_nkw=scope&_trksid=actual",
    )
    headers = {header.name.decode(): header.value.decode() for header in plan.headers}
    assert not plan.profile_default_headers
    assert headers["sec-fetch-mode"] == "cors"
    assert headers["sec-fetch-dest"] == "empty"
    assert headers["sec-fetch-site"] == "same-origin"
    assert headers["referer"].endswith("&_trksid=actual")
    assert headers["accept"] == "text/html,image/webp"
    assert headers["priority"] == "u=1, i"
    assert headers["x-pl-config"] == "local-token"
    assert headers["x-wire-request"] == "true"
    assert headers["x-wire-regions"].startswith("results,media-provider,")
    assert headers["sec-ch-ua-platform"] == '"Linux"'
    assert "Chrome/154.0.0.0" in headers["user-agent"]
    assert "sec-fetch-user" not in headers
    assert "upgrade-insecure-requests" not in headers
    assert "sec-ch-ua-full-version" not in headers
    assert "cookie" not in headers
    assert not plan.follow_redirects
    assert "local-token" not in str(plan.as_json())


def test_search_plans_opt_into_bounded_reconnect_without_changing_default_plan_identity() -> None:
    request = EbaySearchRequest(query="scope")
    home = ebay_homepage_plan(network_path=("test",))
    search = ebay_search_plan(request, network_path=home.routing)
    fetch = ebay_fetch_plan(
        request, network_path=home.routing, page_number=2, token="local", referer=search.url
    )
    assert all(plan.reconnect_on_broken_pipe for plan in (home, search, fetch))
    ordinary = RequestPlan(url="https://example.invalid/")
    assert not ordinary.reconnect_on_broken_pipe
    assert "reconnect_on_broken_pipe" not in ordinary.as_json()
    assert "reconnect_on_broken_pipe" not in ordinary.model_dump_json()


@pytest.mark.parametrize(
    "referer",
    [
        "https://www.ebay.com.evil.example/sch/i.html",
        "https://www.ebay.com:444/sch/i.html",
        "https://other.example/sch/i.html",
        "https://www.ebay.com/splashui/challenge",
    ],
)
def test_fetch_plan_rejects_foreign_or_nonsearch_referer(referer: str) -> None:
    with pytest.raises(ValueError, match="preceding search page"):
        ebay_fetch_plan(
            EbaySearchRequest(query="scope"),
            network_path=("test",),
            page_number=2,
            token="local-token",
            referer=referer,
        )


@pytest.mark.anyio
async def test_current_configuration_reaches_fragments_and_is_not_reused_across_runs(
    tmp_path: Path,
) -> None:
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        for token in ("first-run-token", "second-run-token"):
            acquirer = _PagingAcquirer(
                (
                    (_FIRST + '<script>{"plConfigToken":"' + token + '"}</script>').encode(),
                    # Fragment configuration must not replace the top-level page's token.
                    (_SECOND + '<script>{"plConfigToken":"fragment-token"}</script>').encode(),
                    _THIRD.encode(),
                )
            )
            result = await collect_ebay_search(
                database,
                request=EbaySearchRequest(query="scope", maximum_pages=3),
                network_path=("test",),
                acquirer=acquirer,
                provenance=_provenance(),
                new_identifier=lambda: str(uuid4()),
                fetch_pagination=True,
            )
            assert result["state"] == "completed"
            assert result["listing_count"] == 3
            assert result["pagination"] == {"mode": "fetch", "fetch_pages": 2}
            assert acquirer.plans[0].profile_default_headers
            for index, plan in enumerate(acquirer.plans[1:], start=1):
                headers = {header.name: header.value for header in plan.headers}
                assert headers[b"x-pl-config"] == token.encode()
                assert headers[b"referer"] == acquirer.plans[index - 1].url.encode()
                assert not plan.profile_default_headers
            _, _, manifest = await database.get_record(str(result["search_run_record_identifier"]))
            assert manifest["pagination"] == result["pagination"]
            operation = await database.operation(str(result["acquisition_operation_identifier"]))
            assert token not in str(operation)
            for identifier in result["acquisition_record_identifiers"]:
                _, _, acquisition = await database.get_record(identifier)
                assert token not in str(acquisition)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "configuration,reason",
    [
        ("", "missing"),
        ('<script>{"plConfigToken":"one","plConfigToken":"two"}</script>', "ambiguous"),
        ('<script>{"plConfigToken":"\\n"}</script>', "invalid"),
    ],
)
async def test_document_pagination_fallback_is_explicit(
    tmp_path: Path, configuration: str, reason: str
) -> None:
    acquirer = _PagingAcquirer(((_FIRST + configuration).encode(), _THIRD.encode()))
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        result = await collect_ebay_search(
            database,
            request=EbaySearchRequest(query="scope", maximum_pages=2),
            network_path=("test",),
            acquirer=acquirer,
            provenance=_provenance(),
            new_identifier=lambda: str(uuid4()),
            fetch_pagination=True,
        )
    assert result["state"] == "completed"
    assert result["pagination"] == {
        "mode": "document",
        "fetch_pages": 0,
        "fallback_reason": f"pagination_configuration_{reason}",
    }
    assert all(plan.profile_default_headers for plan in acquirer.plans)


@pytest.mark.anyio
async def test_fetch_challenge_redirect_is_retained_and_classified_without_following(
    tmp_path: Path,
) -> None:
    class RedirectAcquirer:
        def __init__(self) -> None:
            self.plans: list[RequestPlan] = []

        async def acquire(
            self, plan: RequestPlan, new_identifier: IdentifierFactory
        ) -> Acquisition:
            self.plans.append(plan)
            if len(self.plans) == 1:
                return await _Acquirer(
                    (_FIRST + '<script>{"plConfigToken":"fresh-local-token"}</script>').encode()
                ).acquire(plan, new_identifier)
            assert not plan.follow_redirects
            result = await _Acquirer(b"", status_code=307).acquire(plan, new_identifier)
            result.record["hops"][-1]["response"]["redirect_target"] = (
                "https://www.ebay.com/splashui/challenge?public=example"
            )
            return result

    acquirer = RedirectAcquirer()
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        result = await collect_ebay_search(
            database,
            request=EbaySearchRequest(query="scope", maximum_pages=3),
            network_path=("test",),
            acquirer=acquirer,
            provenance=_provenance(),
            new_identifier=lambda: str(uuid4()),
            fetch_pagination=True,
        )
        assert result["state"] == "response_failed"
        assert result["stopping_reason"] == "challenge"
        assert result["response_classification"]["kind"] == "challenge"
        assert len(acquirer.plans) == 8
        assert all(plan == acquirer.plans[1] for plan in acquirer.plans[1:])
        assert result["page_count"] == 2
        assert result["pagination"]["fetch_pages"] == 1
        _, _, retained = await database.get_record(result["acquisition_record_identifiers"][-1])
        assert retained["hops"][-1]["response"]["status_code"] == 307
