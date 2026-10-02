# pyright: reportPrivateUsage=false

import base64
import hashlib
from pathlib import Path
from time import perf_counter_ns, time_ns
from typing import cast
from uuid import uuid4

import pytest

from carl._tests.test_ebay_item_workers import ITEM
from carl._tests.test_ebay_refresh_workers import (
    test_refresh_waits_for_details_descriptions_and_globally_bounded_images as _run_refresh_fixture,
)
from carl._tests.test_ebay_workers import _directories, _provenance
from carl._tests.test_marketplace_listing import _publish, _record
from carl._tests.test_mixed_workspace import _complete
from carl.core.composed_projection import (
    ComposedListingFilters,
    GetComposedListingRequest,
    ListComposedSearchRequest,
    ListingStatus,
)
from carl.core.ebay import EbaySearchRequest
from carl.core.ebay_price import ebay_price_value, normalize_ebay_scalar_fields
from carl.core.facebook_refresh import SearchRefreshRequest
from carl.core.json import decode_json, encode_json
from carl.core.marketplace_search import (
    CreateMarketplaceSearchRequest,
    EbaySearchTargetSpecification,
)
from carl.core.models import JsonValue, RecordDraft
from carl.core.work import WorkRequester
from carl.core.worker import WorkerSettings
from carl.ebay_workers import EbaySearchWorkerDependencies, build_ebay_worker_registry
from carl.io.sqlite import Database
from carl.io.worker import WorkerRuntimeServices, execute_lease
from carl.marketplace_projection import (
    ebay_candidate_sources,
    expand_search_runs,
    get_marketplace_composed_listing,
    list_marketplace_composed_search,
    marketplace_selected_projections,
    run_listing_identifiers,
    scope_contains_ebay,
)
from carl.review import ReviewApplication


@pytest.mark.anyio
async def test_sale_evidence_remains_separate_from_asking_prices_and_newer_active_status(
    tmp_path: Path,
) -> None:
    item = "256123456789"
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(database, (_record("old-acq", "acquisition"),))
        await _publish(database, (_record("sale-acq", "acquisition"),))
        await _publish(
            database,
            (
                _record(
                    "run", "search_run", request={"query": "Morpheus", "listing_state": "sold"}
                ),
                _record(
                    "detail",
                    "listing_observation",
                    item_identifier=item,
                    acquisition_record_identifier="old-acq",
                    classification="detail",
                    displayed_price="$240.00",
                ),
                _record(
                    "sold-card",
                    "search_listing_occurrence",
                    item_identifier=item,
                    acquisition_record_identifier="sale-acq",
                    search_run_record_identifier="run",
                    listing_state="sold",
                    displayed_price="$180.00",
                    sold_price="$180.00",
                    sold_date="2026-09-29",
                    sold_date_text="Sep 29, 2026",
                    sold_price_status="displayed",
                ),
            ),
        )
        app = ReviewApplication(database, tmp_path)
        before = await app.get_composed_listing(
            GetComposedListingRequest(listing_identifier="ebay:" + item)
        )
        assert before.status.value == ListingStatus.SOLD
        assert before.price and before.price.value == {
            "amount_decimal": "240.00",
            "currency": "USD",
            "formatted_amount": "$240.00",
        }
        assert before.last_sale and before.last_sale.value["sold_price"] == "$180.00"
        assert before.last_sale.value["sold_price_value"]["amount_decimal"] == "180.00"
        assert before.last_sale.evidence.evidence_record_identifier == "sold-card"
        page = await app.list_composed_search(
            ListComposedSearchRequest(search_run_record_identifier="run")
        )
        assert page.listings == ()
        sold_page = await app.list_composed_search(
            ListComposedSearchRequest(
                search_run_record_identifier="run",
                filters=ComposedListingFilters(statuses=(ListingStatus.SOLD,)),
            )
        )
        assert len(sold_page.listings) == 1
        await _publish(database, (_record("active-acq", "acquisition"),))
        # Enough later cards to exceed the public history window; old sale evidence must survive.
        await _publish(
            database,
            tuple(
                _record(
                    f"active-{index}",
                    "search_listing_occurrence",
                    item_identifier=item,
                    acquisition_record_identifier="active-acq",
                    search_run_record_identifier="run",
                    listing_state="active",
                    displayed_price="$245.00",
                )
                for index in range(102)
            ),
        )
        after = await app.get_composed_listing(
            GetComposedListingRequest(listing_identifier="ebay:" + item)
        )
        assert after.status.value == ListingStatus.AVAILABLE
        assert after.last_sale == before.last_sale
        # Offline re-extraction of an old sale must not make it newer than active evidence.
        await _publish(
            database,
            tuple(
                _record(
                    f"reprocessed-sale-{index}",
                    "search_listing_occurrence",
                    item_identifier=item,
                    acquisition_record_identifier="sale-acq",
                    search_run_record_identifier="run",
                    listing_state="sold",
                    sold_price="$180.00",
                    sold_date="2026-09-29",
                    sold_price_status="displayed",
                )
                for index in range(102)
            ),
        )
        repeated = await app.get_composed_listing(
            GetComposedListingRequest(listing_identifier="ebay:" + item)
        )
        assert repeated.status.value == ListingStatus.AVAILABLE
        assert repeated.last_sale and repeated.last_sale.value["sold_price"] == "$180.00"


@pytest.mark.anyio
async def test_sold_only_card_does_not_become_an_asking_price(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(
            database,
            (
                _record(
                    "card",
                    "search_listing_occurrence",
                    item_identifier="256123456789",
                    listing_state="sold",
                    displayed_price="$150",
                    sold_price="$150",
                    sold_date="2026-09-29",
                ),
            ),
        )
        projection = await ReviewApplication(database, tmp_path).get_composed_listing(
            GetComposedListingRequest(listing_identifier="ebay:256123456789")
        )
        assert projection.status.value == ListingStatus.SOLD
        assert projection.price is None
        assert projection.last_sale and projection.last_sale.value["sold_price"] == "$150"


@pytest.mark.parametrize(
    ("displayed", "currency", "amount", "expected_currency"),
    (
        ("$209.72", None, "209.72", "USD"),
        ("US $1,209.72", None, "1209.72", "USD"),
        ("C $49.95", None, "49.95", "CAD"),
        ("AU $49.95", None, "49.95", "AUD"),
        ("£49.95", None, "49.95", "GBP"),
        ("EUR 49.95", None, "49.95", "EUR"),
        ("49.95 CHF", None, "49.95", "CHF"),
        ("49.95", "cad", "49.95", "CAD"),
        ("$49.95", "CAD", "49.95", "CAD"),
        ("$0.00", None, "0.00", "USD"),
    ),
)
def test_ebay_single_price_is_structured(
    displayed: str, currency: str | None, amount: str, expected_currency: str
) -> None:
    assert ebay_price_value(displayed, currency=currency) == {
        "amount_decimal": amount,
        "currency": expected_currency,
        "formatted_amount": displayed,
    }


@pytest.mark.parametrize(
    "displayed", ("$10.00 to $20.00", "$10 - $20", "Approx. $49.95", "Negotiable", "1.234,56 €")
)
def test_ebay_ambiguous_prices_remain_display_only(displayed: str) -> None:
    assert ebay_price_value(displayed) == displayed


def test_ebay_conflicting_currency_remains_display_only() -> None:
    assert ebay_price_value("US $49.95", currency="CAD") == "US $49.95"
    assert ebay_price_value("EUR 49.95 USD") == "EUR 49.95 USD"


def test_ebay_bare_amount_does_not_invent_currency() -> None:
    assert ebay_price_value("49.95") == {
        "amount_decimal": "49.95",
        "formatted_amount": "49.95",
    }


@pytest.mark.anyio
async def test_ebay_card_condition_shipping_and_detail_currency_projection(tmp_path: Path) -> None:
    item = "256123456789"
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(
            database,
            (
                _record(
                    "card",
                    "search_listing_occurrence",
                    item_identifier=item,
                    title="Used eyepiece",
                    displayed_price="$209.72",
                    condition="Pre-Owned",
                    shipping_text="+$8.00 shipping",
                ),
            ),
        )
        app = ReviewApplication(database, tmp_path)
        card = await app.get_composed_listing(
            GetComposedListingRequest(listing_identifier="ebay:" + item)
        )
        assert card.price and card.price.value["amount_decimal"] == "209.72"
        assert card.condition and card.condition.value == "Pre-Owned"
        assert card.shipping and card.shipping.value == "+$8.00 shipping"
        assert card.condition.evidence.evidence_record_identifier == "card"
        await _publish(
            database,
            (
                _record(
                    "detail",
                    "listing_observation",
                    item_identifier=item,
                    classification="detail",
                    displayed_price="199.95",
                    currency="CAD",
                    condition="Open Box",
                ),
            ),
        )
        detail = await app.get_composed_listing(
            GetComposedListingRequest(listing_identifier="ebay:" + item)
        )
        assert detail.price and detail.price.value["currency"] == "CAD"
        assert detail.price.value["amount_decimal"] == "199.95"
        assert detail.condition and detail.condition.value == "Open Box"
        assert detail.shipping == card.shipping
        assert detail.price.evidence.evidence_record_identifier == "detail"


def test_legacy_ebay_scalar_snapshot_price_upgrade() -> None:
    from carl.core.composed_projection import ScalarFieldValues

    old = ScalarFieldValues(price="$209.72", last_sale={"sold_price": "US $180.00"})
    upgraded = normalize_ebay_scalar_fields(old)
    assert upgraded.price["amount_decimal"] == "209.72"
    assert upgraded.last_sale["sold_price"] == "US $180.00"
    assert upgraded.last_sale["sold_price_value"]["currency"] == "USD"
    assert old.price == "$209.72"


@pytest.mark.anyio
async def test_ebay_projection_gallery_description_and_acquisition_order(tmp_path: Path) -> None:
    item = "256123456789"
    url = "https://i.ebayimg.com/images/one.jpg"
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(database, (_record("old-acq", "acquisition"),))
        await _publish(database, (_record("new-acq", "acquisition"),))
        await _publish(
            database,
            (
                _record(
                    "new-detail",
                    "listing_observation",
                    item_identifier=item,
                    acquisition_record_identifier="new-acq",
                    classification="detail",
                    title="New evidence",
                    gallery_urls=[url],
                    description_url="https://vi.vipr.ebaydesc.com/description",
                ),
                _record(
                    "reference",
                    "gallery_image_reference",
                    item_identifier=item,
                    observation_record_identifier="new-detail",
                    url=url,
                ),
                _record(
                    "image",
                    "image_result",
                    item_identifier=item,
                    observation_record_identifier="new-detail",
                    reference_record_identifier="reference",
                    url=url,
                    state="saved",
                    image_artifact_identifier="image-file",
                ),
                _record(
                    "description",
                    "description_result",
                    item_identifier=item,
                    observation_record_identifier="new-detail",
                    state="saved",
                    description="Seller details",
                ),
                _record(
                    "old-reprocessed",
                    "listing_observation",
                    item_identifier=item,
                    acquisition_record_identifier="old-acq",
                    classification="detail",
                    title="Old evidence",
                ),
            ),
        )
        app = ReviewApplication(database, tmp_path)
        full = await get_marketplace_composed_listing(
            app, GetComposedListingRequest(listing_identifier="ebay:" + item)
        )
        compact = await get_marketplace_composed_listing(
            app,
            GetComposedListingRequest(
                listing_identifier="ebay:" + item, maximum_gallery_images=0, maximum_analyses=0
            ),
        )
        assert full.canonical_source_url == f"https://www.ebay.com/itm/{item}"
        assert full.title and full.title.value == "New evidence"
        assert full.title.evidence.observation_record_identifier == "new-detail"
        assert full.status.value == ListingStatus.AVAILABLE
        assert full.description and full.description.value == "Seller details"
        assert full.description.evidence.evidence_record_identifier == "description"
        assert full.gallery and full.gallery.saved_image_count == 1
        assert full.gallery.images[0].descriptor.image_artifact_identifier == "image-file"
        assert compact.gallery and compact.gallery.images == ()
        assert compact.projection_revision == full.projection_revision
        sources = await ebay_candidate_sources(database, ("ebay:" + item,))
        assert {source.listing_identifier for source in sources} == {"ebay:" + item}
        assert {source.observation_record_identifier for source in sources} == {
            "new-detail",
            "old-reprocessed",
        }


@pytest.mark.anyio
async def test_ebay_projection_cursor_freezes_new_detail_and_membership(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(
            database,
            (
                _record("run", "search_run", request={"query": "scope"}),
                _record("other-run", "search_run", request={"query": "unrelated"}),
                *(
                    _record(
                        f"card-{i}",
                        "search_listing_occurrence",
                        search_run_record_identifier="run",
                        item_identifier=f"25612345678{i}",
                        title=f"Card {i}",
                    )
                    for i in range(3)
                ),
            ),
        )
        app = ReviewApplication(database, tmp_path)
        request = ListComposedSearchRequest(
            search_run_record_identifier="run", page_size=1, maximum_candidate_listings_examined=1
        )
        first = await list_marketplace_composed_search(app, request)
        assert first.listings[0].listing_identifier == "ebay:256123456780"
        assert first.next_cursor
        raw_cursor = decode_json(
            base64.urlsafe_b64decode(
                first.next_cursor + "=" * (-len(first.next_cursor) % 4)
            ).decode()
        )
        assert isinstance(raw_cursor, dict)
        tampered = dict(cast(dict[str, JsonValue], raw_cursor))
        tampered["runs"] = ["other-run"]
        replacement = base64.urlsafe_b64encode(encode_json(tampered).encode()).decode().rstrip("=")
        with pytest.raises(ValueError, match="scope"):
            _ = await list_marketplace_composed_search(
                app, request.model_copy(update={"cursor": replacement})
            )
        tampered = dict(cast(dict[str, JsonValue], raw_cursor))
        tampered["boundary"] = 2**63 - 1
        replacement = base64.urlsafe_b64encode(encode_json(tampered).encode()).decode().rstrip("=")
        with pytest.raises(ValueError, match="boundary"):
            _ = await list_marketplace_composed_search(
                app, request.model_copy(update={"cursor": replacement})
            )
        await _publish(
            database,
            (
                _record(
                    "later-detail",
                    "listing_observation",
                    item_identifier="256123456781",
                    classification="detail",
                    title="Changed later",
                ),
                _record(
                    "later-card",
                    "search_listing_occurrence",
                    search_run_record_identifier="run",
                    item_identifier="256123456779",
                    title="Late card",
                ),
            ),
        )
        second = await list_marketplace_composed_search(
            app, request.model_copy(update={"cursor": first.next_cursor})
        )
        assert second.listings[0].listing_identifier == "ebay:256123456781"
        assert second.listings[0].title and second.listings[0].title.value == "Card 1"
        assert second.next_cursor
        third = await list_marketplace_composed_search(
            app, request.model_copy(update={"cursor": second.next_cursor})
        )
        assert third.listings[0].listing_identifier == "ebay:256123456782"
        assert third.next_cursor is None
        _, count, selected = await marketplace_selected_projections(
            app, ("run",), ("ebay:256123456781", "ebay:256123456770")
        )
        assert count == 1
        assert [projection.listing_identifier for projection in selected] == ["ebay:256123456781"]
        resized = await list_marketplace_composed_search(
            app, request.model_copy(update={"page_size": 2, "cursor": first.next_cursor})
        )
        assert resized.listings == second.listings
        assert resized.as_of_completion_sequence == first.as_of_completion_sequence
        # Cursors issued before page size was excluded still work at their original size.
        legacy_cursor = dict(cast(dict[str, JsonValue], raw_cursor))
        legacy_cursor["scope"] = hashlib.sha256(
            request.model_dump_json(exclude={"cursor"}).encode()
        ).hexdigest()
        legacy_encoded = (
            base64.urlsafe_b64encode(encode_json(legacy_cursor).encode()).decode().rstrip("=")
        )
        legacy_page = await list_marketplace_composed_search(
            app, request.model_copy(update={"cursor": legacy_encoded})
        )
        assert legacy_page.listings == second.listings
        for update in (
            {"search_run_record_identifier": "other-run"},
            {"filters": ComposedListingFilters(statuses=(ListingStatus.SOLD,))},
            {"maximum_gallery_images_per_listing": 1},
        ):
            with pytest.raises(ValueError, match="cursor"):
                _ = await list_marketplace_composed_search(
                    app, request.model_copy(update={**update, "cursor": first.next_cursor})
                )


@pytest.mark.anyio
async def test_group_expansion_uses_real_completed_work_metadata(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        app = ReviewApplication(database, tmp_path)
        group = await app.create_marketplace_search(
            CreateMarketplaceSearchRequest(
                targets=(EbaySearchTargetSpecification(search=EbaySearchRequest(query="scope")),)
            )
        )

        async def collect(*_args: object, **_kwargs: object) -> dict[str, JsonValue]:
            await _publish(
                database,
                (
                    _record("run", "search_run", request={"query": "scope"}),
                    _record(
                        "card",
                        "search_listing_occurrence",
                        search_run_record_identifier="run",
                        item_identifier="256123456789",
                        title="Card",
                    ),
                    _record(
                        "card-2",
                        "search_listing_occurrence",
                        search_run_record_identifier="run",
                        item_identifier="256123456790",
                        title="Second card",
                    ),
                ),
            )
            return {
                "state": "completed",
                "search_run_record_identifier": "run",
                "acquisition_record_identifiers": [],
                "extraction_record_identifiers": [],
                "listing_count": 1,
            }

        registry = build_ebay_worker_registry(
            EbaySearchWorkerDependencies(
                database=database,
                directories=_directories(tmp_path),
                new_identifier=lambda: str(uuid4()),
                collector=collect,
            )
        )
        settings = WorkerSettings(
            worker_count=1,
            lease_duration_ns=60_000_000_000,
            renewal_interval_ns=10_000_000_000,
            idle_poll_interval_ns=1_000_000,
        )
        claim = await database.claim_work(
            supported_capabilities=registry.capabilities,
            worker_identifier="worker",
            lease_token="lease",
            lease_duration_ns=settings.lease_duration_ns,
            utc_now_ns=time_ns,
            event_identifier=str(uuid4()),
        )
        assert claim.lease
        _ = await execute_lease(
            database=database,
            registry=registry,
            settings=settings,
            services=WorkerRuntimeServices(
                new_identifier=lambda: str(uuid4()),
                utc_now_ns=time_ns,
                monotonic_ns=perf_counter_ns,
                code_provenance=_provenance,
                invocation=lambda: {},
            ),
            lease=claim.lease,
        )
        assert await scope_contains_ebay(database, (group.record_identifier,))
        assert await expand_search_runs(
            database,
            (group.record_identifier,),
            as_of_completion_sequence=await database.current_completion_boundary(),
        ) == ("run",)
        assert await run_listing_identifiers(database, group.record_identifier) == (
            "ebay:256123456789",
            "ebay:256123456790",
        )
        page = await list_marketplace_composed_search(
            app,
            ListComposedSearchRequest(
                search_run_record_identifier=group.record_identifier, page_size=1
            ),
        )
        assert page.listings[0].listing_identifier == "ebay:256123456789"
        assert page.next_cursor
        await database.attach_work_request(
            claim.lease.work_item_identifier,
            WorkRequester(
                request_identifier=str(uuid4()),
                kind=("test", "late_requester"),
                identifier="extra",
                context={},
            ),
            event_identifier=str(uuid4()),
            requested_at_utc_ns=time_ns(),
        )
        await _publish(
            database,
            (
                RecordDraft(
                    identifier="disabled-target",
                    kind=("carl", "marketplace", "search_target_state"),
                    schema_version=1,
                    value={
                        "search_record_identifier": group.record_identifier,
                        "target_record_identifier": group.targets[0].record_identifier,
                        "enabled": False,
                    },
                ),
            ),
        )
        continued = await list_marketplace_composed_search(
            app,
            ListComposedSearchRequest(
                search_run_record_identifier=group.record_identifier,
                page_size=1,
                cursor=page.next_cursor,
            ),
        )
        assert continued.listings[0].listing_identifier == "ebay:256123456790"


@pytest.mark.anyio
async def test_ebay_refresh_ancestry_exact_limit_and_lookahead(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        await _publish(
            database,
            (
                _record("run-0", "search_run", request={"query": "scope"}),
                _record(
                    "original-card",
                    "search_listing_occurrence",
                    search_run_record_identifier="run-0",
                    item_identifier="256123456789",
                    title="Historical item",
                ),
            ),
        )
        app = ReviewApplication(database, tmp_path)
        for index in range(1, 101):
            refresh = await app.request_search_refresh(
                SearchRefreshRequest(base_search_run_record_identifier=f"run-{index - 1}")
            )
            await _complete(
                database,
                refresh.work_identifier,
                records=(_record(f"run-{index}", "search_run", request={"query": "scope"}),),
                result={
                    "refreshed_search_run_record_identifier": f"run-{index}",
                    "state": "completed",
                },
            )
        exact = await list_marketplace_composed_search(
            app,
            ListComposedSearchRequest(
                search_run_record_identifier="run-99", maximum_ancestry_runs=100
            ),
        )
        assert exact.included_ancestry_run_count == 100
        assert not exact.older_ancestry_truncated
        assert exact.listings[0].listing_identifier == "ebay:256123456789"
        assert (
            exact.listings[0].search_membership
            and exact.listings[0].search_membership.seen_run_count == 1
        )
        overflow = await list_marketplace_composed_search(
            app,
            ListComposedSearchRequest(
                search_run_record_identifier="run-100", maximum_ancestry_runs=100
            ),
        )
        assert overflow.included_ancestry_run_count == 100
        assert overflow.older_ancestry_truncated


@pytest.mark.anyio
async def test_released_refresh_checkpoint_gallery_is_visible(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Reuse the full offline worker scenario: actual checkpoint publication,
    # release, validated image children and coordinator completion.
    await _run_refresh_fixture(tmp_path, monkeypatch, True)
    async with Database.managed(tmp_path / "carl.sqlite3") as database:
        projection = await get_marketplace_composed_listing(
            ReviewApplication(database, tmp_path),
            GetComposedListingRequest(listing_identifier="ebay:" + ITEM),
        )
        assert projection.gallery and projection.gallery.saved_image_count == 1
        image = projection.gallery.images[0]
        assert image.descriptor.image_artifact_identifier
        assert image.descriptor.gallery_reference_record_identifier
        async with database._connections.reader() as connection:
            cursor = await connection.execute(
                "SELECT operation.state FROM objects AS object JOIN operations AS operation ON operation.id=object.created_by_operation_id WHERE object.id=?",
                (image.descriptor.gallery_reference_record_identifier,),
            )
            row = await cursor.fetchone()
        assert row and row[0] == "failed"
        assert (
            image.evidence
            and image.evidence.evidence_record_identifier
            == image.descriptor.image_result_record_identifier
        )
