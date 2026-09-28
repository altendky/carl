import gzip
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from uuid import uuid4

import anyio
import httpx
import pytest

from carl._tests.test_facebook import HTML
from carl.app import collect_listing, extract_acquisition
from carl.core.components import Component, ComponentId
from carl.core.composed_projection import (
    GetComposedListingRequest,
    ListComposedSearchRequest,
    ListingObservationCandidate,
    ListingStatus,
    ProjectionEvidence,
    ProjectionSourceKind,
)
from carl.core.facebook_images import GalleryImageReference
from carl.core.http import stored_response_charset
from carl.core.json import decode_json, encode_json
from carl.core.models import CodeProvenance, NamedOutput, RecordDraft
from carl.io.httpx import AcquisitionFailure, DirectHttpxAcquirer
from carl.io.sqlite import DATABASE_SCHEMA, Database
from carl.review import ReviewApplication


def test_content_type_charset_uses_standard_parameter_parsing() -> None:
    charset, source = stored_response_charset(
        [
            {
                "name_latin1": "Content-Type",
                "value_latin1": 'text/html; boundary="contains charset=wrong"; charset="UTF-8"',
            }
        ]
    )

    assert charset == "utf-8"
    assert source == "http_content_type"


@pytest.mark.anyio
async def test_read_does_not_create_a_missing_database(tmp_path: Path) -> None:
    path = tmp_path / "missing.sqlite3"

    with pytest.raises(FileNotFoundError):
        async with Database.managed(path) as database:
            await database.operation("missing")

    assert not path.exists()


@pytest.mark.anyio
async def test_database_records_structured_initial_schema_identity(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True):
        pass

    with closing(sqlite3.connect(path)) as connection:
        row = connection.execute(
            "SELECT value FROM schema_metadata WHERE key = 'schema_identity_json'"
        ).fetchone()

    assert row is not None
    assert decode_json(row[0]) == DATABASE_SCHEMA.model_dump(mode="json")


@pytest.mark.anyio
async def test_version_two_database_adds_external_artifacts_table(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True):
        pass

    version_two_identity = DATABASE_SCHEMA.model_copy(update={"version": 2})
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("DROP TABLE external_artifacts")
        connection.execute(
            "UPDATE schema_metadata SET value = ? WHERE key = 'schema_identity_json'",
            (encode_json(version_two_identity.model_dump(mode="json")),),
        )
        connection.execute("UPDATE schema_metadata SET value = '2' WHERE key = 'schema_version'")
        connection.execute(
            "UPDATE schema_metadata SET value = ? WHERE key = 'schema_definition_sha256'",
            ("bdd3d1b6a8dc041e7bf86e6c5881a06df910c0efe72612bb2135174b97db553b",),
        )
        connection.commit()

    async with Database.managed(path):
        pass

    with closing(sqlite3.connect(path)) as connection:
        table = connection.execute(
            "SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'external_artifacts'"
        ).fetchone()
        metadata = dict(connection.execute("SELECT key, value FROM schema_metadata"))

    assert table == ("external_artifacts",)
    assert decode_json(metadata["schema_identity_json"]) == DATABASE_SCHEMA.model_dump(mode="json")
    assert metadata["schema_version"] == str(DATABASE_SCHEMA.version)


@pytest.mark.anyio
async def test_version_three_database_adds_bounded_lookup_indexes(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True):
        pass

    index_names = {
        "objects_kind",
        "objects_operation_kind",
        "records_gallery_observation",
        "records_saved_image_reference",
        "records_saved_image_rendition",
        "operation_inputs_object_name_operation",
        "work_items_analysis_lookup",
    }
    version_three_identity = DATABASE_SCHEMA.model_copy(update={"version": 3})
    with closing(sqlite3.connect(path)) as connection:
        for index_name in index_names:
            connection.execute(f"DROP INDEX {index_name}")
        connection.execute(
            "UPDATE schema_metadata SET value = ? WHERE key = 'schema_identity_json'",
            (encode_json(version_three_identity.model_dump(mode="json")),),
        )
        connection.execute("UPDATE schema_metadata SET value = '3' WHERE key = 'schema_version'")
        connection.execute(
            "UPDATE schema_metadata SET value = ? WHERE key = 'schema_definition_sha256'",
            ("a43327bac12773649f7817d6d33c126b473d92664a366fdf4fb82465401d9b29",),
        )
        connection.commit()

    async with Database.managed(path):
        pass

    with closing(sqlite3.connect(path)) as connection:
        actual_indexes = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_schema WHERE type = 'index'")
        }
        metadata = dict(connection.execute("SELECT key, value FROM schema_metadata"))

    assert index_names <= actual_indexes
    assert decode_json(metadata["schema_identity_json"]) == DATABASE_SCHEMA.model_dump(mode="json")
    assert metadata["schema_version"] == str(DATABASE_SCHEMA.version)


@pytest.mark.anyio
async def test_version_four_database_adds_composed_projection_indexes(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True):
        pass

    index_names = {
        "records_listing_observation_listing",
        "records_search_occurrence_run_position",
        "records_search_occurrence_listing",
    }
    version_four_identity = DATABASE_SCHEMA.model_copy(update={"version": 4})
    with closing(sqlite3.connect(path)) as connection:
        for index_name in index_names:
            connection.execute(f"DROP INDEX {index_name}")
        connection.execute(
            "UPDATE schema_metadata SET value = ? WHERE key = 'schema_identity_json'",
            (encode_json(version_four_identity.model_dump(mode="json")),),
        )
        connection.execute("UPDATE schema_metadata SET value = '4' WHERE key = 'schema_version'")
        connection.execute(
            "UPDATE schema_metadata SET value = ? WHERE key = 'schema_definition_sha256'",
            ("22ea940b93d3d57686107e46756656ecb08c139ef95e214366726421ebee7d4c",),
        )
        connection.commit()

    async with Database.managed(path):
        pass

    with closing(sqlite3.connect(path)) as connection:
        actual_indexes = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_schema WHERE type = 'index'")
        }
        metadata = dict(connection.execute("SELECT key, value FROM schema_metadata"))

    assert index_names <= actual_indexes
    assert decode_json(metadata["schema_identity_json"]) == DATABASE_SCHEMA.model_dump(mode="json")
    assert metadata["schema_version"] == str(DATABASE_SCHEMA.version)


@pytest.mark.anyio
async def test_version_five_database_adds_review_claim_tables(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True):
        pass

    version_five_identity = DATABASE_SCHEMA.model_copy(update={"version": 5})
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("DROP TABLE review_mutation_requests")
        connection.execute("DROP TABLE review_listing_claims")
        connection.execute(
            "UPDATE schema_metadata SET value = ? WHERE key = 'schema_identity_json'",
            (encode_json(version_five_identity.model_dump(mode="json")),),
        )
        connection.execute("UPDATE schema_metadata SET value = '5' WHERE key = 'schema_version'")
        connection.execute(
            "UPDATE schema_metadata SET value = ? WHERE key = 'schema_definition_sha256'",
            ("0b9b916daaa4f41ccdd9242c628e2502bc777b1b5348e048ebe6228cb44fb65c",),
        )
        connection.commit()

    async with Database.managed(path):
        pass

    with closing(sqlite3.connect(path)) as connection:
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_schema WHERE type = 'table'")
        }
        metadata = dict(connection.execute("SELECT key, value FROM schema_metadata"))
    assert {"review_listing_claims", "review_mutation_requests"} <= tables
    assert decode_json(metadata["schema_identity_json"]) == DATABASE_SCHEMA.model_dump(mode="json")
    assert metadata["schema_version"] == str(DATABASE_SCHEMA.version)


@pytest.mark.anyio
async def test_version_six_database_adds_work_requester_index(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True):
        pass

    version_six_identity = DATABASE_SCHEMA.model_copy(update={"version": 6})
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("DROP INDEX work_requests_requester")
        connection.execute(
            "UPDATE schema_metadata SET value = ? WHERE key = 'schema_identity_json'",
            (encode_json(version_six_identity.model_dump(mode="json")),),
        )
        connection.execute("UPDATE schema_metadata SET value = '6' WHERE key = 'schema_version'")
        connection.execute(
            "UPDATE schema_metadata SET value = ? WHERE key = 'schema_definition_sha256'",
            ("78a8206e8c39e88566ddb364c3e1db62d0c58359edd9ea6e170dbd3a468cf514",),
        )
        connection.commit()

    async with Database.managed(path):
        pass

    with closing(sqlite3.connect(path)) as connection:
        index = connection.execute(
            "SELECT name FROM sqlite_schema WHERE type = 'index' AND name = ?",
            ("work_requests_requester",),
        ).fetchone()
        metadata = dict(connection.execute("SELECT key, value FROM schema_metadata"))
    assert index == ("work_requests_requester",)
    assert decode_json(metadata["schema_identity_json"]) == DATABASE_SCHEMA.model_dump(mode="json")
    assert metadata["schema_version"] == str(DATABASE_SCHEMA.version)


@pytest.mark.anyio
async def test_composed_projection_storage_queries_are_targeted_and_bounded(tmp_path: Path) -> None:
    path = tmp_path / "carl.sqlite3"
    async with Database.managed(path, initialize=True):
        pass

    with closing(sqlite3.connect(path)) as connection:
        connection.execute("PRAGMA foreign_keys = ON")

        def operation(identifier: str, ended_at_utc: str) -> None:
            connection.execute(
                """
                INSERT INTO operations VALUES (?, ?, 1, ?, '{}', '{}', 'completed', ?, ?, 1,
                                                '{}', NULL)
                """,
                (
                    identifier,
                    encode_json(["test", identifier]),
                    encode_json(
                        CodeProvenance(
                            repository_url=None,
                            commit_hash="a" * 40,
                            worktree_state="clean",
                            package_version="test",
                            python_implementation="CPython",
                            python_version="3.14",
                            dependencies=(),
                            lockfile_sha256=None,
                        ).model_dump(mode="json")
                    ),
                    "2026-09-26T00:00:00+00:00",
                    ended_at_utc,
                ),
            )

        def completed_work(
            identifier: str,
            operation_identifier: str,
            kind: tuple[str, ...],
        ) -> None:
            connection.execute(
                """
                INSERT INTO work_items VALUES (?, ?, 1, '{}', '{}', 'completed', 0, 0, 0,
                                               NULL, NULL, NULL, 1, '{}', NULL)
                """,
                (identifier, encode_json(list(kind))),
            )
            connection.execute(
                "INSERT INTO work_operations VALUES (?, ?, 1, 'lease', 'worker')",
                (operation_identifier, identifier),
            )
            connection.execute(
                "INSERT INTO work_events(id, work_item_id, event_kind, recorded_at_utc_ns, "
                "data_json) VALUES (?, ?, 'completed', 1, '{}')",
                (f"{identifier}-completed", identifier),
            )

        def record(
            identifier: str,
            kind: tuple[str, ...],
            operation_identifier: str,
            value: object,
        ) -> None:
            connection.execute(
                "INSERT INTO objects VALUES (?, 'record', ?, ?)",
                (identifier, encode_json(list(kind)), operation_identifier),
            )
            connection.execute(
                "INSERT INTO records VALUES (?, 1, ?)",
                (identifier, encode_json(value)),
            )

        operation("search-operation", "2026-09-26T00:00:01+00:00")
        completed_work(
            "search-work",
            "search-operation",
            ("carl", "facebook", "work", "collect_search"),
        )
        record(
            "search-run",
            ("carl", "facebook", "search_run"),
            "search-operation",
            {
                "search_run_identifier": "internal-search-run",
                "traversal": {"stopping_reason": "no_next_page"},
            },
        )
        for edge_index, (listing_identifier, flags) in enumerate(
            (
                ("1", {"is_sold": True}),
                ("2", {"is_live": True}),
                ("3", {"is_live": True}),
            )
        ):
            record(
                f"occurrence-{listing_identifier}",
                ("carl", "facebook", "search_listing_occurrence"),
                "search-operation",
                {
                    "listing_identifier": listing_identifier,
                    "search_run_identifier": "internal-search-run",
                    "acquisition_record_identifier": "search-acquisition",
                    "page_ordinal": 1,
                    "edge_index": edge_index,
                    "original": {
                        **flags,
                        **(
                            {
                                "marketplace_listing_title": "Search-only refrigerator",
                                "listing_price": {
                                    "amount": "125.00",
                                    "formatted_amount": "$125",
                                },
                                "location": {
                                    "reverse_geocode": {
                                        "city": "Example City",
                                        "state": "PA",
                                        "city_page": {"display_name": "Example City, Pennsylvania"},
                                    }
                                },
                                "marketplace_listing_seller": {
                                    "id": "seller-2",
                                    "name": "Example Seller",
                                },
                                "primary_listing_photo": {
                                    "id": "preview-2",
                                    "image": {"uri": "https://example.fbcdn.net/preview-2.jpg"},
                                },
                            }
                            if listing_identifier == "2"
                            else {}
                        ),
                    },
                },
            )

        operation("acquisition-operation", "2026-09-26T00:00:02+00:00")
        completed_work(
            "acquisition-work",
            "acquisition-operation",
            ("carl", "facebook", "work", "collect_item"),
        )
        record(
            "item-acquisition",
            ("carl", "http", "acquisition"),
            "acquisition-operation",
            {},
        )
        operation("extraction-operation", "2026-09-26T00:00:03+00:00")
        completed_work(
            "extraction-work",
            "extraction-operation",
            ("carl", "facebook", "work", "extract_item"),
        )
        image_url = "https://example.fbcdn.net/image.jpg"
        record(
            "item-observation",
            ("carl", "facebook", "listing_observation"),
            "extraction-operation",
            {
                "listing_id": "1",
                "acquisition_record_id": "item-acquisition",
                "response_classification": {"kind": "full_listing"},
                "fields": {},
                "images": [
                    {
                        "original_url": image_url,
                        "role": "listing_gallery",
                        "gallery_order": 0,
                        "photo_id": "photo-1",
                        "declared_dimensions": {"width": 640, "height": 480},
                        "source": {
                            "acquisition_record_id": "item-acquisition",
                            "block_index": 0,
                            "json_path": ["listing", "image", 0],
                        },
                    }
                ],
            },
        )
        reference = GalleryImageReference(
            listing_id="1",
            listing_observation_record_identifier="item-observation",
            acquisition_record_identifier="item-acquisition",
            block_index=0,
            json_path=("listing", "image", 0),
            original_url=image_url,
            gallery_order=0,
            photo_id="photo-1",
            declared_width=640,
            declared_height=480,
        )
        operation("gallery-operation", "2026-09-26T00:00:04+00:00")
        completed_work(
            "gallery-work",
            "gallery-operation",
            ("carl", "facebook", "work", "gallery"),
        )
        record(
            "gallery-reference",
            ("carl", "facebook", "gallery_image_reference"),
            "gallery-operation",
            reference.model_dump(mode="json"),
        )
        operation("image-operation", "2026-09-26T00:00:05+00:00")
        completed_work(
            "image-work",
            "image-operation",
            ("carl", "facebook", "work", "extract_image"),
        )
        record(
            "image-result",
            ("carl", "facebook", "image_result"),
            "image-operation",
            {
                "state": "saved",
                "image_reference_record_identifier": "gallery-reference",
                "source_photo_id": "photo-1",
                "original_url": image_url,
            },
        )
        operation("new-rendition-operation", "2026-09-26T00:00:06+00:00")
        completed_work(
            "new-rendition-work",
            "new-rendition-operation",
            ("carl", "facebook", "work", "extract_image"),
        )
        record(
            "new-rendition-result",
            ("carl", "facebook", "image_result"),
            "new-rendition-operation",
            {
                "state": "saved",
                "source_photo_id": "photo-1",
                "original_url": image_url,
            },
        )
        connection.commit()

    async with Database.managed(path) as database:
        boundary = await database.current_completion_boundary()
        search_run = await database.facebook_projection_search_run(
            "search-run", as_of_completion_sequence=boundary
        )
        memberships = await database.facebook_projection_membership_candidates(
            (("search-run", "internal-search-run"),),
            as_of_completion_sequence=boundary,
            maximum_listings=10,
        )
        later_memberships = await database.facebook_projection_membership_candidates(
            (("search-run", "internal-search-run"),),
            as_of_completion_sequence=boundary,
            maximum_listings=10,
            after_position=(
                memberships[0].candidate.search_run_completion_sequence,
                memberships[0].candidate.listing_identifier,
            ),
        )
        membership_occurrences = await database.facebook_projection_membership_occurrences(
            ("1", "2"),
            (("search-run", "internal-search-run"),),
            as_of_completion_sequence=boundary,
        )
        observations = await database.facebook_projection_item_observations(
            ("1",), as_of_completion_sequence=boundary, maximum_per_listing=100
        )
        statuses = await database.facebook_projection_search_occurrences(
            ("1", "2"), as_of_completion_sequence=boundary
        )
        search_cards = await database.facebook_projection_search_cards(
            ("1", "2"),
            as_of_completion_sequence=boundary,
            maximum_per_listing=2,
        )
        references = await database.facebook_projection_gallery_reference_identifiers(
            ("item-observation",), as_of_completion_sequence=boundary
        )
        direct_images = await database.facebook_projection_saved_image_results_for_references(
            ("gallery-reference",), as_of_completion_sequence=boundary
        )
        rendition_images = await database.facebook_projection_saved_image_results_for_renditions(
            (("photo-1", image_url),), as_of_completion_sequence=boundary
        )
        application = ReviewApplication(database=database, repository_root=tmp_path)
        sold_listing = await application.get_composed_listing(
            GetComposedListingRequest(
                listing_identifier="1",
                search_run_record_identifier="search-run",
            )
        )
        search_only_listing = await application.get_composed_listing(
            GetComposedListingRequest(listing_identifier="2")
        )
        default_page = await application.list_composed_search(
            ListComposedSearchRequest(search_run_record_identifier="search-run")
        )
        bounded_page = await application.list_composed_search(
            ListComposedSearchRequest(
                search_run_record_identifier="search-run",
                maximum_candidate_listings_examined=2,
                page_size=2,
            )
        )
        exact_boundary_page = await application.list_composed_search(
            ListComposedSearchRequest(
                search_run_record_identifier="search-run",
                maximum_candidate_listings_examined=3,
                page_size=3,
            )
        )
        with pytest.raises(KeyError, match="missing-guide"):
            await application.get_composed_listing(
                GetComposedListingRequest(
                    listing_identifier="1",
                    product_guide_record_identifier="missing-guide",
                )
            )

    assert boundary == 6
    assert search_run.internal_search_run_identifier == "internal-search-run"
    assert search_run.stopping_reason is not None
    assert search_run.stopping_reason.value == "no_next_page"
    assert [item.candidate.listing_identifier for item in memberships] == ["1", "2", "3"]
    assert [item.candidate.listing_identifier for item in later_memberships] == ["2", "3"]
    assert [item.listing_identifier for item in membership_occurrences] == ["1", "2"]
    assert observations[0].evidence.acquisition_completion_sequence == 2
    assert observations[0].evidence.observation_completion_sequence == 3
    assert {status.listing_identifier: status.value for status in statuses} == {
        "1": ListingStatus.SOLD,
        "2": ListingStatus.AVAILABLE,
    }
    assert statuses[0].evidence.warnings == ("search_card_ordered_by_search_completion",)
    assert [card.listing_identifier for card in search_cards] == ["1", "2"]
    assert tuple(references) == (reference,)
    assert tuple(references.values()) == ("gallery-reference",)
    assert direct_images[0].result_record_identifier == "image-result"
    assert rendition_images[0].result_record_identifier == "image-result"
    assert direct_images[0].evidence.observation_completion_sequence == 5
    assert sold_listing.status.value is ListingStatus.SOLD
    assert sold_listing.gallery is not None
    assert sold_listing.gallery.saved_image_count == 1
    assert (
        sold_listing.gallery.images[0].descriptor.image_result_record_identifier
        == "new-rendition-result"
    )
    assert sold_listing.gallery.images[0].evidence is not None
    assert sold_listing.gallery.images[0].evidence.observation_completion_sequence == 6
    assert sold_listing.search_membership is not None
    assert sold_listing.search_membership.seen_in_selected_run
    assert search_only_listing.title is not None
    assert search_only_listing.title.value == "Search-only refrigerator"
    assert search_only_listing.title.evidence.source_kind.value == "search_card"
    assert search_only_listing.price is not None
    assert search_only_listing.price.value == {
        "amount_decimal": "125.00",
        "formatted_amount": "$125",
    }
    assert search_only_listing.location is not None
    assert search_only_listing.location.value == "Example City, Pennsylvania"
    assert search_only_listing.seller is not None
    assert search_only_listing.seller.value == {
        "id": "seller-2",
        "name": "Example Seller",
    }
    assert search_only_listing.preview_image is not None
    assert (
        search_only_listing.preview_image.descriptor.original_url
        == "https://example.fbcdn.net/preview-2.jpg"
    )
    assert search_only_listing.gallery is None
    assert search_only_listing.search_membership is None
    assert [listing.listing_identifier for listing in default_page.listings] == ["2", "3"]
    assert default_page.listings[0].status.value is ListingStatus.AVAILABLE
    assert default_page.listings[0].title is not None
    assert default_page.listings[0].title.value == "Search-only refrigerator"
    assert default_page.listings[0].preview_image is not None
    assert bounded_page.next_cursor is not None
    assert bounded_page.candidate_examination_limit_reached
    assert exact_boundary_page.next_cursor is None
    assert not exact_boundary_page.candidate_examination_limit_reached


@pytest.mark.anyio
async def test_projection_resolves_only_the_selected_gallery_per_listing(tmp_path: Path) -> None:
    class GalleryDatabase:
        requested_observations: tuple[str, ...] = ()
        requested_renditions: tuple[tuple[str | None, str], ...] = ()

        async def facebook_projection_gallery_reference_identifiers(
            self,
            observation_identifiers: tuple[str, ...],
            *,
            as_of_completion_sequence: int,
        ) -> dict[GalleryImageReference, str]:
            assert as_of_completion_sequence == 4
            self.requested_observations = observation_identifiers
            return {}

        async def facebook_projection_saved_image_results_for_references(
            self,
            reference_identifiers: tuple[str, ...],
            *,
            as_of_completion_sequence: int,
        ) -> tuple[()]:
            assert not reference_identifiers
            assert as_of_completion_sequence == 4
            return ()

        async def facebook_projection_saved_image_results_for_renditions(
            self,
            renditions: tuple[tuple[str | None, str], ...],
            *,
            as_of_completion_sequence: int,
        ) -> tuple[()]:
            assert as_of_completion_sequence == 4
            self.requested_renditions = renditions
            return ()

    def observation(listing: str, name: str, sequence: int) -> ListingObservationCandidate:
        return ListingObservationCandidate(
            listing_identifier=listing,
            response_classification="full_listing",
            observation={
                "listing_id": listing,
                "images": [
                    {
                        "original_url": f"https://example.fbcdn.net/{name}/{index}.jpg",
                        "role": "listing_gallery",
                        "gallery_order": index,
                        "photo_id": f"{name}-{index}",
                        "declared_dimensions": {"width": 100, "height": 100},
                        "source": {
                            "acquisition_record_id": f"acquisition-{name}",
                            "block_index": 0,
                            "json_path": ["listing_photos", index, "image"],
                        },
                    }
                    for index in range(100)
                ],
            },
            evidence=ProjectionEvidence(
                evidence_record_identifier=name,
                observation_record_identifier=name,
                acquisition_record_identifier=f"acquisition-{name}",
                producing_operation_identifier=f"operation-{name}",
                acquisition_completion_sequence=sequence,
                observation_completion_sequence=sequence,
                source_kind=ProjectionSourceKind.ITEM_PAGE,
            ),
        )

    database = GalleryDatabase()
    application = ReviewApplication(
        database=cast(Database, cast(object, database)), repository_root=tmp_path
    )
    galleries = await application._projection_galleries(
        {
            "1": (observation("1", "old-1", 1), observation("1", "new-1", 3)),
            "2": (observation("2", "old-2", 2), observation("2", "new-2", 4)),
        },
        as_of_completion_sequence=4,
    )

    assert database.requested_observations == ("new-1", "new-2")
    assert len(database.requested_renditions) == 200
    assert all(gallery is not None for gallery in galleries.values())


@pytest.mark.anyio
async def test_initialization_rejects_unknown_database_before_mutation(tmp_path: Path) -> None:
    path = tmp_path / "unknown.sqlite3"
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("CREATE TABLE sentinel(value TEXT NOT NULL) STRICT")

    with pytest.raises(RuntimeError, match="no Carl schema identity"):
        async with Database.managed(path, initialize=True):
            pass

    with closing(sqlite3.connect(path)) as connection:
        tables = connection.execute(
            "SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name"
        ).fetchall()
        journal_mode = connection.execute("PRAGMA journal_mode").fetchone()

    assert tables == [("sentinel",)]
    assert journal_mode == ("delete",)


@pytest.mark.anyio
async def test_initialization_rejects_stale_schema_with_current_version(
    tmp_path: Path,
) -> None:
    path = tmp_path / "stale.sqlite3"
    with closing(sqlite3.connect(path)) as connection:
        connection.execute(
            "CREATE TABLE schema_metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT"
        )
        connection.execute(
            "INSERT INTO schema_metadata VALUES (?, ?)",
            (
                "schema_identity_json",
                encode_json(DATABASE_SCHEMA.model_dump(mode="json")),
            ),
        )
        connection.execute(
            "INSERT INTO schema_metadata VALUES ('schema_version', ?)",
            (str(DATABASE_SCHEMA.version),),
        )
        connection.execute(
            "CREATE TABLE scheduling_constraints(identifier_parts_json TEXT PRIMARY KEY) STRICT"
        )
        connection.commit()

    with pytest.raises(
        RuntimeError,
        match="Unsupported database schema metadata: schema_definition_sha256",
    ):
        async with Database.managed(path, initialize=True):
            pass


class _CompleteStream(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield HTML.encode()


def _handler(request: httpx.Request) -> httpx.Response:
    assert request.headers["accept-encoding"] == "gzip, deflate, br, zstd"
    return httpx.Response(
        200,
        headers=[
            ("Content-Type", "text/html; charset=utf-8"),
            ("Date", "Fri, 18 Sep 2026 20:00:00 GMT"),
        ],
        stream=_CompleteStream(),
    )


@pytest.mark.anyio
async def test_collection_and_repeatable_offline_extraction(tmp_path: Path) -> None:
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        result = await collect_listing(
            database,
            url="https://www.facebook.com/marketplace/item/123/",
            headers=(),
            acquirer=DirectHttpxAcquirer(httpx.MockTransport(_handler)),
        )

        observation_kind, _, observation = await database.get_record(
            result["observation_record_id"]
        )
        assert observation_kind == ("carl", "facebook", "listing_observation")
        assert observation["listing_id"] == "123"
        assert observation["acquisition_record_id"] == result["acquisition_record_id"]
        assert observation["extractor"]["component_parts"] == [
            "carl",
            "facebook",
            "extract",
            "embedded_json",
        ]
        assert observation["response_classification"]["kind"] == "full_listing"
        assert all(block["record_id"] for block in observation["json_blocks"])

        offline = await extract_acquisition(database, result["acquisition_record_id"])
        assert offline["observation_record_id"] != result["observation_record_id"]
        assert offline["extraction_state"] == "extracted"

        acquisition_operation = await database.operation(result["acquisition_operation_id"])
        assert acquisition_operation["state"] == "completed"
        assert acquisition_operation["duration_ns"] > 0
        assert acquisition_operation["code_provenance"]["worktree_state"] in {"clean", "dirty"}
        assert acquisition_operation["invocation"]["original_argv"]
        assert acquisition_operation["invocation"]["async_library"] == {
            "state": "available",
            "name": "trio",
        }


class _BrokenStream(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield b"partial"
        raise httpx.ReadError("fixture interrupted")


def _broken_handler(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, headers={"Content-Type": "text/html"}, stream=_BrokenStream())


class _ByteStream(httpx.AsyncByteStream):
    def __init__(self, content: bytes):
        self.content = content

    async def __aiter__(self):
        yield self.content


@pytest.mark.anyio
async def test_incomplete_response_records_failure_without_partial_body(tmp_path: Path) -> None:
    database_path = tmp_path / "carl.sqlite3"
    async with Database.managed(database_path, initialize=True) as database:
        with pytest.raises(AcquisitionFailure):
            await collect_listing(
                database,
                url="https://www.facebook.com/marketplace/item/123/",
                headers=(),
                acquirer=DirectHttpxAcquirer(httpx.MockTransport(_broken_handler)),
            )

    with closing(sqlite3.connect(database_path)) as connection:
        assert connection.execute(
            "SELECT count(*) FROM operations WHERE state = 'failed'"
        ).fetchone() == (1,)
        failure = connection.execute(
            "SELECT result_json FROM operations WHERE state = 'failed'"
        ).fetchone()
        assert failure is not None
        assert '"reason":"incomplete_transfer"' in failure[0]
        assert connection.execute("SELECT count(*) FROM artifacts").fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM content").fetchone() == (0,)


@pytest.mark.anyio
@pytest.mark.parametrize("encoding", ["gzip", "unknown"])
async def test_undecodable_response_fails_without_storing_body(
    tmp_path: Path, encoding: str
) -> None:
    database_path = tmp_path / "carl.sqlite3"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": "text/html", "Content-Encoding": encoding},
            stream=_ByteStream(b"invalid compressed response"),
        )

    async with Database.managed(database_path, initialize=True) as database:
        with pytest.raises(AcquisitionFailure) as failure:
            await collect_listing(
                database,
                url="https://www.facebook.com/marketplace/item/123/",
                headers=(),
                acquirer=DirectHttpxAcquirer(httpx.MockTransport(handler)),
            )

    assert failure.value.result["stopping_condition"] == "content_decoding_failure"
    with closing(sqlite3.connect(database_path)) as connection:
        result_row = connection.execute(
            "SELECT result_json FROM operations WHERE state = 'failed'"
        ).fetchone()
        assert result_row is not None
        result = decode_json(result_row[0])
        assert isinstance(result, dict)
        hops = result["hops"]
        assert isinstance(hops, list) and len(hops) == 1
        response = hops[0]["response"]
        assert response["status_code"] == 200
        assert response["body"]["state"] == "unavailable"
        assert response["body"]["reason"] == "content_decoding_failure"
        assert response["body"]["content_encoding_headers"] == [encoding]
        assert connection.execute("SELECT count(*) FROM artifacts").fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM content").fetchone() == (0,)


@pytest.mark.anyio
async def test_decodable_compressed_response_is_retained_and_extracted(tmp_path: Path) -> None:
    encoded = gzip.compress(HTML.encode())

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"Content-Type": "text/html", "Content-Encoding": "gzip"},
            stream=_ByteStream(encoded),
        )

    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        result = await collect_listing(
            database,
            url="https://www.facebook.com/marketplace/item/123/",
            headers=(),
            acquirer=DirectHttpxAcquirer(httpx.MockTransport(handler)),
        )

        assert result["extraction_state"] == "extracted"
        acquisition_kind, _, acquisition = await database.get_record(
            result["acquisition_record_id"]
        )
        assert acquisition_kind == ("carl", "http", "acquisition")
        assert acquisition["hops"][0]["response"]["body"]["state"] == "available"
        assert acquisition["hops"][0]["response"]["body"]["bytes"] == len(HTML.encode())
        assert acquisition["hops"][0]["response"]["body"]["received_content_bytes"] == len(encoded)
        body_identifier = acquisition["hops"][0]["response"]["body"]["artifact_id"]
        body_metadata, stored_body = await database.get_artifact(body_identifier)
        assert stored_body == HTML.encode()
        assert body_metadata["representation"]["content_decoded"] is True
        assert body_metadata["representation"]["storage_compression"] == {"kind": "none"}


@pytest.mark.anyio
async def test_concurrent_writers_publish_complete_operations(tmp_path: Path) -> None:
    database_path = tmp_path / "carl.sqlite3"
    component = Component(ComponentId(("carl", "test", "writer")), 1, lambda: None)
    provenance = CodeProvenance(
        repository_url=None,
        commit_hash=None,
        worktree_state="dirty",
        package_version="test",
        python_implementation="test",
        python_version="test",
        dependencies=(),
        lockfile_sha256=None,
    )

    async def write_one(database: Database, index: int) -> None:
        operation_id = str(uuid4())
        record_id = str(uuid4())
        await database.begin_operation(
            operation_id=operation_id,
            component=component,
            provenance=provenance,
            invocation={},
            configuration={"index": index},
            started_at_utc=datetime.now(UTC).isoformat(),
        )
        await database.complete_operation(
            operation_id=operation_id,
            records=(
                RecordDraft(
                    identifier=record_id,
                    kind=("carl", "test", "record"),
                    schema_version=1,
                    value={"index": index},
                ),
            ),
            artifacts=(),
            outputs=(NamedOutput(name=("record",), object_identifier=record_id),),
            result={"index": index},
            ended_at_utc=datetime.now(UTC).isoformat(),
            duration_ns=1,
        )

    async with (
        Database.managed(database_path, initialize=True) as database,
        anyio.create_task_group() as task_group,
    ):
        for index in range(12):
            task_group.start_soon(write_one, database, index)

    with closing(sqlite3.connect(database_path)) as connection:
        assert connection.execute(
            "SELECT count(*) FROM operations WHERE state = 'completed'"
        ).fetchone() == (12,)
        assert connection.execute("SELECT count(*) FROM records").fetchone() == (12,)
