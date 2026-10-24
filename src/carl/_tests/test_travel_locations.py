"""Location evidence selection, uncertainty and geographic validation."""

import pytest

from carl.core.composed_projection import (
    ListingObservationCandidate,
    ProjectionEvidence,
    ProjectionSourceKind,
    SearchCardCandidate,
)
from carl.core.models import JsonValue
from carl.core.travel_locations import listing_travel_destination


def _evidence(sequence: int, *, card: bool = False) -> ProjectionEvidence:
    return ProjectionEvidence(
        evidence_record_identifier=f"evidence-{sequence}",
        observation_record_identifier=f"observation-{sequence}",
        acquisition_record_identifier=f"acquisition-{sequence}",
        producing_operation_identifier=f"operation-{sequence}",
        acquisition_completion_sequence=sequence,
        observation_completion_sequence=sequence,
        observed_at_utc="2026-01-01T00:00:00Z",
        completed_at_utc="2026-01-01T00:00:00Z",
        source_kind=ProjectionSourceKind.SEARCH_CARD if card else ProjectionSourceKind.ITEM_PAGE,
    )


def _item(sequence: int, **fields: JsonValue) -> ListingObservationCandidate:
    return ListingObservationCandidate(
        listing_identifier="123",
        response_classification="full_listing",
        evidence=_evidence(sequence),
        observation={
            "fields": {
                name: {"state": "present", "evidence": [{"state": "present", "normalized": value}]}
                for name, value in fields.items()
            }
        },
    )


def _card(sequence: int, **original: JsonValue) -> SearchCardCandidate:
    return SearchCardCandidate(
        listing_identifier="123", evidence=_evidence(sequence, card=True), original=original
    )


def test_item_coordinates_follow_existing_newest_field_selection() -> None:
    destination = listing_travel_destination(
        observations=(
            _item(1, location={"latitude": 1, "longitude": 2}),
            _item(2, location={"coordinates": {"latitude": 3, "longitude": 4}}),
        ),
        search_cards=(_card(3, location={"latitude": 5, "longitude": 6}),),
        label="Example City, PA",
    )
    assert destination.coordinate is not None
    assert (destination.coordinate.latitude, destination.coordinate.longitude) == (3.0, 4.0)
    assert destination.precision == "source_approximate"


def test_missing_newer_field_does_not_erase_retained_coordinates() -> None:
    destination = listing_travel_destination(
        observations=(_item(1, location={"latitude": 1, "longitude": 2}), _item(2)),
        search_cards=(),
        label=None,
    )
    assert destination.coordinate is not None
    assert destination.coordinate.latitude == 1.0


def test_conflicting_location_fields_are_not_arbitrarily_selected() -> None:
    destination = listing_travel_destination(
        observations=(
            _item(
                1,
                location={"latitude": 1, "longitude": 2},
                item_location={"latitude": 3, "longitude": 4},
            ),
        ),
        search_cards=(),
        label="Example City",
    )
    assert destination.coordinate is None
    assert destination.unavailable_reason == "conflicting_coordinates"


def test_conflicting_fragments_do_not_fall_back_to_an_older_coordinate_or_geocoding() -> None:
    conflicting = _item(2, location={"latitude": 3, "longitude": 4})
    conflicting.observation["fields"]["location"] = {
        "state": "conflicting",
        "evidence": [
            {"state": "present", "normalized": {"latitude": 3, "longitude": 4}},
            {"state": "present", "normalized": {"latitude": 5, "longitude": 6}},
        ],
    }
    destination = listing_travel_destination(
        observations=(_item(1, location={"latitude": 1, "longitude": 2}), conflicting),
        search_cards=(),
        label="Example City",
    )
    assert destination.coordinate is None
    assert destination.unavailable_reason == "conflicting_coordinates"


def test_country_context_is_added_to_the_composed_display_label() -> None:
    destination = listing_travel_destination(
        observations=(),
        search_cards=(
            _card(
                1,
                location_text={"text": "Example City"},
                location={"reverse_geocode": {"country": "Canada"}},
            ),
        ),
        label="Example City",
    )
    assert destination.label == "Example City, Canada"


def test_search_card_coordinates_are_newest_and_approximate() -> None:
    destination = listing_travel_destination(
        observations=(),
        search_cards=(
            _card(1, location={"latitude": 1, "longitude": 2}),
            _card(
                2,
                location={
                    "latitude": 3,
                    "longitude": 4,
                    "reverse_geocode": {
                        "city": "Example City",
                        "state": "PA",
                        "country": "United States",
                    },
                },
            ),
        ),
        label=None,
    )
    assert destination.coordinate is not None
    assert destination.coordinate.latitude == 3.0
    assert destination.label == "Example City, PA, United States"


def test_search_center_is_never_a_listing_destination() -> None:
    destination = listing_travel_destination(
        observations=(),
        search_cards=(_card(1, buyLocation={"latitude": 1, "longitude": 2}),),
        label=None,
    )
    assert destination.coordinate is None
    assert destination.unavailable_reason == "location_unavailable"


@pytest.mark.parametrize(
    "coordinate",
    [
        {"latitude": True, "longitude": 2},
        {"latitude": 1, "longitude": False},
        {"latitude": float("nan"), "longitude": 2},
        {"latitude": 1, "longitude": float("inf")},
        {"latitude": 91, "longitude": 2},
        {"latitude": 1, "longitude": -181},
        {"latitude": 1},
        {"latitude": "1", "longitude": 2},
    ],
)
def test_malformed_coordinates_remain_explicitly_unknown(coordinate: JsonValue) -> None:
    destination = listing_travel_destination(
        observations=(_item(1, location=coordinate),), search_cards=(), label=None
    )
    assert destination.coordinate is None
    assert destination.unavailable_reason == "invalid_coordinates"


def test_locality_label_fallback_retains_country_and_normalizes_whitespace() -> None:
    destination = listing_travel_destination(
        observations=(),
        search_cards=(
            _card(
                1,
                location={
                    "reverse_geocode": {
                        "city_page": {"display_name": " Example City,   Pennsylvania "},
                        "country_name": "United States",
                    }
                },
            ),
        ),
        label=None,
    )
    assert destination.label == "Example City, Pennsylvania, United States"
    assert destination.coordinate is None
    assert destination.precision == "geocoded_locality"


def test_structured_projection_coordinate_value_is_supported() -> None:
    destination = listing_travel_destination(
        observations=(),
        search_cards=(),
        label={"coordinates": {"latitude": 0, "longitude": 0}},
    )
    assert destination.coordinate is not None
    assert destination.coordinate.latitude == 0.0


def test_unknown_and_oversized_labels_do_not_become_geocoding_queries() -> None:
    for label in (None, "   ", "x" * 501, {"seller": {"latitude": 1, "longitude": 2}}):
        destination = listing_travel_destination(observations=(), search_cards=(), label=label)
        assert destination.coordinate is None
        assert destination.label is None
        assert destination.unavailable_reason == "location_unavailable"
