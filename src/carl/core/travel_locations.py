"""Resolve retained listing location evidence without inventing precise addresses."""

from collections.abc import Mapping
from typing import cast

from pydantic import ValidationError

from carl.core.composed_projection import (
    ListingObservationCandidate,
    SearchCardCandidate,
    evidence_recency_key,
    search_card_field_value,
    select_field,
)
from carl.core.models import JsonValue
from carl.core.travel import TravelCoordinate, TravelDestination


def _mapping(value: JsonValue) -> dict[str, JsonValue] | None:
    return cast(dict[str, JsonValue], value) if isinstance(value, dict) else None


def _coordinate(value: JsonValue) -> tuple[TravelCoordinate | None, bool]:
    mapping = _mapping(value)
    if mapping is None:
        return None, False
    coordinates = _mapping(mapping.get("coordinates", mapping))
    if coordinates is None:
        return None, "coordinates" in mapping
    if not {"latitude", "longitude"}.intersection(coordinates):
        return None, False
    try:
        return TravelCoordinate.model_validate(
            {name: coordinates.get(name) for name in ("latitude", "longitude")}
        ), False
    except ValidationError:
        return None, True


def _text(value: JsonValue) -> str | None:
    if isinstance(value, dict):
        value = cast(dict[str, JsonValue], value).get("text")
    if not isinstance(value, str):
        return None
    cleaned = " ".join(value.split())
    return cleaned if cleaned and len(cleaned) <= 500 else None


def _location_label(value: JsonValue) -> str | None:
    if (text := _text(value)) is not None:
        return text
    mapping = _mapping(value)
    if mapping is None:
        return None
    reverse = _mapping(mapping.get("reverse_geocode"))
    if reverse is None:
        return None
    city_page = _mapping(reverse.get("city_page"))
    label = _text(city_page.get("display_name")) if city_page is not None else None
    if label is None:
        label = (
            ", ".join(part for name in ("city", "state") if (part := _text(reverse.get(name))))
            or None
        )
    country = _text(reverse.get("country")) or _text(reverse.get("country_name"))
    if label and country and country.casefold() not in label.casefold():
        label = f"{label}, {country}"
    return label if label is None or len(label) <= 500 else None


def _location_values(original: Mapping[str, JsonValue]) -> tuple[JsonValue, ...]:
    # These are listing fields. Query variables such as buyLocation are search centers.
    return tuple(original[name] for name in ("location", "item_location") if name in original)


def _item_coordinate_conflict(observations: tuple[ListingObservationCandidate, ...]) -> bool:
    newest = sorted(
        observations, key=lambda item: evidence_recency_key(item.evidence), reverse=True
    )
    for name in ("location", "item_location"):
        for candidate in newest:
            observation = _mapping(candidate.observation)
            fields = _mapping(observation.get("fields")) if observation is not None else None
            field = _mapping(fields.get(name)) if fields is not None else None
            if field is None or field.get("state") not in {"present", "conflicting"}:
                continue
            evidence = field.get("evidence")
            if not isinstance(evidence, list):
                continue
            coordinates = [
                coordinate
                for raw in cast(list[JsonValue], evidence)
                if (item := _mapping(raw)) is not None and item.get("state") == "present"
                if (coordinate := _coordinate(item.get("normalized", item.get("original")))[0])
                is not None
            ]
            if any(coordinate != coordinates[0] for coordinate in coordinates[1:]):
                return True
            break
    return False


def listing_travel_destination(
    *,
    observations: tuple[ListingObservationCandidate, ...],
    search_cards: tuple[SearchCardCandidate, ...],
    label: JsonValue,
) -> TravelDestination:
    """Prefer item evidence, then retained card locations, then a locality label."""

    boundary = max(
        (candidate.evidence.observation_completion_sequence for candidate in observations),
        default=0,
    )
    selected = tuple(
        field.value
        for name in ("location", "item_location")
        if (field := select_field(name, observations, as_of_completion_sequence=boundary))
        is not None
    )
    destination_label = _location_label(label)
    if destination_label is None:
        destination_label = next(
            (text for value in selected if (text := _location_label(value)) is not None),
            None,
        )
    cards = sorted(search_cards, key=lambda card: evidence_recency_key(card.evidence), reverse=True)
    if destination_label is None:
        for card in cards:
            destination_label = next(
                (
                    text
                    for value in (
                        card.original.get("location_text"),
                        *_location_values(card.original),
                    )
                    if (text := _location_label(value)) is not None
                ),
                None,
            ) or _text(search_card_field_value("location_text", card.original))
            if destination_label is not None:
                break
    if destination_label is not None:
        for value in (
            *selected,
            *(_value for card in cards for _value in _location_values(card.original)),
        ):
            mapping = _mapping(value)
            reverse = _mapping(mapping.get("reverse_geocode")) if mapping is not None else None
            if reverse is not None:
                country = _text(reverse.get("country")) or _text(reverse.get("country_name"))
                if country:
                    if country.casefold() not in destination_label.casefold():
                        destination_label = _text(f"{destination_label}, {country}")
                    break
    if _item_coordinate_conflict(observations):
        return TravelDestination(
            label=destination_label, unavailable_reason="conflicting_coordinates"
        )
    invalid = False
    for values in (selected, *(_location_values(card.original) for card in cards), (label,)):
        coordinates: list[TravelCoordinate] = []
        for value in values:
            coordinate, malformed = _coordinate(value)
            invalid = invalid or malformed
            if coordinate is not None:
                coordinates.append(coordinate)
        if coordinates:
            if any(coordinate != coordinates[0] for coordinate in coordinates[1:]):
                return TravelDestination(
                    label=destination_label, unavailable_reason="conflicting_coordinates"
                )
            return TravelDestination(
                coordinate=coordinates[0], label=destination_label, precision="source_approximate"
            )
    if destination_label is not None:
        return TravelDestination(label=destination_label, precision="geocoded_locality")
    return TravelDestination(
        unavailable_reason="invalid_coordinates" if invalid else "location_unavailable"
    )
