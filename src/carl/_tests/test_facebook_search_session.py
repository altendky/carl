import json

import pytest

from carl.core.facebook import parse_json_blocks
from carl.core.facebook_search_session import (
    SearchSessionMaterialIssueKind,
    derive_jazoest,
    extract_search_session_material,
    pagination_variables,
)


def _blocks(*requirements: object) -> tuple[dict[str, object], ...]:
    html = (
        '<script type="application/json">'
        + json.dumps({"nested": {"require": list(requirements)}})
        + "</script>"
    )
    return parse_json_blocks(html)


def _lsd(token: str = "synthetic-token") -> list[object]:
    return ["LSD", [], {"token": token}, 1]


def _site_data(*, hsi: str = "123456") -> list[object]:
    return [
        "SiteData",
        [],
        {
            "hsi": hsi,
            "__spin_r": 987,
            "__spin_b": "trunk",
            "__spin_t": 654,
            "unrelated": "retained only in raw evidence",
        },
        2,
    ]


def test_extracts_named_modules_and_serializes_only_redacted_evidence() -> None:
    token = "synthetic-token"
    extraction = extract_search_session_material(
        _blocks(_lsd(token), _site_data()),
        acquisition_record_identifier="acquisition-1",
    )

    assert extraction.issues == ()
    assert extraction.material is not None
    assert extraction.material.reveal_graphql_form_fields() == (
        ("lsd", token),
        ("jazoest", derive_jazoest(token)),
        ("__hsi", "123456"),
        ("__spin_r", "987"),
        ("__spin_b", "trunk"),
        ("__spin_t", "654"),
    )
    assert extraction.evidence is not None
    assert extraction.evidence.lsd.state == "redacted"
    assert extraction.evidence.lsd.sources[0].json_path[-3:] == (0, 2, "token")
    assert extraction.evidence.jazoest.derivation == (
        "facebook",
        "jazoest",
        "unicode_code_point_sum",
        "1",
    )
    serialized = json.dumps(extraction.model_dump(mode="json"))
    for protected_value in (
        token,
        derive_jazoest(token),
        "123456",
        "987",
        "trunk",
        "654",
    ):
        assert protected_value not in serialized
    assert token not in repr(extraction)


def test_jazoest_uses_unicode_code_point_sum_and_rejects_empty_token() -> None:
    assert derive_jazoest("Aé") == f"2{ord('A') + ord('é')}"
    with pytest.raises(ValueError, match="cannot be empty"):
        _ = derive_jazoest("")


def test_module_like_data_outside_require_is_not_session_material() -> None:
    html = '<script type="application/json">' + json.dumps({"decoy": _lsd()}) + "</script>"

    extraction = extract_search_session_material(
        parse_json_blocks(html),
        acquisition_record_identifier="acquisition-1",
    )

    assert extraction.material is None
    assert {issue.kind for issue in extraction.issues} == {
        SearchSessionMaterialIssueKind.MISSING_LSD_MODULE,
        SearchSessionMaterialIssueKind.MISSING_SITE_DATA_MODULE,
    }


def test_named_modules_are_found_in_define_tables() -> None:
    html = (
        '<script type="application/json">'
        + json.dumps({"require": [["loader", [], {"define": [_lsd(), _site_data()]}]]})
        + "</script>"
    )

    extraction = extract_search_session_material(
        parse_json_blocks(html),
        acquisition_record_identifier="acquisition-1",
    )

    assert extraction.material is not None
    assert extraction.issues == ()


def test_malformed_and_conflicting_modules_fail_closed() -> None:
    malformed_lsd: list[object] = ["LSD", [], {"token": ""}]
    malformed_site_data: list[object] = ["SiteData", "not-dependencies", {}]
    extraction = extract_search_session_material(
        _blocks(
            malformed_lsd,
            _lsd("first-token"),
            _lsd("second-token"),
            malformed_site_data,
            _site_data(),
        ),
        acquisition_record_identifier="acquisition-1",
    )

    assert extraction.material is None
    assert extraction.evidence is None
    assert {issue.kind for issue in extraction.issues} == {
        SearchSessionMaterialIssueKind.MALFORMED_LSD_MODULE,
        SearchSessionMaterialIssueKind.AMBIGUOUS_LSD_MODULE,
        SearchSessionMaterialIssueKind.MALFORMED_SITE_DATA_MODULE,
    }


def test_identical_repeated_modules_retain_all_sources() -> None:
    extraction = extract_search_session_material(
        _blocks(_lsd(), _site_data(), _lsd(), _site_data()),
        acquisition_record_identifier="acquisition-1",
    )

    assert extraction.material is not None
    assert extraction.evidence is not None
    assert len(extraction.evidence.lsd.sources) == 2
    assert len(extraction.evidence.hsi.sources) == 2


def test_malformed_duplicate_is_reported_but_not_claimed_as_value_provenance() -> None:
    malformed_lsd: list[object] = ["LSD", [], {"token": ""}]
    extraction = extract_search_session_material(
        _blocks(malformed_lsd, _lsd(), _site_data()),
        acquisition_record_identifier="acquisition-1",
    )

    assert extraction.material is not None
    assert extraction.evidence is not None
    assert len(extraction.evidence.lsd.sources) == 1
    assert [issue.kind for issue in extraction.issues] == [
        SearchSessionMaterialIssueKind.MALFORMED_LSD_MODULE
    ]


def test_pagination_variables_are_a_deep_copy_with_only_cursor_replaced() -> None:
    original = {
        "count": 24,
        "cursor": None,
        "params": {"browse_request_params": {"filter_radius_km": 97}},
    }

    result = pagination_variables(original, cursor="opaque-next-cursor")

    assert result == {
        "count": 24,
        "cursor": "opaque-next-cursor",
        "params": {"browse_request_params": {"filter_radius_km": 97}},
    }
    assert original["cursor"] is None
    assert result["params"] is not original["params"]
    with pytest.raises(ValueError, match="cannot be empty"):
        _ = pagination_variables(original, cursor="")
