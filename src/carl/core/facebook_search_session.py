"""Pure handling of protected Facebook search bootstrap session material."""

from copy import deepcopy
from typing import Literal, cast

from pydantic import Field, SecretStr

from carl.core.facebook_search_protocol import SearchSourceReference
from carl.core.models import JsonStringEnumeration, JsonValue, StrictModel


class FacebookBootstrapModule(JsonStringEnumeration):
    LSD = "LSD"
    SITE_DATA = "SiteData"


class SearchSessionMaterialIssueKind(JsonStringEnumeration):
    MISSING_LSD_MODULE = "missing_lsd_module"
    MALFORMED_LSD_MODULE = "malformed_lsd_module"
    AMBIGUOUS_LSD_MODULE = "ambiguous_lsd_module"
    MISSING_SITE_DATA_MODULE = "missing_site_data_module"
    MALFORMED_SITE_DATA_MODULE = "malformed_site_data_module"
    AMBIGUOUS_SITE_DATA_MODULE = "ambiguous_site_data_module"


class SearchSessionMaterialIssue(StrictModel):
    kind: SearchSessionMaterialIssueKind
    sources: tuple[SearchSourceReference, ...] = ()


class RedactedSessionFieldEvidence(StrictModel):
    """Non-secret evidence that a protected bootstrap value was extracted."""

    state: Literal["redacted"] = "redacted"
    character_count: int = Field(ge=1)
    sources: tuple[SearchSourceReference, ...]
    derivation: tuple[str, ...] | None = None


class SearchSessionMaterialEvidence(StrictModel):
    lsd: RedactedSessionFieldEvidence
    jazoest: RedactedSessionFieldEvidence
    hsi: RedactedSessionFieldEvidence
    spin_revision: RedactedSessionFieldEvidence
    spin_branch: RedactedSessionFieldEvidence
    spin_timestamp: RedactedSessionFieldEvidence


class ProtectedSearchSessionMaterial(StrictModel):
    """Runtime request material excluded from repr and every model serialization."""

    lsd: SecretStr = Field(exclude=True, repr=False)
    jazoest: SecretStr = Field(exclude=True, repr=False)
    hsi: SecretStr = Field(exclude=True, repr=False)
    spin_revision: SecretStr = Field(exclude=True, repr=False)
    spin_branch: SecretStr = Field(exclude=True, repr=False)
    spin_timestamp: SecretStr = Field(exclude=True, repr=False)

    def reveal_graphql_form_fields(self) -> tuple[tuple[str, str], ...]:
        """Reveal protected values only at the request-adapter boundary."""

        return (
            ("lsd", self.lsd.get_secret_value()),
            ("jazoest", self.jazoest.get_secret_value()),
            ("__hsi", self.hsi.get_secret_value()),
            ("__spin_r", self.spin_revision.get_secret_value()),
            ("__spin_b", self.spin_branch.get_secret_value()),
            ("__spin_t", self.spin_timestamp.get_secret_value()),
        )


class SearchSessionMaterialExtraction(StrictModel):
    material: ProtectedSearchSessionMaterial | None
    evidence: SearchSessionMaterialEvidence | None
    issues: tuple[SearchSessionMaterialIssue, ...]


class _ModuleCandidate(StrictModel):
    payload: dict[str, JsonValue]
    source: SearchSourceReference


class _ModuleSearch(StrictModel):
    candidates: tuple[_ModuleCandidate, ...]
    malformed_sources: tuple[SearchSourceReference, ...]


def derive_jazoest(lsd: str) -> str:
    """Derive Facebook's anonymous form value from an LSD token."""

    if not lsd:
        raise ValueError("An LSD token cannot be empty")
    return f"2{sum(ord(character) for character in lsd)}"


def pagination_variables(
    complete_variables: dict[str, JsonValue],
    *,
    cursor: str,
) -> dict[str, JsonValue]:
    """Create independent pagination variables with a replacement cursor."""

    if not cursor:
        raise ValueError("A pagination cursor cannot be empty")
    variables = deepcopy(complete_variables)
    variables["cursor"] = cursor
    return variables


def _walk_module_tables(
    value: object,
    *,
    path: tuple[str | int, ...] = (),
) -> tuple[tuple[list[object], tuple[str | int, ...]], ...]:
    found: list[tuple[list[object], tuple[str | int, ...]]] = []
    pending: list[tuple[object, tuple[str | int, ...]]] = [(value, path)]
    while pending:
        node, node_path = pending.pop()
        if isinstance(node, dict):
            mapping = cast(dict[object, object], node)
            for table_name in ("define", "require"):
                table = mapping.get(table_name)
                if isinstance(table, list):
                    found.append((cast(list[object], table), (*node_path, table_name)))
            for key, child in reversed(tuple(mapping.items())):
                if isinstance(key, str):
                    pending.append((child, (*node_path, key)))
        elif isinstance(node, list):
            sequence = cast(list[object], node)
            for index, child in reversed(tuple(enumerate(sequence))):
                pending.append((child, (*node_path, index)))
    return tuple(found)


def _module_search(
    blocks: tuple[dict[str, JsonValue], ...],
    *,
    acquisition_record_identifier: str,
    module: FacebookBootstrapModule,
) -> _ModuleSearch:
    candidates: list[_ModuleCandidate] = []
    malformed_sources: list[SearchSourceReference] = []
    for block in blocks:
        block_index = block.get("block_index")
        value = block.get("value")
        if not isinstance(block_index, int) or value is None:
            continue
        for table, table_path in _walk_module_tables(value):
            for index, entry in enumerate(table):
                if not isinstance(entry, list) or not entry or entry[0] != module.value:
                    continue
                module_entry = cast(list[object], entry)
                source = SearchSourceReference(
                    acquisition_record_identifier=acquisition_record_identifier,
                    block_index=block_index,
                    json_path=(*table_path, index),
                )
                if (
                    len(module_entry) < 3
                    or not isinstance(module_entry[1], list)
                    or not isinstance(module_entry[2], dict)
                ):
                    malformed_sources.append(source)
                    continue
                payload = cast(dict[object, object], module_entry[2])
                if any(not isinstance(key, str) for key in payload):
                    malformed_sources.append(source)
                    continue
                candidates.append(
                    _ModuleCandidate(
                        payload=cast(dict[str, JsonValue], payload),
                        source=source,
                    )
                )
    return _ModuleSearch(
        candidates=tuple(candidates),
        malformed_sources=tuple(malformed_sources),
    )


def _sources_for_field(
    candidates: tuple[_ModuleCandidate, ...],
    field: str,
) -> tuple[SearchSourceReference, ...]:
    return tuple(
        candidate.source.model_copy(update={"json_path": (*candidate.source.json_path, 2, field)})
        for candidate in candidates
    )


def _issue_kind(
    module: FacebookBootstrapModule,
    condition: Literal["missing", "malformed", "ambiguous"],
) -> SearchSessionMaterialIssueKind:
    return {
        (FacebookBootstrapModule.LSD, "missing"): (
            SearchSessionMaterialIssueKind.MISSING_LSD_MODULE
        ),
        (FacebookBootstrapModule.LSD, "malformed"): (
            SearchSessionMaterialIssueKind.MALFORMED_LSD_MODULE
        ),
        (FacebookBootstrapModule.LSD, "ambiguous"): (
            SearchSessionMaterialIssueKind.AMBIGUOUS_LSD_MODULE
        ),
        (FacebookBootstrapModule.SITE_DATA, "missing"): (
            SearchSessionMaterialIssueKind.MISSING_SITE_DATA_MODULE
        ),
        (FacebookBootstrapModule.SITE_DATA, "malformed"): (
            SearchSessionMaterialIssueKind.MALFORMED_SITE_DATA_MODULE
        ),
        (FacebookBootstrapModule.SITE_DATA, "ambiguous"): (
            SearchSessionMaterialIssueKind.AMBIGUOUS_SITE_DATA_MODULE
        ),
    }[(module, condition)]


def _validate_module(
    search: _ModuleSearch,
    *,
    module: FacebookBootstrapModule,
    fields: tuple[str, ...],
) -> tuple[
    dict[str, JsonValue] | None,
    tuple[_ModuleCandidate, ...],
    tuple[SearchSessionMaterialIssue, ...],
]:
    issues = (
        [
            SearchSessionMaterialIssue(
                kind=_issue_kind(module, "malformed"),
                sources=search.malformed_sources,
            )
        ]
        if search.malformed_sources
        else []
    )
    if not search.candidates:
        if not search.malformed_sources:
            issues.append(SearchSessionMaterialIssue(kind=_issue_kind(module, "missing")))
        return None, (), tuple(issues)

    valid: list[_ModuleCandidate] = []
    malformed: list[SearchSourceReference] = []
    for candidate in search.candidates:
        if all(_valid_field(candidate.payload.get(field), field=field) for field in fields):
            valid.append(candidate)
        else:
            malformed.append(candidate.source)
    if malformed:
        issues.append(
            SearchSessionMaterialIssue(
                kind=_issue_kind(module, "malformed"),
                sources=tuple(malformed),
            )
        )
    if not valid:
        return None, (), tuple(issues)
    first = valid[0].payload
    if any(
        any(candidate.payload[field] != first[field] for field in fields) for candidate in valid[1:]
    ):
        issues.append(
            SearchSessionMaterialIssue(
                kind=_issue_kind(module, "ambiguous"),
                sources=tuple(candidate.source for candidate in valid),
            )
        )
        return None, (), tuple(issues)
    return first, tuple(valid), tuple(issues)


def _valid_field(value: JsonValue, *, field: str) -> bool:
    if field in {"__spin_r", "__spin_t"}:
        return isinstance(value, int) and not isinstance(value, bool) and value >= 0
    if field == "hsi":
        return isinstance(value, str) and value.isascii() and value.isdecimal()
    return isinstance(value, str) and bool(value)


def _evidence(
    value: str,
    sources: tuple[SearchSourceReference, ...],
    *,
    derivation: tuple[str, ...] | None = None,
) -> RedactedSessionFieldEvidence:
    return RedactedSessionFieldEvidence(
        character_count=len(value),
        sources=sources,
        derivation=derivation,
    )


def extract_search_session_material(
    blocks: tuple[dict[str, JsonValue], ...],
    *,
    acquisition_record_identifier: str,
) -> SearchSessionMaterialExtraction:
    """Extract request-only values from already parsed bootstrap JSON blocks."""

    lsd_search = _module_search(
        blocks,
        acquisition_record_identifier=acquisition_record_identifier,
        module=FacebookBootstrapModule.LSD,
    )
    site_data_search = _module_search(
        blocks,
        acquisition_record_identifier=acquisition_record_identifier,
        module=FacebookBootstrapModule.SITE_DATA,
    )
    lsd_payload, lsd_candidates, lsd_issues = _validate_module(
        lsd_search,
        module=FacebookBootstrapModule.LSD,
        fields=("token",),
    )
    site_data_payload, site_data_candidates, site_data_issues = _validate_module(
        site_data_search,
        module=FacebookBootstrapModule.SITE_DATA,
        fields=("hsi", "__spin_r", "__spin_b", "__spin_t"),
    )
    issues = (*lsd_issues, *site_data_issues)
    if lsd_payload is None or site_data_payload is None:
        return SearchSessionMaterialExtraction(material=None, evidence=None, issues=issues)

    lsd = lsd_payload["token"]
    hsi = site_data_payload["hsi"]
    spin_revision = site_data_payload["__spin_r"]
    spin_branch = site_data_payload["__spin_b"]
    spin_timestamp = site_data_payload["__spin_t"]
    assert isinstance(lsd, str)
    assert isinstance(hsi, str)
    assert isinstance(spin_revision, int)
    assert isinstance(spin_branch, str)
    assert isinstance(spin_timestamp, int)
    jazoest = derive_jazoest(lsd)
    lsd_sources = _sources_for_field(lsd_candidates, "token")
    return SearchSessionMaterialExtraction(
        material=ProtectedSearchSessionMaterial(
            lsd=SecretStr(lsd),
            jazoest=SecretStr(jazoest),
            hsi=SecretStr(hsi),
            spin_revision=SecretStr(str(spin_revision)),
            spin_branch=SecretStr(spin_branch),
            spin_timestamp=SecretStr(str(spin_timestamp)),
        ),
        evidence=SearchSessionMaterialEvidence(
            lsd=_evidence(lsd, lsd_sources),
            jazoest=_evidence(
                jazoest,
                lsd_sources,
                derivation=("facebook", "jazoest", "unicode_code_point_sum", "1"),
            ),
            hsi=_evidence(str(hsi), _sources_for_field(site_data_candidates, "hsi")),
            spin_revision=_evidence(
                str(spin_revision),
                _sources_for_field(site_data_candidates, "__spin_r"),
            ),
            spin_branch=_evidence(
                spin_branch,
                _sources_for_field(site_data_candidates, "__spin_b"),
            ),
            spin_timestamp=_evidence(
                str(spin_timestamp),
                _sources_for_field(site_data_candidates, "__spin_t"),
            ),
        ),
        issues=issues,
    )
