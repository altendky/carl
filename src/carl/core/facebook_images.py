"""Pure Facebook gallery-image selection, validation, and work definitions."""

import hashlib
import warnings
from dataclasses import dataclass
from io import BytesIO
from urllib.parse import urlsplit

from PIL import Image, UnidentifiedImageError
from pydantic import Field, model_validator

from carl.core.content_encoding import decoded_stored_body
from carl.core.facebook_work import (
    FACEBOOK_NETWORK_PATH_RATE_MAXIMUM_STARTS,
    facebook_network_path_constraint,
)
from carl.core.http import RequestPlan, stored_content_encodings, stored_header_values
from carl.core.models import JsonStringEnumeration, JsonValue, StrictModel
from carl.core.work import (
    ConcurrencyConstraint,
    Constraint,
    NetworkActivityDefinition,
    SchedulingScope,
    SchedulingScopeKind,
    SchedulingSubjectKind,
    SlidingWindowRateConstraint,
    UniformHoldoffConstraint,
    WorkDefinition,
)

COLLECT_IMAGE_WORK_KIND = ("carl", "facebook", "work", "collect_image")
EXTRACT_IMAGE_WORK_KIND = ("carl", "facebook", "work", "extract_image")
IMAGE_NETWORK_ACTIVITY_KIND = ("carl", "facebook", "network_activity", "image")
LEGACY_COLLECT_IMAGE_WORK_SCHEMA_VERSION = 1
COLLECT_IMAGE_WORK_SCHEMA_VERSION = 2
EXTRACT_IMAGE_WORK_SCHEMA_VERSION = 1
IMAGE_RATE_PERIOD_NS = 60_000_000_000
IMAGE_RATE_MAXIMUM_STARTS = FACEBOOK_NETWORK_PATH_RATE_MAXIMUM_STARTS
IMAGE_SHORT_RATE_PERIOD_NS = 1_000_000_000
IMAGE_SHORT_RATE_MAXIMUM_STARTS = 3
IMAGE_MAXIMUM_ACTIVE = 10
IMAGE_SESSION_MAXIMUM_ACTIVE = 10
IMAGE_HOLDOFF_MINIMUM_NS = 0
IMAGE_HOLDOFF_MAXIMUM_NS = 100_000_000
MAX_IMAGE_PIXELS = 40_000_000


class ImageDownloadState(JsonStringEnumeration):
    SAVED = "saved"
    FAILED = "failed"


class ImageReuseMatchKind(JsonStringEnumeration):
    EXACT_RENDITION = "exact_rendition"
    SOURCE_PHOTO_ADEQUATE_DIMENSIONS = "source_photo_adequate_dimensions"


class ImageFailureSourceKind(JsonStringEnumeration):
    SEARCH_RUN = "search_run"
    SEARCH_REFRESH = "search_refresh"


class RetryImageFailuresRequest(StrictModel):
    source_identifier: str = Field(min_length=1)
    maximum_items: int = Field(default=1000, ge=1, le=10_000)


class RetryImageFailuresResult(StrictModel):
    source_identifier: str
    source_kind: ImageFailureSourceKind
    matched_terminal_failures: int = Field(ge=0)
    retried: int = Field(ge=0)
    remaining_terminal_failures: int = Field(ge=0)
    retried_work_identifier_sample: tuple[str, ...] = Field(max_length=20)


class ImageReuseRecord(StrictModel):
    match_kind: ImageReuseMatchKind


class ImageReuseResolution(StrictModel):
    image_reuse_record_identifier: str = Field(min_length=1)
    gallery_image_reference_record_identifier: str = Field(min_length=1)
    source_image_result_record_identifier: str = Field(min_length=1)
    match_kind: ImageReuseMatchKind


class SavedImageCandidate(StrictModel):
    image_result_record_identifier: str = Field(min_length=1)
    source_photo_id: str | None
    original_url: str
    width: int = Field(gt=0)
    height: int = Field(gt=0)


@dataclass(frozen=True, slots=True)
class ImageReuseDecision:
    reference_index: int
    candidate: SavedImageCandidate
    match_kind: ImageReuseMatchKind


@dataclass(frozen=True, slots=True)
class ImageFollowupPlan:
    reuse_decisions: tuple[ImageReuseDecision, ...]
    download_groups: tuple[tuple[int, ...], ...]


class GalleryImageReference(StrictModel):
    listing_id: str = Field(pattern=r"^[0-9]+$")
    listing_observation_record_identifier: str = Field(min_length=1)
    acquisition_record_identifier: str = Field(min_length=1)
    block_index: int = Field(ge=0)
    json_path: tuple[str | int, ...]
    original_url: str
    role: str = "listing_gallery"
    gallery_order: int = Field(ge=0)
    photo_id: str | None = None
    declared_width: int | None = Field(default=None, gt=0)
    declared_height: int | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_reference(self) -> "GalleryImageReference":
        parsed = urlsplit(self.original_url)
        if (
            parsed.scheme != "https"
            or parsed.hostname is None
            or not parsed.hostname.endswith(".fbcdn.net")
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
        ):
            raise ValueError("Gallery image URL must be a credential-free Facebook CDN URL")
        if self.role != "listing_gallery":
            raise ValueError("Only target listing gallery images are collectible")
        return self

    @property
    def rendition_identity(self) -> tuple[str, ...]:
        return (
            "facebook_marketplace",
            "source_image_rendition",
            self.photo_id or "missing_source_photo_id",
            hashlib.sha256(self.original_url.encode("utf-8")).hexdigest(),
        )


def gallery_references(
    *, observation_identifier: str, observation: JsonValue
) -> tuple[GalleryImageReference, ...]:
    if not isinstance(observation, dict):
        raise ValueError("Listing observation is malformed")
    listing_id = observation.get("listing_id")
    images = observation.get("images")
    if not isinstance(listing_id, str) or not isinstance(images, list):
        raise ValueError("Listing observation has no valid image references")
    references: list[GalleryImageReference] = []
    for image in images:
        if not isinstance(image, dict):
            raise ValueError("Gallery image reference is malformed")
        source = image.get("source")
        declared = image.get("declared_dimensions")
        if not isinstance(source, dict) or not isinstance(declared, dict):
            raise ValueError("Gallery image provenance is malformed")
        references.append(
            GalleryImageReference(
                listing_id=listing_id,
                listing_observation_record_identifier=observation_identifier,
                acquisition_record_identifier=source["acquisition_record_id"],
                block_index=source["block_index"],
                json_path=tuple(source["json_path"]),
                original_url=image["original_url"],
                role=image["role"],
                gallery_order=image["gallery_order"],
                photo_id=image.get("photo_id"),
                declared_width=declared.get("width"),
                declared_height=declared.get("height"),
            )
        )
    return tuple(references)


def _best_candidate(
    reference: GalleryImageReference,
    exact_candidates: dict[tuple[str | None, str], tuple[SavedImageCandidate, ...]],
    source_candidates: dict[str, tuple[SavedImageCandidate, ...]],
) -> tuple[SavedImageCandidate, ImageReuseMatchKind] | None:
    exact = exact_candidates.get(
        (reference.photo_id, reference.original_url),
        (),
    )
    if exact:
        return (
            max(
                exact,
                key=lambda candidate: (
                    candidate.width * candidate.height,
                    candidate.width,
                    candidate.height,
                    candidate.image_result_record_identifier,
                ),
            ),
            ImageReuseMatchKind.EXACT_RENDITION,
        )
    if (
        reference.photo_id is None
        or reference.declared_width is None
        or reference.declared_height is None
    ):
        return None
    adequate = tuple(
        candidate
        for candidate in source_candidates.get(reference.photo_id, ())
        if candidate.width >= reference.declared_width
        and candidate.height >= reference.declared_height
    )
    if not adequate:
        return None
    return (
        max(
            adequate,
            key=lambda candidate: (
                candidate.width * candidate.height,
                candidate.width,
                candidate.height,
                candidate.image_result_record_identifier,
            ),
        ),
        ImageReuseMatchKind.SOURCE_PHOTO_ADEQUATE_DIMENSIONS,
    )


def image_reuse_record(match_kind: ImageReuseMatchKind) -> ImageReuseRecord:
    return ImageReuseRecord(match_kind=match_kind)


def plan_image_followups(
    references: tuple[GalleryImageReference, ...],
    saved_images: tuple[SavedImageCandidate, ...],
    maximum_images: int,
    excluded_renditions: frozenset[tuple[str | None, str]] = frozenset(),
) -> ImageFollowupPlan:
    """Reuse adequate source photos and group the remaining exact requests."""

    if maximum_images < 0:
        raise ValueError("The maximum image count cannot be negative")
    exact_candidates_lists: dict[tuple[str | None, str], list[SavedImageCandidate]] = {}
    source_candidates_lists: dict[str, list[SavedImageCandidate]] = {}
    for candidate in saved_images:
        exact_candidates_lists.setdefault(
            (candidate.source_photo_id, candidate.original_url), []
        ).append(candidate)
        if candidate.source_photo_id is not None:
            source_candidates_lists.setdefault(candidate.source_photo_id, []).append(candidate)
    exact_candidates = {
        key: tuple(candidates) for key, candidates in exact_candidates_lists.items()
    }
    source_candidates = {
        key: tuple(candidates) for key, candidates in source_candidates_lists.items()
    }
    selected: dict[tuple[str | None, str], list[int]] = {}
    reuse_decisions: list[ImageReuseDecision] = []
    for index, reference in enumerate(references):
        reusable = _best_candidate(reference, exact_candidates, source_candidates)
        if reusable is not None:
            candidate, match_kind = reusable
            reuse_decisions.append(
                ImageReuseDecision(
                    reference_index=index,
                    candidate=candidate,
                    match_kind=match_kind,
                )
            )
            continue
        key = (reference.photo_id, reference.original_url)
        if key in excluded_renditions:
            continue
        if key not in selected and len(selected) >= maximum_images:
            continue
        selected.setdefault(key, []).append(index)
    return ImageFollowupPlan(
        reuse_decisions=tuple(reuse_decisions),
        download_groups=tuple(tuple(group) for group in selected.values()),
    )


class CollectImagePayload(StrictModel):
    reference_record_identifier: str = Field(min_length=1)
    reference: GalleryImageReference
    request_plan: RequestPlan

    @model_validator(mode="after")
    def validate_request(self) -> "CollectImagePayload":
        if (
            self.request_plan.method != "GET"
            or self.request_plan.url != self.reference.original_url
        ):
            raise ValueError("Image request must GET the exact signed gallery URL")
        if self.request_plan.follow_redirects:
            raise ValueError("Image redirects require a separately reviewed route decision")
        return self


class ExtractImagePayload(StrictModel):
    acquisition_record_identifier: str = Field(min_length=1)
    reference_record_identifier: str = Field(min_length=1)


def collect_image_work(
    *, identifier: str, payload: CollectImagePayload, not_before_utc_ns: int
) -> WorkDefinition:
    return WorkDefinition(
        identifier=identifier,
        kind=COLLECT_IMAGE_WORK_KIND,
        payload_schema_version=COLLECT_IMAGE_WORK_SCHEMA_VERSION,
        payload=payload.model_dump(mode="json", by_alias=True),
        deduplication_identity=(
            *payload.reference.rendition_identity,
            "routing",
            *payload.request_plan.routing,
        ),
        not_before_utc_ns=not_before_utc_ns,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(
                kind=SchedulingScopeKind.NETWORK_PATH, identity=payload.request_plan.routing
            ),
            SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=COLLECT_IMAGE_WORK_KIND),
        ),
    )


def extract_image_work(
    *, identifier: str, payload: ExtractImagePayload, not_before_utc_ns: int
) -> WorkDefinition:
    return WorkDefinition(
        identifier=identifier,
        kind=EXTRACT_IMAGE_WORK_KIND,
        payload_schema_version=EXTRACT_IMAGE_WORK_SCHEMA_VERSION,
        payload=payload.model_dump(mode="json"),
        deduplication_identity=(
            "facebook_marketplace",
            "image_extraction",
            payload.acquisition_record_identifier,
            payload.reference_record_identifier,
        ),
        not_before_utc_ns=not_before_utc_ns,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=EXTRACT_IMAGE_WORK_KIND),
        ),
    )


def legacy_image_network_constraint_identifiers(
    routing: tuple[str, ...],
) -> tuple[tuple[str, ...], ...]:
    """Old limits matching images, including source rules scoped too broadly."""

    return (
        ("carl", "facebook", "image", "network_activity_concurrency", "all_cdns"),
        (
            "carl",
            "facebook",
            "image",
            "network_activity_concurrency",
            "all_cdns",
            "v2",
        ),
        *legacy_image_session_work_constraint_identifiers(),
        ("carl", "facebook", "image", "network_activity_rate", *routing),
        ("carl", "facebook", "image", "network_activity_rate", "all_cdns"),
        ("carl", "facebook", "image", "network_activity_holdoff"),
        ("carl", "facebook", "image", "network_activity_rate", "v2", *routing),
        ("carl", "facebook", "search", "network_activity_rate", *routing),
        ("carl", "facebook", "item", "network_activity_rate", *routing),
    )


def legacy_image_session_work_constraint_identifiers() -> tuple[tuple[str, ...], ...]:
    return (("carl", "facebook", "image", "work_concurrency", "v1"),)


def image_network_constraints(routing: tuple[str, ...]) -> tuple[Constraint, ...]:
    return (
        image_session_work_constraint(),
        ConcurrencyConstraint(
            identifier=(
                "carl",
                "facebook",
                "image",
                "network_activity_concurrency",
                "all_cdns",
                "v3",
            ),
            subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
            scope=SchedulingScope(
                kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND,
                identity=IMAGE_NETWORK_ACTIVITY_KIND,
            ),
            maximum_active=IMAGE_MAXIMUM_ACTIVE,
        ),
        facebook_network_path_constraint(routing),
        SlidingWindowRateConstraint(
            identifier=("carl", "facebook", "image", "network_activity_rate", "all_cdns", "v2"),
            subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
            scope=SchedulingScope(
                kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND,
                identity=IMAGE_NETWORK_ACTIVITY_KIND,
            ),
            maximum_starts=IMAGE_RATE_MAXIMUM_STARTS,
            period_ns=IMAGE_RATE_PERIOD_NS,
        ),
        SlidingWindowRateConstraint(
            identifier=(
                "carl",
                "facebook",
                "image",
                "network_activity_rate",
                "all_cdns",
                "second",
                "v2",
            ),
            subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
            scope=SchedulingScope(
                kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND,
                identity=IMAGE_NETWORK_ACTIVITY_KIND,
            ),
            maximum_starts=IMAGE_SHORT_RATE_MAXIMUM_STARTS,
            period_ns=IMAGE_SHORT_RATE_PERIOD_NS,
        ),
        UniformHoldoffConstraint(
            identifier=("carl", "facebook", "image", "network_activity_holdoff", "v2"),
            subject_kind=SchedulingSubjectKind.NETWORK_ACTIVITY,
            scope=SchedulingScope(
                kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND,
                identity=IMAGE_NETWORK_ACTIVITY_KIND,
            ),
            minimum_ns=IMAGE_HOLDOFF_MINIMUM_NS,
            maximum_ns=IMAGE_HOLDOFF_MAXIMUM_NS,
        ),
    )


def image_session_work_constraint() -> ConcurrencyConstraint:
    """Bound image work independently of the selected provider transport."""

    return ConcurrencyConstraint(
        identifier=("carl", "facebook", "image", "work_concurrency", "v2"),
        subject_kind=SchedulingSubjectKind.WORK_ITEM,
        scope=SchedulingScope(
            kind=SchedulingScopeKind.WORK_KIND,
            identity=COLLECT_IMAGE_WORK_KIND,
        ),
        maximum_active=IMAGE_SESSION_MAXIMUM_ACTIVE,
    )


def image_network_activity(
    *,
    identifier: str,
    operation_identifier: str,
    network_session_identifier: str,
    attempt: int,
    routing: tuple[str, ...],
    url: str,
) -> NetworkActivityDefinition:
    hostname = urlsplit(url).hostname
    if hostname is None:
        raise ValueError("Image request has no host")
    return NetworkActivityDefinition(
        identifier=identifier,
        kind=IMAGE_NETWORK_ACTIVITY_KIND,
        operation_identifier=operation_identifier,
        network_session_identifier=network_session_identifier,
        ordinal=1,
        attempt=attempt,
        scopes=(
            SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
            SchedulingScope(kind=SchedulingScopeKind.NETWORK_PATH, identity=routing),
            SchedulingScope(
                kind=SchedulingScopeKind.NETWORK_ACTIVITY_KIND, identity=IMAGE_NETWORK_ACTIVITY_KIND
            ),
            SchedulingScope(
                kind=SchedulingScopeKind.REMOTE_ORIGIN, identity=("https", hostname, "443")
            ),
        ),
    )


class VerifiedImage(StrictModel):
    content: bytes
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    media_type: str
    format: str
    width: int = Field(gt=0)
    height: int = Field(gt=0)
    header_media_type: str | None
    content_encodings_removed: tuple[str, ...]


def verify_image(
    stored_body: bytes, headers: JsonValue, representation: JsonValue
) -> VerifiedImage:
    encodings = stored_content_encodings(headers)
    decoded = decoded_stored_body(stored_body, representation, encodings)
    header_types = stored_header_values(headers, "content-type")
    header_type = header_types[-1].partition(";")[0].strip().lower() if header_types else None
    if header_type is not None and not header_type.startswith("image/"):
        raise ValueError("Response Content-Type is not an image")
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            with Image.open(BytesIO(decoded)) as image:
                if image.width * image.height > MAX_IMAGE_PIXELS:
                    raise ValueError("Image dimensions exceed pixel limit")
                image.verify()
            with Image.open(BytesIO(decoded)) as image:
                if image.width * image.height > MAX_IMAGE_PIXELS:
                    raise ValueError("Image dimensions exceed pixel limit")
                _ = image.load()
                image_format = image.format
                width, height = image.size
    except (
        UnidentifiedImageError,
        OSError,
        ValueError,
        Image.DecompressionBombError,
        Image.DecompressionBombWarning,
    ) as error:
        raise ValueError("Image content failed decoding or verification") from error
    if image_format is None or width <= 0 or height <= 0:
        raise ValueError("Image has no supported format or dimensions")
    media_type = Image.MIME.get(image_format)
    if media_type is None:
        raise ValueError("Image format has no MIME type")
    return VerifiedImage(
        content=decoded,
        sha256=hashlib.sha256(decoded).hexdigest(),
        media_type=media_type,
        format=image_format,
        width=width,
        height=height,
        header_media_type=header_type,
        content_encodings_removed=encodings,
    )
