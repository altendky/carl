"""Bounded bot-challenge retries and resumable completed price partitions."""

from typing import Literal

from pydantic import Field, model_validator

from carl.core.facebook_search import PricePartitionSearchTraversalState
from carl.core.models import JsonValue, StrictModel

FACEBOOK_CHALLENGE_MAXIMUM_ATTEMPTS = 5


def facebook_challenge_retry_delay_ns(attempt: int) -> int:
    """Wait 10, 20, 40, then at most 60 minutes between challenge attempts."""
    if attempt < 1:
        raise ValueError("Challenge retry attempt must be positive")
    return min(60, 10 * 2 ** min(attempt - 1, 3)) * 60_000_000_000


class PartitionSearchRetryCheckpoint(StrictModel):
    """Completed partitions immediately before a challenged partition request."""

    version: Literal[1] = 1
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    traversal_state: PricePartitionSearchTraversalState
    page_references: tuple[dict[str, JsonValue], ...]
    challenge_attempts: int = Field(ge=0)
    challenge_retry_attempts: int = Field(default=0, ge=0)
    challenge_page_ordinal: int = Field(ge=1)
    retry_budget_start_attempt: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def validate_completed_partitions(self) -> "PartitionSearchRetryCheckpoint":
        state = self.traversal_state
        if state.stopping_reason is not None or state.partitions_processed >= len(state.partitions):
            raise ValueError("Partition retry requires an incomplete, unstopped traversal")
        if len(self.page_references) != state.partitions_processed:
            raise ValueError("Page references must match the completed partition count")
        if self.challenge_page_ordinal != state.partitions_processed + 1:
            raise ValueError("The challenged page must follow the completed partitions")
        for ordinal, page in enumerate(self.page_references, start=1):
            if type(page.get("page_ordinal")) is not int or page["page_ordinal"] != ordinal:
                raise ValueError("Completed page references must have contiguous ordinals")
            occurrences = page.get("listing_occurrence_records")
            if not isinstance(occurrences, list) or any(
                not isinstance(identifier, str) or not identifier.strip()
                for identifier in occurrences
            ):
                raise ValueError("Listing occurrence references must be a list of nonempty strings")
            if not isinstance(page.get("requested_search"), dict):
                raise ValueError("Completed page references require their requested search")
            expected_partition = state.partitions[ordinal - 1].model_dump(mode="json")
            if page.get("price_partition") != expected_partition:
                raise ValueError("Completed page references must match the planned price partition")
        return self
