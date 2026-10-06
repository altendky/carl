"""Source-neutral refresh failure reporting and bounded item recovery requests."""

from typing import Literal

from pydantic import Field

from carl.core.models import JsonValue, StrictModel
from carl.core.worker import CompletedWork, TerminalFailureWork


class RefreshFailureSummary(StrictModel):
    severity: Literal["success", "partial_failure", "failed"]
    selected_items: int = Field(ge=0)
    failed_items: int = Field(ge=0)
    successful_items: int = Field(ge=0)
    reason: str | None = None


def refresh_failure_summary(result: dict[str, JsonValue]) -> RefreshFailureSummary:
    def count(name: str) -> int:
        value = result.get(name, 0)
        return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0

    selected = count("selected_unique_listings")
    failed = count("item_failures")
    mostly_failed = failed > 0 and (selected == 0 or failed * 2 >= selected)
    partial = (
        failed
        + count("description_failures")
        + count("new_images_failed")
        + count("resumed_images_failed")
        > 0
    )
    return RefreshFailureSummary(
        severity="failed" if mostly_failed else "partial_failure" if partial else "success",
        selected_items=selected,
        failed_items=failed,
        successful_items=max(0, selected - failed),
        reason="at_least_half_of_selected_items_failed" if mostly_failed else None,
    )


class RetryItemFailuresRequest(StrictModel):
    refresh_work_identifier: str = Field(min_length=1)
    maximum_items: int = Field(default=1000, ge=1, le=10_000)


class RetryItemFailuresResult(StrictModel):
    refresh_work_identifier: str
    marketplace: Literal["facebook", "ebay"]
    matched_terminal_failures: int = Field(ge=0)
    retryable_terminal_failures: int = Field(ge=0)
    retried: int = Field(ge=0)
    remaining_terminal_failures: int = Field(ge=0)
    refresh_resumed: bool
    retried_work_identifier_sample: tuple[str, ...] = Field(max_length=20)


def classify_refresh_completion(outcome: CompletedWork) -> CompletedWork | TerminalFailureWork:
    """A refresh losing at least half its selected item pages is a failed refresh."""
    result = dict(outcome.result)
    summary = refresh_failure_summary(result).model_dump(mode="json")
    result["failure_summary"] = summary
    if summary["severity"] == "failed":
        result["state"] = "failed"
        return TerminalFailureWork(
            inputs=outcome.inputs,
            records=outcome.records,
            artifacts=outcome.artifacts,
            outputs=outcome.outputs,
            error={"kind": "mostly_failed_refresh", "failure_summary": summary},
            result=result,
        )
    return outcome.model_copy(update={"result": result})
