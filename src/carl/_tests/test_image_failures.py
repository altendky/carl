"""Compact diagnostics recover old HTTP failures and require quota evidence."""

# pyright: reportPrivateUsage=false

from pathlib import Path

import pytest

from carl._tests.test_ebay_item_workers import _async_provenance, _identifiers, _retain_records
from carl.core.facebook_images import COLLECT_IMAGE_WORK_KIND
from carl.core.facebook_refresh import REFRESH_SEARCH_WORK_KIND
from carl.core.image_failures import image_acquisition_failure_kind
from carl.core.json import encode_json
from carl.core.models import RecordDraft
from carl.core.work import SchedulingScope, SchedulingScopeKind, WorkDefinition, WorkRequester
from carl.io.image_failures import refresh_image_failure_reason_counts
from carl.io.sqlite import Database
from carl.review import ReviewApplication


@pytest.mark.parametrize("status", [403, 429, 407, 502])
def test_http_error_does_not_imply_proxy_quota(status: int) -> None:
    assert (
        image_acquisition_failure_kind({"hops": [{"response": {"status_code": status}}]})
        == f"site_http_{status}"
    )


@pytest.mark.anyio
@pytest.mark.parametrize("via_image_result", [False, True])
async def test_status_recovers_historical_failure_causes_and_deduplicates_children(
    tmp_path: Path,
    via_image_result: bool,
) -> None:
    identifiers = _identifiers()
    async with Database.managed(tmp_path / "test.sqlite3", initialize=True) as database:
        await _retain_records(
            database,
            (
                RecordDraft(
                    identifier="old-acquisition",
                    kind=("carl", "http", "acquisition"),
                    schema_version=1,
                    value={"hops": [{"response": {"status_code": 403}}]},
                ),
                RecordDraft(
                    identifier="old-image-result",
                    kind=("carl", "ebay", "image_result"),
                    schema_version=1,
                    value={"acquisition_record_identifier": "old-acquisition"},
                ),
            ),
            identifiers,
        )
        for identifier in ("refresh", "legacy-http", "quota", "timeout", "pending", "unrelated"):
            kind = REFRESH_SEARCH_WORK_KIND if identifier == "refresh" else COLLECT_IMAGE_WORK_KIND
            _ = await database.enqueue_work(
                WorkDefinition(
                    identifier=identifier,
                    kind=kind,
                    payload_schema_version=1,
                    payload={},
                    deduplication_identity=(identifier,),
                    not_before_utc_ns=0,
                    scopes=(
                        SchedulingScope(kind=SchedulingScopeKind.OVERALL, identity=()),
                        SchedulingScope(kind=SchedulingScopeKind.WORK_KIND, identity=kind),
                    ),
                ),
                WorkRequester(
                    request_identifier=identifiers(),
                    kind=("carl", "facebook", "search_refresh", "image"),
                    identifier="other" if identifier == "unrelated" else "refresh",
                    context={
                        "search_refresh_work_identifier": (
                            "other" if identifier in {"unrelated", "refresh"} else "refresh"
                        )
                    },
                ),
                event_identifier=identifiers(),
                enqueued_at_utc_ns=1,
            )
        # Model older durable checkpoints with generic work errors. The HTTP
        # cause was retained only in their acquisition record.
        async with database._connections.writer() as connection:
            for identifier, result, error in (
                (
                    "legacy-http",
                    {
                        **(
                            {"image_result_record_identifier": "old-image-result"}
                            if via_image_result
                            else {"acquisition_record_identifier": "old-acquisition"}
                        ),
                        "state": "failed",
                    },
                    {"kind": "image_validation_or_storage_failure", "type": "ValueError"},
                ),
                (
                    "quota",
                    {
                        "acquisition": {
                            "network_provider_result": {"category": "proxy_quota_exhausted"}
                        }
                    },
                    {"kind": "image_acquisition_failure"},
                ),
                ("timeout", {}, {"kind": "unhandled_handler_error", "type": "TimeoutError"}),
                ("unrelated", {}, {"kind": "unrelated_failure"}),
            ):
                _ = await connection.execute(
                    "UPDATE work_items SET state='terminal_failure',result_json=?,error_json=? WHERE id=?",
                    (encode_json(result), encode_json(error), identifier),
                )
            _ = await connection.execute(
                "UPDATE work_items SET state='completed',result_json=? WHERE id='refresh'",
                (
                    encode_json(
                        {
                            "stage": "complete",
                            "state": "completed_with_failures",
                            "image_collection_work_identifiers": [
                                "legacy-http",
                                "quota",
                                "timeout",
                            ],
                        }
                    ),
                ),
            )
        counts = await refresh_image_failure_reason_counts(database, "refresh")
        assert {value.kind: value.count for value in counts} == {
            "site_http_403": 1,
            "proxy_quota_exhausted": 1,
            "transport_timeout": 1,
        }
        app = ReviewApplication(
            database, tmp_path, utc_now_ns=lambda: 2, code_provenance=_async_provenance
        )
        status = await app.get_work_status("refresh")
        assert status.details is None
        assert status.search_refresh_progress is not None
        assert status.search_refresh_progress.image_failure_reason_counts == counts
