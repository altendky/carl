"""Live fair offers do not add a database round trip for every contender."""

from __future__ import annotations

# Trace the real writer so native worker callbacks remain visible to the budget.
# pyright: reportPrivateUsage=false
from pathlib import Path

import apsw
import pytest

from carl._tests.test_fair_storage import _seed
from carl._tests.test_stage_storage import NOW, _claim
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


async def test_one_real_claim_batches_eligibility_for_all_live_offers(
    tmp_path: Path, request: pytest.FixtureRequest
) -> None:
    counts: dict[int, int] = {}
    for size in (1, 32, 256):
        async with Database.managed(
            tmp_path / f"offers-{size}.sqlite3", initialize=True
        ) as database:
            identifiers = tuple(f"offer-{index:03d}" for index in range(size))
            for identifier in identifiers:
                await _seed(database, identifier, root="shared-root", workspace="shared-workspace")
            await database.enable_fair_scheduling("test-host", NOW)
            await database.offer_fair_dispatch_candidates(
                scope="test-host", work_identifiers=identifiers, now_utc_ns=NOW
            )
            writer = database._connections._writer
            statements: list[str] = []

            def trace(event: dict[str, object], *, recorded: list[str] = statements) -> None:
                # SQLite emits additional STMT notifications for trigger bodies.
                # Count each executed application statement, including native SQL.
                if not event.get("trigger"):
                    statement = event["sql"]
                    assert isinstance(statement, str)
                    recorded.append(statement)

            await writer.trace_v2(apsw.SQLITE_TRACE_STMT, trace, id="fair-admission-cost")
            try:
                claim = await _claim(database, "stage-offer-000")
            finally:
                await writer.trace_v2(0, None, id="fair-admission-cost")
            assert claim.lease is not None
            admissions = await database.fair_admissions("test-host")
            assert len(admissions) == 1
            assert admissions[0].work_item_identifier == "offer-000"
            assert admissions[0].sequence == 1
            assert (await database.work("offer-000"))["attempt"] == 1
            assert statements
            counts[size] = len(statements)
    request.node.user_properties.extend(
        (f"admission_sql_calls_{size}_offers", count) for size, count in counts.items()
    )
    # Allow a small fixed number of extra metadata queries, not one per offer.
    assert max(counts.values()) <= min(counts.values()) + 8, counts
