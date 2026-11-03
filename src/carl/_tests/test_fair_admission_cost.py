"""Live fair offers do not add a database round trip for every contender."""

from __future__ import annotations

# The forwarding writer counts Python-to-SQL calls, excluding trigger internals.
# pyright: reportPrivateUsage=false
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import Any, cast

import apsw
import pytest

from carl._tests.test_fair_storage import _seed
from carl._tests.test_stage_storage import NOW, _claim
from carl.io.db import AsyncConnection
from carl.io.sqlite import Database

pytestmark = pytest.mark.anyio


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@dataclass
class _CountingWriter:
    connection: AsyncConnection
    statements: list[str] = field(default_factory=list)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.connection, name)

    async def __aenter__(self) -> Any:
        return await self.connection.__aenter__()

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: TracebackType | None,
    ) -> Any:
        return await self.connection.__aexit__(exception_type, exception, traceback)

    async def execute(self, statement: str, bindings: apsw.Bindings | None = None) -> Any:
        self.statements.append(statement)
        return await self.connection.execute(statement, bindings)

    async def executemany(self, statement: str, bindings: Iterable[apsw.Bindings]) -> Any:
        self.statements.append(statement)
        return await self.connection.executemany(statement, bindings)


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
            original = database._connections._writer
            counter = _CountingWriter(original)
            database._connections._writer = cast(AsyncConnection, cast(object, counter))
            try:
                claim = await _claim(database, "stage-offer-000")
            finally:
                database._connections._writer = original
            assert claim.lease is not None
            admissions = await database.fair_admissions("test-host")
            assert len(admissions) == 1
            assert admissions[0].work_item_identifier == "offer-000"
            assert admissions[0].sequence == 1
            assert (await database.work("offer-000"))["attempt"] == 1
            counts[size] = len(counter.statements)
    request.node.user_properties.extend(
        (f"admission_sql_calls_{size}_offers", count) for size, count in counts.items()
    )
    # Allow a small fixed number of extra metadata queries, not one per offer.
    assert max(counts.values()) <= min(counts.values()) + 8, counts
