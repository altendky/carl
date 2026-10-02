"""Durable, fenced outage pause with unbilled direct connectivity probes."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import anyio

from carl.core.json import encode_json
from carl.core.models import JsonValue
from carl.io.sqlite import Database

PROBE_INTERVAL_NS = 30_000_000_000
PROBE_LEASE_NS = 15_000_000_000
HEALTHY_CACHE_NS = 1_000_000_000


async def probe_internet_connectivity() -> dict[str, JsonValue]:
    """Resolve independent hostnames and open TCP, without proxy or HTTP requests.

    Either success proves basic DNS/TCP reachability. This does not assert that
    any particular proxy, authentication, or destination service is healthy.
    """

    results: list[JsonValue] = []

    async def probe(host: str) -> None:
        try:
            with anyio.fail_after(3):
                stream = await anyio.connect_tcp(host, 443)
                await stream.aclose()
        except (OSError, TimeoutError) as error:
            results.append({"host": host, "reachable": False, "type": type(error).__name__})
        else:
            results.append({"host": host, "reachable": True})

    async with anyio.create_task_group() as task_group:
        task_group.start_soon(probe, "one.one.one.one")
        task_group.start_soon(probe, "dns.google")
    return {
        "reachable": any(
            isinstance(result, dict) and result.get("reachable") for result in results
        ),
        "checks": results,
    }


async def work_retry_budget(database: Database, identifier: str) -> tuple[tuple[int, ...], int]:
    async with database._connections.reader() as connection:  # pyright: ignore[reportPrivateUsage]
        cursor = await connection.execute(
            """
            SELECT json_extract(data_json, '$.attempt'),
                   json_extract(data_json, '$.retry_budget_start_attempt')
            FROM work_events WHERE work_item_id = ? AND (
                json_extract(data_json, '$.reason.kind') = 'connectivity_outage'
                OR json_extract(data_json, '$.retry_budget_start_attempt') IS NOT NULL
            ) ORDER BY sequence
            """,
            (identifier,),
        )
        rows = await cursor.fetchall()
    return (
        tuple(row[0] for row in rows if isinstance(row[0], int)),
        max((row[1] for row in rows if isinstance(row[1], int)), default=0),
    )


@dataclass(frozen=True, slots=True)
class ConnectivityMonitor:
    database: Database
    new_identifier: Callable[[], str]
    utc_now_ns: Callable[[], int]
    probe: Callable[[], Awaitable[dict[str, JsonValue]]] = probe_internet_connectivity

    async def available(self, *, after_failure: bool = False) -> bool:
        """Probe only when demanded by failure or due during a durable pause.

        Claiming a probe temporarily closes admission across worker processes.
        A crashed prober can be replaced after its fenced lease expires.
        """

        token = self.new_identifier()
        while True:
            now = self.utc_now_ns()
            async with self.database._connections.writer() as connection:  # pyright: ignore[reportPrivateUsage]
                cursor = await connection.execute(
                    "SELECT paused, next_probe_utc_ns, probe_token, probe_expires_utc_ns, checked_at_utc_ns FROM network_connectivity WHERE singleton = 1"
                )
                row = await cursor.fetchone()
                if row is None:
                    if not after_failure:
                        return True
                    await connection.execute(
                        "INSERT INTO network_connectivity(singleton, paused, next_probe_utc_ns) VALUES (1, 0, 0)"
                    )
                    row = (0, 0, None, None, None)
                busy = row[2] is not None and isinstance(row[3], int) and row[3] > now
                checked_at = row[4]
                if not busy:
                    if row[0] == 1 and row[1] > now:
                        return False
                    if row[0] == 0 and (
                        not after_failure
                        or (isinstance(checked_at, int) and now - checked_at < HEALTHY_CACHE_NS)
                    ):
                        return True
                    await connection.execute(
                        "UPDATE network_connectivity SET paused = 1, probe_token = ?, probe_expires_utc_ns = ? WHERE singleton = 1",
                        (token, now + PROBE_LEASE_NS),
                    )
            if not busy:
                break
            await anyio.sleep(0.1)
        try:
            result = await self.probe()
        except Exception as error:
            result = {"reachable": False, "probe_failure_type": type(error).__name__}
        reachable = result.get("reachable") is True
        now = self.utc_now_ns()
        async with self.database._connections.writer() as connection:  # pyright: ignore[reportPrivateUsage]
            await connection.execute(
                """
                UPDATE network_connectivity
                SET paused = ?, next_probe_utc_ns = ?, checked_at_utc_ns = ?,
                    probe_token = NULL, probe_expires_utc_ns = NULL, result_json = ?
                WHERE singleton = 1 AND probe_token = ?
                """,
                (int(not reachable), now + PROBE_INTERVAL_NS, now, encode_json(result), token),
            )
            if await connection.changes() != 1:
                # A replacement prober owns the decision; never override it.
                return False
        return reachable
