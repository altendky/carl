"""Private bounded routing cache and cross-process provider start pacing."""

import json
import math
import os
import sqlite3
import stat
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from time import time
from typing import Any, Literal, cast

import anyio

from carl.core.models import JsonValue
from carl.io.configuration import ConfigurationFailure, _require_private_directory
from carl.io.paths import CarlDirectories

MAX_CACHE_VALUE_BYTES = 65_536
type RoutingLane = Literal["geocode", "matrix"]
_INTERVALS: dict[RoutingLane, float] = {"geocode": 0.65, "matrix": 1.6}
_PROVIDER = "openrouteservice"


@dataclass(frozen=True, slots=True)
class RoutingGateDecision:
    action_identifier: str
    granted_at: float | None
    next_eligible_at: float
    reason: Literal["pacing", "provider_cooldown"] | None

    @property
    def granted(self) -> bool:
        return self.granted_at is not None


def _finite(value: float) -> float:
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError("Routing cache times and intervals must be finite numbers")
    return float(value)


def _json_float(value: str) -> float:
    return _finite(float(value))


class RoutingCache:
    def __init__(self, directories: CarlDirectories, *, now: Callable[[], float] = time):
        self.directories = directories
        self.now = now

    def _connect(self) -> sqlite3.Connection:
        path = self.directories.routing_cache_file
        self.directories.cache.mkdir(parents=True, exist_ok=True)
        _require_private_directory(path.parent, ConfigurationFailure)
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        except FileExistsError:
            info = path.lstat()
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) & 0o077
            ):
                raise ConfigurationFailure("routing_cache_permissions") from None
        else:
            os.close(descriptor)
        connection = sqlite3.connect(path, timeout=5)
        try:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS estimates (key TEXT PRIMARY KEY, expires REAL NOT NULL, value TEXT NOT NULL)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS pacing (lane TEXT PRIMARY KEY, next_start REAL NOT NULL)"
            )
            connection.execute("CREATE INDEX IF NOT EXISTS estimates_expiry ON estimates(expires)")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS provider_cooldowns (provider TEXT PRIMARY KEY, blocked_until REAL NOT NULL)"
            )
            return connection
        except BaseException:
            connection.close()
            raise

    def _get(self, key: str, now: float) -> JsonValue | None:
        with closing(self._connect()) as connection, connection:
            row = connection.execute(
                """
                SELECT CAST(value AS BLOB) FROM estimates
                WHERE key=? AND expires>? AND typeof(value)='text'
                  AND length(CAST(value AS BLOB))<=?
                """,
                (key, _finite(now), MAX_CACHE_VALUE_BYTES),
            ).fetchone()
        if row is None:
            return None
        try:
            return cast(
                JsonValue,
                json.loads(
                    row[0].decode("utf-8"), parse_float=_json_float, parse_constant=_json_float
                ),
            )
        except (ValueError, RecursionError):
            return None

    async def get(self, key: str, now: float) -> JsonValue | None:
        return await anyio.to_thread.run_sync(self._get, key, now)

    def _put(self, key: str, value: Any, expires: float) -> None:
        encoded = json.dumps(value, allow_nan=False, ensure_ascii=False)
        if len(encoded.encode("utf-8")) > MAX_CACHE_VALUE_BYTES:
            raise ValueError("Routing cache value exceeds its 64 KiB limit")
        expiry = _finite(expires)
        with closing(self._connect()) as connection, connection:
            connection.execute(
                "DELETE FROM estimates WHERE key IN (SELECT key FROM estimates WHERE expires<=? ORDER BY expires LIMIT 100)",
                (_finite(self.now()),),
            )
            connection.execute(
                "INSERT OR REPLACE INTO estimates VALUES (?,?,?)", (key, expiry, encoded)
            )

    async def put(self, key: str, value: Any, expires: float) -> None:
        await anyio.to_thread.run_sync(self._put, key, value, expires)

    def _reserve(self, lane: str, interval: float, now: float) -> float:
        """Retain the legacy future-reservation API for existing direct callers."""

        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT next_start FROM pacing WHERE lane=?", (lane,)
            ).fetchone()
            start = max(now, row[0] if row else now)
            connection.execute(
                "INSERT OR REPLACE INTO pacing VALUES (?,?)", (lane, start + interval)
            )
        return start - now

    async def reserve(self, lane: str, interval: float, now: float) -> float:
        return await anyio.to_thread.run_sync(self._reserve, lane, interval, now)

    def _try_gate(
        self, lane: RoutingLane, action_identifier: str, now: float
    ) -> RoutingGateDecision:
        """Check the current lane, never treating an old action as a reusable grant."""

        now = _finite(now)
        if lane not in _INTERVALS:
            raise ValueError("Unknown routing pacing lane")
        if (
            not isinstance(action_identifier, str)
            or not 1 <= len(action_identifier) <= 200
            or action_identifier != action_identifier.strip()
            or any(ord(character) < 32 or ord(character) == 127 for character in action_identifier)
        ):
            raise ValueError("Routing actions require a bounded public identifier")
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            now = max(now, _finite(self.now()))
            row = connection.execute(
                "SELECT next_start FROM pacing WHERE lane=?", (lane,)
            ).fetchone()
            next_start = _finite(row[0]) if row else now
            row = connection.execute(
                "SELECT blocked_until FROM provider_cooldowns WHERE provider=?", (_PROVIDER,)
            ).fetchone()
            blocked_until = _finite(row[0]) if row else now
            deadline = max(next_start, blocked_until)
            if now < deadline:
                return RoutingGateDecision(
                    action_identifier,
                    None,
                    deadline,
                    "provider_cooldown" if now < blocked_until else "pacing",
                )
            deadline = now + _INTERVALS[lane]
            connection.execute("INSERT OR REPLACE INTO pacing VALUES (?,?)", (lane, deadline))
        return RoutingGateDecision(action_identifier, now, deadline, None)

    async def try_gate(
        self, lane: RoutingLane, *, action_identifier: str, now: float
    ) -> RoutingGateDecision:
        """Try immediately before dispatch; denied polls consume no future start slot.

        Grants belong only to this invocation, even for a repeated action ID.
        Dispatch authorization and replay fencing remain in the evidence ledger.
        """

        return await anyio.to_thread.run_sync(self._try_gate, lane, action_identifier, now)

    def _block_until(self, deadline: float) -> float:
        deadline = _finite(deadline)
        with closing(self._connect()) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT blocked_until FROM provider_cooldowns WHERE provider=?", (_PROVIDER,)
            ).fetchone()
            deadline = max(deadline, _finite(row[0]) if row else deadline)
            connection.execute(
                "INSERT OR REPLACE INTO provider_cooldowns VALUES (?,?)", (_PROVIDER, deadline)
            )
        return deadline

    async def block_until(self, deadline: float) -> float:
        """Persist a provider-wide cooldown, extending rather than shortening it."""

        return await anyio.to_thread.run_sync(self._block_until, deadline)
