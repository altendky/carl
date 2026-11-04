"""Read stage admission preflight in the caller's existing writer transaction."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from carl.core.stages import StageClaimResult, StageDefinition, StageWaitKind

if TYPE_CHECKING:
    from apsw import Connection, SQLiteValue


def _text(value: SQLiteValue) -> str:
    if not isinstance(value, str):
        raise ValueError("Expected SQLite text value")
    return value


def _integer(value: SQLiteValue) -> int:
    if not isinstance(value, int):
        raise ValueError("Expected SQLite integer value")
    return value


@dataclass(frozen=True, slots=True)
class Preflight:
    stage: tuple[SQLiteValue, ...]
    definition: StageDefinition
    work: tuple[SQLiteValue, ...] | None = None
    network_stage: bool = False
    wait: StageClaimResult | None = None
    persist_wait: bool = False


def preflight(connection: Connection, *, stage_identifier: str, now_utc_ns: int) -> Preflight:
    """Keep validation order and wait decisions together in one native handoff.

    The callback only reads. The caller persists eligible waits and performs fair
    arbitration and atomic reservations while retaining this writer transaction.
    """
    stage = connection.execute(
        "SELECT definition_json, state, execution, eligible_at_utc_ns, wait_kind, created_at_utc_ns, progress_waiting FROM work_stages WHERE id = ?",
        (stage_identifier,),
    ).fetchone()
    if stage is None:
        raise KeyError(stage_identifier)
    definition = StageDefinition.model_validate_json(_text(stage[0]))
    if stage[1] != "pending":
        return Preflight(
            stage,
            definition,
            wait=StageClaimResult(
                lease=None,
                wait_kind=StageWaitKind.OWNERSHIP,
                next_eligible_at_utc_ns=now_utc_ns + 100_000_000,
            ),
        )
    if stage[6] == 1:
        return Preflight(
            stage,
            definition,
            wait=StageClaimResult(lease=None, wait_kind=StageWaitKind.DEPENDENCY),
        )
    work = connection.execute(
        "SELECT state, eligible_at_utc_ns, kind_parts_json, payload_schema_version FROM work_items WHERE id = ?",
        (definition.work_item_identifier,),
    ).fetchone()
    assert work is not None
    if work[0] not in {"pending", "leased"}:
        return Preflight(
            stage,
            definition,
            wait=StageClaimResult(lease=None, wait_kind=StageWaitKind.OWNERSHIP),
        )
    eligible = max(definition.not_before_utc_ns, _integer(work[1]))
    if eligible > now_utc_ns:
        return Preflight(
            stage,
            definition,
            wait=StageClaimResult(
                lease=None,
                wait_kind=StageWaitKind.TIME,
                next_eligible_at_utc_ns=eligible,
            ),
            persist_wait=True,
        )
    dependency = connection.execute(
        """
        SELECT 1 FROM stage_dependencies AS dependency
        JOIN work_items AS work ON work.id = dependency.dependency_work_id
        WHERE dependency.stage_id = ?
          AND work.state NOT IN ('completed','terminal_failure','cancelled') LIMIT 1
        """,
        (stage_identifier,),
    ).fetchone()
    if dependency is not None:
        return Preflight(
            stage,
            definition,
            wait=StageClaimResult(
                lease=None,
                wait_kind=StageWaitKind.DEPENDENCY,
                next_eligible_at_utc_ns=now_utc_ns + 100_000_000,
            ),
            persist_wait=True,
        )
    network_stage = definition.network_activity is not None or any(
        request.name == "http" for request in definition.resources
    )
    if network_stage:
        connectivity = connection.execute(
            "SELECT paused, next_probe_utc_ns FROM network_connectivity WHERE singleton = 1"
        ).fetchone()
        if connectivity is not None and connectivity[0] == 1:
            return Preflight(
                stage,
                definition,
                wait=StageClaimResult(
                    lease=None,
                    wait_kind=StageWaitKind.CONNECTIVITY,
                    next_eligible_at_utc_ns=max(
                        now_utc_ns + 100_000_000, _integer(connectivity[1])
                    ),
                ),
                persist_wait=True,
            )
    return Preflight(stage, definition, work=work, network_stage=network_stage)
