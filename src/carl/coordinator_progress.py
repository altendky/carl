"""Group subscriptions with explicit fallbacks for retained consumer edges."""

from collections.abc import Sequence
from typing import Protocol

from carl.core.work_progress import WorkProgressWatch


class ConsumerProgressDatabase(Protocol):
    async def missing_consumer_edges(
        self,
        parent_work_identifier: str,
        requester_kind: tuple[str, ...],
        child_ids: tuple[str, ...],
    ) -> tuple[str, ...]: ...


async def consumer_progress_watch(
    database: ConsumerProgressDatabase,
    *,
    parent_work_identifier: str,
    requester_kind: tuple[str, ...],
    work_identifiers: Sequence[str],
) -> WorkProgressWatch:
    missing: list[str] = []
    for offset in range(0, len(work_identifiers), 1000):
        missing.extend(
            await database.missing_consumer_edges(
                parent_work_identifier,
                requester_kind,
                tuple(work_identifiers[offset : offset + 1000]),
            )
        )
    return WorkProgressWatch(
        consumer_requester_kinds=(requester_kind,),
        work_identifiers=tuple(dict.fromkeys(missing)),
    )
