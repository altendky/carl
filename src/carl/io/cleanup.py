"""Bounded resource cleanup that preserves the error being unwound.

Only resource finalizers belong here, not task-group or cancel-scope exits.
"""

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import anyio

_LOGGER = logging.getLogger(__name__)


def report_cleanup_failure(
    primary_error: BaseException | None, error: BaseException, phase: str
) -> None:
    """Report safe metadata, never provider exception text or credentials."""

    note = f"Carl cleanup failed during {phase}: {type(error).__name__}"
    _LOGGER.error("%s", note)
    if primary_error is not None:
        primary_error.add_note(note)


@asynccontextmanager
async def shielded_cleanup(
    phase: str,
    *,
    primary_error: BaseException | None,
    timeout_seconds: float = 10,
) -> AsyncIterator[None]:
    """Finish bounded cleanup; a secondary failure must not replace the primary."""

    try:
        with anyio.fail_after(timeout_seconds, shield=True):
            yield
    except Exception as error:
        report_cleanup_failure(primary_error, error, phase)
        if primary_error is None:
            raise
