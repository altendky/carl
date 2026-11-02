"""Bound retained image integrity checks under shared CPU admission."""

from carl.core.content_encoding import MAX_DECODED_BYTES
from carl.io.resource_permits import resource_operation
from carl.io.sqlite import Database


@resource_operation("cpu")
async def image_artifact_available(database: Database, identifier: str) -> bool:
    """Reuse only intact image bytes within the existing decoded-image limit."""

    try:
        _ = await database.get_artifact(identifier, maximum_bytes=MAX_DECODED_BYTES)
    except (KeyError, ValueError, OSError):
        return False
    return True
