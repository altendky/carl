"""Large saved images remain renderable MCP image blocks across both call paths."""

import base64
import hashlib
from io import BytesIO
from pathlib import Path

import pytest
from mcp import Client
from mcp.types import CallToolResult, ImageContent
from PIL import Image

from carl.io.mcp_response_budget import (
    HARD_RESPONSE_BYTES,
    BudgetedMCPServer,
    response_bytes,
)
from carl.io.sqlite import Database
from carl.mcp_server import build_server
from carl.review import ResolvedImage, ReviewApplication


def _large_png() -> bytes:
    pixels = bytes(range(256)) * 192
    image = Image.frombytes("RGB", (128, 128), pixels)
    output = BytesIO()
    image.save(output, format="PNG", compress_level=0)
    content = output.getvalue()
    assert len(content) > HARD_RESPONSE_BYTES
    return content


@pytest.mark.anyio
@pytest.mark.parametrize("protocol", (False, True), ids=("direct-sdk", "mcp-client"))
async def test_large_listing_image_preserves_native_image_and_exact_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, protocol: bool
) -> None:
    content = _large_png()
    artifact = "retained-listing-image"
    digest = hashlib.sha256(content).hexdigest()
    requested: list[str] = []

    async def get_image(_application: ReviewApplication, artifact_identifier: str) -> ResolvedImage:
        requested.append(artifact_identifier)
        return ResolvedImage(
            artifact_identifier=artifact,
            media_type="image/png",
            sha256=digest,
            content=content,
        )

    monkeypatch.setattr(ReviewApplication, "get_image", get_image)
    async with Database.managed(tmp_path / "carl.sqlite3", initialize=True) as database:
        server = build_server(ReviewApplication(database, tmp_path))
        assert isinstance(server, BudgetedMCPServer)
        original_meta = dict(server.response_store.result_meta)
        arguments = {"artifact_identifier": artifact}
        if protocol:
            async with Client(server) as client:
                result = await client.call_tool("get_listing_image", arguments)
        else:
            result = await server.call_tool("get_listing_image", arguments)

        assert isinstance(result, CallToolResult)
        assert not result.is_error
        assert result.structured_content is None
        assert len(result.content) == 1
        image = result.content[0]
        assert isinstance(image, ImageContent)
        assert image.mime_type == "image/png"
        assert image.meta == {"carl/artifactIdentifier": artifact, "carl/sha256": digest}
        returned = base64.b64decode(image.data, validate=True)
        assert returned == content
        assert hashlib.sha256(returned).hexdigest() == digest
        with Image.open(BytesIO(returned)) as decoded:
            _ = decoded.load()
            assert decoded.format == "PNG" and decoded.size == (128, 128)
            assert decoded.tobytes() == bytes(range(256)) * 192
        assert response_bytes(result) > HARD_RESPONSE_BYTES
        assert result.meta == {
            **original_meta,
            "carl/response": {"response_bytes": response_bytes(result), "has_more": False},
        }
        assert server.response_store.retained_bytes == 0
        assert server.response_store.pending_cursors == ()
        assert requested == [artifact]
