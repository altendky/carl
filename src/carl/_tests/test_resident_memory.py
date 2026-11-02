"""Session accounting includes private data lengths without returning values."""

import httpx
import pytest

from carl.io.httpx import close_httpx_client
from carl.io.resident_memory import httpx_retained_bytes


@pytest.mark.anyio
async def test_httpx_cookie_accounting_includes_scopes_and_per_entry_overhead() -> None:
    async with httpx.AsyncClient() as client:
        baseline = httpx_retained_bytes(client)
        client.cookies.set("private_cookie", "private_value", domain="example.com", path="/a")
        first = httpx_retained_bytes(client)
        assert first - baseline == 1024 + sum(
            map(len, ("private_cookie", "private_value", "example.com", "/a"))
        )
        client.cookies.set("private_cookie", "private_value", domain="example.com", path="/b")
        assert httpx_retained_bytes(client) - first == first - baseline
        client.cookies.clear()
        assert httpx_retained_bytes(client) == baseline


@pytest.mark.anyio
async def test_httpx_early_closed_flag_is_not_physical_cleanup_proof() -> None:
    class _BrokenClose(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, request=request)

        async def aclose(self) -> None:
            raise RuntimeError("Transport is still owned")

    client = httpx.AsyncClient(transport=_BrokenClose())
    closed = await close_httpx_client(client, primary_error=ValueError("Original failure"))
    assert client.is_closed
    assert closed is False
