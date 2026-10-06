from collections.abc import AsyncGenerator

import anyio
import httpx
import pytest

from carl.core.http import RequestPlan
from carl.core.routing import LocalSocks5Endpoint
from carl.io.httpx import AcquisitionFailure, ClientHttpxAcquirer, close_httpx_client
from carl.io.proton import ManagedProtonTransportFailure, _probe_exit_ip


@pytest.mark.anyio
async def test_cookie_cleanup_failure_still_closes_transport() -> None:
    closed = False
    original = RuntimeError("cookie cleanup failed")

    class Cookies(httpx.Cookies):
        def clear(self, *args: object, **kwargs: object) -> None:
            raise original

    class Client(httpx.AsyncClient):
        async def aclose(self) -> None:
            nonlocal closed
            await anyio.lowlevel.checkpoint()
            await super().aclose()
            closed = True
            raise OSError("secondary close failure")

    client = Client(transport=httpx.MockTransport(lambda _: httpx.Response(200)))
    client._cookies = Cookies()
    with pytest.raises(RuntimeError) as raised:
        await close_httpx_client(client)
    assert raised.value is original
    assert closed
    assert original.__notes__ == ["Carl cleanup failed during http_client_close: OSError"]


@pytest.mark.anyio
@pytest.mark.parametrize("ending", ("body_limit", "cancel"))
async def test_response_cleanup_failure_preserves_acquisition_or_cancellation(ending: str) -> None:
    closes = 0
    scope: anyio.CancelScope

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncGenerator[bytes]:
            if ending == "cancel":
                scope.cancel()
                await anyio.sleep_forever()
            yield b"too many bytes"

        async def aclose(self) -> None:
            nonlocal closes
            await anyio.lowlevel.checkpoint()
            closes += 1
            raise OSError("secret response close diagnostic")

    transport = httpx.MockTransport(lambda _: httpx.Response(200, stream=Stream()))
    async with httpx.AsyncClient(transport=transport) as client:
        acquirer = ClientHttpxAcquirer(
            client=client,
            expected_routing=("direct",),
            routing_observation=None,
            authentication="anonymous",
        )
        with anyio.CancelScope() as scope:
            if ending == "cancel":
                await acquirer.acquire(RequestPlan(url="https://example.test/"), lambda: "body")
            else:
                with pytest.raises(AcquisitionFailure) as raised:
                    await acquirer.acquire(
                        RequestPlan(url="https://example.test/", max_body_bytes=1), lambda: "body"
                    )
                assert raised.value.result["stopping_condition"] == "body_size_limit"
                assert "secret response close diagnostic" not in repr(raised.value.__notes__)
        assert scope.cancelled_caught is (ending == "cancel")
    assert closes == 1


@pytest.mark.anyio
async def test_proton_probe_response_cleanup_failure_still_closes_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    closed = False

    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncGenerator[bytes]:
            raise ValueError("body failed")
            yield b""

        async def aclose(self) -> None:
            await anyio.lowlevel.checkpoint()
            raise OSError("secret response close failure")

    class Client(httpx.AsyncClient):
        async def aclose(self) -> None:
            nonlocal closed
            await super().aclose()
            closed = True

    client = Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=Stream())))
    monkeypatch.setattr("carl.io.proton.httpx.AsyncClient", lambda **_: client)
    with pytest.raises(ManagedProtonTransportFailure) as raised:
        await _probe_exit_ip(LocalSocks5Endpoint(port=31080), timeout_seconds=2)
    assert raised.value.code == "proton_egress_probe_failed"
    assert closed
    assert "secret response close failure" not in repr(raised.value.__notes__)
