"""The shared resource-finalizer contract under cancellation and secondary errors."""

import logging

import anyio
import pytest

from carl.io.cleanup import shielded_cleanup


@pytest.mark.anyio
async def test_resource_cleanup_finishes_inside_a_cancelled_scope() -> None:
    closed = False
    with anyio.CancelScope() as scope:
        scope.cancel()
        async with shielded_cleanup("fixture_close", primary_error=None):
            await anyio.lowlevel.checkpoint()
            closed = True
    assert closed


@pytest.mark.anyio
async def test_cleanup_error_preserves_primary_and_logs_only_safe_metadata(
    caplog: pytest.LogCaptureFixture,
) -> None:
    primary = ValueError("original fixture failure")
    with (
        caplog.at_level(logging.ERROR, logger="carl.io.cleanup"),
        pytest.raises(ValueError) as caught,
    ):
        try:
            raise primary
        finally:
            async with shielded_cleanup("fixture_close", primary_error=primary):
                raise RuntimeError("secret fixture value")
    assert caught.value is primary
    assert any("fixture_close: RuntimeError" in note for note in primary.__notes__)
    assert "RuntimeError" in caplog.text
    assert "secret fixture value" not in caplog.text


@pytest.mark.anyio
async def test_cleanup_error_without_primary_is_not_suppressed() -> None:
    failure = RuntimeError("fixture close failure")
    with pytest.raises(RuntimeError) as caught:
        async with shielded_cleanup("fixture_close", primary_error=None):
            raise failure
    assert caught.value is failure


@pytest.mark.anyio
async def test_cleanup_timeout_is_reported_without_replacing_primary(
    caplog: pytest.LogCaptureFixture,
) -> None:
    primary = ValueError("original fixture failure")
    with caplog.at_level(logging.ERROR, logger="carl.io.cleanup"):
        async with shielded_cleanup("fixture_close", primary_error=primary, timeout_seconds=0):
            await anyio.sleep_forever()
    assert any("fixture_close: TimeoutError" in note for note in primary.__notes__)
    assert "TimeoutError" in caplog.text
