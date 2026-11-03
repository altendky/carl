"""Every benchmark profile runs offline through genuine runtime ownership."""

# Inspect the private capture boundary only to forbid a paid executable fallback.
# pyright: reportPrivateUsage=false

import json
import sys
from pathlib import Path

import anyio
import httpx
import pytest
from pydantic import ValidationError

from carl.benchmark_cli import scheduler
from carl.core.benchmark import BenchmarkConfig, BenchmarkMode, BenchmarkScenario
from carl.core.json import encode_json
from carl.io.claude import ClaudeCli
from carl.io.scheduler_benchmark import run_scheduler_benchmark


@pytest.fixture(params=("trio", "asyncio"))
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return str(request.param)


@pytest.mark.anyio
@pytest.mark.parametrize("scenario", tuple(BenchmarkScenario))
async def test_profiles_are_offline_bounded_and_use_real_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scenario: BenchmarkScenario
) -> None:
    async def no_network(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("Benchmark attempted a real network connection")

    async def no_http(
        _transport: httpx.AsyncHTTPTransport, _request: httpx.Request
    ) -> httpx.Response:
        raise AssertionError("Benchmark bypassed its MockTransport")

    original = ClaudeCli._capture

    async def capture(
        self: ClaudeCli, argv: tuple[str, ...], directory: Path | None, timeout_seconds: int
    ):
        assert argv[:3] == (sys.executable, "-m", "carl.io.benchmark_analysis_child")
        return await original(self, argv, directory, timeout_seconds)

    monkeypatch.setattr(anyio, "connect_tcp", no_network)
    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", no_http)
    monkeypatch.setattr(ClaudeCli, "_capture", capture)
    monkeypatch.setenv("BENCHMARK_SECRET_SENTINEL", "do-not-print-this-secret")
    config = BenchmarkConfig(
        scenario=scenario,
        items=8 if scenario == BenchmarkScenario.REQUESTER_FAIRNESS else 2,
        image_width=16,
        image_height=16,
        chunk_delay_ms=0,
        analysis_delay_ms=0,
        history_rounds=2,
        timeout_seconds=30,
        capacities={"http": 2, "cpu": 2, "analysis": 1},
    )
    report = await run_scheduler_benchmark(
        config, repository_root=Path(__file__).resolve().parents[3], work_parent=tmp_path
    )
    result = report.iterations[0]
    assert report.workload == "synthetic_offline"
    assert result.elapsed_ns > 0 and result.setup_ns > 0 and result.cleanup_ns >= 0
    assert result.completed_per_second is None
    assert all(
        units <= report.capacities[name] for name, units in result.sampled_peak_held_units.items()
    )
    encoded = encode_json(report.model_dump(mode="json"))
    assert len(encoded.encode()) <= 262144
    assert str(tmp_path) not in encoded and "do-not-print-this-secret" not in encoded
    assert tuple(tmp_path.iterdir()) == ()
    if scenario == BenchmarkScenario.ANALYSIS_PRESSURE:
        assert result.analysis_invocations == config.items and result.completed == config.items
    elif scenario == BenchmarkScenario.CANCEL_CHECKPOINT_RESTART:
        assert result.analysis_invocations == 1 and result.cancelled == 1
        assert result.counters["database_reopens"] == 1
    elif scenario == BenchmarkScenario.IDLE_PROGRESS_ROOTS:
        assert result.synthetic_parent_executions == 0
    elif scenario == BenchmarkScenario.WATCH_HISTORY_FANOUT:
        assert result.counters["inactive_members_before"] >= config.items * config.history_rounds
        assert result.synthetic_parent_executions == config.items
        assert result.counters["timed_sqlite_vm_steps"] > 0
    elif scenario in {BenchmarkScenario.HTTP_IMAGES, BenchmarkScenario.CPU_DECODE_PRESSURE}:
        assert result.capture_invocations == config.items and result.completed == config.items


@pytest.mark.anyio
async def test_throughput_uses_real_elapsed_and_reports_completed_denominator(
    tmp_path: Path,
) -> None:
    config = BenchmarkConfig(
        scenario=BenchmarkScenario.HTTP_IMAGES,
        mode=BenchmarkMode.THROUGHPUT,
        items=2,
        image_width=16,
        image_height=16,
        chunk_delay_ms=0,
        capacities={"http": 1, "cpu": 1, "analysis": 1},
    )
    report = await run_scheduler_benchmark(
        config, repository_root=Path(__file__).resolve().parents[3], work_parent=tmp_path
    )
    result = report.iterations[0]
    assert result.completed_per_second == result.completed * 1_000_000_000 / result.elapsed_ns


@pytest.mark.parametrize("capacities", ({"secret": 1}, {"cpu": 0}, {"http": 257}))
def test_config_rejects_unknown_or_unbounded_capacities(capacities: dict[str, int]) -> None:
    with pytest.raises(ValidationError):
        _ = BenchmarkConfig(scenario=BenchmarkScenario.HTTP_IMAGES, capacities=capacities)


def test_cli_writes_safe_atomic_report_and_preserves_existing_output(tmp_path: Path) -> None:
    output = tmp_path / "report.json"
    scheduler(
        BenchmarkScenario.HTTP_IMAGES,
        items=2,
        image_width=8,
        image_height=8,
        chunk_delay_ms=0,
        cpu=1,
        http=1,
        analysis=1,
        output=output,
        work_parent=tmp_path / "runs",
    )
    raw = output.read_text()
    assert json.loads(raw)["workload"] == "synthetic_offline"
    assert str(tmp_path) not in raw
    with pytest.raises(FileExistsError):
        scheduler(BenchmarkScenario.HTTP_IMAGES, output=output)
    assert output.read_text() == raw
