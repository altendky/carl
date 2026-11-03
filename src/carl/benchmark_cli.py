"""Explicit offline scheduler benchmark commands."""

import os
import tempfile
from pathlib import Path

import anyio
from cyclopts import App

from carl.core.benchmark import BenchmarkConfig, BenchmarkMode, BenchmarkScenario
from carl.core.json import encode_json
from carl.io.scheduler_benchmark import run_scheduler_benchmark

benchmark_app = App(name="benchmark", help="Run synthetic offline scheduler workloads")


@benchmark_app.command
def scheduler(
    scenario: BenchmarkScenario,
    *,
    mode: BenchmarkMode = BenchmarkMode.VERIFY,
    seed: int = 1,
    items: int = 32,
    repetitions: int = 1,
    warmups: int = 0,
    image_width: int = 1024,
    image_height: int = 1024,
    chunk_delay_ms: int = 1,
    analysis_delay_ms: int = 50,
    history_rounds: int = 8,
    timeout_seconds: int = 120,
    cpu: int | None = None,
    http: int | None = None,
    analysis: int | None = None,
    output: Path | None = None,
    work_parent: Path | None = None,
) -> None:
    """Measure a private synthetic database; never contacts a provider or paid CLI."""
    if output is not None and output.exists():
        raise FileExistsError("Benchmark output already exists")
    capacities = {
        key: value
        for key, value in (("cpu", cpu), ("http", http), ("analysis", analysis))
        if value is not None
    }
    config = BenchmarkConfig(
        scenario=scenario,
        mode=mode,
        seed=seed,
        items=items,
        repetitions=repetitions,
        warmups=warmups,
        image_width=image_width,
        image_height=image_height,
        chunk_delay_ms=chunk_delay_ms,
        analysis_delay_ms=analysis_delay_ms,
        history_rounds=history_rounds,
        timeout_seconds=timeout_seconds,
        capacities=capacities,
    )

    async def perform() -> str:
        report = await run_scheduler_benchmark(
            config, repository_root=Path.cwd(), work_parent=work_parent
        )
        return encode_json(report.model_dump(mode="json"), indent=2)

    result = anyio.run(perform, backend="trio")
    if output is None:
        print(result)
        return
    # Publish a new report atomically without overwriting another run.
    descriptor, temporary = tempfile.mkstemp(prefix=".carl-benchmark-", dir=output.parent)
    path = Path(temporary)
    try:
        with os.fdopen(descriptor, "w") as stream:
            _ = stream.write(result + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.link(path, output)
    finally:
        path.unlink(missing_ok=True)
