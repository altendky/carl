"""Explicit offline backend for real owned-native-process lifetime tests."""

import os
import time
from dataclasses import dataclass
from pathlib import Path

from carl.core.native_routing import NativeRoutingDataset, NativeRoutingMetric, NativeRoutingQuery


@dataclass
class FakeBackend:
    root: Path
    count: int = 0

    def query(self, query: NativeRoutingQuery) -> tuple[NativeRoutingMetric, ...]:
        self.count += 1
        _ = (self.root / "query-started").write_text(f"{os.getpid()} {self.count}")
        if (self.root / "exit-query").exists():
            os._exit(91)
        while (self.root / "block-query").exists() and not (self.root / "proceed").exists():
            time.sleep(0.01)
        return ((float(os.getpid()), float(self.count)),) * len(query.destinations)


def create_backend(dataset: NativeRoutingDataset) -> FakeBackend:
    root = dataset.dataset_path.parent
    _ = (root / "native.pid").write_text(str(os.getpid()))
    _ = (root / "initialize-started").write_text("started")
    while (root / "block-initialize").exists() and not (root / "proceed").exists():
        time.sleep(0.01)
    return FakeBackend(root)
