"""What does @trace cost?

Measures, in a throwaway project directory:

* per-call overhead of a traced function in live mode (fingerprint already
  memoized for the run; SQLite writes happen on a background thread),
* per-call latency of a replayed step (cache lookup + rehydration),
* cold fingerprint time of the functions in ``corpus.py``,
* cache-key cost for arguments of increasing size.

    python benchmarks/overhead.py
"""

from __future__ import annotations

import asyncio
import importlib.util
import logging
import os
import platform
import sqlite3
import statistics
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

CORPUS = Path(__file__).with_name("corpus.py")


def _median_us(fn: Callable[[], Any], repeat: int) -> float:
    samples = []
    for _ in range(repeat):
        start = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - start)
    return statistics.median(samples) * 1e6


def main() -> None:
    workdir = Path(tempfile.mkdtemp(prefix="logram_overhead_"))
    (workdir / "pyproject.toml").write_text("")
    os.chdir(workdir)
    os.environ["LOGRAM_PROJECT_ROOT"] = str(workdir)
    os.environ.pop("LOGRAM_REPLAY", None)
    logging.disable(logging.WARNING)

    import logram
    from logram.decorators import _logical_args_for_vcr, clear_logic_snapshot_cache
    from logram.oracle import clear_oracle_cache, compute_logic_fingerprint

    def plain(x: int) -> int:
        return x + 1

    traced = logram.trace(name="traced_step")(plain)
    n = 2000

    async def record() -> None:
        logram.init(project="overhead", input_id="doc")
        for i in range(n):
            traced(i)
        await logram.finalize(status="success")

    rows: list[tuple[str, str]] = []

    baseline = _median_us(lambda: plain(1), n)
    logram.init(project="overhead", input_id="doc")
    traced(0)  # fingerprint once, as a real run would
    counter = iter(range(1, 10**9))
    live = _median_us(lambda: traced(next(counter)), n)
    asyncio.run(logram.flush())
    rows.append(("untraced call", f"{baseline:.2f} µs"))
    rows.append(("traced call, live (per call)", f"{live:.0f} µs"))

    asyncio.run(record())
    os.environ["LOGRAM_REPLAY"] = "true"
    logram.init(project="overhead", input_id="doc")
    replay_counter = iter(range(n))
    replay = _median_us(lambda: traced(next(replay_counter)), n - 1)
    asyncio.run(logram.flush())
    os.environ.pop("LOGRAM_REPLAY")
    conn = sqlite3.connect(workdir / ".logram" / "logram.db")
    (replayed,) = conn.execute(
        "SELECT COUNT(*) FROM steps WHERE status = 'REPLAYED' AND run_id = "
        "(SELECT run_id FROM runs ORDER BY created_at DESC LIMIT 1)"
    ).fetchone()
    conn.close()
    assert replayed == n - 1, f"expected {n - 1} cache hits, got {replayed}"
    rows.append(("traced call, replayed (per call)", f"{replay:.0f} µs"))

    spec = importlib.util.spec_from_file_location("corpus", CORPUS)
    assert spec is not None and spec.loader is not None
    corpus = importlib.util.module_from_spec(spec)
    sys.modules["corpus"] = corpus
    spec.loader.exec_module(corpus)
    steps = [getattr(corpus, name) for name in corpus.STEPS]

    def fingerprint_all() -> None:
        clear_oracle_cache()
        clear_logic_snapshot_cache()
        for step in steps:
            compute_logic_fingerprint(step)

    cold = _median_us(fingerprint_all, 20) / len(steps)
    rows.append((f"cold fingerprint, per step ({len(steps)} corpus steps)", f"{cold / 1000:.2f} ms"))

    def step(payload: Any) -> Any:
        return payload

    payloads = {
        "cache key: small dict": {"doc": "d1", "page": 3, "prompt": "extract totals"},
        "cache key: 10 KB prompt": "x" * 10_000,
        "cache key: list of 100 dicts": [{"id": i, "text": f"line {i}", "bbox": [i, i, i + 1, i + 1]} for i in range(100)],
        "cache key: list of 10k dicts": [{"id": i, "text": f"line {i}", "bbox": [i, i, i + 1, i + 1]} for i in range(10_000)],
        "cache key: 5 MB of bytes": b"\x01" * 5_000_000,
    }
    for label, payload in payloads.items():
        repeat = 10 if "10k" in label else 200
        us = _median_us(lambda p=payload: _logical_args_for_vcr(step, (p,), {}), repeat)
        rows.append((label, f"{us / 1000:.2f} ms" if us >= 1000 else f"{us:.0f} µs"))

    print(f"Python {platform.python_version()} · {platform.machine()} · {platform.system()}\n")
    print("| measurement | median |")
    print("|---|---|")
    for label, value in rows:
        print(f"| {label} | {value} |")


if __name__ == "__main__":
    main()
