"""The soundness benchmark doubles as a regression test for the fingerprint."""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path

import pytest

BENCHMARK = Path(__file__).resolve().parent.parent / "benchmarks" / "mutation_soundness.py"


def test_mutation_benchmark_finds_no_false_hit(workspace: Path) -> None:
    spec = importlib.util.spec_from_file_location("mutation_soundness", BENCHMARK)
    assert spec is not None and spec.loader is not None
    benchmark = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(benchmark)

    logging.disable(logging.WARNING)
    try:
        results = benchmark.run()
    finally:
        logging.disable(logging.NOTSET)

    assert results["false_hits"] == []
    invalidated = {k: n for k, n in results["cosmetic"].items() if k.endswith("|invalidated")}
    assert invalidated == {}
    changed = sum(n for k, n in results["semantic"].items() if k.split("|")[1] in ("correct_miss", "false_hit"))
    assert changed > 100, "the corpus no longer exercises enough behaviour changes"


@pytest.fixture(autouse=True)
def _restore_cwd(monkeypatch: pytest.MonkeyPatch, workspace: Path) -> None:
    # benchmark.run() changes the working directory; monkeypatch restores it.
    monkeypatch.chdir(workspace)
