from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path

import pytest
from typer.testing import CliRunner

import logram
import logram.cli as cli


@pytest.fixture
def cli_paths(workspace: Path) -> Path:
    # The CLI resolves the store from LOGRAM_PROJECT_ROOT, set by ``workspace``.
    return workspace


def _record_run_with_blobs() -> None:
    @logram.stateful(include=["cache"])
    class Pipeline:
        def __init__(self) -> None:
            self.cache: bytes | None = None

        @logram.trace()
        def run(self, image: bytes) -> bytes:
            self.cache = b"state bytes"
            return b"output bytes"

    async def main() -> None:
        logram.init(project="blobs", input_id="doc")
        Pipeline().run(b"input bytes")
        await logram.finalize(status="success")

    asyncio.run(main())


def _blob_file(assets: Path, payload: bytes) -> Path:
    return assets / f"{hashlib.sha256(payload).hexdigest()}.bin"


def test_clean_only_deletes_unreferenced_blobs(cli_paths: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _record_run_with_blobs()
    assets = cli_paths / ".logram_assets"
    referenced = [_blob_file(assets, p) for p in (b"input bytes", b"output bytes", b"state bytes")]
    assert all(p.exists() for p in referenced)

    orphan = _blob_file(assets, b"nobody points at me")
    orphan.write_bytes(b"nobody points at me")
    unrelated = assets / "notes.txt"
    unrelated.write_text("not a blob")

    # Run from a subdirectory: references must still resolve against the project root.
    subdir = cli_paths / "src"
    subdir.mkdir()
    monkeypatch.chdir(subdir)
    result = CliRunner().invoke(cli.app, ["clean"], input="y\n")

    assert result.exit_code == 0, result.output
    assert all(p.exists() for p in referenced)
    assert not orphan.exists()
    assert unrelated.exists()


GOLDEN_PIPELINE = """
import asyncio
import os

import logram


@logram.trace()
def score(x):
    return {body}


async def main():
    logram.init(project="golden", input_id=os.environ.get("LOGRAM_INPUT_ID", "doc-a"))
    print([score(x) for x in range(3)])
    await logram.finalize(status="success")


asyncio.run(main())
"""


def _golden_run_ids(db: Path) -> list[str]:
    import sqlite3

    conn = sqlite3.connect(db)
    try:
        return [r[0] for r in conn.execute("SELECT run_id FROM runs ORDER BY created_at")]
    finally:
        conn.close()


def test_golden_test_detects_regression_in_any_call_and_fails(cli_paths: Path) -> None:
    from .conftest import run_script

    script = cli_paths / "pipeline.py"
    script.write_text(GOLDEN_PIPELINE.format(body="x * 10"))
    run_script(script)
    run_script(script, env={"LOGRAM_INPUT_ID": "doc-b"})
    runner = CliRunner()
    for run_id in _golden_run_ids(cli_paths / ".logram" / "logram.db"):
        assert runner.invoke(cli.app, ["golden", "add", run_id]).exit_code == 0

    unchanged = runner.invoke(cli.app, ["test", str(script)])
    assert unchanged.exit_code == 0, unchanged.output
    assert unchanged.output.count("no regression") == 2

    # Only the first of three calls changes: every call must be compared.
    script.write_text(GOLDEN_PIPELINE.format(body="-1 if x == 0 else x * 10"))
    regressed = runner.invoke(cli.app, ["test", str(script)])
    assert regressed.exit_code == 1, regressed.output
    assert regressed.output.count("1 step(s) differ") == 2


def _record_nested_run(project: str = "nested") -> None:
    import time

    @logram.trace()
    def inner() -> int:
        time.sleep(0.1)
        return 1

    @logram.trace()
    def outer() -> int:
        return inner()

    async def main() -> None:
        logram.init(project=project, input_id="doc")
        outer()
        await logram.finalize(status="success")

    asyncio.run(main())


def test_inspect_accepts_negative_run_offsets(cli_paths: Path) -> None:
    _record_nested_run("first")
    _record_nested_run("second")
    runner = CliRunner()
    latest = runner.invoke(cli.app, ["inspect", "-1"])
    previous = runner.invoke(cli.app, ["inspect", "-2"])
    assert latest.exit_code == 0, latest.output
    assert previous.exit_code == 0, previous.output
    assert "second" in latest.output
    assert "first" in previous.output


def test_inspect_total_counts_nested_steps_once(cli_paths: Path) -> None:
    import re

    _record_nested_run()
    result = CliRunner().invoke(cli.app, ["inspect", "last"])
    assert result.exit_code == 0, result.output
    total = float(re.search(r"Total: ([0-9.]+)s", result.output).group(1))
    assert 0.09 <= total < 0.18, result.output
