"""Safety gates of the MCP tools that execute user scripts."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

pytest.importorskip("fastmcp")

from logram import mcp_server  # noqa: E402

from .conftest import run_script  # noqa: E402


@pytest.fixture(autouse=True)
def fresh_budget() -> None:
    mcp_server._reset_replay_counter()


def _write(path: Path, source: str) -> Path:
    path.write_text(textwrap.dedent(source))
    return path


FLAKY = """
import asyncio, os
import logram

@logram.trace()
def flaky():
    if os.environ.get("FAIL") == "1":
        raise RuntimeError("boom")
    return "ok"

async def main():
    logram.init(project="demo", input_id="doc")
    try:
        flaky()
        await logram.finalize(status="success")
    except RuntimeError:
        await logram.finalize(status="failed")

asyncio.run(main())
"""


def test_verify_against_golden_dataset_enforces_path_jail(workspace: Path, tmp_path_factory) -> None:
    outside = _write(tmp_path_factory.mktemp("outside") / "evil.py", "print('should not run')\n")
    report = mcp_server.verify_against_golden_dataset("demo", str(outside))
    assert "Path Jail" in report


def test_forcing_a_step_whose_last_run_failed_is_refused_without_spending_budget(workspace: Path) -> None:
    script = _write(workspace / "flaky.py", FLAKY)
    run_script(script, env={"FAIL": "1"})

    report = mcp_server.run_surgical_replay("flaky.py", force_step="flaky")
    assert "Aborted" in report
    assert mcp_server._replay_counter == 0


def test_forcing_a_step_whose_last_run_succeeded_is_allowed(workspace: Path) -> None:
    script = _write(workspace / "flaky.py", FLAKY)
    run_script(script, env={"FAIL": "1"})
    run_script(script)

    report = mcp_server.run_surgical_replay("flaky.py", force_step="flaky")
    assert "Replay: SUCCESS" in report


def test_replay_that_hangs_is_stopped(workspace: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write(workspace / "hang.py", "import time\ntime.sleep(30)\n")
    monkeypatch.setattr(mcp_server, "SCRIPT_TIMEOUT_SEC", 1)
    report = mcp_server.run_surgical_replay("hang.py")
    assert "timed out" in report.lower()
