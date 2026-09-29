"""The shipped examples must keep working."""

from __future__ import annotations

import shutil
from pathlib import Path

from .conftest import run_script, step_statuses

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"


def _summary(stdout: str) -> str:
    return next(line for line in stdout.splitlines() if line.startswith("summary:"))


def test_invoice_pipeline_replays_and_reruns_only_the_edited_step(workspace: Path) -> None:
    script = workspace / "invoice_pipeline.py"
    shutil.copy(EXAMPLES / "invoice_pipeline.py", script)
    env = {"FAKE_LLM_LATENCY": "0"}
    db = workspace / ".logram" / "logram.db"

    live = run_script(script, env=env)
    replay = run_script(script, replay=True, env=env)
    assert _summary(replay.stdout) == _summary(live.stdout)
    assert set(step_statuses(db).values()) == {"REPLAYED"}

    source = script.read_text()
    script.write_text(source.replace("in one sentence for an accountant", "in two sentences"))
    run_script(script, replay=True, env=env)
    assert step_statuses(db) == {"extract_page": "REPLAYED", "merge": "REPLAYED", "summarize": "SUCCESS"}
