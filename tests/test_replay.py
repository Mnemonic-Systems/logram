"""End-to-end record/replay, each run in its own interpreter as a user would do it."""

from __future__ import annotations

import sqlite3
import subprocess
import sys
import textwrap
from pathlib import Path

from .conftest import run_script, step_statuses

PIPELINE = """
import asyncio
import os
import sys

import logram

PROMPTS = [f"prompt {{i}}" for i in range(20)]
{mutation}


@logram.trace()
def pick(i):
    print("EXECUTED pick", file=sys.stderr)
    return PROMPTS[i]


@logram.trace()
def shout(text):
    print("EXECUTED shout", file=sys.stderr)
    return text.upper()


async def main():
    logram.init(project="demo", input_id="doc-1")
    print("RESULT", shout(pick(19)))
    await logram.finalize(status="success")


asyncio.run(main())
"""


def write_pipeline(directory: Path, mutation: str = "") -> Path:
    script = directory / "pipeline.py"
    script.write_text(textwrap.dedent(PIPELINE.format(mutation=mutation)))
    return script


def result_line(proc: subprocess.CompletedProcess[str]) -> str:
    return next(line for line in proc.stdout.splitlines() if line.startswith("RESULT"))


def test_unchanged_pipeline_replays_every_step(workspace: Path) -> None:
    script = write_pipeline(workspace)
    live = run_script(script)
    replay = run_script(script, replay=True)

    assert result_line(replay) == result_line(live) == "RESULT PROMPT 19"
    assert "EXECUTED" not in replay.stderr
    assert step_statuses(workspace / ".logram" / "logram.db") == {
        "pick": "REPLAYED",
        "shout": "REPLAYED",
    }


def test_changed_global_beyond_preview_is_not_replayed(workspace: Path) -> None:
    script = write_pipeline(workspace)
    run_script(script)

    write_pipeline(workspace, mutation='PROMPTS[19] = "edited"')
    replay = run_script(script, replay=True)

    assert result_line(replay) == "RESULT EDITED"
    assert step_statuses(workspace / ".logram" / "logram.db") == {
        "pick": "SUCCESS",
        "shout": "SUCCESS",
    }


def test_failed_step_is_never_replayed(workspace: Path) -> None:
    script = workspace / "flaky.py"
    script.write_text(
        textwrap.dedent(
            """
            import asyncio, os, sys
            import logram

            @logram.trace()
            def flaky():
                print("EXECUTED flaky", file=sys.stderr)
                if os.environ.get("FAIL") == "1":
                    raise RuntimeError("boom")
                return "ok"

            async def main():
                logram.init(project="demo", input_id="doc-1")
                try:
                    print("RESULT", flaky())
                    await logram.finalize(status="success")
                except RuntimeError:
                    print("RESULT failed")
                    await logram.finalize(status="failed")

            asyncio.run(main())
            """
        )
    )
    first = run_script(script, env={"FAIL": "1"})
    assert result_line(first) == "RESULT failed"

    second = run_script(script, replay=True)
    assert result_line(second) == "RESULT ok"
    assert "EXECUTED flaky" in second.stderr


def test_pydantic_output_is_rehydrated(workspace: Path) -> None:
    (workspace / "models.py").write_text(
        "from pydantic import BaseModel\n\nclass Invoice(BaseModel):\n    total: int\n"
    )
    script = workspace / "typed.py"
    script.write_text(
        textwrap.dedent(
            """
            import asyncio
            import logram
            from models import Invoice

            @logram.trace()
            def extract() -> Invoice:
                return Invoice(total=42)

            async def main():
                logram.init(project="demo", input_id="doc-1")
                out = extract()
                print("RESULT", type(out).__name__, out.total)
                await logram.finalize(status="success")

            asyncio.run(main())
            """
        )
    )
    run_script(script)
    replay = run_script(script, replay=True)
    assert result_line(replay) == "RESULT Invoice 42"


def test_db_path_environment_variable_is_honoured(workspace: Path, tmp_path_factory) -> None:
    db_path = tmp_path_factory.mktemp("elsewhere") / "custom.db"
    script = write_pipeline(workspace)
    run_script(script, env={"LOGRAM_DB_PATH": str(db_path)})

    assert db_path.exists()
    assert not (workspace / ".logram" / "logram.db").exists()
    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM steps").fetchone()[0] == 2
    finally:
        conn.close()


def test_import_has_no_filesystem_side_effects(workspace: Path) -> None:
    subprocess.run(
        [sys.executable, "-c", "import logram"],
        cwd=workspace,
        env={"LOGRAM_PROJECT_ROOT": str(workspace), "PATH": ""},
        check=True,
    )
    assert not (workspace / ".logram").exists()
    assert not (workspace / ".logram_assets").exists()
