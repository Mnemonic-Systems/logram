from __future__ import annotations

import importlib.util
import itertools
import os
import sqlite3
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import pytest

import logram
import logram.decorators as decorators
from logram.oracle import clear_oracle_cache, compute_logic_fingerprint, snapshot_digest
from logram.serializer import BlobManager
from logram.storage import TraceStorage

_LOGRAM_ENV_VARS = (
    "LOGRAM_REPLAY",
    "LOGRAM_FORCE_STEP",
    "LOGRAM_FORCE_FROM",
    "LOGRAM_DB_PATH",
    "LOGRAM_PROJECT_ROOT",
    "LOGRAM_INPUT_ID",
)

_module_counter = itertools.count()


@pytest.fixture(autouse=True)
def workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Run every test inside an isolated project directory with a fresh store."""
    for var in _LOGRAM_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    (tmp_path / "pyproject.toml").write_text("")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("LOGRAM_PROJECT_ROOT", str(tmp_path))

    fresh_storage = TraceStorage()
    monkeypatch.setattr(decorators, "storage", fresh_storage)
    monkeypatch.setattr(decorators, "blobs", BlobManager())
    monkeypatch.setattr(logram, "storage", fresh_storage)
    clear_oracle_cache()
    decorators.clear_logic_snapshot_cache()
    return tmp_path


def load_module(source: str, directory: Path | None = None, *, name: str | None = None) -> Any:
    """Import ``source`` as a fresh module backed by a real file (inspect.getsource needs one)."""
    name = name or f"_logram_test_mod_{next(_module_counter)}"
    path = (directory or Path.cwd()) / f"{name}.py"
    path.write_text(textwrap.dedent(source))
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def fingerprint(func: Any) -> str:
    clear_oracle_cache()
    snapshot, _ = compute_logic_fingerprint(func)
    return snapshot_digest(snapshot)


def fingerprint_of(source: str, name: str = "f") -> str:
    return fingerprint(getattr(load_module(source), name))


def run_script(
    script: Path,
    *,
    replay: bool = False,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a pipeline script in a separate interpreter, as a user would."""
    full_env = {k: v for k, v in os.environ.items() if k not in _LOGRAM_ENV_VARS}
    full_env["LOGRAM_PROJECT_ROOT"] = str(script.parent)
    if replay:
        full_env["LOGRAM_REPLAY"] = "true"
    full_env.update(env or {})
    proc = subprocess.run(
        [sys.executable, str(script)],
        cwd=script.parent,
        env=full_env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    return proc


def step_statuses(db_path: Path) -> dict[str, str]:
    """Return ``{step_name: status}`` for the most recent run in ``db_path``."""
    conn = sqlite3.connect(db_path)
    try:
        (run_id,) = conn.execute(
            "SELECT run_id FROM runs ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        rows = conn.execute(
            "SELECT name, status FROM steps WHERE run_id = ? AND parent_step_id IS NULL",
            (run_id,),
        ).fetchall()
    finally:
        conn.close()
    return dict(rows)
