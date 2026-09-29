# ruff: noqa: BLE001
# pylint: disable=broad-exception-caught

from __future__ import annotations

import difflib
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import typer
from rich.panel import Panel
from rich.progress_bar import ProgressBar
from rich.syntax import Syntax
from rich.text import Text

from ..serializer import _detect_project_root, resolve_assets_dir
from ..storage import resolve_db_path
from ..theme import (
    PANEL_BOX,
    console,
)

# The ``logram`` package directory (templates live next to it).
_PACKAGE_DIR = Path(__file__).resolve().parent.parent


def _store_path() -> Path:
    """Trace store location, resolved on every call (LOGRAM_DB_PATH or project root)."""
    return resolve_db_path()


def _assets_path() -> Path:
    return resolve_assets_dir()

_BLOB_FILE_RE = re.compile(r"^[0-9a-f]{64}\.[A-Za-z0-9]+$")

@dataclass(slots=True)
class StepRecord:
    step_id: str
    run_id: str
    parent_step_id: str | None
    name: str
    status: str
    duration: float
    timestamp: float
    logic_hash: str | None
    inputs: Any
    output: Any
    error: Any

def _educational_db_missing_message() -> None:
    console.print()
    console.print(
        Panel(
            Text.assemble(
                ("No Logram database found.\n\n", "bold"),
                ("Run an instrumented pipeline with ", "lg.muted"),
                ("logram.trace", "lg.brand"),
                (" first, then retry.\n\n", "lg.muted"),
                ("Expected: ", "lg.muted"),
                (".logram/logram.db", "lg.brand"),
                ("  ·  assets: ", "lg.muted"),
                (".logram_assets/", "lg.brand"),
            ),
            box=PANEL_BOX,
            border_style="lg.muted",
            padding=(1, 2),
        )
    )
    console.print()

def _connect_db(require_exists: bool = True) -> sqlite3.Connection | None:
    if require_exists and not _store_path().exists():
        _educational_db_missing_message()
        return None

    _store_path().parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(_store_path())
    conn.row_factory = sqlite3.Row
    return conn

def _parse_json(value: str | None) -> Any:
    if value is None:
        return None
    try:
        return json.loads(value)
    except Exception:
        return value

def _format_dt(ts: float | int | None) -> str:
    if not ts:
        return "-"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(ts)))

def _relative_time(ts: float | int | None) -> str:
    if not ts:
        return "-"
    diff = time.time() - float(ts)
    if diff < 60:
        return f"{int(diff)}s ago"
    if diff < 3600:
        return f"{int(diff / 60)}m ago"
    if diff < 86400:
        return f"{int(diff / 3600)}h ago"
    if diff < 604800:
        return f"{int(diff / 86400)}d ago"
    return time.strftime("%Y-%m-%d", time.localtime(float(ts)))

def _format_duration(seconds: float | int | None) -> str:
    if not seconds:
        return "0.000s"
    return f"{float(seconds):.3f}s"

def _format_human_duration(seconds: float | int | None) -> str:
    total = max(0, int(round(float(seconds or 0.0))))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h}h {m}m {s}s"
    if m > 0:
        return f"{m}m {s}s"
    return f"{s}s"

def _format_currency(amount: float) -> str:
    return f"{amount:,.2f} €".replace(",", " ")

def _ratio_progress_row(label: str, ratio: float, *, color: str) -> tuple[str, ProgressBar, str]:
    bounded = max(0.0, min(1.0, ratio))
    return (
        label,
        ProgressBar(
            total=100,
            completed=bounded * 100.0,
            width=38,
            complete_style=color,
            finished_style=color,
            pulse_style=color,
        ),
        f"{bounded * 100.0:.1f}%",
    )

def _load_steps_for_run(conn: sqlite3.Connection, run_id: str) -> list[StepRecord]:
    rows = conn.execute(
        """
        SELECT step_id, run_id, parent_step_id, name, status, duration, timestamp,
               logic_hash, inputs_json, output_json, error_json
        FROM steps
        WHERE run_id = ?
        ORDER BY timestamp ASC
        """,
        (run_id,),
    ).fetchall()

    out: list[StepRecord] = []
    for r in rows:
        out.append(
            StepRecord(
                step_id=r["step_id"],
                run_id=r["run_id"],
                parent_step_id=r["parent_step_id"],
                name=r["name"],
                status=r["status"],
                duration=float(r["duration"] or 0.0),
                timestamp=float(r["timestamp"] or 0.0),
                logic_hash=r["logic_hash"],
                inputs=_parse_json(r["inputs_json"]),
                output=_parse_json(r["output_json"]),
                error=_parse_json(r["error_json"]),
            )
        )
    return out

def _extract_blobs(obj: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if node.get("__af_blob__") is True:
                found.append(node)
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(obj)
    return found

def _referenced_blob_hashes(conn: sqlite3.Connection) -> set[str]:
    """Hashes of every blob referenced by step inputs/outputs/errors or tracked state."""
    hashes: set[str] = set()
    for query in (
        "SELECT inputs_json, output_json, error_json FROM steps",
        "SELECT value_json FROM values_registry",
    ):
        try:
            rows = conn.execute(query).fetchall()
        except sqlite3.OperationalError:
            continue  # older store without this table
        for row in rows:
            for raw in row:
                for blob in _extract_blobs(_parse_json(raw)):
                    if isinstance(blob.get("hash"), str):
                        hashes.add(blob["hash"])
    return hashes

def _orphan_assets(conn: sqlite3.Connection) -> list[Path]:
    """Blob files (``<sha256>.<ext>``) that nothing in the store references anymore."""
    if not _assets_path().is_dir():
        return []
    referenced = _referenced_blob_hashes(conn)
    return [
        p
        for p in _assets_path().iterdir()
        if p.is_file() and _BLOB_FILE_RE.match(p.name) and p.name.split(".", 1)[0] not in referenced
    ]

def _resolve_blob_file(blob: dict[str, Any]) -> Path:
    path = Path(str(blob.get("path", "")))
    return path if path.is_absolute() else _detect_project_root() / path

def _json_text(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        return str(value)

def _table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        return any(r["name"] == column for r in rows)
    except Exception:
        return False

def _logic_registry_globals_expr(conn: sqlite3.Connection, alias: str = "lr") -> str:
    if _table_has_column(conn, "logic_registry", "resolved_globals"):
        return f"COALESCE({alias}.resolved_globals, {alias}.globals_json)"
    return f"{alias}.globals_json"

def _step_dict_by_name(conn: sqlite3.Connection, run_id: str) -> dict[str, dict[str, Any]]:
    globals_expr = _logic_registry_globals_expr(conn, "lr")
    rows = conn.execute(
        f"""
        SELECT s.name, s.logic_hash, s.inputs_json, s.output_json, s.status, s.duration,
               lr.source_code, {globals_expr} AS resolved_globals
        FROM steps s
        LEFT JOIN logic_registry lr ON lr.logic_hash = s.logic_hash
        WHERE s.run_id = ?
        ORDER BY s.timestamp ASC
        """,
        (run_id,),
    ).fetchall()
    data: dict[str, dict[str, Any]] = {}
    for row in rows:
        row_dict = dict(row)
        data[row_dict["name"]] = row_dict
    return data

def _outputs_by_step_name(conn: sqlite3.Connection, run_id: str) -> dict[str, list[str]]:
    """Every output of every call, grouped by step name, in a run-order-independent form."""
    outputs: dict[str, list[str]] = defaultdict(list)
    for row in conn.execute("SELECT name, output_json FROM steps WHERE run_id = ?", (run_id,)):
        outputs[row["name"]].append(_json_text(_parse_json(row["output_json"])))
    return {name: sorted(values) for name, values in outputs.items()}

def _step_rows_for_alias_resolution(conn: sqlite3.Connection, run_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT step_id, parent_step_id, name, logic_hash, timestamp
        FROM steps
        WHERE run_id = ?
        ORDER BY timestamp ASC
        """,
        (run_id,),
    ).fetchall()
    return [dict(r) for r in rows]

def _infer_aliased_missing_step_names(
    *,
    source_rows: list[dict[str, Any]],
    source_by_name: dict[str, dict[str, Any]],
    other_by_name: dict[str, dict[str, Any]],
) -> set[str]:
    missing_names = set(source_by_name) - set(other_by_name)
    if not missing_names:
        return set()

    by_id: dict[str, dict[str, Any]] = {
        str(r.get("step_id")): r for r in source_rows if r.get("step_id")
    }

    aliased: set[str] = set()
    for missing_name in missing_names:
        row = source_by_name.get(missing_name)
        if not row:
            continue
        cursor = by_id.get(str(row.get("step_id") or ""))
        hops = 0
        while cursor is not None and hops < 200:
            hops += 1
            ancestor_name = str(cursor.get("name") or "")
            if ancestor_name and ancestor_name in other_by_name:
                hash_src = cursor.get("logic_hash")
                hash_other = other_by_name[ancestor_name].get("logic_hash")
                if hash_src and hash_src == hash_other:
                    aliased.add(missing_name)
                    break
            parent_id = cursor.get("parent_step_id")
            cursor = by_id.get(str(parent_id)) if parent_id else None
    return aliased

def _unified_diff_text(a: str, b: str, from_label: str, to_label: str) -> str:
    lines = list(
        difflib.unified_diff(
            a.splitlines(),
            b.splitlines(),
            fromfile=from_label,
            tofile=to_label,
            lineterm="",
        )
    )
    return "\n".join(lines) if lines else "(no differences)"

def _unified_diff_text_ctx(a: str, b: str, from_label: str, to_label: str, context: int = 3) -> str:
    lines = list(
        difflib.unified_diff(
            a.splitlines(),
            b.splitlines(),
            fromfile=from_label,
            tofile=to_label,
            n=context,
            lineterm="",
        )
    )
    return "\n".join(lines)

def _print_git_style_diff(title: str, diff_text: str) -> None:
    console.print(
        Panel(
            Syntax(diff_text, "diff", theme="monokai", line_numbers=True, word_wrap=True),
            title=f"[lg.muted]{title}[/lg.muted]",
            title_align="left",
            box=PANEL_BOX,
            border_style="lg.muted",
        )
    )

def _collect_multiline_text_paths(value: Any, prefix: str = "") -> dict[str, str]:
    out: dict[str, str] = {}

    if isinstance(value, str):
        if "\n" in value:
            out[prefix or "<root>"] = value
        return out

    if isinstance(value, dict):
        for k, v in value.items():
            child = f"{prefix}.{k}" if prefix else str(k)
            out.update(_collect_multiline_text_paths(v, child))
        return out

    if isinstance(value, list):
        for i, v in enumerate(value):
            child = f"{prefix}[{i}]" if prefix else f"[{i}]"
            out.update(_collect_multiline_text_paths(v, child))

    return out

def _has_multiline_text(value: Any) -> bool:
    return len(_collect_multiline_text_paths(value)) > 0

def _is_missing_capture(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, dict)):
        return len(value) == 0
    return False

def _render_text_or_json_diff(
    *,
    title_prefix: str,
    step: str,
    run_a: str,
    run_b: str,
    section_name: str,
    value_a: Any,
    value_b: Any,
) -> bool:
    printed = False

    if _has_multiline_text(value_a) or _has_multiline_text(value_b):
        paths_a = _collect_multiline_text_paths(value_a)
        paths_b = _collect_multiline_text_paths(value_b)
        all_paths = sorted(set(paths_a) | set(paths_b))
        for path in all_paths:
            text_a = paths_a[path] if path in paths_a else "<not captured>"
            text_b = paths_b[path] if path in paths_b else "<not captured>"

            diff_text = _unified_diff_text_ctx(
                text_a,
                text_b,
                f"{run_a}:{step}:{section_name}:{path}",
                f"{run_b}:{step}:{section_name}:{path}",
                context=3,
            )
            if diff_text:
                _print_git_style_diff(f"{title_prefix} · {step} · {path}", diff_text)
                printed = True
        return printed

    text_a = "<not captured>" if _is_missing_capture(value_a) else _json_text(value_a)
    text_b = "<not captured>" if _is_missing_capture(value_b) else _json_text(value_b)
    diff_text = _unified_diff_text_ctx(
        text_a,
        text_b,
        f"{run_a}:{step}:{section_name}",
        f"{run_b}:{step}:{section_name}",
        context=3,
    )
    if diff_text:
        _print_git_style_diff(f"{title_prefix} · {step}", diff_text)
        printed = True
    return printed

def _open_with_system(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        try:
            from PIL import Image

            with Image.open(path) as img:
                img.show()
                return True
        except Exception:
            pass

        if sys.platform == "darwin":
            subprocess.run(["open", str(path)], check=False)
            return True
        if os.name == "nt":
            subprocess.run(f'start "" "{path}"', shell=True, check=False)
            return True

        subprocess.run(["xdg-open", str(path)], check=False)
        return True
    except Exception:
        return False

def _copy_to_clipboard(text: str) -> bool:
    try:
        if sys.platform == "darwin":
            proc = subprocess.run(["pbcopy"], input=text, text=True, check=False)
            return proc.returncode == 0
        if os.name == "nt":
            proc = subprocess.run(["clip"], input=text, text=True, check=False, shell=True)
            return proc.returncode == 0

        if subprocess.run(["which", "xclip"], capture_output=True, check=False).returncode == 0:
            proc = subprocess.run(["xclip", "-selection", "clipboard"], input=text, text=True, check=False)
            return proc.returncode == 0
        if subprocess.run(["which", "wl-copy"], capture_output=True, check=False).returncode == 0:
            proc = subprocess.run(["wl-copy"], input=text, text=True, check=False)
            return proc.returncode == 0
    except Exception:
        return False
    return False

def _sum_tokens_from_obj(obj: Any) -> int:
    total = 0
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str) and "token" in k.lower() and isinstance(v, (int, float)):
                total += int(v)
            total += _sum_tokens_from_obj(v)
    elif isinstance(obj, list):
        for item in obj:
            total += _sum_tokens_from_obj(item)
    return total

def _sum_metric_keys(obj: Any, keys: tuple[str, ...]) -> float:
    total = 0.0
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str) and any(tag in k.lower() for tag in keys) and isinstance(v, (int, float)):
                total += float(v)
            total += _sum_metric_keys(v, keys)
    elif isinstance(obj, list):
        for item in obj:
            total += _sum_metric_keys(item, keys)
    return total

# ---------------------------------------------------------------------------
# Smart run resolution
# ---------------------------------------------------------------------------


def _top_level_steps(steps: list[StepRecord]) -> list[StepRecord]:
    """Steps not nested in another step of the same run (their durations don't overlap)."""
    ids = {s.step_id for s in steps}
    return [s for s in steps if not s.parent_step_id or s.parent_step_id not in ids]

# Lets "-1", "-2"… reach the run_id argument instead of being parsed as options.
_RUN_ID_ARG_SETTINGS = {"ignore_unknown_options": True}

def _resolve_run_id(conn: sqlite3.Connection, token: str) -> str:
    """Resolve a shorthand token (last, fail, -1, -2…) to a real run_id."""
    t = token.strip()

    if t == "last":
        row = conn.execute("SELECT run_id FROM runs ORDER BY created_at DESC LIMIT 1").fetchone()
        if not row:
            console.print()
            console.print(Panel(Text("No runs found in this database.", style="lg.muted"), box=PANEL_BOX, border_style="lg.muted", padding=(0, 2)))
            raise typer.Exit(1)
        return row["run_id"]

    if t in ("last-failed", "fail", "failed"):
        row = conn.execute(
            "SELECT run_id FROM runs WHERE UPPER(status) IN ('FAILED','FAILURE','ERROR') ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if not row:
            console.print()
            console.print(Panel(Text("No failed run found in this database.", style="lg.muted"), box=PANEL_BOX, border_style="lg.muted", padding=(0, 2)))
            raise typer.Exit(1)
        return row["run_id"]

    if t.startswith("-") and t[1:].isdigit():
        offset = int(t[1:]) - 1
        row = conn.execute("SELECT run_id FROM runs ORDER BY created_at DESC LIMIT 1 OFFSET ?", (offset,)).fetchone()
        if not row:
            console.print()
            console.print(Panel(Text(f"No run found at index {t}.", style="lg.muted"), box=PANEL_BOX, border_style="lg.muted", padding=(0, 2)))
            raise typer.Exit(1)
        return row["run_id"]

    return t

_SHORTHAND_COMPLETIONS: list[tuple[str, str]] = [
    ("last", "Most recent run"),
    ("last-failed", "Most recent failed run"),
    ("fail", "Most recent failed run (alias)"),
    ("-1", "Most recent run"),
    ("-2", "Second most recent run"),
    ("-3", "Third most recent run"),
]

def _complete_run_id(incomplete: str) -> list[tuple[str, str]]:
    """Shell completion callback: shorthands + real run_ids from the DB, as (value, help)."""
    completions: list[tuple[str, str]] = [
        (token, desc) for token, desc in _SHORTHAND_COMPLETIONS if token.startswith(incomplete)
    ]
    if not _store_path().exists():
        return completions
    try:
        conn = sqlite3.connect(_store_path())
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                "SELECT run_id, project, status, created_at FROM runs WHERE run_id LIKE ? ORDER BY created_at DESC LIMIT 20",
                (f"{incomplete}%",),
            ).fetchall()
            for row in rows:
                help_text = f"{row['project'] or '-'} · {row['status'] or '?'} · {_relative_time(row['created_at'])}"
                completions.append((row["run_id"], help_text))
        finally:
            conn.close()
    except Exception:
        pass
    return completions


_TEMPLATES_DIR = _PACKAGE_DIR / "templates"

_AGENT_RULES_FILES: list[tuple[str, str]] = [
    # (template filename,  destination filename in user project)
    ("LOGRAM_AGENT_RULES.md", "LOGRAM_AGENT_RULES.md"),
    ("cursorrules",           ".cursorrules"),
    ("CLAUDE.md",             "CLAUDE.md"),
]

def _write_agent_rules_files(cwd: Path, *, force: bool = False) -> list[tuple[str, str]]:
    """Write bundled agent rules files to cwd. Returns list of (dest_name, status)."""
    results: list[tuple[str, str]] = []
    for template_name, dest_name in _AGENT_RULES_FILES:
        template = _TEMPLATES_DIR / template_name
        dest = cwd / dest_name
        if not template.exists():
            results.append((dest_name, "template missing"))
            continue
        if not force and dest.exists():
            results.append((dest_name, "skipped (exists)"))
            continue
        try:
            dest.write_text(template.read_text(encoding="utf-8"), encoding="utf-8")
            results.append((dest_name, "written"))
        except Exception as exc:
            results.append((dest_name, f"error: {exc}"))
    return results

def _claude_desktop_config_path() -> Path | None:
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "Claude" / "claude_desktop_config.json"
    if os.name == "nt":
        appdata = os.environ.get("APPDATA", "")
        if appdata:
            return Path(appdata) / "Claude" / "claude_desktop_config.json"
    return None
