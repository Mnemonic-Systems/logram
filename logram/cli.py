# ruff: noqa: BLE001
# pylint: disable=broad-exception-caught

from __future__ import annotations

import difflib
import json
import logging
import os
import sqlite3
import subprocess
import sys
import time
import webbrowser
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import typer
from rich import box
from rich.panel import Panel
from rich.progress_bar import ProgressBar
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text
from rich.tree import Tree

from .analysis import find_all_divergences
from .metrics import aggregate_roi_stats, aggregate_token_efficiency, top_inputs_by_savings
from .theme import (
    PANEL_BOX,
    TABLE_BOX,
    console,
    duration_text,
    hint_line,
    status_badge,
    step_badge,
    step_color,
    step_icon,
)

APP_NAME = "Logram Control Center"
DB_PATH = Path(".logram") / "logram.db"
ASSETS_DIR = Path(".logram_assets")

_TEMPLATES_DIR = Path(__file__).parent / "templates"
_AGENT_RULES_FILES: list[tuple[str, str]] = [
    # (template filename,  destination filename in user project)
    ("LOGRAM_AGENT_RULES.md", "LOGRAM_AGENT_RULES.md"),
    ("cursorrules",           ".cursorrules"),
    ("CLAUDE.md",             "CLAUDE.md"),
]
_GITIGNORE_ENTRIES = [".logram/", ".logram_assets/"]

app = typer.Typer(
    name="logram",
    help="CLI Logram: inspection, time-machine, qualité et maintenance.",
    no_args_is_help=True,
    rich_markup_mode="rich",
)
golden_app = typer.Typer(help="Gestion des runs Golden.")
app.add_typer(golden_app, name="golden")

mcp_app = typer.Typer(help="Serveur MCP Logram pour agents de coding (Claude, Cursor…).")
app.add_typer(mcp_app, name="mcp")


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
    if require_exists and not DB_PATH.exists():
        _educational_db_missing_message()
        return None

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
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
    return "\n".join(lines) if lines else "(aucune différence)"


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
            text_a = paths_a[path] if path in paths_a else "<Variable non capturée>"
            text_b = paths_b[path] if path in paths_b else "<Variable non capturée>"

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

    text_a = "<Variable non capturée>" if _is_missing_capture(value_a) else _json_text(value_a)
    text_b = "<Variable non capturée>" if _is_missing_capture(value_b) else _json_text(value_b)
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
# Commands
# ---------------------------------------------------------------------------


@app.command("list")
def list_runs(
    group_by_input: bool = typer.Option(False, "--group-by-input", help="Regrouper les runs par input_id."),
    project: str | None = typer.Option(None, "--project", help="Filtrer par projet."),
    full: bool = typer.Option(False, "--full", help="Afficher les champs complets (pas d'ellipsis)."),
    copy_field: str | None = typer.Option(
        None,
        "--copy-field",
        help="Copier un champ d'une ligne (run_id|project|input_id|version_id|status|duration|created_at).",
    ),
    copy_index: int = typer.Option(1, "--copy-index", min=1, help="Index de ligne (1 = première ligne)."),
) -> None:
    """Affiche les runs historisés."""
    conn = _connect_db()
    if conn is None:
        raise typer.Exit(1)

    try:
        where = "WHERE 1=1"
        params: list[Any] = []
        if project:
            where += " AND r.project = ?"
            params.append(project)

        rows = conn.execute(
            f"""
            SELECT
                r.run_id,
                r.project,
                r.input_id,
                r.version_id,
                r.status,
                COALESCE(SUM(s.duration), 0) AS duration,
                r.created_at
            FROM runs r
            LEFT JOIN steps s ON s.run_id = r.run_id
            {where}
            GROUP BY r.run_id, r.project, r.input_id, r.version_id, r.status, r.created_at
            ORDER BY r.created_at DESC
            """,
            params,
        ).fetchall()

        if not rows:
            console.print()
            console.print(Text("  No runs found.", style="lg.muted"))
            console.print()
            return

        allowed_copy_fields = {
            "run_id",
            "project",
            "input_id",
            "version_id",
            "status",
            "duration",
            "created_at",
        }
        if copy_field and copy_field not in allowed_copy_fields:
            console.print()
            console.print(
                Panel(
                    Text.assemble(
                        (f"Invalid field: {copy_field}\n", "lg.error"),
                        ("Allowed: ", "lg.muted"),
                        (", ".join(sorted(allowed_copy_fields)), "lg.brand"),
                    ),
                    box=PANEL_BOX,
                    border_style="lg.error",
                    padding=(0, 2),
                )
            )
            raise typer.Exit(1)

        if group_by_input:
            if copy_field:
                console.print()
                console.print(
                    Panel(
                        Text(
                            "--copy-field is not supported with --group-by-input.",
                            style="lg.muted",
                        ),
                        box=PANEL_BOX,
                        border_style="lg.muted",
                        padding=(0, 2),
                    )
                )
                raise typer.Exit(1)

            grouped: dict[str, list[sqlite3.Row]] = defaultdict(list)
            for r in rows:
                grouped[r["input_id"] or "unknown_input"].append(r)

            for input_id, items in grouped.items():
                table = Table(
                    box=TABLE_BOX,
                    show_edge=False,
                    show_lines=False,
                    expand=True,
                    pad_edge=False,
                    header_style="lg.header",
                )
                table.add_column("ID", style="lg.brand", no_wrap=True)
                table.add_column("Project", style="lg.muted")
                table.add_column("Version", style="lg.muted", overflow="fold" if full else "ellipsis")
                table.add_column("Status")
                table.add_column("Duration", justify="right")
                table.add_column("Created", style="lg.muted", justify="right")
                for r in items:
                    table.add_row(
                        r["run_id"],
                        r["project"] or "-",
                        r["version_id"] or "-",
                        status_badge(r["status"]),
                        duration_text(r["duration"]),
                        _relative_time(r["created_at"]),
                    )
                console.print()
                console.print(
                    Panel(
                        table,
                        title=f"[lg.muted]input · {input_id}[/lg.muted]",
                        title_align="left",
                        box=PANEL_BOX,
                        border_style="lg.muted",
                        padding=(0, 1),
                    )
                )
            return

        table = Table(
            box=TABLE_BOX,
            show_edge=False,
            show_lines=False,
            expand=False,
            pad_edge=False,
            header_style="lg.header",
        )
        table.add_column("ID", style="lg.brand", no_wrap=True, max_width=44)
        table.add_column("Project", style="lg.muted", no_wrap=True, max_width=18)
        if full:
            table.add_column("Input", overflow="fold", max_width=24)
        table.add_column("Status", no_wrap=True, min_width=12)
        table.add_column("Duration", justify="right", no_wrap=True, min_width=8)
        table.add_column("Created", style="lg.muted", justify="right", no_wrap=True, min_width=10)

        for r in rows:
            row_data = [
                r["run_id"],
                r["project"] or "-",
            ]
            if full:
                row_data.append(r["input_id"] or "-")
            row_data += [
                status_badge(r["status"]),
                duration_text(r["duration"]),
                _relative_time(r["created_at"]),
            ]
            table.add_row(*row_data)

        console.print()
        console.print(Text(" logram · runs ", style="lg.muted"))
        console.print(table)
        console.print()
        console.print(
            hint_line(
                f"{len(rows)} runs total",
                "lg list --all to see more",
                "lg list --group-by-input to group by document",
            )
        )
        console.print()

        if copy_field:
            if copy_index > len(rows):
                console.print(
                    Panel(
                        Text(f"copy-index {copy_index} out of range (1..{len(rows)}).", style="lg.error"),
                        box=PANEL_BOX,
                        border_style="lg.error",
                        padding=(0, 2),
                    )
                )
                raise typer.Exit(1)

            row = rows[copy_index - 1]
            if copy_field == "duration":
                value = _format_duration(row["duration"])
            elif copy_field == "created_at":
                value = _format_dt(row["created_at"])
            else:
                value = str(row[copy_field] or "-")

            if _copy_to_clipboard(value):
                console.print(
                    Text.assemble(
                        ("  ✓ copied  ", "lg.badge.success"),
                        (f"  {copy_field}", "lg.brand"),
                        (f" from row #{copy_index}  ", "lg.muted"),
                        (value, "bold"),
                    )
                )
            else:
                console.print(
                    Text.assemble(
                        ("  clipboard unavailable  ", "lg.muted"),
                        (value, "bold"),
                    )
                )
    finally:
        conn.close()


@app.command()
def inspect(run_id: str) -> None:
    """Affiche l'arbre chronologique d'exécution d'un run."""
    conn = _connect_db()
    if conn is None:
        raise typer.Exit(1)

    try:
        run = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if not run:
            console.print()
            console.print(
                Panel(
                    Text.assemble(("Run not found: ", "lg.muted"), (run_id, "lg.brand")),
                    box=PANEL_BOX,
                    border_style="lg.error",
                    padding=(0, 2),
                )
            )
            raise typer.Exit(1)

        steps = _load_steps_for_run(conn, run_id)
        if not steps:
            console.print()
            console.print(
                Panel(
                    Text.assemble(("No steps recorded for run ", "lg.muted"), (run_id, "lg.brand")),
                    box=PANEL_BOX,
                    border_style="lg.muted",
                    padding=(0, 2),
                )
            )
            return

        by_parent: dict[str | None, list[StepRecord]] = defaultdict(list)
        by_id: dict[str, StepRecord] = {}
        for s in steps:
            by_parent[s.parent_step_id].append(s)
            by_id[s.step_id] = s

        for children in by_parent.values():
            children.sort(key=lambda x: x.timestamp)

        # Header
        console.print()
        header = Text()
        header.append(run_id, style="bold lg.brand")
        header.append("  ·  ", style="lg.muted")
        header.append(run["project"] or "-", style="lg.muted")
        header.append("  ·  ", style="lg.muted")
        header.append(_format_dt(run["created_at"]), style="lg.muted")
        header.append("  ·  ", style="lg.muted")
        header.append(run["status"] or "-", style=step_color(run["status"]))
        console.print(header)
        console.print()

        # Tree
        tree = Tree(
            Text(run["project"] or run_id, style="bold"),
            guide_style="lg.muted",
        )

        root_steps = list(by_parent.get(None, []))
        root_steps += [s for s in steps if s.parent_step_id and s.parent_step_id not in by_id]
        seen: set[str] = set()

        def _step_label(step: StepRecord) -> Text:
            t = Text()
            icon = step_icon(step.status)
            color = step_color(step.status)
            t.append(f"{icon} ", style=color)
            t.append(step.name, style=f"bold {color}")
            t.append("  ")
            t.append(_format_duration(step.duration), style="lg.dur.fast" if step.duration <= 2.5 else "lg.dur.slow")
            t.append("  ")
            t.append_text(step_badge(step.status))
            return t

        def add_node(parent_tree: Tree, step: StepRecord) -> None:
            if step.step_id in seen:
                return
            seen.add(step.step_id)
            node = parent_tree.add(_step_label(step))
            for child in by_parent.get(step.step_id, []):
                add_node(node, child)

        for root in root_steps:
            add_node(tree, root)

        for step in steps:
            if step.step_id not in seen:
                add_node(tree, step)

        console.print(tree)

        # Summary footer
        total_dur = sum(s.duration for s in steps)
        live_dur = sum(
            s.duration
            for s in steps
            if s.status.upper() not in {"REPLAYED", "CACHE_HIT", "REPLAY_HIT"}
        )
        replayed_count = sum(
            1 for s in steps if s.status.upper() in {"REPLAYED", "CACHE_HIT", "REPLAY_HIT"}
        )

        console.print()
        summary = Text()
        summary.append("Total: ", style="lg.muted")
        summary.append(f"{total_dur:.2f}s", style="bold")
        summary.append("   Live: ", style="lg.muted")
        summary.append(f"{live_dur:.2f}s", style="lg.warning")
        summary.append("   Replayed: ", style="lg.muted")
        summary.append(f"{replayed_count} steps", style="lg.brand")
        console.print(summary)
        console.print()
        console.print(hint_line("lg view <step_id> to inspect", f"lg diff <run_a> {run_id} to compare"))
        console.print()

    finally:
        conn.close()


@app.command()
def view(step_id: str) -> None:
    """Affiche le détail d'une étape (inputs, outputs JSON, blobs)."""
    conn = _connect_db()
    if conn is None:
        raise typer.Exit(1)

    try:
        row = conn.execute(
            """
            SELECT s.*, r.project, r.input_id
            FROM steps s
            LEFT JOIN runs r ON r.run_id = s.run_id
            WHERE s.step_id = ?
            """,
            (step_id,),
        ).fetchone()
        if not row:
            console.print()
            console.print(
                Panel(
                    Text.assemble(("Step not found: ", "lg.muted"), (step_id, "lg.brand")),
                    box=PANEL_BOX,
                    border_style="lg.error",
                    padding=(0, 2),
                )
            )
            raise typer.Exit(1)

        inputs = _parse_json(row["inputs_json"])
        output = _parse_json(row["output_json"])
        error = _parse_json(row["error_json"])

        # Header
        console.print()
        header = Text()
        header.append(row["name"] or "-", style="bold lg.brand")
        header.append("  ·  ", style="lg.muted")
        header.append(str(row["step_id"] or "-"), style="lg.muted")
        header.append("  ·  ", style="lg.muted")
        header.append(str(row["run_id"] or "-"), style="lg.muted")
        header.append("  ·  ", style="lg.muted")
        header.append_text(step_badge(row["status"]))
        header.append("  ·  ", style="lg.muted")
        header.append(_format_duration(row["duration"]), style="lg.dur.fast" if float(row["duration"] or 0) <= 2.5 else "lg.dur.slow")
        console.print(header)

        if row["logic_hash"]:
            lh = Text()
            lh.append("  hash ", style="lg.muted")
            lh.append(str(row["logic_hash"])[:16], style="lg.muted")
            lh.append("  ·  ", style="lg.muted")
            lh.append(_format_dt(row["timestamp"]), style="lg.muted")
            console.print(lh)
        console.print()

        # Inputs
        console.print(
            Panel(
                Syntax(_json_text(inputs), "json", theme="monokai", line_numbers=False),
                title="[lg.muted]► Inputs[/lg.muted]",
                title_align="left",
                box=PANEL_BOX,
                border_style="lg.muted",
            )
        )

        # Output
        console.print(
            Panel(
                Syntax(_json_text(output), "json", theme="monokai", line_numbers=False),
                title="[lg.muted]► Output[/lg.muted]",
                title_align="left",
                box=PANEL_BOX,
                border_style="lg.muted",
            )
        )

        # Error
        if error:
            console.print(
                Panel(
                    Syntax(_json_text(error), "json", theme="monokai", line_numbers=False),
                    title="[lg.muted]► Error[/lg.muted]",
                    title_align="left",
                    box=PANEL_BOX,
                    border_style="lg.error",
                )
            )

        # Blobs
        blobs = _extract_blobs(output)
        if blobs:
            blob_table = Table(
                box=TABLE_BOX,
                show_edge=False,
                show_lines=False,
                header_style="lg.header",
            )
            blob_table.add_column("hash", style="lg.brand")
            blob_table.add_column("size", justify="right", style="lg.muted")
            blob_table.add_column("path")
            for blob in blobs:
                blob_table.add_row(
                    str(blob.get("hash", "-")),
                    str(blob.get("size", "-")),
                    str(blob.get("path", "-")),
                )
            console.print()
            console.print(blob_table)

        console.print()
        console.print(hint_line(f"lg inspect {row['run_id']} to see full tree", "lg open <step_id> to open blob"))
        console.print()

    finally:
        conn.close()


@app.command()
def replay(
    script_py: str,
    force: list[str] | None = typer.Option(None, "--force", "-f", help="Step(s) à forcer en LIVE (répétable: --force a --force b)."),
    from_step: str | None = typer.Option(None, "--from", help="Cascade LIVE depuis cette étape (toutes les étapes descendantes → LIVE)."),
) -> None:
    """Relance un script en mode replay (LOGRAM_REPLAY=true)."""
    conn = _connect_db(require_exists=False)
    if conn is None:
        raise typer.Exit(1)

    script_path = Path(script_py)
    if not script_path.exists():
        console.print()
        console.print(
            Panel(
                Text.assemble(("Script not found: ", "lg.muted"), (script_py, "lg.brand")),
                box=PANEL_BOX,
                border_style="lg.error",
                padding=(0, 2),
            )
        )
        raise typer.Exit(1)

    try:
        # Pre-run banner
        console.print()
        meta = Text()
        meta.append(script_path.name, style="bold")
        meta.append("  ·  ", style="lg.muted")
        if force:
            meta.append(f"{len(force)} step(s) forced LIVE", style="lg.warning")
            meta.append("  ·  ", style="lg.muted")
        if from_step:
            meta.append(f"cascade from {from_step}", style="lg.warning")
            meta.append("  ·  ", style="lg.muted")
        meta.append("VCR mode", style="lg.muted")
        console.print(meta)

        if force:
            for step_name in force:
                forced_label = Text()
                forced_label.append("  ↦ forced LIVE  ", style="lg.badge.live")
                forced_label.append(f"  {step_name}", style="lg.warning")
                console.print(forced_label)

        if from_step:
            cascade_label = Text()
            cascade_label.append("  ↓ cascade from  ", style="lg.badge.live")
            cascade_label.append(f"  {from_step}", style="lg.warning")
            console.print(cascade_label)

        console.print()

        # Invalidate cached rows for explicitly forced steps
        if force:
            total_deleted = 0
            for step_name in force:
                deleted = conn.execute(
                    "DELETE FROM steps WHERE name = ? AND status = 'SUCCESS'",
                    (step_name,),
                ).rowcount
                total_deleted += deleted
            conn.commit()
            if total_deleted:
                invalidated = Text()
                invalidated.append(f"  {total_deleted} cache entries invalidated", style="lg.muted")
                invalidated.append(f"  for: {', '.join(force)}", style="lg.muted")
                console.print(invalidated)
                console.print()

        # Build env for subprocess
        env = os.environ.copy()
        env["LOGRAM_REPLAY"] = "true"
        if force:
            env["LOGRAM_FORCE_STEP"] = ",".join(force)
        if from_step:
            env["LOGRAM_FORCE_FROM"] = from_step

        console.rule(style="lg.muted")
        result = subprocess.run([sys.executable, str(script_path)], env=env, check=False)
        console.rule(style="lg.muted")
        console.print()

        if result.returncode == 0:
            done = Text()
            done.append("  ✓ replay complete  ", style="lg.badge.success")
            done.append(f"  {script_path.name}", style="lg.brand")
            console.print(done)
        else:
            fail = Text()
            fail.append("  ✗ replay failed  ", style="lg.badge.failed")
            fail.append(f"  exit {result.returncode}", style="lg.error")
            console.print(fail)

        console.print()
        console.print(hint_line("lg list to see the new run", "lg diff <old> <new> to compare"))
        console.print()

        if result.returncode != 0:
            raise typer.Exit(result.returncode)

    finally:
        conn.close()


@app.command()
def diff(
    run_a: str,
    run_b: str,
    code: bool = typer.Option(False, "--code", "-c", help="Afficher uniquement le diff du code source."),
    globals_only: bool = typer.Option(False, "--globals", "-g", help="Afficher uniquement le diff des globals/prompts."),
    inputs: bool = typer.Option(False, "--inputs", "-i", help="Afficher uniquement le diff des inputs."),
    outputs: bool = typer.Option(False, "--outputs", "-o", help="Afficher uniquement le diff des outputs."),
) -> None:
    """Compare deux run_id (code + data)."""
    conn = _connect_db()
    if conn is None:
        raise typer.Exit(1)

    try:
        meta_a = conn.execute("SELECT run_id, input_id FROM runs WHERE run_id = ?", (run_a,)).fetchone()
        meta_b = conn.execute("SELECT run_id, input_id FROM runs WHERE run_id = ?", (run_b,)).fetchone()
        if not meta_a or not meta_b:
            console.print()
            console.print(
                Panel(
                    Text("Could not find both requested runs.", style="lg.error"),
                    box=PANEL_BOX,
                    border_style="lg.error",
                    padding=(0, 2),
                )
            )
            raise typer.Exit(1)

        if meta_a["input_id"] != meta_b["input_id"]:
            console.print()
            warning = Text()
            warning.append("  input_id mismatch  ", style="lg.badge.live")
            warning.append(f"  {meta_a['input_id']}  vs  {meta_b['input_id']}", style="lg.muted")
            console.print(warning)

        data_a = _step_dict_by_name(conn, run_a)
        data_b = _step_dict_by_name(conn, run_b)
        rows_a = _step_rows_for_alias_resolution(conn, run_a)
        rows_b = _step_rows_for_alias_resolution(conn, run_b)

        aliased_only_in_a = _infer_aliased_missing_step_names(
            source_rows=rows_a,
            source_by_name=data_a,
            other_by_name=data_b,
        )
        aliased_only_in_b = _infer_aliased_missing_step_names(
            source_rows=rows_b,
            source_by_name=data_b,
            other_by_name=data_a,
        )

        step_names = sorted((set(data_a) | set(data_b)) - aliased_only_in_a - aliased_only_in_b)

        filters_used = any([code, globals_only, inputs, outputs])
        show_code = code or not filters_used
        show_globals = globals_only or not filters_used
        show_inputs = inputs or not filters_used
        show_outputs = outputs or not filters_used

        # Summary table
        if not filters_used:
            console.print()

            header = Text()
            header.append("diff", style="bold lg.muted")
            header.append("  ·  ", style="lg.muted")
            header.append(run_a, style="lg.brand")
            header.append("  →  ", style="lg.muted")
            header.append(run_b, style="lg.brand")
            console.print(header)
            console.print()

            table = Table(
                box=TABLE_BOX,
                show_edge=False,
                show_lines=False,
                expand=False,
                pad_edge=False,
                header_style="lg.header",
            )
            table.add_column("step", style="lg.brand")
            table.add_column("logic_hash")
            table.add_column("source")
            table.add_column("globals")
            table.add_column("callees")

        diff_steps: list[str] = []
        step_cmp: dict[str, dict[str, Any]] = {}
        for step in step_names:
            a = data_a.get(step)
            b = data_b.get(step)
            a_hash = a.get("logic_hash") if a else None
            b_hash = b.get("logic_hash") if b else None

            src_a = (a.get("source_code") or "") if a else ""
            src_b = (b.get("source_code") or "") if b else ""
            glob_a_obj = _parse_json(a.get("resolved_globals")) if a and a.get("resolved_globals") else {}
            glob_b_obj = _parse_json(b.get("resolved_globals")) if b and b.get("resolved_globals") else {}
            glob_a = _json_text(glob_a_obj)
            glob_b = _json_text(glob_b_obj)

            in_a_obj = _parse_json(a.get("inputs_json")) if a and a.get("inputs_json") else None
            in_b_obj = _parse_json(b.get("inputs_json")) if b and b.get("inputs_json") else None
            out_a_obj = _parse_json(a.get("output_json")) if a and a.get("output_json") else None
            out_b_obj = _parse_json(b.get("output_json")) if b and b.get("output_json") else None

            identical_logic = bool(a_hash) and a_hash == b_hash
            hash_changed = not identical_logic
            source_changed = False if identical_logic else src_a != src_b
            globals_changed = False if identical_logic else glob_a != glob_b
            inputs_changed = False if identical_logic else _json_text(in_a_obj) != _json_text(in_b_obj)
            outputs_changed = _json_text(out_a_obj) != _json_text(out_b_obj)

            deep_nodes: list[dict[str, Any]] = []
            if hash_changed and a_hash and b_hash:
                try:
                    all_nodes = find_all_divergences(conn, a_hash, b_hash, path=[step])
                    deep_nodes = [n for n in all_nodes if n["depth"] > 0]
                except Exception:
                    pass

            step_cmp[step] = {
                "src_a": src_a,
                "src_b": src_b,
                "glob_a": glob_a_obj,
                "glob_b": glob_b_obj,
                "in_a": in_a_obj,
                "in_b": in_b_obj,
                "out_a": out_a_obj,
                "out_b": out_b_obj,
                "source_changed": source_changed,
                "globals_changed": globals_changed,
                "inputs_changed": inputs_changed,
                "outputs_changed": outputs_changed,
                "identical_logic": identical_logic,
                "deep_nodes": deep_nodes,
            }

            if hash_changed or source_changed or globals_changed:
                diff_steps.append(step)

            if not filters_used:
                def _changed_text(changed: bool, identical: bool) -> Text:
                    if identical:
                        return Text("identical", style="lg.muted")
                    return Text("changed", style="lg.warning") if changed else Text("same", style="lg.muted")

                if identical_logic or not deep_nodes:
                    callees_cell = Text("—", style="lg.muted")
                else:
                    names = ", ".join(n["callee_name"] for n in deep_nodes[:2])
                    suffix = f" +{len(deep_nodes) - 2}" if len(deep_nodes) > 2 else ""
                    callees_cell = Text(f"{names}{suffix}", style="lg.warning")

                table.add_row(
                    step,
                    _changed_text(hash_changed, identical_logic),
                    _changed_text(source_changed, identical_logic),
                    _changed_text(globals_changed, identical_logic),
                    callees_cell,
                )

        if not filters_used:
            console.print(table)
            console.print()

        code_diffs_printed = 0
        globals_diffs_printed = 0
        inputs_diffs_printed = 0
        outputs_diffs_printed = 0

        for step in step_names:
            cmp_row = step_cmp[step]

            if show_code and bool(cmp_row["source_changed"]):
                code_diff_text = _unified_diff_text_ctx(
                    str(cmp_row["src_a"]),
                    str(cmp_row["src_b"]),
                    f"{run_a}:{step}:source",
                    f"{run_b}:{step}:source",
                    context=3,
                )
                if code_diff_text:
                    _print_git_style_diff(f"source diff · {step}", code_diff_text)
                    code_diffs_printed += 1

            if show_globals and bool(cmp_row["globals_changed"]):
                if _render_text_or_json_diff(
                    title_prefix="globals diff",
                    step=step,
                    run_a=run_a,
                    run_b=run_b,
                    section_name="resolved_globals",
                    value_a=cmp_row["glob_a"],
                    value_b=cmp_row["glob_b"],
                ):
                    globals_diffs_printed += 1

            if show_inputs and bool(cmp_row["inputs_changed"]):
                if _render_text_or_json_diff(
                    title_prefix="inputs diff",
                    step=step,
                    run_a=run_a,
                    run_b=run_b,
                    section_name="inputs",
                    value_a=cmp_row["in_a"],
                    value_b=cmp_row["in_b"],
                ):
                    inputs_diffs_printed += 1

            if show_outputs and bool(cmp_row["outputs_changed"]):
                if _render_text_or_json_diff(
                    title_prefix="outputs diff",
                    step=step,
                    run_a=run_a,
                    run_b=run_b,
                    section_name="outputs",
                    value_a=cmp_row["out_a"],
                    value_b=cmp_row["out_b"],
                ):
                    outputs_diffs_printed += 1

            nodes = cmp_row.get("deep_nodes", [])
            if nodes and not filters_used:
                tree_label = Text()
                tree_label.append("callee tree  ", style="lg.muted")
                tree_label.append(step, style="bold lg.brand")
                tree_label.append(f"  ·  {len(nodes)} node(s) changed", style="lg.muted")
                callee_tree = Tree(tree_label, guide_style="lg.muted")

                for node in nodes:
                    path_str = " → ".join(node["path"])
                    wc = ", ".join(node["what_changed"])
                    node_label = Text()
                    node_label.append(node["callee_name"], style="bold lg.warning")
                    node_label.append(f"  depth {node['depth']}  ", style="lg.muted")
                    node_label.append(wc, style="lg.brand")
                    branch = callee_tree.add(node_label)
                    branch.add(Text(path_str, style="lg.muted"))

                console.print()
                console.print(callee_tree)

                for node in nodes:
                    if node.get("source_diff"):
                        _print_git_style_diff(
                            f"callee source diff · {node['callee_name']}",
                            node["source_diff"],
                        )
                    for key, change in (node.get("globals_diff") or {}).items():
                        diff_text = _unified_diff_text_ctx(
                            str(change.get("before") or ""),
                            str(change.get("after") or ""),
                            f"{node['callee_name']}:{key}:before",
                            f"{node['callee_name']}:{key}:after",
                            context=3,
                        )
                        if diff_text:
                            _print_git_style_diff(
                                f"callee globals diff · {node['callee_name']} · {key}",
                                diff_text,
                            )

        def _no_diff_line(label: str) -> None:
            console.print(Text.assemble(("  ✓ no ", "lg.success"), (label, "lg.muted"), (" differences", "lg.success")))

        if show_code and code_diffs_printed == 0:
            _no_diff_line("source")
        if show_globals and globals_diffs_printed == 0:
            _no_diff_line("globals/prompts")
        if show_inputs and inputs_diffs_printed == 0:
            _no_diff_line("inputs")
        if show_outputs and outputs_diffs_printed == 0:
            _no_diff_line("outputs")

        aliased_hidden_total = len(aliased_only_in_a) + len(aliased_only_in_b)
        if aliased_hidden_total > 0:
            console.print()
            console.print(
                Text(
                    f"  {aliased_hidden_total} sub-step(s) treated as REPLAY aliases (ancestor with identical logic_hash).",
                    style="lg.muted",
                )
            )
        console.print()

    finally:
        conn.close()


@app.command()
def recover(logic_hash: str) -> None:
    """Affiche le code source exact et les globals d'un logic_hash."""
    conn = _connect_db()
    if conn is None:
        raise typer.Exit(1)

    try:
        globals_expr = _logic_registry_globals_expr(conn, "logic_registry")
        row = conn.execute(
            f"""
            SELECT logic_hash, name, source_code, {globals_expr} AS resolved_globals, signature
            FROM logic_registry
            WHERE logic_hash = ?
            """,
            (logic_hash,),
        ).fetchone()
        if not row:
            console.print()
            console.print(
                Panel(
                    Text.assemble(("logic_hash not found: ", "lg.muted"), (logic_hash, "lg.brand")),
                    box=PANEL_BOX,
                    border_style="lg.error",
                    padding=(0, 2),
                )
            )
            raise typer.Exit(1)

        console.print()
        header = Text()
        header.append("recover", style="bold lg.muted")
        header.append("  ·  ", style="lg.muted")
        header.append(row["name"] or "-", style="bold lg.brand")
        header.append("  ·  ", style="lg.muted")
        header.append(str(row["logic_hash"])[:16] if row["logic_hash"] else "-", style="lg.muted")
        if row["signature"]:
            header.append("  ·  ", style="lg.muted")
            header.append(str(row["signature"])[:40], style="lg.muted")
        console.print(header)
        console.print()

        console.print(
            Panel(
                Syntax(row["source_code"] or "", "python", theme="monokai"),
                title="[lg.muted]► Source code[/lg.muted]",
                title_align="left",
                box=PANEL_BOX,
                border_style="lg.muted",
            )
        )
        console.print(
            Panel(
                Syntax(_json_text(_parse_json(row["resolved_globals"])), "json", theme="monokai"),
                title="[lg.muted]► Globals / prompts[/lg.muted]",
                title_align="left",
                box=PANEL_BOX,
                border_style="lg.muted",
            )
        )
        console.print()

    finally:
        conn.close()


@app.command()
def restore(run_id: str) -> None:
    """MVP anti-erreur: affiche les blocs de code à recopier pour revenir à l'état d'un run."""
    conn = _connect_db()
    if conn is None:
        raise typer.Exit(1)

    try:
        globals_expr = _logic_registry_globals_expr(conn, "lr")
        rows = conn.execute(
            f"""
            SELECT DISTINCT s.name, s.logic_hash, lr.source_code, {globals_expr} AS resolved_globals, lr.signature
            FROM steps s
            JOIN logic_registry lr ON lr.logic_hash = s.logic_hash
            WHERE s.run_id = ?
            ORDER BY s.timestamp ASC
            """,
            (run_id,),
        ).fetchall()

        if not rows:
            console.print()
            console.print(
                Panel(
                    Text.assemble(("No restorable blocks found for run ", "lg.muted"), (run_id, "lg.brand")),
                    box=PANEL_BOX,
                    border_style="lg.muted",
                    padding=(0, 2),
                )
            )
            return

        console.print()
        header = Text()
        header.append("restore", style="bold lg.muted")
        header.append("  ·  ", style="lg.muted")
        header.append(run_id, style="bold lg.brand")
        console.print(header)
        intro = Text("Copy-paste the blocks below to restore the logical state of this run.", style="lg.muted")
        console.print(intro)
        console.print()

        for idx, row in enumerate(rows, start=1):
            step_header = Text()
            step_header.append(f"  {idx}  ", style="bold lg.badge.replayed")
            step_header.append(f"  {row['name']}", style="bold lg.brand")
            step_header.append("  ·  ", style="lg.muted")
            step_header.append(str(row["logic_hash"])[:16] if row["logic_hash"] else "-", style="lg.muted")
            console.print(step_header)
            console.print()

            body = (row["source_code"] or "").strip() or "# source unavailable"
            console.print(
                Panel(
                    Syntax(body, "python", theme="monokai"),
                    title=f"[lg.muted]► step {row['name']} · source[/lg.muted]",
                    title_align="left",
                    box=PANEL_BOX,
                    border_style="lg.muted",
                )
            )
            console.print(
                Panel(
                    Syntax(_json_text(_parse_json(row["resolved_globals"])), "json", theme="monokai"),
                    title=f"[lg.muted]► step {row['name']} · globals[/lg.muted]",
                    title_align="left",
                    box=PANEL_BOX,
                    border_style="lg.muted",
                )
            )
            console.print()

    finally:
        conn.close()


@app.command("open")
def open_step(step_id: str) -> None:
    """Ouvre automatiquement un blob image d'une étape (si présent)."""
    conn = _connect_db()
    if conn is None:
        raise typer.Exit(1)

    try:
        row = conn.execute("SELECT output_json FROM steps WHERE step_id = ?", (step_id,)).fetchone()
        if not row:
            console.print()
            console.print(
                Panel(
                    Text.assemble(("Step not found: ", "lg.muted"), (step_id, "lg.brand")),
                    box=PANEL_BOX,
                    border_style="lg.error",
                    padding=(0, 2),
                )
            )
            raise typer.Exit(1)

        output = _parse_json(row["output_json"])
        blobs = _extract_blobs(output)
        if not blobs:
            console.print()
            console.print(Text("  No blob detected on this step.", style="lg.muted"))
            console.print()
            return

        for blob in blobs:
            path = Path(str(blob.get("path", "")))
            if path.suffix.lower() in {".png", ".jpg", ".jpeg", ".bmp", ".gif", ".webp", ".tiff"} and _open_with_system(path):
                console.print()
                opened = Text()
                opened.append("  ✓ opened  ", style="lg.badge.success")
                opened.append(f"  {path}", style="lg.brand")
                console.print(opened)
                console.print()
                return

        console.print()
        console.print(Text("  Blob(s) found but no openable image file.", style="lg.muted"))
        console.print()

    finally:
        conn.close()


@app.command()
def ui(
    host: str = typer.Option("127.0.0.1", "--host", help="Host de binding du serveur API"),
    port: int = typer.Option(8000, "--port", min=1, max=65535, help="Port du serveur API"),
    dashboard_url: str = typer.Option("http://localhost:3000", "--dashboard-url", help="URL du dashboard web"),
    open_browser: bool = typer.Option(True, "--open-browser/--no-open-browser", help="Ouvre automatiquement le dashboard"),
    reload: bool = typer.Option(False, "--reload", help="Active l'auto-reload (dev uniquement)"),
) -> None:
    """Lance le serveur FastAPI Logram (read API) pour le dashboard web."""
    if not DB_PATH.exists():
        _educational_db_missing_message()
        raise typer.Exit(1)

    try:
        import uvicorn
    except Exception as exc:
        console.print()
        console.print(
            Panel(
                Text.assemble(("uvicorn not available: ", "lg.muted"), (str(exc), "lg.error")),
                box=PANEL_BOX,
                border_style="lg.error",
                padding=(0, 2),
            )
        )
        raise typer.Exit(1) from exc

    if open_browser:
        try:
            opened = webbrowser.open(dashboard_url)
            if not opened:
                console.print(Text(f"  Could not open browser — visit {dashboard_url}", style="lg.muted"))
        except Exception as exc:
            console.print(Text(f"  Could not open browser: {exc}", style="lg.muted"))

    console.print()
    info = Text()
    info.append("logram ui", style="bold lg.brand")
    info.append("  ·  ", style="lg.muted")
    info.append(f"http://{host}:{port}", style="lg.brand")
    info.append("  ·  db ", style="lg.muted")
    info.append(str(DB_PATH), style="lg.muted")
    console.print(info)
    console.print()

    logging.getLogger("uvicorn.access").setLevel(logging.INFO)
    uvicorn.run("logram.server.app:app", host=host, port=port, reload=reload, log_level="info")


@golden_app.command("add")
def golden_add(run_id: str) -> None:
    """Tag un run en GOLDEN."""
    conn = _connect_db()
    if conn is None:
        raise typer.Exit(1)

    try:
        row = conn.execute("SELECT tags FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if not row:
            console.print()
            console.print(
                Panel(
                    Text.assemble(("Run not found: ", "lg.muted"), (run_id, "lg.brand")),
                    box=PANEL_BOX,
                    border_style="lg.error",
                    padding=(0, 2),
                )
            )
            raise typer.Exit(1)

        tags = _parse_json(row["tags"]) or []
        if not isinstance(tags, list):
            tags = []
        if "GOLDEN" not in tags:
            tags.append("GOLDEN")
            conn.execute("UPDATE runs SET tags = ?, updated_at = ? WHERE run_id = ?", (json.dumps(tags), time.time(), run_id))
            conn.commit()

        console.print()
        done = Text()
        done.append("  ✓ golden  ", style="lg.badge.success")
        done.append(f"  {run_id}", style="lg.brand")
        console.print(done)
        console.print()

    finally:
        conn.close()


@app.command()
def test(script_py: str) -> None:
    """Relance un script sur tous les inputs GOLDEN et génère un rapport de régression."""
    conn = _connect_db()
    if conn is None:
        raise typer.Exit(1)

    script_path = Path(script_py)
    if not script_path.exists():
        console.print()
        console.print(
            Panel(
                Text.assemble(("Script not found: ", "lg.muted"), (script_py, "lg.brand")),
                box=PANEL_BOX,
                border_style="lg.error",
                padding=(0, 2),
            )
        )
        raise typer.Exit(1)

    try:
        golden_runs = conn.execute(
            """
            SELECT run_id, input_id, project, created_at
            FROM runs
            WHERE tags LIKE '%GOLDEN%'
            ORDER BY created_at DESC
            """
        ).fetchall()

        if not golden_runs:
            console.print()
            console.print(Text("  No GOLDEN runs found. Tag a run with: lg golden add <run_id>", style="lg.muted"))
            console.print()
            return

        baseline_by_input: dict[str, sqlite3.Row] = {}
        for r in golden_runs:
            baseline_by_input.setdefault(r["input_id"], r)

        console.print()
        header = Text()
        header.append("golden test", style="bold lg.muted")
        header.append("  ·  ", style="lg.muted")
        header.append(script_path.name, style="bold lg.brand")
        header.append(f"  ·  {len(baseline_by_input)} input(s)", style="lg.muted")
        console.print(header)
        console.print()

        report = Table(
            box=TABLE_BOX,
            show_edge=False,
            show_lines=False,
            expand=False,
            pad_edge=False,
            header_style="lg.header",
        )
        report.add_column("input_id", style="lg.brand")
        report.add_column("baseline")
        report.add_column("new run")
        report.add_column("result")
        report.add_column("details", style="lg.muted")

        for input_id, baseline in baseline_by_input.items():
            before = conn.execute("SELECT MAX(created_at) AS ts FROM runs").fetchone()["ts"]

            env = os.environ.copy()
            env["LOGRAM_REPLAY"] = "true"
            env["LOGRAM_INPUT_ID"] = str(input_id)

            proc = subprocess.run([sys.executable, str(script_path)], env=env, check=False)
            if proc.returncode != 0:
                report.add_row(
                    input_id,
                    baseline["run_id"],
                    "-",
                    status_badge("FAILED"),
                    f"exit={proc.returncode}",
                )
                continue

            new_run = conn.execute(
                """
                SELECT run_id, created_at
                FROM runs
                WHERE input_id = ? AND created_at > ?
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (input_id, before or 0),
            ).fetchone()

            if not new_run:
                report.add_row(
                    input_id,
                    baseline["run_id"],
                    "-",
                    Text(" ? unknown ", style="dim"),
                    "no new run detected",
                )
                continue

            base_steps = _step_dict_by_name(conn, baseline["run_id"])
            new_steps = _step_dict_by_name(conn, new_run["run_id"])
            all_steps = set(base_steps) | set(new_steps)

            regressions = 0
            for name in all_steps:
                b_out = _json_text(_parse_json(base_steps[name]["output_json"])) if name in base_steps else "<missing>"
                n_out = _json_text(_parse_json(new_steps[name]["output_json"])) if name in new_steps else "<missing>"
                if b_out != n_out:
                    regressions += 1

            if regressions == 0:
                report.add_row(
                    input_id,
                    baseline["run_id"],
                    new_run["run_id"],
                    status_badge("SUCCESS"),
                    "no regression",
                )
            else:
                report.add_row(
                    input_id,
                    baseline["run_id"],
                    new_run["run_id"],
                    status_badge("FAILED"),
                    f"{regressions} step(s) differ",
                )

        console.print(report)
        console.print()

    finally:
        conn.close()


@app.command()
def stats(
    run_id_arg: str | None = typer.Argument(None, metavar="[RUN_ID]"),
    run_id: str | None = typer.Option(None, "--run-id", help="Scope Run: stats d'un run précis."),
    project: str | None = typer.Option(None, "--project", help="Scope Projet: filtre sur le nom de projet/pipeline."),
    input_id: str | None = typer.Option(None, "--input-id", help="Scope Input: filtre sur un document précis."),
    hourly_rate: float = typer.Option(10.0, "--hourly-rate", min=0.0, help="TJM horaire pour estimer le gain financier."),
) -> None:
    """Tableau de bord ROI avec scopes Global / Projet / Input / Run."""
    conn = _connect_db()
    if conn is None:
        raise typer.Exit(1)

    try:
        if run_id_arg and run_id and run_id_arg != run_id:
            console.print()
            console.print(
                Panel(
                    Text("Parameter conflict: provide RUN_ID as argument or --run-id, not both.", style="lg.error"),
                    box=PANEL_BOX,
                    border_style="lg.error",
                    padding=(0, 2),
                )
            )
            raise typer.Exit(1)

        selected_run_id = run_id_arg or run_id

        scope = "global"
        if selected_run_id:
            scope = f"run · {selected_run_id}"
        elif input_id:
            scope = f"input · {input_id}"
        elif project:
            scope = f"project · {project}"

        kpis = aggregate_roi_stats(
            conn,
            project=project,
            input_id=input_id,
            run_id=selected_run_id,
        )

        run_count = int(kpis["run_count"])
        total_steps = int(kpis["total_steps"])
        replayed_steps = int(kpis["replayed_steps"])
        total_project_time = float(kpis["total_project_time"])
        resource_saved = float(kpis["resource_time_saved"])
        wait_saved = float(kpis["wait_time_saved"])
        efficiency_ratio = float(kpis["efficiency_ratio"])

        if run_count == 0 or total_steps == 0:
            console.print()
            console.print(
                Panel(
                    Text.assemble(
                        ("No data found for scope: ", "lg.muted"),
                        (scope, "bold lg.brand"),
                        ("\n\nRun an instrumented pipeline with logram.trace then retry.", "lg.muted"),
                    ),
                    box=PANEL_BOX,
                    border_style="lg.muted",
                    padding=(1, 2),
                )
            )
            return

        financial_gain = (wait_saved / 3600.0) * hourly_rate
        replay_step_ratio = (replayed_steps / total_steps) if total_steps > 0 else 0.0
        replay_duration_ratio = (resource_saved / total_project_time) if total_project_time > 0 else 0.0

        # Scope header
        console.print()
        scope_line = Text()
        scope_line.append("stats", style="bold lg.muted")
        scope_line.append("  ·  ", style="lg.muted")
        scope_line.append(scope, style="bold lg.brand")
        scope_line.append(f"  ·  {run_count} run(s)  ·  {total_steps} steps ({replayed_steps} replayed)", style="lg.muted")
        console.print(scope_line)
        console.print()

        # ROI KPIs
        kpi_table = Table(
            box=TABLE_BOX,
            show_edge=False,
            show_lines=False,
            expand=False,
            pad_edge=False,
            header_style="lg.header",
        )
        kpi_table.add_column("metric", style="lg.muted")
        kpi_table.add_column("value", justify="right")
        kpi_table.add_row("Resource time saved", Text(_format_human_duration(resource_saved), style="lg.brand"))
        kpi_table.add_row("Human wait saved", Text(_format_human_duration(wait_saved), style="lg.success bold"))
        kpi_table.add_row("Total compute time", Text(_format_human_duration(total_project_time), style=""))
        kpi_table.add_row("Efficiency ratio", Text(f"{efficiency_ratio * 100:.1f}%", style="lg.success"))
        kpi_table.add_row("Financial gain (est.)", Text(_format_currency(financial_gain), style="lg.warning"))
        console.print(kpi_table)
        console.print()

        # Progress bars
        ratios_table = Table(
            box=TABLE_BOX,
            show_edge=False,
            show_lines=False,
            expand=False,
            pad_edge=False,
            header_style="lg.header",
        )
        ratios_table.add_column("indicator", style="lg.muted")
        ratios_table.add_column("bar", justify="center")
        ratios_table.add_column("ratio", justify="right")
        ratios_table.add_row(*_ratio_progress_row("wait saved / total", efficiency_ratio, color="bright_green"))
        ratios_table.add_row(*_ratio_progress_row("replayed (duration)", replay_duration_ratio, color="bright_cyan"))
        ratios_table.add_row(*_ratio_progress_row("replayed (steps)", replay_step_ratio, color="magenta"))
        console.print(ratios_table)
        console.print()

        if selected_run_id:
            run_table = Table(
                box=TABLE_BOX,
                show_edge=False,
                show_lines=False,
                expand=False,
                pad_edge=False,
                header_style="lg.header",
            )
            run_table.add_column("signal", style="lg.muted")
            run_table.add_column("value", justify="right")
            run_table.add_row("Cache coverage (duration)", Text(f"{replay_duration_ratio * 100:.1f}% replayed", style="lg.brand"))
            run_table.add_row("Cache coverage (steps)", Text(f"{replay_step_ratio * 100:.1f}% replayed", style="lg.brand"))
            run_table.add_row("Wait saved", Text(_format_human_duration(wait_saved), style="lg.success"))
            run_table.add_row("Resource saved", Text(_format_human_duration(resource_saved), style="lg.success"))
            console.print(run_table)
            console.print()

        # Token efficiency
        token_kpis = aggregate_token_efficiency(
            conn,
            project=project,
            input_id=input_id,
            run_id=selected_run_id,
        )
        tokens_spent_live = int(token_kpis["tokens_spent_live"])
        tokens_saved_cache = int(token_kpis["tokens_saved_cache"])
        total_bypass_rate = float(token_kpis["total_bypass_rate"])
        token_total = tokens_spent_live + tokens_saved_cache

        token_table = Table(
            box=TABLE_BOX,
            show_edge=False,
            show_lines=False,
            expand=False,
            pad_edge=False,
            header_style="lg.header",
        )
        token_table.add_column("tokens", style="lg.muted")
        token_table.add_column("value", justify="right")
        token_table.add_row("Spent (live)", Text(f"{tokens_spent_live:,}".replace(",", " "), style="lg.warning"))
        token_table.add_row("Saved (cache)", Text(f"{tokens_saved_cache:,}".replace(",", " "), style="lg.success"))
        token_table.add_row("Bypass rate", Text(f"{total_bypass_rate * 100:.1f}%", style="lg.brand"))
        token_table.add_row("Total", Text(f"{token_total:,}".replace(",", " "), style=""))
        console.print(token_table)
        console.print()

        # Top docs
        top_docs = top_inputs_by_savings(
            conn,
            project=project,
            input_id=input_id,
            run_id=selected_run_id,
            limit=3,
        )

        if top_docs:
            top_table = Table(
                box=TABLE_BOX,
                show_edge=False,
                show_lines=False,
                expand=False,
                pad_edge=False,
                header_style="lg.header",
            )
            top_table.add_column("#", justify="right", style="lg.muted")
            top_table.add_column("input_id", style="lg.brand")
            top_table.add_column("runs", justify="right", style="lg.muted")
            top_table.add_column("wait saved", justify="right")
            top_table.add_column("resource saved", justify="right")
            for idx, row in enumerate(top_docs, start=1):
                top_table.add_row(
                    str(idx),
                    row["input_id"],
                    str(row["run_count"]),
                    Text(_format_human_duration(row["wait_time_saved"]), style="lg.success"),
                    Text(_format_human_duration(row["resource_time_saved"]), style="lg.brand"),
                )
            top_label = Text("top inputs by savings", style="lg.muted")
            console.print(top_label)
            console.print(top_table)
            console.print()

    finally:
        conn.close()


@app.command()
def clean() -> None:
    """Propose le nettoyage des runs échoués et assets orphelins."""
    conn = _connect_db()
    if conn is None:
        raise typer.Exit(1)

    try:
        failed_runs = conn.execute("SELECT run_id FROM runs WHERE UPPER(status) IN ('FAILED', 'FAILURE', 'ERROR')").fetchall()
        failed_count = len(failed_runs)

        db_blob_paths: set[str] = set()
        rows = conn.execute("SELECT output_json FROM steps").fetchall()
        for row in rows:
            output = _parse_json(row["output_json"])
            for blob in _extract_blobs(output):
                p = blob.get("path")
                if isinstance(p, str):
                    db_blob_paths.add(str(Path(p)))

        ASSETS_DIR.mkdir(parents=True, exist_ok=True)
        all_assets = [p for p in ASSETS_DIR.rglob("*") if p.is_file()]
        orphan_assets = [p for p in all_assets if str(p) not in db_blob_paths]

        console.print()
        preview = Table(
            box=TABLE_BOX,
            show_edge=False,
            show_lines=False,
            expand=False,
            pad_edge=False,
            header_style="lg.header",
        )
        preview.add_column("item", style="lg.muted")
        preview.add_column("count", justify="right")
        preview.add_row("Failed runs", Text(str(failed_count), style="lg.error" if failed_count else "lg.muted"))
        preview.add_row("Orphan assets", Text(str(len(orphan_assets)), style="lg.warning" if orphan_assets else "lg.muted"))
        console.print(preview)
        console.print()

        if failed_count > 0 and typer.confirm("Delete failed runs (and their steps)?", default=False):
            ids = [r["run_id"] for r in failed_runs]
            placeholders = ",".join("?" for _ in ids)
            conn.execute(f"DELETE FROM steps WHERE run_id IN ({placeholders})", ids)
            conn.execute(f"DELETE FROM runs WHERE run_id IN ({placeholders})", ids)
            conn.commit()
            done = Text()
            done.append("  ✓ deleted  ", style="lg.badge.success")
            done.append(f"  {failed_count} failed run(s)", style="lg.muted")
            console.print(done)

        if orphan_assets and typer.confirm("Delete orphan assets?", default=False):
            deleted = 0
            for p in orphan_assets:
                try:
                    p.unlink(missing_ok=True)
                    deleted += 1
                except Exception:
                    continue
            done = Text()
            done.append("  ✓ deleted  ", style="lg.badge.success")
            done.append(f"  {deleted} orphan asset(s)", style="lg.muted")
            console.print(done)

        console.print()

    finally:
        conn.close()


@mcp_app.command("start")
def mcp_start(
    db_path: str | None = typer.Option(
        None, "--db-path", help="Chemin vers logram.db (défaut: LOGRAM_DB_PATH ou .logram/logram.db)."
    ),
) -> None:
    """Lance le serveur MCP Logram en mode stdio (pour Claude Desktop ou Cursor)."""
    if db_path:
        os.environ["LOGRAM_DB_PATH"] = db_path

    effective_db = os.environ.get("LOGRAM_DB_PATH", ".logram/logram.db")

    console.print()
    info = Text()
    info.append("logram mcp", style="bold lg.brand")
    info.append("  ·  db ", style="lg.muted")
    info.append(effective_db, style="lg.muted")
    console.print(info)
    console.print()

    try:
        from logram.mcp_server import main as mcp_main
    except ImportError as exc:
        console.print(
            Panel(
                Text.assemble(
                    ("fastmcp not installed: ", "lg.muted"),
                    (str(exc), "lg.error"),
                    ("\n\npip install fastmcp", "lg.brand"),
                ),
                box=PANEL_BOX,
                border_style="lg.error",
                padding=(1, 2),
            )
        )
        raise typer.Exit(1) from exc

    mcp_main()


@mcp_app.command("config")
def mcp_config(
    db_path: str | None = typer.Option(
        None, "--db-path", help="Chemin absolu vers logram.db à inclure dans la config."
    ),
) -> None:
    """Affiche le bloc JSON à copier dans Cursor (Settings › MCP) ou Claude Desktop."""
    python_bin = sys.executable
    effective_db = db_path or os.environ.get("LOGRAM_DB_PATH", str(Path(".logram") / "logram.db"))

    config: dict[str, Any] = {
        "logram": {
            "command": python_bin,
            "args": ["-m", "logram.mcp_server"],
            "env": {
                "LOGRAM_DB_PATH": str(Path(effective_db).resolve()),
            },
        }
    }

    config_json = json.dumps(config, indent=2)

    console.print()
    console.print(
        Panel(
            Syntax(config_json, "json", theme="monokai"),
            title="[lg.muted]► mcp config[/lg.muted]",
            title_align="left",
            box=PANEL_BOX,
            border_style="lg.muted",
        )
    )
    console.print()

    targets = Text()
    targets.append("Claude Desktop  ", style="lg.muted")
    targets.append("~/Library/Application Support/Claude/claude_desktop_config.json\n", style="lg.brand")
    targets.append("Cursor          ", style="lg.muted")
    targets.append("Settings › MCP › Add server\n", style="lg.brand")
    targets.append("Claude Code     ", style="lg.muted")
    targets.append(".claude/settings.json › mcpServers", style="lg.brand")
    console.print(targets)
    console.print()

    if _copy_to_clipboard(config_json):
        copied = Text()
        copied.append("  ✓ copied  ", style="lg.badge.success")
        copied.append("  JSON config copied to clipboard", style="lg.muted")
        console.print(copied)
        console.print()


# ---------------------------------------------------------------------------
# Agent rules helpers
# ---------------------------------------------------------------------------


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


def _update_gitignore(cwd: Path) -> str:
    """Ensure Logram entries are in .gitignore. Returns a brief status string."""
    gitignore = cwd / ".gitignore"
    try:
        existing = gitignore.read_text(encoding="utf-8") if gitignore.exists() else ""
        missing = [e for e in _GITIGNORE_ENTRIES if e not in existing]
        if not missing:
            return "up to date"
        block = "\n# Logram local trace store (DO NOT COMMIT)\n" + "\n".join(missing) + "\n"
        with gitignore.open("a", encoding="utf-8") as f:
            f.write(block)
        return "added " + "  ".join(missing)
    except Exception as exc:
        return f"error: {exc}"


# ---------------------------------------------------------------------------
# lg init
# ---------------------------------------------------------------------------


@app.command("init")
def init_project(
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing agent rules files."),
) -> None:
    """Bootstrap Logram in the current project: write agent rules files and update .gitignore."""
    cwd = Path.cwd()
    console.print()

    rules_results = _write_agent_rules_files(cwd, force=force)
    gi_status = _update_gitignore(cwd)

    report = Table(
        box=TABLE_BOX,
        show_edge=False,
        show_lines=False,
        expand=False,
        pad_edge=False,
        header_style="lg.header",
    )
    report.add_column("file", style="lg.brand")
    report.add_column("status", justify="left")

    for dest_name, status in rules_results:
        if "written" in status:
            badge = Text(" ✓ written ", style="lg.badge.success")
        elif "skipped" in status:
            badge = Text(" – exists  ", style="lg.muted")
        else:
            badge = Text(f" ✗ {status} ", style="lg.badge.failed")
        report.add_row(dest_name, badge)

    if "error" in gi_status:
        gi_badge = Text(f" ✗ {gi_status} ", style="lg.badge.failed")
    elif "up to date" in gi_status:
        gi_badge = Text(" – up to date ", style="lg.muted")
    else:
        gi_badge = Text(f" ✓ {gi_status} ", style="lg.badge.success")
    report.add_row(".gitignore", gi_badge)

    console.print(
        Panel(
            report,
            title="[lg.muted]logram init[/lg.muted]",
            title_align="left",
            box=PANEL_BOX,
            border_style="lg.muted",
            padding=(0, 1),
        )
    )
    console.print()

    any_written = any("written" in s for _, s in rules_results)
    if any_written:
        hint = Text()
        hint.append("  agent rules files written — ", style="lg.muted")
        hint.append("commit them alongside your pipeline code", style="lg.brand")
        console.print(hint)
        console.print()


# ---------------------------------------------------------------------------
# MCP install helpers
# ---------------------------------------------------------------------------

def _claude_desktop_config_path() -> Path | None:
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / "Claude" / "claude_desktop_config.json"
    if os.name == "nt":
        appdata = os.environ.get("APPDATA", "")
        if appdata:
            return Path(appdata) / "Claude" / "claude_desktop_config.json"
    return None


def _resolve_python_for_mcp() -> tuple[str, str]:
    project_root = str(Path(__file__).parent.parent)

    try:
        r = subprocess.run(
            ["poetry", "env", "info", "-p"],
            capture_output=True,
            text=True,
            cwd=project_root,
        )
        if r.returncode == 0:
            poetry_python = str(Path(r.stdout.strip()) / "bin" / "python")
            if Path(poetry_python).exists():
                return poetry_python, "Poetry venv"
    except FileNotFoundError:
        pass

    check = subprocess.run(
        [sys.executable, "-c", "import logram.mcp_server"],
        capture_output=True,
        cwd=str(Path.home()),
    )
    if check.returncode == 0:
        return sys.executable, "current venv"

    raise RuntimeError(
        "No Python found that can import logram.mcp_server.\n"
        "Run from the project: poetry run logram mcp install"
    )


def _logram_mcp_entry(python: str, db: Path) -> dict[str, Any]:
    return {
        "command": python,
        "args": ["-m", "logram.mcp_server"],
        "env": {"LOGRAM_DB_PATH": str(db)},
    }


def _install_claude_code(python: str, db: Path) -> tuple[bool, str]:
    import shutil as _shutil

    if not _shutil.which("claude"):
        return False, "command `claude` not found in PATH"

    subprocess.run(
        ["claude", "mcp", "remove", "logram", "--scope", "local"],
        capture_output=True,
    )

    result = subprocess.run(
        [
            "claude", "mcp", "add", "logram",
            "--scope", "local",
            "-e", f"LOGRAM_DB_PATH={db}",
            "--",
            python, "-m", "logram.mcp_server",
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        return True, "installed → ~/.claude.json (scope local)"

    detail = (result.stderr or result.stdout or f"exit {result.returncode}").strip()
    return False, f"failed: {detail}"


def _install_claude_desktop(python: str, db: Path) -> tuple[bool, str]:
    config_path = _claude_desktop_config_path()
    if config_path is None:
        return False, "Claude Desktop not supported on this system"

    new_entry = _logram_mcp_entry(python, db)

    existing: dict[str, Any] = {}
    if config_path.exists():
        try:
            with config_path.open() as f:
                existing = json.load(f)
        except Exception as exc:
            return False, f"could not read config: {exc}"

    mcp_servers: dict[str, Any] = existing.get("mcpServers", {})
    current_entry = mcp_servers.get("logram")

    if current_entry == new_entry:
        return True, "already up to date — no changes"

    if current_entry is not None:
        old_lines = json.dumps({"logram": current_entry}, indent=2).splitlines()
        new_lines = json.dumps({"logram": new_entry}, indent=2).splitlines()
        diff_lines = list(
            difflib.unified_diff(old_lines, new_lines, fromfile="current", tofile="new", lineterm="")
        )
        if diff_lines:
            console.print(
                Panel(
                    Syntax("\n".join(diff_lines), "diff", theme="monokai"),
                    title="[lg.muted]► Claude Desktop config diff[/lg.muted]",
                    title_align="left",
                    box=PANEL_BOX,
                    border_style="lg.warning",
                )
            )
        if not typer.confirm("Overwrite existing Claude Desktop config?", default=False):
            return False, "aborted — config unchanged"

    mcp_servers["logram"] = new_entry
    existing["mcpServers"] = mcp_servers
    config_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with config_path.open("w") as f:
            json.dump(existing, f, indent=2, ensure_ascii=False)
            f.write("\n")
        return True, f"updated → {config_path}"
    except Exception as exc:
        return False, f"could not write: {exc}"


# ---------------------------------------------------------------------------
# lg mcp install
# ---------------------------------------------------------------------------

_MCP_TOOLS = [
    ("list_runs", "List recent runs"),
    ("get_investigation_brief", "Diagnostic brief for a failed run"),
    ("get_step_source", "Code + globals captured at runtime"),
    ("analyze_logic_divergence", "Diff between two runs"),
    ("run_surgical_replay", "Validate a fix in ~2s"),
    ("verify_against_golden_dataset", "Certify no regression"),
]


@mcp_app.command("install")
def mcp_install(
    db_path: str | None = typer.Option(
        None, "--db-path", help="Chemin absolu vers logram.db (défaut: LOGRAM_DB_PATH ou .logram/logram.db)."
    ),
) -> None:
    """Installe automatiquement le serveur MCP Logram dans les agents de coding détectés."""
    import shutil as _shutil

    if db_path:
        os.environ["LOGRAM_DB_PATH"] = db_path
    db = Path(os.environ.get("LOGRAM_DB_PATH", str(DB_PATH))).resolve()

    try:
        import fastmcp  # noqa: F401
    except ImportError:
        console.print()
        console.print(
            Panel(
                Text.assemble(
                    ("fastmcp not installed.\n\n", "bold lg.error"),
                    ("pip install fastmcp", "lg.brand"),
                ),
                box=PANEL_BOX,
                border_style="lg.error",
                padding=(1, 2),
            )
        )
        raise typer.Exit(1)

    claude_code_ok = bool(_shutil.which("claude"))
    desktop_path = _claude_desktop_config_path()
    claude_desktop_ok = desktop_path is not None

    if not claude_code_ok and not claude_desktop_ok:
        console.print()
        console.print(
            Panel(
                Text("No compatible agent detected on this system.", style="lg.muted"),
                box=PANEL_BOX,
                border_style="lg.error",
                padding=(0, 2),
            )
        )
        raise typer.Exit(1)

    try:
        python, python_label = _resolve_python_for_mcp()
    except RuntimeError as exc:
        console.print()
        console.print(
            Panel(
                Text(str(exc), style="lg.muted"),
                box=PANEL_BOX,
                border_style="lg.error",
                padding=(1, 2),
            )
        )
        raise typer.Exit(1)

    console.print()
    console.print(Text(f"  python  {python}  ({python_label})", style="lg.muted"))
    console.print()

    menu_lines: list[str] = []
    choices: list[str] = []
    if claude_code_ok:
        menu_lines.append("  1  Claude Code    (~/.claude.json, scope local)")
        choices.append("1")
    if claude_desktop_ok:
        menu_lines.append(f"  2  Claude Desktop  ({desktop_path})")
        choices.append("2")
    if claude_code_ok and claude_desktop_ok:
        menu_lines.append("  3  Both")
        choices.append("3")

    install_menu = Table.grid(padding=(0, 2))
    for line in menu_lines:
        parts = line.split("  ", 2)
        t = Text()
        if len(parts) >= 3:
            t.append(f"  {parts[1].strip()}  ", style="bold lg.brand")
            t.append(parts[2], style="lg.muted")
        else:
            t.append(line, style="lg.muted")
        install_menu.add_row(t)

    console.print(
        Panel(
            install_menu,
            title="[lg.muted]mcp install[/lg.muted]",
            title_align="left",
            box=PANEL_BOX,
            border_style="lg.muted",
            padding=(0, 1),
        )
    )
    console.print()

    choice = typer.prompt(f"Choose [{'/'.join(choices)}]", default=choices[-1])
    if choice not in choices:
        console.print(Text(f"  Invalid choice: {choice!r}", style="lg.error"))
        raise typer.Exit(1)

    results: list[tuple[str, bool, str]] = []
    if choice in ("1", "3"):
        results.append(("Claude Code", *_install_claude_code(python, db)))
    if choice in ("2", "3"):
        results.append(("Claude Desktop", *_install_claude_desktop(python, db)))

    console.print()
    report = Table(
        box=TABLE_BOX,
        show_edge=False,
        show_lines=False,
        expand=False,
        pad_edge=False,
        header_style="lg.header",
    )
    report.add_column("agent", style="lg.brand")
    report.add_column("status", justify="center")
    report.add_column("detail", style="lg.muted")
    for agent, ok, msg in results:
        report.add_row(
            agent,
            Text(" ✓ ok ", style="lg.badge.success") if ok else Text(" ✗ failed ", style="lg.badge.failed"),
            msg,
        )
    console.print(report)
    console.print()

    if any(ok for _, ok, _ in results):
        tools_table = Table(
            box=TABLE_BOX,
            show_edge=False,
            show_lines=False,
            expand=False,
            pad_edge=False,
        )
        tools_table.add_column("tool", style="lg.brand")
        tools_table.add_column("description", style="lg.muted")
        for name, desc in _MCP_TOOLS:
            tools_table.add_row(name, desc)

        console.print(
            Panel(
                tools_table,
                title="[lg.muted]available mcp tools[/lg.muted]",
                title_align="left",
                box=PANEL_BOX,
                border_style="lg.muted",
                padding=(0, 1),
            )
        )
        console.print()

        # Write agent rules files to cwd (skip silently if they already exist).
        rules_results = _write_agent_rules_files(Path.cwd())
        written = [(n, s) for n, s in rules_results if "written" in s]
        if written:
            rules_table = Table(
                box=TABLE_BOX,
                show_edge=False,
                show_lines=False,
                expand=False,
                pad_edge=False,
            )
            rules_table.add_column("file", style="lg.brand")
            rules_table.add_column("", style="lg.muted")
            for dest_name, _ in written:
                rules_table.add_row(dest_name, "written")
            console.print(
                Panel(
                    rules_table,
                    title="[lg.muted]agent rules[/lg.muted]",
                    title_align="left",
                    box=PANEL_BOX,
                    border_style="lg.muted",
                    padding=(0, 1),
                )
            )
            console.print()


def main() -> None:
    app()


if __name__ == "__main__":
    main()
