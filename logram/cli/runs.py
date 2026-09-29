# ruff: noqa: BLE001
# pylint: disable=broad-exception-caught

from __future__ import annotations

import sqlite3
import time
from collections import defaultdict
from typing import Any

import typer
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text
from rich.tree import Tree

from ..theme import (
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
from ._app import app
from ._common import (
    _RUN_ID_ARG_SETTINGS,
    StepRecord,
    _complete_run_id,
    _connect_db,
    _copy_to_clipboard,
    _extract_blobs,
    _format_dt,
    _format_duration,
    _json_text,
    _load_steps_for_run,
    _open_with_system,
    _parse_json,
    _relative_time,
    _resolve_blob_file,
    _resolve_run_id,
    _top_level_steps,
)

# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


@app.command("list")
def list_runs(
    group_by_input: bool = typer.Option(False, "--group-by-input", help="Group runs by input_id."),
    project: str | None = typer.Option(None, "--project", help="Filtrer par projet."),
    full: bool = typer.Option(False, "--full", help="Show full field values (no ellipsis)."),
    copy_field: str | None = typer.Option(
        None,
        "--copy-field",
        help="Copy one field of a row to the clipboard (run_id|project|input_id|version_id|status|duration|created_at).",
    ),
    copy_index: int = typer.Option(1, "--copy-index", min=1, help="Row index for --copy-field (1 = first row)."),
) -> None:
    """List recorded runs."""
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

@app.command(context_settings=_RUN_ID_ARG_SETTINGS)
def inspect(run_id: str = typer.Argument(..., autocompletion=_complete_run_id)) -> None:
    """Show the step tree of a run. Accepts: last, fail, -1, -2…"""
    conn = _connect_db()
    if conn is None:
        raise typer.Exit(1)

    try:
        run_id = _resolve_run_id(conn, run_id)
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
        # Nested steps run inside their parent: count each root once.
        roots = _top_level_steps(steps)
        total_dur = sum(s.duration for s in roots)
        live_dur = sum(
            s.duration
            for s in roots
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
        console.print(hint_line("lg view <step_id> to inspect", "lg diff last to compare with previous", "lg live to watch runs"))
        console.print()

    finally:
        conn.close()

@app.command()
def view(step_id: str) -> None:
    """Show one step: inputs, output, error and blobs."""
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

@app.command("open")
def open_step(step_id: str) -> None:
    """Open a step's image blob in the system viewer."""
    conn = _connect_db()
    if conn is None:
        raise typer.Exit(1)

    try:
        row = conn.execute("SELECT inputs_json, output_json FROM steps WHERE step_id = ?", (step_id,)).fetchone()
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

        # Inputs first: that is what a VLM step actually received.
        blobs = _extract_blobs(_parse_json(row["inputs_json"])) + _extract_blobs(_parse_json(row["output_json"]))
        if not blobs:
            console.print()
            console.print(Text("  No blob detected on this step.", style="lg.muted"))
            console.print()
            return

        for blob in blobs:
            path = _resolve_blob_file(blob)
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
def live(
    interval: int = typer.Option(500, "--interval", min=100, help="Polling interval in milliseconds."),
) -> None:
    """Live step tree of the running pipeline (polls the store)."""
    from rich.console import Group
    from rich.live import Live

    _SPINNER = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    _tick = 0

    conn = _connect_db()
    if conn is None:
        raise typer.Exit(1)

    def _build() -> Any:
        nonlocal _tick
        _tick += 1
        spin = _SPINNER[_tick % len(_SPINNER)]

        active = conn.execute(
            "SELECT * FROM runs WHERE UPPER(status) IN ('RUNNING','STARTED') ORDER BY created_at DESC LIMIT 1"
        ).fetchone()

        run = active
        is_active = True
        if run is None:
            run = conn.execute("SELECT * FROM runs ORDER BY created_at DESC LIMIT 1").fetchone()
            is_active = False

        if run is None:
            return Text(f"  {spin} No runs yet. Waiting…", style="lg.muted")

        run_id = run["run_id"]
        steps = _load_steps_for_run(conn, run_id)

        header = Text()
        header.append(run_id, style="bold lg.brand")
        header.append("  ·  ", style="lg.muted")
        header.append(run["project"] or "-", style="lg.muted")
        header.append("  ·  ", style="lg.muted")
        header.append(_format_dt(run["created_at"]), style="lg.muted")
        header.append("  ·  ", style="lg.muted")
        if is_active:
            header.append(" ⚡ live ", style="lg.badge.live")
        else:
            header.append_text(status_badge(run["status"]))

        if not steps:
            if is_active:
                return Group(header, Text(""), Text(f"  {spin} starting…", style="lg.warning"))
            return Group(header, Text("  No steps recorded.", style="lg.muted"))

        by_parent: dict[str | None, list[StepRecord]] = defaultdict(list)
        by_id: dict[str, StepRecord] = {}
        for s in steps:
            by_parent[s.parent_step_id].append(s)
            by_id[s.step_id] = s
        for children in by_parent.values():
            children.sort(key=lambda x: x.timestamp)

        tree = Tree(Text(run["project"] or run_id, style="bold"), guide_style="lg.muted")
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

        total_dur = sum(s.duration for s in _top_level_steps(steps))
        footer = Text(f"  {len(steps)} step(s)  ·  {total_dur:.2f}s", style="lg.muted")

        parts: list[Any] = [header, Text(""), tree, Text(""), footer]
        if is_active:
            parts.append(Text(f"  {spin} running…", style="lg.warning"))
        else:
            parts.append(Text("  Waiting for new run…", style="lg.muted"))

        return Group(*parts)

    try:
        refresh = max(1, 1000 // interval)
        with Live(console=console, refresh_per_second=refresh) as live_view:
            while True:
                try:
                    live_view.update(_build())
                except Exception:
                    pass
                time.sleep(interval / 1000.0)
    except KeyboardInterrupt:
        pass
    finally:
        conn.close()

    console.print()
    console.print(hint_line("lg inspect <run_id> to see full tree", "lg list to browse runs", "lg diff last to compare last two"))
    console.print()
