# ruff: noqa: BLE001
# pylint: disable=broad-exception-caught

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import typer
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from ..theme import (
    PANEL_BOX,
    TABLE_BOX,
    console,
    hint_line,
    status_badge,
)
from ._app import app, golden_app
from ._common import _connect_db, _outputs_by_step_name, _parse_json


@app.command()
def replay(
    script_py: str,
    force: list[str] | None = typer.Option(None, "--force", "-f", help="Step(s) to force live (repeatable: --force a --force b)."),
    from_step: str | None = typer.Option(None, "--from", help="Run this step and every later step live."),
) -> None:
    """Rerun a script in replay mode (LOGRAM_REPLAY=true)."""
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

@golden_app.command("add")
def golden_add(run_id: str) -> None:
    """Tag a run as a golden (reference) run."""
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
    """Replay a script on every golden input and report regressions (exit 1 on any)."""
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

        failed_inputs = 0
        for input_id, baseline in baseline_by_input.items():
            before = conn.execute("SELECT MAX(created_at) AS ts FROM runs").fetchone()["ts"]

            env = os.environ.copy()
            env["LOGRAM_REPLAY"] = "true"
            env["LOGRAM_INPUT_ID"] = str(input_id)

            proc = subprocess.run([sys.executable, str(script_path)], env=env, check=False)
            if proc.returncode != 0:
                failed_inputs += 1
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
                failed_inputs += 1
                report.add_row(
                    input_id,
                    baseline["run_id"],
                    "-",
                    Text(" ? unknown ", style="dim"),
                    "no new run detected",
                )
                continue

            # A step can run many times (one call per tile, per page...): compare
            # every call, independently of the order parallel workers finished in.
            base_steps = _outputs_by_step_name(conn, baseline["run_id"])
            new_steps = _outputs_by_step_name(conn, new_run["run_id"])
            regressions = sum(
                1 for name in set(base_steps) | set(new_steps) if base_steps.get(name) != new_steps.get(name)
            )

            if regressions == 0:
                report.add_row(
                    input_id,
                    baseline["run_id"],
                    new_run["run_id"],
                    status_badge("SUCCESS"),
                    "no regression",
                )
            else:
                failed_inputs += 1
                report.add_row(
                    input_id,
                    baseline["run_id"],
                    new_run["run_id"],
                    status_badge("FAILED"),
                    f"{regressions} step(s) differ",
                )

        console.print(report)
        console.print()
        if failed_inputs:
            raise typer.Exit(1)

    finally:
        conn.close()
