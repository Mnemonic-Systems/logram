# ruff: noqa: BLE001
# pylint: disable=broad-exception-caught

from __future__ import annotations

import typer
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from ..metrics import aggregate_roi_stats, aggregate_token_efficiency, top_inputs_by_savings
from ..theme import (
    PANEL_BOX,
    TABLE_BOX,
    console,
)
from ._app import app
from ._common import (
    _RUN_ID_ARG_SETTINGS,
    _complete_run_id,
    _connect_db,
    _format_currency,
    _format_human_duration,
    _ratio_progress_row,
    _resolve_run_id,
)


@app.command(context_settings=_RUN_ID_ARG_SETTINGS)
def stats(
    run_id_arg: str | None = typer.Argument(None, metavar="[RUN_ID]", autocompletion=_complete_run_id),
    run_id: str | None = typer.Option(None, "--run-id", help="Stats for one run."),
    project: str | None = typer.Option(None, "--project", help="Only runs of this project."),
    input_id: str | None = typer.Option(None, "--input-id", help="Only runs on this input_id."),
    hourly_rate: float = typer.Option(10.0, "--hourly-rate", min=0.0, help="Hourly rate used to value the time saved."),
) -> None:
    """Time, tokens and cost saved by replay (global, per project, input or run)."""
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
        if selected_run_id:
            selected_run_id = _resolve_run_id(conn, selected_run_id)

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
