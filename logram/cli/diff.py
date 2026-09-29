# ruff: noqa: BLE001
# pylint: disable=broad-exception-caught

from __future__ import annotations

from typing import Any

import typer
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text
from rich.tree import Tree

from ..analysis import find_all_divergences
from ..theme import (
    PANEL_BOX,
    TABLE_BOX,
    console,
    hint_line,
)
from ._app import app
from ._common import (
    _RUN_ID_ARG_SETTINGS,
    _complete_run_id,
    _connect_db,
    _infer_aliased_missing_step_names,
    _json_text,
    _logic_registry_globals_expr,
    _parse_json,
    _print_git_style_diff,
    _render_text_or_json_diff,
    _resolve_run_id,
    _step_dict_by_name,
    _step_rows_for_alias_resolution,
    _unified_diff_text_ctx,
)


@app.command(context_settings=_RUN_ID_ARG_SETTINGS)
def diff(
    run_a: str | None = typer.Argument(None, autocompletion=_complete_run_id),
    run_b: str | None = typer.Argument(None, autocompletion=_complete_run_id),
    ss: bool = typer.Option(False, "--ss", help="Compare the last run with the last successful run on the same input_id."),
    code: bool = typer.Option(False, "--code", "-c", help="Only show source code differences."),
    globals_only: bool = typer.Option(False, "--globals", "-g", help="Only show globals/prompt differences."),
    inputs: bool = typer.Option(False, "--inputs", "-i", help="Only show input differences."),
    outputs: bool = typer.Option(False, "--outputs", "-o", help="Only show output differences."),
) -> None:
    """Compare two runs (code, globals, inputs, outputs). Accepts: last, fail, -1… · --ss: last run vs last success."""
    conn = _connect_db()
    if conn is None:
        raise typer.Exit(1)

    try:
        # --- shorthand resolution ---
        if ss:
            _last = conn.execute("SELECT run_id, input_id FROM runs ORDER BY created_at DESC LIMIT 1").fetchone()
            if not _last:
                console.print()
                console.print(Panel(Text("No runs found.", style="lg.muted"), box=PANEL_BOX, border_style="lg.muted", padding=(0, 2)))
                raise typer.Exit(1)
            _last_success = conn.execute(
                "SELECT run_id FROM runs WHERE input_id = ? AND UPPER(status) IN ('SUCCESS','CACHE_HIT','REPLAY_HIT') ORDER BY created_at DESC LIMIT 1",
                (_last["input_id"],),
            ).fetchone()
            if not _last_success:
                console.print()
                console.print(Panel(Text("No SUCCESS run found for this input_id.", style="lg.muted"), box=PANEL_BOX, border_style="lg.muted", padding=(0, 2)))
                raise typer.Exit(1)
            run_a = _last["run_id"]
            run_b = _last_success["run_id"]
            if run_a == run_b:
                console.print()
                console.print(Panel(Text("Last run is already a SUCCESS — nothing to compare with --ss.", style="lg.muted"), box=PANEL_BOX, border_style="lg.muted", padding=(0, 2)))
                raise typer.Exit(0)
        elif run_a == "last" and run_b is None:
            _last = conn.execute("SELECT run_id, input_id FROM runs ORDER BY created_at DESC LIMIT 1").fetchone()
            if not _last:
                console.print()
                console.print(Panel(Text("No runs found.", style="lg.muted"), box=PANEL_BOX, border_style="lg.muted", padding=(0, 2)))
                raise typer.Exit(1)
            _prev = conn.execute(
                "SELECT run_id FROM runs WHERE input_id = ? AND run_id != ? ORDER BY created_at DESC LIMIT 1",
                (_last["input_id"], _last["run_id"]),
            ).fetchone()
            if not _prev:
                console.print()
                console.print(Panel(Text("Only one run found for this input_id — nothing to compare.", style="lg.muted"), box=PANEL_BOX, border_style="lg.muted", padding=(0, 2)))
                raise typer.Exit(1)
            run_a = _last["run_id"]
            run_b = _prev["run_id"]
        elif run_a is not None and run_b is None:
            console.print()
            console.print(Panel(Text("Provide two run_ids, use 'last' alone, or use --ss.", style="lg.muted"), box=PANEL_BOX, border_style="lg.muted", padding=(0, 2)))
            raise typer.Exit(1)
        elif run_a is None or run_b is None:
            console.print()
            console.print(Panel(Text("Usage: lg diff last  ·  lg diff --ss  ·  lg diff <run_a> <run_b>", style="lg.muted"), box=PANEL_BOX, border_style="lg.muted", padding=(0, 2)))
            raise typer.Exit(1)
        else:
            run_a = _resolve_run_id(conn, run_a)
            run_b = _resolve_run_id(conn, run_b)

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
        console.print(hint_line(
            f"lg inspect {run_a} · lg inspect {run_b} for step details",
            "lg diff --ss to compare last vs last-success",
            "lg stats for ROI",
        ))
        console.print()

    finally:
        conn.close()

@app.command()
def recover(logic_hash: str) -> None:
    """Show the exact source code and globals recorded for a logic_hash."""
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

@app.command(context_settings=_RUN_ID_ARG_SETTINGS)
def restore(run_id: str = typer.Argument(..., autocompletion=_complete_run_id)) -> None:
    """Print the code and constants of a run as blocks to copy back. Accepts: last, fail, -1…"""
    conn = _connect_db()
    if conn is None:
        raise typer.Exit(1)

    try:
        run_id = _resolve_run_id(conn, run_id)
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
