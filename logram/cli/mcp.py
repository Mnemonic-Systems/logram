# ruff: noqa: BLE001
# pylint: disable=broad-exception-caught

from __future__ import annotations

import difflib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import typer
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text

from ..storage import resolve_db_path
from ..theme import (
    PANEL_BOX,
    TABLE_BOX,
    console,
)
from ._app import mcp_app
from ._common import (
    _PACKAGE_DIR,
    _claude_desktop_config_path,
    _copy_to_clipboard,
    _store_path,
    _write_agent_rules_files,
)


@mcp_app.command("start")
def mcp_start(
    db_path: str | None = typer.Option(
        None, "--db-path", help="Path to logram.db (default: LOGRAM_DB_PATH or <project>/.logram/logram.db)."
    ),
) -> None:
    """Start the MCP server on stdio (for Claude Code, Claude Desktop or Cursor)."""
    if db_path:
        os.environ["LOGRAM_DB_PATH"] = db_path

    effective_db = str(resolve_db_path())

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
                    ("\n\npip install \"logram-sdk[mcp]\"", "lg.brand"),
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
        None, "--db-path", help="Absolute path to logram.db to put in the config."
    ),
) -> None:
    """Print the JSON block to add to Cursor (Settings › MCP) or Claude Desktop."""
    python_bin = sys.executable
    effective_db = db_path or str(resolve_db_path())

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
# MCP install helpers
# ---------------------------------------------------------------------------


def _resolve_python_for_mcp() -> tuple[str, str]:
    project_root = str(_PACKAGE_DIR.parent)

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
        None, "--db-path", help="Absolute path to logram.db (default: LOGRAM_DB_PATH or <project>/.logram/logram.db)."
    ),
) -> None:
    """Register the MCP server with the coding agents found on this machine."""
    import shutil as _shutil

    if db_path:
        os.environ["LOGRAM_DB_PATH"] = db_path
    db = Path(os.environ.get("LOGRAM_DB_PATH", str(_store_path()))).resolve()

    try:
        import fastmcp  # noqa: F401
    except ImportError:
        console.print()
        console.print(
            Panel(
                Text.assemble(
                    ("fastmcp not installed.\n\n", "bold lg.error"),
                    ("pip install \"logram-sdk[mcp]\"", "lg.brand"),
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
