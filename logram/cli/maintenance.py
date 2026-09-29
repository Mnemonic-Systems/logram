# ruff: noqa: BLE001
# pylint: disable=broad-exception-caught

from __future__ import annotations

import json
import logging
import os
import shutil
import sqlite3
import sys
import webbrowser
from importlib.metadata import version as _pkg_version
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
)
from ._app import app, install_hint
from ._common import (
    _claude_desktop_config_path,
    _connect_db,
    _educational_db_missing_message,
    _orphan_assets,
    _store_path,
    _write_agent_rules_files,
)

_GITIGNORE_ENTRIES = [".logram/", ".logram_assets/"]

@app.command()
def ui(
    host: str = typer.Option("127.0.0.1", "--host", help="Address the API server binds to."),
    port: int = typer.Option(8000, "--port", min=1, max=65535, help="API server port."),
    dashboard_url: str = typer.Option("http://localhost:3000", "--dashboard-url", help="URL du dashboard web"),
    open_browser: bool = typer.Option(True, "--open-browser/--no-open-browser", help="Open the dashboard in a browser."),
    reload: bool = typer.Option(False, "--reload", help="Active l'auto-reload (dev uniquement)"),
) -> None:
    """Start the read-only API server used by the web dashboard."""
    if not _store_path().exists():
        _educational_db_missing_message()
        raise typer.Exit(1)

    try:
        import uvicorn
    except Exception as exc:
        console.print()
        console.print(
            Panel(
                Text.assemble(
                    ("uvicorn not available: ", "lg.muted"),
                    (str(exc), "lg.error"),
                    ("\n\n" + install_hint("server"), "lg.brand"),
                ),
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
    info.append(str(_store_path()), style="lg.muted")
    console.print(info)
    console.print()

    logging.getLogger("uvicorn.access").setLevel(logging.INFO)
    uvicorn.run("logram.server.app:app", host=host, port=port, reload=reload, log_level="info")

@app.command()
def clean() -> None:
    """Offer to delete failed runs and unreferenced blobs."""
    conn = _connect_db()
    if conn is None:
        raise typer.Exit(1)

    try:
        failed_runs = conn.execute("SELECT run_id FROM runs WHERE UPPER(status) IN ('FAILED', 'FAILURE', 'ERROR')").fetchall()
        failed_count = len(failed_runs)

        orphan_assets = _orphan_assets(conn)

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

@app.command()
def doctor() -> None:
    """Check the environment: Python, store, MCP wiring, pending cleanup."""
    rows: list[tuple[str, Text, str]] = []

    def _ok() -> Text:
        return Text(" ✓ ok ", style="lg.badge.success")

    def _warn() -> Text:
        return Text(" ⚠ warn ", style="lg.badge.live")

    def _fail() -> Text:
        return Text(" ✗ fail ", style="lg.badge.failed")

    def _na() -> Text:
        return Text(" – n/a ", style="dim")

    # --- Environnement ---
    py = sys.version_info
    py_detail = f"{py.major}.{py.minor}.{py.micro}"
    rows.append(("Python", _ok() if py >= (3, 10) else _warn(), py_detail + ("" if py >= (3, 10) else " (3.10+ recommended)")))

    try:
        lg_ver = _pkg_version("logram-sdk")
        rows.append(("Logram SDK", _ok(), lg_ver))
    except Exception:
        rows.append(("Logram SDK", _fail(), "not installed via pip"))

    # --- Stockage ---
    logram_dir = _store_path().parent
    if logram_dir.exists():
        writable = os.access(logram_dir, os.W_OK)
        try:
            shown = str(logram_dir.relative_to(Path.cwd()))
        except ValueError:
            shown = str(logram_dir)
        rows.append((".logram/", _ok() if writable else _warn(), shown + (" (read-only!)" if not writable else "")))
    else:
        rows.append((".logram/", _fail(), "directory missing — run your pipeline first"))

    if _store_path().exists():
        db_size = _store_path().stat().st_size
        try:
            _c = sqlite3.connect(_store_path())
            _c.row_factory = sqlite3.Row
            run_count = _c.execute("SELECT COUNT(*) AS n FROM runs").fetchone()["n"]
            _c.close()
            rows.append(("logram.db", _ok(), f"{db_size // 1024} KB · {run_count} run(s)"))
        except Exception as exc:
            rows.append(("logram.db", _warn(), f"readable but query failed: {exc}"))
    else:
        rows.append(("logram.db", _fail(), "not found — run your pipeline first"))

    # --- Agent integration: Claude Code ---
    claude_bin = shutil.which("claude")
    if claude_bin:
        claude_json = Path.home() / ".claude.json"
        if claude_json.exists():
            try:
                data = json.loads(claude_json.read_text(encoding="utf-8"))
                if "logram" in (data.get("mcpServers") or {}):
                    rows.append(("Claude Code MCP", _ok(), "logram found in ~/.claude.json"))
                else:
                    rows.append(("Claude Code MCP", _warn(), "claude found but logram not in ~/.claude.json · run: lg mcp install"))
            except Exception:
                rows.append(("Claude Code MCP", _warn(), "claude found but ~/.claude.json unreadable"))
        else:
            rows.append(("Claude Code MCP", _warn(), "claude found but ~/.claude.json not present"))
    else:
        rows.append(("Claude Code MCP", _na(), "claude CLI not found in PATH"))

    # --- Agent integration: Claude Desktop ---
    desktop_path = _claude_desktop_config_path()
    if desktop_path and desktop_path.exists():
        try:
            data = json.loads(desktop_path.read_text(encoding="utf-8"))
            if "logram" in (data.get("mcpServers") or {}):
                rows.append(("Claude Desktop MCP", _ok(), "logram found in config"))
            else:
                rows.append(("Claude Desktop MCP", _warn(), "config found but logram not wired · run: lg mcp install"))
        except Exception:
            rows.append(("Claude Desktop MCP", _warn(), "config unreadable"))
    elif desktop_path:
        rows.append(("Claude Desktop MCP", _na(), "config file not found"))
    else:
        rows.append(("Claude Desktop MCP", _na(), "not supported on this platform"))

    # --- Agent integration: Cursor ---
    cursor_mcp: Path | None = None
    for candidate in [Path.cwd(), *list(Path.cwd().parents)[:3]]:
        p = candidate / ".cursor" / "mcp.json"
        if p.exists():
            cursor_mcp = p
            break
    if cursor_mcp:
        try:
            data = json.loads(cursor_mcp.read_text(encoding="utf-8"))
            if "logram" in (data.get("mcpServers") or {}):
                rows.append(("Cursor MCP", _ok(), f"logram found in {cursor_mcp.relative_to(Path.cwd())}"))
            else:
                rows.append(("Cursor MCP", _warn(), f"found {cursor_mcp.name} but logram not wired"))
        except Exception:
            rows.append(("Cursor MCP", _warn(), "mcp.json found but unreadable"))
    else:
        rows.append(("Cursor MCP", _na(), "no .cursor/mcp.json found in project tree"))

    # --- Nettoyage ---
    failed_count = 0
    orphan_count = 0
    if _store_path().exists():
        try:
            _c = sqlite3.connect(_store_path())
            _c.row_factory = sqlite3.Row
            failed_count = _c.execute(
                "SELECT COUNT(*) AS n FROM runs WHERE UPPER(status) IN ('FAILED','FAILURE','ERROR')"
            ).fetchone()["n"]
            orphan_count = len(_orphan_assets(_c))
            _c.close()
        except Exception:
            pass

    cleanup_detail_parts = []
    if failed_count:
        cleanup_detail_parts.append(f"{failed_count} failed run(s)")
    if orphan_count:
        cleanup_detail_parts.append(f"{orphan_count} orphan blob(s)")
    cleanup_detail = "  ·  ".join(cleanup_detail_parts) or "clean"
    rows.append(("Cleanup", _ok() if not cleanup_detail_parts else _warn(), cleanup_detail))

    # --- Render ---
    table = Table(
        box=TABLE_BOX,
        show_edge=False,
        show_lines=False,
        expand=False,
        pad_edge=False,
        header_style="lg.header",
    )
    table.add_column("check", style="lg.brand", min_width=20)
    table.add_column("status", justify="center", min_width=10)
    table.add_column("detail", style="lg.muted")
    for check, status, detail in rows:
        table.add_row(check, status, detail)

    console.print()
    console.print(
        Panel(
            table,
            title="[lg.muted]logram doctor[/lg.muted]",
            title_align="left",
            box=PANEL_BOX,
            border_style="lg.muted",
            padding=(0, 1),
        )
    )
    console.print()
    console.print(hint_line("lg mcp install to wire an agent", "lg clean to remove orphans", "lg list to browse runs"))
    console.print()

# ---------------------------------------------------------------------------
# Agent rules helpers
# ---------------------------------------------------------------------------



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
