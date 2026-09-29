# ruff: noqa: BLE001
# pylint: disable=broad-exception-caught

from __future__ import annotations

import typer

APP_NAME = "Logram Control Center"

app = typer.Typer(
    name="logram",
    help="Inspect, replay, diff and maintain Logram traces.",
    no_args_is_help=True,
    rich_markup_mode="rich",
)

golden_app = typer.Typer(help="Manage golden (reference) runs.")

app.add_typer(golden_app, name="golden")

mcp_app = typer.Typer(help="MCP server for coding agents (Claude, Cursor…).")

app.add_typer(mcp_app, name="mcp")
