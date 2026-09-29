# ruff: noqa: BLE001
# pylint: disable=broad-exception-caught

from __future__ import annotations

import typer

from .. import __version__

APP_NAME = "Logram Control Center"

app = typer.Typer(
    name="logram",
    help="Inspect, replay, diff and maintain Logram traces.",
    no_args_is_help=True,
    rich_markup_mode="rich",
)

# Not on PyPI: extras are installed from the repository.
REPO_URL = "git+https://github.com/Mnemonic-Systems/logram.git"


def install_hint(extra: str) -> str:
    return f'pip install "logram-sdk[{extra}] @ {REPO_URL}"'


def _print_version(value: bool) -> None:
    if value:
        typer.echo(f"logram {__version__}")
        raise typer.Exit()


@app.callback()
def _root(
    version: bool = typer.Option(
        False, "--version", callback=_print_version, is_eager=True, help="Show the version and exit."
    ),
) -> None:
    pass

golden_app = typer.Typer(help="Manage golden (reference) runs.")

app.add_typer(golden_app, name="golden")

mcp_app = typer.Typer(help="MCP server for coding agents (Claude, Cursor…).")

app.add_typer(mcp_app, name="mcp")
