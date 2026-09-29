"""The ``logram`` / ``lg`` command line.

Commands are grouped by topic; importing a module registers its commands on
``app``, and the import order below is the order shown by ``logram --help``.
"""

from __future__ import annotations

# isort: off
from . import runs  # noqa: F401  list, inspect, view, open, live
from . import diff  # noqa: F401  diff, recover, restore
from . import replay  # noqa: F401  replay, test, golden
from . import stats  # noqa: F401  stats
from . import maintenance  # noqa: F401  init, doctor, clean, ui
from . import mcp  # noqa: F401  mcp start / config / install
# isort: on
from ._app import app


def main() -> None:
    app()


__all__ = ["app", "main"]
