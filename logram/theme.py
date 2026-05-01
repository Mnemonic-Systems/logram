from __future__ import annotations

from rich import box
from rich.console import Console
from rich.text import Text
from rich.theme import Theme

LG_THEME = Theme(
    {
        "lg.brand": "bright_cyan",
        "lg.success": "bright_green",
        "lg.error": "bright_red",
        "lg.warning": "yellow",
        "lg.muted": "bright_black",
        "lg.header": "bold bright_black",
        "lg.badge.success": "bold black on green",
        "lg.badge.failed": "bold white on red",
        "lg.badge.replayed": "bold black on cyan",
        "lg.badge.live": "bold black on yellow",
        "lg.dur.fast": "bright_green",
        "lg.dur.slow": "yellow",
    }
)

console = Console(theme=LG_THEME)

PANEL_BOX = box.ROUNDED
TABLE_BOX = box.SIMPLE_HEAD


def status_badge(status: str | None) -> Text:
    s = (status or "").strip().upper()
    if s in {"SUCCESS", "CACHE_HIT", "REPLAY_HIT"}:
        return Text(" ✓ success ", style="lg.badge.success")
    if s in {"FAILED", "FAILURE", "ERROR"}:
        return Text(" ✗ failed ", style="lg.badge.failed")
    if s == "REPLAYED":
        return Text(" ↦ replayed ", style="lg.badge.replayed")
    if s == "LIVE":
        return Text(" ⚡ live ", style="lg.badge.live")
    return Text(f" {(status or '?').lower()} ", style="dim")


def step_icon(status: str | None) -> str:
    s = (status or "").strip().upper()
    if s in {"CACHE_HIT", "REPLAY_HIT", "REPLAYED"}:
        return "↦"
    if s == "LIVE":
        return "⚡"
    if s == "SUCCESS":
        return "✓"
    if s in {"FAILED", "FAILURE", "ERROR"}:
        return "✗"
    return "○"


def step_color(status: str | None) -> str:
    s = (status or "").strip().upper()
    if s in {"CACHE_HIT", "REPLAY_HIT", "REPLAYED"}:
        return "lg.brand"
    if s == "LIVE":
        return "lg.warning"
    if s == "SUCCESS":
        return "lg.success"
    if s in {"FAILED", "FAILURE", "ERROR"}:
        return "lg.error"
    return "lg.muted"


def step_badge(status: str | None) -> Text:
    s = (status or "").strip().upper()
    if s in {"CACHE_HIT", "REPLAY_HIT", "REPLAYED"}:
        return Text(" REPLAY ", style="lg.badge.replayed")
    if s == "LIVE":
        return Text(" LIVE ", style="lg.badge.live")
    if s == "SUCCESS":
        return Text(" SUCCESS ", style="lg.badge.success")
    if s in {"FAILED", "FAILURE", "ERROR"}:
        return Text(" FAILED ", style="lg.badge.failed")
    return Text(f" {(status or '?').upper()} ", style="dim")


def duration_text(seconds: float | int | None, threshold: float = 2.5) -> Text:
    val = float(seconds or 0.0)
    style = "lg.dur.fast" if val <= threshold else "lg.dur.slow"
    return Text(f"{val:.3f}s", style=style)


def hint_line(*parts: str) -> Text:
    t = Text(style="lg.muted")
    for i, part in enumerate(parts):
        if i > 0:
            t.append(" · ")
        t.append(part)
    return t
