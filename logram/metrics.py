from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Any, Iterable


@dataclass(slots=True)
class ReplayStepTiming:
    status: str
    duration: float
    started_at: float | None
    finished_at: float | None


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not isfinite(out):
        return None
    return out


def _iter_replayed_steps(steps: Iterable[ReplayStepTiming | dict[str, Any]]) -> Iterable[ReplayStepTiming]:
    def _row_get(row: Any, key: str) -> Any:
        if hasattr(row, "get"):
            return row.get(key)
        try:
            return row[key]
        except (KeyError, IndexError, TypeError):
            return None

    for step in steps:
        if isinstance(step, ReplayStepTiming):
            row = step
        else:
            row = ReplayStepTiming(
                status=str(_row_get(step, "status") or ""),
                duration=float(_as_float(_row_get(step, "duration")) or 0.0),
                started_at=_as_float(_row_get(step, "started_at")),
                finished_at=_as_float(_row_get(step, "finished_at")),
            )
        if row.status.upper() == "REPLAYED":
            yield row


def compute_resource_time_saved(steps: Iterable[ReplayStepTiming | dict[str, Any]]) -> float:
    """Machine effort saved: arithmetic sum of REPLAYED durations."""
    total = 0.0
    for step in _iter_replayed_steps(steps):
        if step.duration > 0:
            total += step.duration
    return total


def compute_wait_time_saved(steps: Iterable[ReplayStepTiming | dict[str, Any]]) -> float:
    """Human wait-time saved: union duration of REPLAYED [started_at, finished_at] intervals."""
    intervals: list[tuple[float, float]] = []
    for step in _iter_replayed_steps(steps):
        start = step.started_at
        end = step.finished_at
        if start is None or end is None:
            continue
        if end < start:
            continue
        intervals.append((start, end))

    if not intervals:
        return 0.0

    intervals.sort(key=lambda it: it[0])
    merged_total = 0.0

    cur_start, cur_end = intervals[0]
    for start, end in intervals[1:]:
        if start <= cur_end:
            if end > cur_end:
                cur_end = end
            continue

        merged_total += max(0.0, cur_end - cur_start)
        cur_start, cur_end = start, end

    merged_total += max(0.0, cur_end - cur_start)
    return merged_total


def compute_time_savings(steps: Iterable[ReplayStepTiming | dict[str, Any]]) -> tuple[float, float]:
    """Return (resource_time_saved, wait_time_saved), in seconds."""
    steps_list = list(steps)
    return compute_resource_time_saved(steps_list), compute_wait_time_saved(steps_list)


def compute_token_totals(steps: Iterable[dict[str, Any] | Any]) -> tuple[int, int]:
    """Return (total_prompt_tokens, total_completion_tokens) across provided steps."""

    def _row_get(row: Any, key: str) -> Any:
        if hasattr(row, "get"):
            return row.get(key)
        try:
            return row[key]
        except (KeyError, IndexError, TypeError):
            return None

    total_prompt = 0
    total_completion = 0
    for row in steps:
        p = _row_get(row, "prompt_tokens")
        c = _row_get(row, "completion_tokens")
        try:
            if p is not None:
                total_prompt += max(0, int(p))
        except (TypeError, ValueError):
            pass
        try:
            if c is not None:
                total_completion += max(0, int(c))
        except (TypeError, ValueError):
            pass
    return total_prompt, total_completion


def _scope_where_clause(
    *,
    project: str | None,
    input_id: str | None,
    run_id: str | None,
    alias: str = "r",
) -> tuple[str, list[Any]]:
    parts: list[str] = []
    params: list[Any] = []

    if project:
        parts.append(f"{alias}.project = ?")
        params.append(project)
    if input_id:
        parts.append(f"{alias}.input_id = ?")
        params.append(input_id)
    if run_id:
        parts.append(f"{alias}.run_id = ?")
        params.append(run_id)

    if not parts:
        return "", params
    return "WHERE " + " AND ".join(parts), params


def aggregate_roi_stats(
    conn: Any,
    *,
    project: str | None = None,
    input_id: str | None = None,
    run_id: str | None = None,
) -> dict[str, float | int]:
    """Aggregate ROI KPIs for a given scope using SQL-first queries.

    Returns keys:
      - run_count
      - total_steps
      - replayed_steps
      - total_project_time
      - resource_time_saved
      - wait_time_saved
      - efficiency_ratio
    """
    where_runs, params_runs = _scope_where_clause(
        project=project,
        input_id=input_id,
        run_id=run_id,
        alias="r",
    )

    run_row = conn.execute(
        f"""
        SELECT COUNT(*) AS run_count
        FROM runs r
        {where_runs}
        """,
        params_runs,
    ).fetchone()
    run_count = int(run_row["run_count"] or 0) if run_row else 0

    step_row = conn.execute(
        f"""
        SELECT
          COUNT(*) AS total_steps,
          COALESCE(SUM(s.duration), 0) AS total_project_time,
          COALESCE(SUM(CASE WHEN UPPER(s.status) = 'REPLAYED' THEN 1 ELSE 0 END), 0) AS replayed_steps,
          COALESCE(SUM(CASE WHEN UPPER(s.status) = 'REPLAYED' THEN s.duration ELSE 0 END), 0) AS resource_time_saved
        FROM steps s
        JOIN runs r ON r.run_id = s.run_id
        {where_runs}
        """,
        params_runs,
    ).fetchone()

    total_steps = int(step_row["total_steps"] or 0) if step_row else 0
    replayed_steps = int(step_row["replayed_steps"] or 0) if step_row else 0
    total_project_time = float(step_row["total_project_time"] or 0.0) if step_row else 0.0
    resource_time_saved = float(step_row["resource_time_saved"] or 0.0) if step_row else 0.0

    replay_intervals = conn.execute(
        f"""
        SELECT s.status, s.duration, s.started_at, s.finished_at
        FROM steps s
        JOIN runs r ON r.run_id = s.run_id
        {where_runs}
        AND UPPER(s.status) = 'REPLAYED'
        """
        if where_runs
        else """
        SELECT s.status, s.duration, s.started_at, s.finished_at
        FROM steps s
        JOIN runs r ON r.run_id = s.run_id
        WHERE UPPER(s.status) = 'REPLAYED'
        """,
        params_runs,
    ).fetchall()
    wait_time_saved = compute_wait_time_saved(replay_intervals)

    efficiency_ratio = (wait_time_saved / total_project_time) if total_project_time > 0 else 0.0

    return {
        "run_count": run_count,
        "total_steps": total_steps,
        "replayed_steps": replayed_steps,
        "total_project_time": total_project_time,
        "resource_time_saved": resource_time_saved,
        "wait_time_saved": wait_time_saved,
        "efficiency_ratio": efficiency_ratio,
    }


def top_inputs_by_savings(
    conn: Any,
    *,
    project: str | None = None,
    input_id: str | None = None,
    run_id: str | None = None,
    limit: int = 3,
) -> list[dict[str, Any]]:
    """Return top input documents by replay savings using SQL GROUP BY."""
    where_runs, params_runs = _scope_where_clause(
        project=project,
        input_id=input_id,
        run_id=run_id,
        alias="r",
    )

    rows = conn.execute(
        f"""
        SELECT
          COALESCE(r.input_id, 'unknown_input') AS input_id,
          COUNT(*) AS run_count,
          COALESCE(SUM(r.wait_time_saved), 0) AS wait_time_saved,
          COALESCE(SUM(r.resource_time_saved), 0) AS resource_time_saved
        FROM runs r
        {where_runs}
        GROUP BY COALESCE(r.input_id, 'unknown_input')
        ORDER BY wait_time_saved DESC, resource_time_saved DESC
        LIMIT ?
        """,
        [*params_runs, int(limit)],
    ).fetchall()

    return [
        {
            "input_id": str(r["input_id"]),
            "run_count": int(r["run_count"] or 0),
            "wait_time_saved": float(r["wait_time_saved"] or 0.0),
            "resource_time_saved": float(r["resource_time_saved"] or 0.0),
        }
        for r in rows
    ]


def aggregate_token_efficiency(
    conn: Any,
    *,
    project: str | None = None,
    input_id: str | None = None,
    run_id: str | None = None,
) -> dict[str, int | float]:
    """Aggregate token spend/savings per scope.

    - Tokens Spent (Live): SUCCESS steps
    - Tokens Saved (Cache): REPLAYED steps
    - Total Bypass Rate: saved / (saved + live)
    """
    where_runs, params_runs = _scope_where_clause(
        project=project,
        input_id=input_id,
        run_id=run_id,
        alias="r",
    )

    row = conn.execute(
        f"""
        SELECT
          COALESCE(SUM(CASE WHEN UPPER(s.status) = 'SUCCESS' THEN COALESCE(s.prompt_tokens, 0) ELSE 0 END), 0) AS live_prompt_tokens,
          COALESCE(SUM(CASE WHEN UPPER(s.status) = 'SUCCESS' THEN COALESCE(s.completion_tokens, 0) ELSE 0 END), 0) AS live_completion_tokens,
          COALESCE(SUM(CASE WHEN UPPER(s.status) = 'REPLAYED' THEN COALESCE(s.prompt_tokens, 0) ELSE 0 END), 0) AS saved_prompt_tokens,
          COALESCE(SUM(CASE WHEN UPPER(s.status) = 'REPLAYED' THEN COALESCE(s.completion_tokens, 0) ELSE 0 END), 0) AS saved_completion_tokens
        FROM steps s
        JOIN runs r ON r.run_id = s.run_id
        {where_runs}
        """,
        params_runs,
    ).fetchone()

    live_prompt = int(row["live_prompt_tokens"] or 0) if row else 0
    live_completion = int(row["live_completion_tokens"] or 0) if row else 0
    saved_prompt = int(row["saved_prompt_tokens"] or 0) if row else 0
    saved_completion = int(row["saved_completion_tokens"] or 0) if row else 0

    tokens_spent_live = live_prompt + live_completion
    tokens_saved_cache = saved_prompt + saved_completion
    denom = tokens_spent_live + tokens_saved_cache
    total_bypass_rate = (tokens_saved_cache / denom) if denom > 0 else 0.0

    return {
        "tokens_spent_live": tokens_spent_live,
        "tokens_saved_cache": tokens_saved_cache,
        "total_bypass_rate": total_bypass_rate,
        "live_prompt_tokens": live_prompt,
        "live_completion_tokens": live_completion,
        "saved_prompt_tokens": saved_prompt,
        "saved_completion_tokens": saved_completion,
    }
