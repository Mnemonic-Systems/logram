"""Logram logic-tree analysis — Motor layer.

Pure functions that operate on logic_registry rows. No MCP, no CLI dependency.
Consumers (MCP, CLI, Dashboard) call these and format the result themselves.
"""

from __future__ import annotations

import difflib
import json
import sqlite3
from typing import Any

_SOURCE_DIFF_TRUNCATE = 2000
_GLOBALS_VALUE_TRUNCATE = 500


def _fetch_logic_row(conn: sqlite3.Connection, logic_hash: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT name, source_code, resolved_globals, called_functions_json
        FROM logic_registry
        WHERE logic_hash = ?
        """,
        (logic_hash,),
    ).fetchone()
    if not row:
        return None
    return dict(row)


def _parse_callees(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}
    try:
        result = json.loads(raw)
        return result if isinstance(result, dict) else {}
    except Exception:
        return {}


def _parse_globals(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        result = json.loads(raw)
        return result if isinstance(result, dict) else {}
    except Exception:
        return {}


def _truncate_value(val: Any) -> Any:
    """Truncate long string values to keep globals_diff agent-readable."""
    if not isinstance(val, str):
        return val
    if len(val) <= _GLOBALS_VALUE_TRUNCATE:
        return val
    return val[:_GLOBALS_VALUE_TRUNCATE] + f"... (truncated, {len(val)} chars total)"


def _build_globals_diff(raw_a: str | None, raw_b: str | None) -> dict[str, Any]:
    """Return {key: {"before": val_a, "after": val_b}} for every key that changed.

    String values longer than _GLOBALS_VALUE_TRUNCATE are truncated.
    Keys only in A → before=val, after=None.
    Keys only in B → before=None, after=val.
    """
    g_a = _parse_globals(raw_a)
    g_b = _parse_globals(raw_b)
    result: dict[str, Any] = {}
    for key in sorted(set(g_a) | set(g_b)):
        val_a = g_a.get(key)
        val_b = g_b.get(key)
        if val_a != val_b:
            entry: dict[str, Any] = {
                "before": _truncate_value(val_a),
                "after":  _truncate_value(val_b),
            }
            raw_a_str = str(val_a) if val_a is not None else ""
            raw_b_str = str(val_b) if val_b is not None else ""
            if (len(raw_a_str) > _GLOBALS_VALUE_TRUNCATE
                    or len(raw_b_str) > _GLOBALS_VALUE_TRUNCATE):
                entry["truncated"] = True
            result[key] = entry
    return result


def _globals_diff_labels(globals_diff: dict[str, Any]) -> list[str]:
    """Derive what_changed labels from a globals_diff dict."""
    labels: list[str] = []
    for key, change in globals_diff.items():
        if change["before"] is None:
            labels.append(f"globals:{key}:added")
        elif change["after"] is None:
            labels.append(f"globals:{key}:removed")
        else:
            labels.append(f"globals:{key}")
    return labels


def _build_source_diff(name: str, code_a: str, code_b: str) -> str:
    lines_a = (code_a or "").splitlines(keepends=True)
    lines_b = (code_b or "").splitlines(keepends=True)
    raw = "".join(difflib.unified_diff(
        lines_a, lines_b,
        fromfile=f"{name} (run A)",
        tofile=f"{name} (run B)",
        n=2,
    ))
    if len(raw) > _SOURCE_DIFF_TRUNCATE:
        return raw[:_SOURCE_DIFF_TRUNCATE] + "\n... (truncated — use get_step_source for full code)"
    return raw


def find_all_divergences(
    conn: sqlite3.Connection,
    hash_a: str,
    hash_b: str,
    path: list[str],
    max_depth: int = 4,
    _results: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Exhaustive tree walk — record every node where hashes differ, always recurse.

    A hash difference is an absolute truth. What to display is the consumer's
    decision (MCP, CLI, Dashboard). This function never filters.

    Returns
    -------
    List of dicts, one per divergent node:
        callee_name   str        — qualified name of the function
        hash_a        str|None   — 12-char hash from run A (None if new in B)
        hash_b        str|None   — 12-char hash from run B (None if removed in A)
        path          list       — ["step_name", "callee", "sub_callee", ...]
        depth         int        — 0 = step itself, 1 = direct callee, …
        what_changed  list       — "source_code", "globals:KEY", "globals:KEY:added",
                                   "globals:KEY:removed", "new_callee", "removed_callee"
        source_diff   str        — unified diff (empty if source unchanged, truncated at 2000)
        globals_diff  dict       — {key: {"before": val, "after": val[, "truncated": true]}}
    """
    results: list[dict[str, Any]] = _results if _results is not None else []

    if len(path) > max_depth:
        return results

    row_a = _fetch_logic_row(conn, hash_a)
    row_b = _fetch_logic_row(conn, hash_b)

    if not row_a or not row_b:
        return results

    name = row_b.get("name") or row_a.get("name") or (path[-1] if path else "root")

    source_changed  = (row_a["source_code"]     or "") != (row_b["source_code"]     or "")
    globals_changed = (row_a["resolved_globals"] or "") != (row_b["resolved_globals"] or "")

    if source_changed or globals_changed:
        source_diff  = _build_source_diff(name, row_a["source_code"] or "", row_b["source_code"] or "") if source_changed else ""
        globals_diff = _build_globals_diff(row_a["resolved_globals"], row_b["resolved_globals"]) if globals_changed else {}

        what: list[str] = []
        if source_changed:
            what.append("source_code")
        what.extend(_globals_diff_labels(globals_diff))

        results.append({
            "callee_name":  name,
            "hash_a":       hash_a,
            "hash_b":       hash_b,
            "path":         list(path),
            "depth":        len(path) - 1,
            "what_changed": what,
            "source_diff":  source_diff,
            "globals_diff": globals_diff,
        })
        # No early return — always continue into callees.

    # Recurse into every callee whose hash differs, regardless of surface changes above.
    callees_a = _parse_callees(row_a["called_functions_json"])
    callees_b = _parse_callees(row_b["called_functions_json"])

    for callee_name, child_hash_a in callees_a.items():
        child_hash_b = callees_b.get(callee_name)
        if not child_hash_b:
            results.append({
                "callee_name":  callee_name,
                "hash_a":       child_hash_a if child_hash_a else None,
                "hash_b":       None,
                "path":         path + [callee_name],
                "depth":        len(path),
                "what_changed": ["removed_callee"],
                "source_diff":  "",
                "globals_diff": {},
            })
        elif child_hash_a != child_hash_b:
            find_all_divergences(conn, child_hash_a, child_hash_b, path + [callee_name], max_depth, results)

    for callee_name, child_hash_b in callees_b.items():
        if callee_name not in callees_a:
            results.append({
                "callee_name":  callee_name,
                "hash_a":       None,
                "hash_b":       child_hash_b if child_hash_b else None,
                "path":         path + [callee_name],
                "depth":        len(path),
                "what_changed": ["new_callee"],
                "source_diff":  "",
                "globals_diff": {},
            })

    return results
