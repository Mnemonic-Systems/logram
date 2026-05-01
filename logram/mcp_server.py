"""Logram MCP Server — Agent-first debugging interface for AI pipelines.

Expose Logram's replay engine and SQLite trace store as MCP tools so that a
coding agent (Claude, Cursor, …) can autonomously diagnose, fix, and validate
AI pipeline failures without human guidance.

Usage:
    lg mcp start          # launch the MCP server (stdio transport)
    lg mcp config         # print the JSON block to paste into Cursor / Claude Desktop

Design: tool functions are plain Python callables. The MCP registration block
at the bottom of this file attaches them to the FastMCP instance. This keeps
every function directly importable and unit-testable without going through the
FunctionTool wrapper.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import threading
from pathlib import Path
from typing import Optional

from fastmcp import FastMCP

from .analysis import find_all_divergences


# ---------------------------------------------------------------------------
# Security constants & circuit breaker state
# ---------------------------------------------------------------------------

REPLAY_SESSION_LIMIT = 5

_replay_counter: int = 0
_replay_lock = threading.Lock()


def _reset_replay_counter() -> None:
    """Reset the per-session replay counter. Call this in tests only."""
    global _replay_counter
    with _replay_lock:
        _replay_counter = 0


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _db_path() -> Path:
    return Path(os.environ.get("LOGRAM_DB_PATH", ".logram/logram.db"))


def _connect() -> sqlite3.Connection:
    db = _db_path()
    if not db.exists():
        raise FileNotFoundError(
            f"Logram database not found at '{db}'. "
            "Run an instrumented pipeline first to populate the trace store."
        )
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_json(value: str | None) -> object:
    if not value:
        return None
    try:
        return json.loads(value)
    except Exception:
        return value


def _json_pretty(value: object) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, indent=2)
    except Exception:
        return str(value)


def _diff_text(a: str, b: str, label_a: str, label_b: str) -> str:
    import difflib
    lines = list(
        difflib.unified_diff(
            a.splitlines(),
            b.splitlines(),
            fromfile=label_a,
            tofile=label_b,
            lineterm="",
            n=3,
        )
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Security helpers (The Jail + The Logic Guard)
# ---------------------------------------------------------------------------

def _is_path_safe(script_path: str) -> tuple[bool, str]:
    """Return (True, '') when script_path is safe to execute, (False, reason) otherwise.

    Rules enforced:
    1. Extension must be .py — prevents executing arbitrary shell scripts.
    2. Resolved path must be inside os.getcwd() — prevents directory traversal
       and access to files outside the project (The Jail).

    This function never raises; callers can trust the bool return value.
    """
    p = Path(script_path)

    if p.suffix != ".py":
        return (
            False,
            f"Only .py files are allowed. Got extension '{p.suffix}'. "
            "Pass the path to a Python script.",
        )

    try:
        resolved = p.resolve()
    except Exception as exc:
        return False, f"Could not resolve path '{script_path}': {exc}"

    cwd = Path.cwd()
    try:
        resolved.relative_to(cwd)
    except ValueError:
        return (
            False,
            f"Security jail violation: '{script_path}' resolves to '{resolved}', "
            f"which is outside the current working directory '{cwd}'. "
            "Only paths within the project directory are permitted.",
        )

    return True, ""


def _logic_unchanged_since_failure(force_step: str) -> bool:
    """Return True when the most recent logic_hash for force_step equals the last FAILED hash.

    This detects retry loops where the agent calls run_surgical_replay repeatedly
    without modifying the code or prompts. When the function returns True, the
    tool aborts and instructs the agent to make a code change first.

    Algorithm:
    - Fetch the logic_hash of the most recent FAILED step record for force_step.
    - Fetch the logic_hash of the most recent step record (any status) for force_step.
    - If both hashes exist and are equal → code has not changed since the failure.

    Limitation: this is a heuristic. It cannot read the current file state before
    running; it compares what Logram last recorded.
    """
    try:
        conn = _connect()
    except FileNotFoundError:
        return False

    try:
        last_failed_row = conn.execute(
            """
            SELECT logic_hash FROM steps
            WHERE name = ? AND status IN ('FAILED', 'FAILURE', 'ERROR')
            ORDER BY timestamp DESC LIMIT 1
            """,
            (force_step,),
        ).fetchone()

        if not last_failed_row or not last_failed_row["logic_hash"]:
            return False

        most_recent_row = conn.execute(
            """
            SELECT logic_hash FROM steps
            WHERE name = ?
            ORDER BY timestamp DESC LIMIT 1
            """,
            (force_step,),
        ).fetchone()

        if not most_recent_row or not most_recent_row["logic_hash"]:
            return False

        return last_failed_row["logic_hash"] == most_recent_row["logic_hash"]
    except Exception:
        return False
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Tool 1 — list_runs
# ---------------------------------------------------------------------------

def list_runs(project: Optional[str] = None, limit: int = 5) -> str:
    """
    ENTRY POINT — call this first before any investigation.

    Lists the most recent pipeline executions stored in the Logram trace store,
    ordered newest-first. Use this to orient yourself: identify the run_id of a
    failing execution, understand the project name, check version history.

    When to call:
    - At the start of any debugging session to get run_ids.
    - When you need to find the last failed run for a given project.
    - When comparing execution history over time.

    Args:
        project: Filter by project name (exact match). Pass None for all projects.
        limit:   Maximum number of runs to return (default 5, max 50).

    Returns:
        JSON array of run objects with fields:
        run_id, project, input_id, version_id, status, created_at.
        Status values: 'success', 'failed', 'running'.
    """
    try:
        conn = _connect()
    except FileNotFoundError as exc:
        return json.dumps({"error": str(exc)})

    try:
        params: list[object] = []
        where = ""
        if project:
            where = "WHERE project = ?"
            params.append(project)

        safe_limit = min(max(1, limit), 50)
        rows = conn.execute(
            f"""
            SELECT run_id, project, input_id, version_id, status, created_at
            FROM runs
            {where}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            [*params, safe_limit],
        ).fetchall()

        return json.dumps(
            [
                {
                    "run_id": r["run_id"],
                    "project": r["project"],
                    "input_id": r["input_id"],
                    "version_id": r["version_id"],
                    "status": r["status"],
                    "created_at": r["created_at"],
                }
                for r in rows
            ],
            ensure_ascii=False,
        )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Tool 2 — get_investigation_brief
# ---------------------------------------------------------------------------

def get_investigation_brief(run_id: str) -> str:
    """
    PRIMARY DIAGNOSTIC TOOL — call this immediately when a run fails.

    Produces a concise, actionable brief about a failed pipeline run:
    which step failed first, what error was raised, what inputs the step
    received, and which logic_hash (code snapshot) was active at the time.

    This single call replaces the manual sequence:
      lg inspect <run_id> → lg view <step_id> → lg recover <logic_hash>

    When to call:
    - As soon as you identify a failed run_id via list_runs.
    - When a user reports unexpected pipeline output without knowing the cause.
    - Before proposing any code fix — always read the brief first.

    Args:
        run_id: The unique identifier of the run to investigate
                (obtained from list_runs).

    Returns:
        A Markdown brief containing:
        - Run metadata (project, version, status)
        - First failed step: name, error type, error message
        - Named inputs to the failed step
        - The logic_hash of the step (use get_step_source to read the code)
        - A suggested next action
    """
    try:
        conn = _connect()
    except FileNotFoundError as exc:
        return str(exc)

    try:
        run = conn.execute(
            "SELECT run_id, project, version_id, status FROM runs WHERE run_id = ?",
            (run_id,),
        ).fetchone()

        if not run:
            return (
                f"Run '{run_id}' not found in the trace store. "
                "Use list_runs() to find valid run_ids."
            )

        steps = conn.execute(
            """
            SELECT step_id, name, status, inputs_json, error_json, logic_hash, timestamp
            FROM steps
            WHERE run_id = ?
            ORDER BY timestamp ASC
            """,
            (run_id,),
        ).fetchall()

        if not steps:
            return (
                f"Run '{run_id}' (project={run['project']}, status={run['status']}) "
                "has no recorded steps. The pipeline may have crashed before tracing began."
            )

        failed = next(
            (s for s in steps if s["status"] in ("FAILED", "FAILURE", "ERROR")),
            None,
        )

        if not failed:
            statuses = ", ".join(s["status"] for s in steps)
            return (
                f"Run '{run_id}' (project={run['project']}) has no FAILED step. "
                f"All step statuses: {statuses}. "
                "The run may have failed at the orchestrator level rather than "
                "inside a traced step."
            )

        error = _parse_json(failed["error_json"])
        inputs = _parse_json(failed["inputs_json"])

        error_type = ""
        error_msg = ""
        if isinstance(error, dict):
            error_type = error.get("type", "")
            error_msg = error.get("message", str(error))
        elif error:
            error_msg = str(error)

        inputs_summary = ""
        if isinstance(inputs, dict):
            inputs_summary = ", ".join(
                f"{k}={repr(v)[:80]}" for k, v in inputs.items()
            )
        elif inputs:
            inputs_summary = str(inputs)[:200]

        step_count = len(steps)
        failed_index = next(
            i for i, s in enumerate(steps) if s["step_id"] == failed["step_id"]
        )

        lines = [
            f"## Investigation Brief — Run `{run_id}`",
            "",
            f"- **Project**: {run['project']}",
            f"- **Version**: {run['version_id']}",
            f"- **Run status**: {run['status']}",
            f"- **Pipeline progress**: step {failed_index + 1}/{step_count} before failure",
            "",
            f"### Failed Step: `{failed['name']}`",
            f"- **Error type**: {error_type or 'unknown'}",
            f"- **Error message**: {error_msg or '(no message recorded)'}",
            f"- **Named inputs**: {inputs_summary or '(none recorded)'}",
            f"- **Logic hash**: `{failed['logic_hash'] or 'not captured'}`",
            "",
            "### Suggested Next Action",
        ]

        if failed["logic_hash"]:
            lines.append(
                f"Call `get_step_source(\"{failed['logic_hash']}\")` to read the exact "
                "source code and global variables (prompts) active when the failure occurred."
            )
        else:
            lines.append(
                "Logic hash not captured for this step. "
                f"Inspect the source file directly for the function `{failed['name']}`."
            )

        return "\n".join(lines)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Tool 3 — get_step_source
# ---------------------------------------------------------------------------

def get_step_source(logic_hash: str) -> str:
    """
    RUNTIME TRUTH READER — retrieves the exact code and config active during a step.

    Logram snapshots the source code and all global variables (including prompt
    strings, temperature constants, model names) of every traced function at the
    moment it runs. This tool returns that snapshot for a given logic_hash.

    This is the authoritative view of "what the pipeline actually did" — not what
    the current file says, but what was captured at runtime.

    When to call:
    - After get_investigation_brief gives you a logic_hash for the failed step.
    - When you want to understand what prompt was sent to the LLM at failure time.
    - Before proposing a fix, to confirm your mental model matches the runtime reality.
    - When comparing two logic_hashes to understand what changed between runs.

    Args:
        logic_hash: The SHA-256 fingerprint of the step's logic snapshot.
                    Obtain from get_investigation_brief or analyze_logic_divergence.

    Returns:
        Markdown document with two sections:
        1. Source code (normalized Python) of the traced function.
        2. Resolved globals: all global variables captured at runtime,
           including prompt strings, constants, and configuration values.
           These are the values that were actually used, not what the file says today.
    """
    try:
        conn = _connect()
    except FileNotFoundError as exc:
        return str(exc)

    try:
        row = conn.execute(
            """
            SELECT name, source_code, resolved_globals, called_functions_json, signature
            FROM logic_registry
            WHERE logic_hash = ?
            """,
            (logic_hash,),
        ).fetchone()

        if not row:
            return (
                f"Logic hash '{logic_hash}' not found in the registry. "
                "This hash may belong to a step that ran before logic snapshotting was enabled, "
                "or the database may have been partially cleaned."
            )

        globals_obj = _parse_json(row["resolved_globals"])
        callees     = _parse_json(row["called_functions_json"]) or {}

        lines = [
            f"## Logic Snapshot — `{logic_hash[:16]}…`",
            "",
            f"**Function**: `{row['name']}`  ",
            f"**Signature**: `{row['signature'] or 'unknown'}`",
            "",
            "### Source Code (runtime snapshot)",
            "```python",
            (row["source_code"] or "# source not captured").strip(),
            "```",
            "",
            "### Resolved Globals (runtime values)",
            "```json",
            _json_pretty(globals_obj or {}),
            "```",
        ]

        if callees:
            lines += [
                "",
                "### Callees (sub-graph navigation)",
                "> Call `get_step_source(\"<hash>\")` on any entry to inspect it.",
                "",
            ]
            for callee_name, callee_hash in sorted(callees.items()):
                lines.append(f"- `{callee_name}` → `{callee_hash}`")

        return "\n".join(lines)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Exposition helper — belongs here, not in the Motor
# ---------------------------------------------------------------------------

def _suggest_next_tool(node: dict) -> str | None:
    wc = node.get("what_changed", [])
    hash_b = node.get("hash_b")
    hash_a = node.get("hash_a")
    if "source_code" in wc and hash_b:
        return f'get_step_source("{hash_b}")'
    if "new_callee" in wc and hash_b:
        return f'get_step_source("{hash_b}")'
    if "removed_callee" in wc and hash_a:
        return f'get_step_source("{hash_a}")  # code from run A that was removed'
    return None


# ---------------------------------------------------------------------------
# Tool 4 — analyze_logic_divergence
# ---------------------------------------------------------------------------

def analyze_logic_divergence(run_id_a: str, run_id_b: str) -> str:
    """
    REGRESSION ANALYST — compares the intelligence (code + config) of two pipeline runs.

    Produces a structured Markdown diff highlighting exactly what changed between
    two executions: which prompts were reworded, which constants were tuned, which
    functions were rewritten. Operates on the logic_registry snapshots so it shows
    what actually ran, not just what the file says today.

    When to call:
    - When a pipeline that used to work started failing after a code or prompt change.
    - When two runs on the same input produce different outputs and you don't know why.
    - Before a production deployment, to audit the diff between the candidate and the
      last known-good run.
    - When a user says "I changed something but I'm not sure what broke it."

    Args:
        run_id_a: The reference run (e.g. the last known-good run).
        run_id_b: The candidate run to compare against the reference.

    Returns:
        Markdown document with one section per step that differs.
        Each section shows the logic_hash change, a unified diff of source code,
        and a unified diff of resolved globals (prompts, constants).
        Steps with identical logic_hashes are omitted — no diff means no change.
    """
    try:
        conn = _connect()
    except FileNotFoundError as exc:
        return str(exc)

    try:
        for rid in (run_id_a, run_id_b):
            exists = conn.execute(
                "SELECT 1 FROM runs WHERE run_id = ?", (rid,)
            ).fetchone()
            if not exists:
                return (
                    f"Run '{rid}' not found. Use list_runs() to find valid run_ids."
                )

        def _steps_by_name(run_id: str) -> dict[str, dict]:
            rows = conn.execute(
                """
                SELECT s.name, s.logic_hash,
                       lr.source_code,
                       COALESCE(lr.resolved_globals, lr.globals_json) AS resolved_globals
                FROM steps s
                LEFT JOIN logic_registry lr ON lr.logic_hash = s.logic_hash
                WHERE s.run_id = ?
                ORDER BY s.timestamp ASC
                """,
                (run_id,),
            ).fetchall()
            return {r["name"]: dict(r) for r in rows}

        steps_a = _steps_by_name(run_id_a)
        steps_b = _steps_by_name(run_id_b)
        all_steps = sorted(set(steps_a) | set(steps_b))

        sections: list[str] = []

        for step in all_steps:
            a = steps_a.get(step)
            b = steps_b.get(step)

            hash_a = (a or {}).get("logic_hash") or ""
            hash_b = (b or {}).get("logic_hash") or ""

            if hash_a and hash_a == hash_b:
                continue

            src_a = ((a or {}).get("source_code") or "").strip()
            src_b = ((b or {}).get("source_code") or "").strip()
            glob_a = _json_pretty(_parse_json((a or {}).get("resolved_globals")))
            glob_b = _json_pretty(_parse_json((b or {}).get("resolved_globals")))

            section_lines: list[str] = [f"## Step `{step}`"]

            if not a:
                section_lines.append(
                    f"> Step present in `{run_id_b}` only (new step added)."
                )
            elif not b:
                section_lines.append(
                    f"> Step present in `{run_id_a}` only (step removed or renamed)."
                )
            else:
                ha_disp = hash_a or "none"
                hb_disp = hash_b or "none"
                section_lines.append(
                    f"- logic_hash: `{ha_disp[:16]}…` → `{hb_disp[:16]}…`"
                )
                if hash_b:
                    section_lines.append(
                        f"  - Inspect new code: `get_step_source(\"{hash_b}\")`"
                    )
                if hash_a:
                    section_lines.append(
                        f"  - Inspect old code: `get_step_source(\"{hash_a}\")`"
                    )

            src_diff = _diff_text(
                src_a, src_b,
                f"{run_id_a}:{step}", f"{run_id_b}:{step}",
            )
            if src_diff:
                section_lines += ["", "### Source diff", "```diff", src_diff, "```"]

            glob_diff = _diff_text(
                glob_a, glob_b,
                f"{run_id_a}:{step}:globals", f"{run_id_b}:{step}:globals",
            )
            if glob_diff:
                section_lines += [
                    "", "### Globals / prompts diff", "```diff", glob_diff, "```"
                ]

            # Always run the exhaustive tree walk.
            # depth == 0 is already covered by src_diff / glob_diff above.
            if hash_a and hash_b:
                all_nodes = find_all_divergences(conn, hash_a, hash_b, path=[step])
                deep = [n for n in all_nodes if n["depth"] > 0]
                if deep:
                    section_lines += [
                        "",
                        f"### Dependency Changes ({len(deep)} node(s))",
                    ]
                    for node in deep:
                        arrow = " → ".join(node["path"])
                        wc    = ", ".join(node["what_changed"])
                        hint  = _suggest_next_tool(node)
                        section_lines.append(
                            f"- **`{node['callee_name']}`** (depth {node['depth']}) "
                            f"— `{arrow}` — `{wc}`"
                        )
                        if hint:
                            section_lines.append(f"  - Next: `{hint}`")
                        if node.get("source_diff"):
                            section_lines += ["  ```diff", node["source_diff"], "  ```"]
                        if node.get("globals_diff"):
                            section_lines += [
                                "  ```json",
                                _json_pretty(node["globals_diff"]),
                                "  ```",
                            ]
                elif not src_diff and not glob_diff:
                    section_lines.append(
                        "> Hash differs but no callee snapshot found in logic_registry. "
                        "The dependency may have run before logic snapshotting was enabled."
                    )

            sections.append("\n".join(section_lines))

        if not sections:
            return (
                f"## No logic divergence detected\n\n"
                f"All steps in `{run_id_a}` and `{run_id_b}` share identical "
                "logic_hashes. If the outputs differ, the cause is in the input "
                "data or external state, not in the pipeline code or prompts."
            )

        total_steps = len(sections)
        header = [
            f"# Logic Divergence: `{run_id_a}` → `{run_id_b}`",
            "",
            f"{total_steps} step(s) with changed logic detected.",
            "---",
            "",
        ]
        return "\n".join(header) + "\n\n".join(sections)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Tool 5 — run_surgical_replay
# ---------------------------------------------------------------------------

def run_surgical_replay(
    script_path: str,
    force_step: Optional[str] = None,
    from_step: Optional[str] = None,
) -> str:
    """
    WARNING: This tool consumes API tokens. Use only after modifying logic or prompts.

    THE TIME MACHINE — validates a code fix in seconds using the Logram replay cache.

    Reruns the pipeline with LOGRAM_REPLAY=true so all unchanged steps replay from
    cache (no LLM calls) while modified steps execute live. This gives instant
    feedback on whether your fix resolves the issue without a full cold run.

    Typical fix-verify loop:
    1. get_investigation_brief → understand the failure
    2. get_step_source        → read the broken code
    3. Edit the source file   → apply your fix
    4. run_surgical_replay    → validate in ~2 seconds
    5. verify_against_golden_dataset → certify no regression

    When to call:
    - ONLY after editing a source file or modifying a prompt/constant.
    - To confirm that a change produces a better output without re-running expensive steps.
    - When you want fast feedback before running the full golden test suite.

    Security gates enforced (cannot be bypassed):
    - PATH JAIL: script_path must be a .py file inside the current working directory.
    - CIRCUIT BREAKER: max {REPLAY_SESSION_LIMIT} replays per session; after that a human
      must manually validate before the agent can continue.
    - LOGIC GUARD: if force_step code/prompts are unchanged since the last failure,
      execution is aborted to prevent wasting API tokens on guaranteed-to-fail replays.

    Args:
        script_path: Relative path to the Python pipeline entry-point (.py files only,
                     must be inside the current working directory).
        force_step:  Name of a single step to force live (invalidates its SUCCESS cache
                     entry). Use ONLY when the step last ran successfully and you want
                     to rerun it anyway. DO NOT use for FAILED steps — they have no
                     cache entry and always run live automatically. Passing force_step
                     for a FAILED step triggers the Logic Guard and aborts execution.
        from_step:   Name of a step from which to cascade live execution. This step
                     and all downstream steps run live; upstream steps replay from cache.

    Returns:
        Markdown report with: status, exit code, replays remaining in session budget,
        estimated API cost, and stderr/stdout snippets on failure.
    """
    global _replay_counter

    # ── Gate 1: Path Jail ─────────────────────────────────────────────────────
    safe, jail_msg = _is_path_safe(script_path)
    if not safe:
        return f"## Security Error — Path Jail Violation\n\n{jail_msg}"

    script = Path(script_path)
    if not script.exists():
        return (
            f"Script not found: '{script_path}'. "
            "Provide the path to the pipeline entry-point Python file."
        )

    # ── Gate 2: Circuit Breaker ───────────────────────────────────────────────
    with _replay_lock:
        if _replay_counter >= REPLAY_SESSION_LIMIT:
            return (
                f"## Replay Budget Exhausted\n\n"
                f"You have reached the session limit of **{REPLAY_SESSION_LIMIT} replays**.\n\n"
                "> **Action required:** Ask the human engineer to manually validate "
                "the pipeline behavior before continuing. This limit prevents runaway "
                "API costs from automated retry loops.\n\n"
                f"- Replays used: {_replay_counter}/{REPLAY_SESSION_LIMIT}\n"
                f"- Replays remaining: 0"
            )
        _replay_counter += 1
        current_count = _replay_counter

    remaining = REPLAY_SESSION_LIMIT - current_count
    budget_line = f"- Replays remaining: {remaining}/{REPLAY_SESSION_LIMIT}"
    cost_line = "- Estimated cost: ~$0.05"

    # ── Gate 3: Logic Guard ───────────────────────────────────────────────────
    if force_step and _logic_unchanged_since_failure(force_step):
        return (
            f"## Execution Aborted — Logic Unchanged Since Last Failure\n\n"
            f"The logic_hash (code + prompts) for step `{force_step}` is **identical** "
            "to the hash recorded at the time of its last failure. "
            "Retrying with the same code will produce the same error.\n\n"
            "> **IMPORTANT — VCR cache mechanics:** Logram only caches steps that "
            "completed with status SUCCESS or REPLAYED. A FAILED step has **no cache "
            "entry**. In replay mode it always runs live automatically — you do NOT "
            "need `force_step` to rerun it after a fix.\n\n"
            f"> **Correct call after editing `{force_step}`:**\n"
            "> ```\n"
            "> run_surgical_replay(\"<script_path>\")  # no force_step argument\n"
            "> ```\n"
            f"> `{force_step}` will run live because its only DB record has status "
            "FAILED (no cache hit).\n\n"
            "> If you intended to re-run a **previously successful** step, modify its "
            "source or global variables first so the logic_hash changes, then retry.\n\n"
            f"{budget_line}\n"
            "- Estimated cost avoided: ~$0.05"
        )

    # ── Execute (shell=False enforced — args passed as list) ──────────────────
    env = os.environ.copy()
    env["LOGRAM_REPLAY"] = "true"
    if force_step:
        env["LOGRAM_FORCE_STEP"] = force_step
    if from_step:
        env["LOGRAM_FORCE_FROM"] = from_step

    result = subprocess.run(
        [sys.executable, str(script)],   # list → shell=False by default
        env=env,
        capture_output=True,
        text=True,
    )

    stderr_snippet = (result.stderr or "")[:2000]
    stdout_snippet = (result.stdout or "")[:1000]

    if result.returncode == 0:
        lines = [
            "## Replay: SUCCESS",
            "- Exit code: 0",
            cost_line,
            budget_line,
            "",
            "> **VCR reminder:** FAILED steps have no cache — they always run live in replay mode.",
            "> Use `force_step` only to invalidate a **SUCCESS** cache entry.",
        ]
        if stdout_snippet:
            lines += ["", "```", stdout_snippet, "```"]
        return "\n".join(lines)

    lines = [
        f"## Replay: FAILED (exit code {result.returncode})",
        cost_line,
        budget_line,
        "",
        "### Stderr",
        "```",
        stderr_snippet or "(no stderr output)",
        "```",
    ]
    if stdout_snippet:
        lines += ["", "### Stdout", "```", stdout_snippet, "```"]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Tool 6 — verify_against_golden_dataset
# ---------------------------------------------------------------------------

def verify_against_golden_dataset(project: str, script_path: str) -> str:
    """
    CERTIFICATION GATE — final regression check before declaring a fix complete.

    Runs the pipeline against all GOLDEN-tagged reference inputs and compares
    outputs against the stored baselines. A GOLDEN run is a previously validated
    execution that represents correct behavior for a specific input document.

    This is the last step before committing a fix. It answers the question:
    "Does my change solve the reported issue without breaking any previously
    working case?"

    When to call:
    - After run_surgical_replay confirms the targeted fix works.
    - Before marking a bug as resolved or creating a pull request.
    - When a user asks "did my change introduce any regressions?"

    Prerequisite: at least one run must be tagged GOLDEN via `lg golden add <run_id>`.
    If no golden runs exist, this tool will say so clearly.

    Args:
        project:     Project name (used for display in the report).
        script_path: Path to the pipeline entry-point script.

    Returns:
        A plain-text report with pass/fail status per input document,
        number of regressed steps per input (if any), and an overall verdict:
        CERTIFIED (all pass) or REGRESSION DETECTED.
    """
    script = Path(script_path)
    if not script.exists():
        return (
            f"Script not found: '{script_path}'. "
            "Provide the path to the pipeline entry-point Python file."
        )

    result = subprocess.run(
        [sys.executable, "-m", "logram.cli", "test", str(script)],
        capture_output=True,
        text=True,
    )

    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "")[:1000].strip()

    if result.returncode == 0:
        lines = [
            f"## Golden Test: PASS — project `{project}`",
            "",
            "All GOLDEN inputs passed. No regressions detected.",
        ]
        if stdout:
            lines += ["", "```", stdout[:2000], "```"]
        return "\n".join(lines)

    lines = [
        f"## Golden Test: REGRESSION DETECTED — project `{project}`",
        "",
        "One or more GOLDEN inputs produced different outputs.",
        "Use analyze_logic_divergence to identify the cause.",
    ]
    if stdout:
        lines += ["", "```", stdout[:2000], "```"]
    if stderr:
        lines += ["", "### Stderr", "```", stderr, "```"]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Tool 7 — compare_step_data
# ---------------------------------------------------------------------------

_DATA_DIFF_TRUNCATE = 2000


def compare_step_data(run_id_a: str, run_id_b: str, step_name: str) -> str:
    """
    DATA ANALYST — compares the runtime inputs and outputs of a step between two runs.

    Use this when analyze_logic_divergence shows no logic change but the step still
    behaves differently, or when you suspect the input data changed between runs.
    Helps distinguish logic-only divergence from data divergence.

    When to call:
    - After analyze_logic_divergence shows no logic diff for a step.
    - When a step that should replay from cache runs live instead.
    - When two runs on supposedly identical inputs produce different outputs.

    Args:
        run_id_a:  Reference run ID (e.g. last known-good run).
        run_id_b:  Candidate run ID to compare against.
        step_name: Exact name of the step to inspect (from list_runs or get_investigation_brief).

    Returns:
        Markdown report with:
        - Step metadata for each run (status, duration)
        - Inputs diff: identical confirmation or unified diff
        - Outputs diff: identical confirmation or unified diff (truncated for large payloads)
        - Verdict: data-only divergence, logic-only, or both
    """
    try:
        conn = _connect()
    except FileNotFoundError as exc:
        return str(exc)

    try:
        def _fetch_step(run_id: str) -> Optional[dict]:
            row = conn.execute(
                """
                SELECT name, status, duration, inputs_json, output_json, error_json
                FROM steps
                WHERE run_id = ? AND name = ?
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (run_id, step_name),
            ).fetchone()
            return dict(row) if row else None

        step_a = _fetch_step(run_id_a)
        step_b = _fetch_step(run_id_b)

        if not step_a and not step_b:
            return (
                f"Step `{step_name}` not found in either run. "
                "Check the step name with get_investigation_brief."
            )

        lines = [
            f"## Step Data Comparison — `{step_name}`",
            "",
        ]

        # Metadata
        def _meta(label: str, step: Optional[dict]) -> str:
            if not step:
                return f"- **{label}**: not present in this run"
            dur = f"{step['duration']:.2f}s" if step.get("duration") else "?"
            return f"- **{label}**: status=`{step['status']}`, duration={dur}"

        lines += [_meta(f"Run A (`{run_id_a[:8]}`)", step_a)]
        lines += [_meta(f"Run B (`{run_id_b[:8]}`)", step_b)]
        lines += [""]

        # Inputs diff
        in_a = _json_pretty(_parse_json((step_a or {}).get("inputs_json"))) if step_a else ""
        in_b = _json_pretty(_parse_json((step_b or {}).get("inputs_json"))) if step_b else ""

        lines.append("### Inputs")
        if in_a == in_b:
            lines.append("> Identical — input data did not change between runs.")
        else:
            raw_diff = _diff_text(in_a, in_b, f"{run_id_a}:inputs", f"{run_id_b}:inputs")
            if len(raw_diff) > _DATA_DIFF_TRUNCATE:
                raw_diff = raw_diff[:_DATA_DIFF_TRUNCATE] + "\n... (truncated)"
            lines += ["```diff", raw_diff or "(no diff content)", "```"]

        lines += [""]

        # Outputs diff
        out_a = _json_pretty(_parse_json((step_a or {}).get("output_json"))) if step_a else ""
        out_b = _json_pretty(_parse_json((step_b or {}).get("output_json"))) if step_b else ""

        lines.append("### Outputs")
        if out_a == out_b:
            lines.append("> Identical — output data did not change between runs.")
        else:
            raw_diff = _diff_text(out_a, out_b, f"{run_id_a}:output", f"{run_id_b}:output")
            if len(raw_diff) > _DATA_DIFF_TRUNCATE:
                raw_diff = raw_diff[:_DATA_DIFF_TRUNCATE] + "\n... (truncated — outputs may contain large payloads)"
            lines += ["```diff", raw_diff or "(no diff content)", "```"]

        lines += [""]

        # Verdict
        inputs_same  = in_a == in_b
        outputs_same = out_a == out_b
        lines.append("### Verdict")
        if inputs_same and outputs_same:
            lines.append(
                "> Inputs and outputs are identical. If a cache miss still occurs, "
                "the cause is in the logic (code or globals). Use `analyze_logic_divergence`."
            )
        elif inputs_same and not outputs_same:
            lines.append(
                "> Inputs identical, outputs differ. The divergence is logic-only. "
                "Use `analyze_logic_divergence` to find which code or prompt changed."
            )
        elif not inputs_same and outputs_same:
            lines.append(
                "> Inputs differ but outputs are identical. The step is robust to this input change."
            )
        else:
            lines.append(
                "> Both inputs and outputs differ. This is a data divergence. "
                "The input change likely invalidated the VCR cache key for this step."
            )

        return "\n".join(lines)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# MCP registration — attach plain functions as tools
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "logram-debugger",
    instructions=(
        "IMPORTANT: this server exposes TOOLS only — there are no resources to list. "
        "Do NOT call listMcpResources. Do NOT read the SQLite file directly. "
        "Do NOT use Bash or file-reading tools to inspect the database. "
        "All pipeline data is accessible exclusively through the tools below.\n\n"
        "MANDATORY WORKFLOW — follow this order:\n"
        "1. list_runs(project=...) — always start here to get run_ids\n"
        "2. get_investigation_brief(run_id) — call as soon as you see a failed run\n"
        "3. get_step_source(logic_hash) — read the exact code + prompts from the brief\n"
        "4. analyze_logic_divergence(run_id_a, run_id_b) — diff two runs when needed\n"
        "4b. compare_step_data(run_id_a, run_id_b, step_name) — diff runtime inputs/outputs "
        "when logic is identical but behavior still differs\n"
        "5. [edit the source file yourself to apply the fix]\n"
        "6. run_surgical_replay(script_path) — validate fix in ~2s. "
        "Pass NO extra args: FAILED steps have no VCR cache and always run live. "
        "force_step is only for invalidating a SUCCESS cache entry.\n"
        "7. verify_against_golden_dataset(project, script_path) — certify no regression\n\n"
        "NEVER skip step 1. NEVER read files before calling the tools."
    ),
)

mcp.tool()(list_runs)
mcp.tool()(get_investigation_brief)
mcp.tool()(get_step_source)
mcp.tool()(analyze_logic_divergence)
mcp.tool()(compare_step_data)
mcp.tool()(run_surgical_replay)
mcp.tool()(verify_against_golden_dataset)


# ---------------------------------------------------------------------------
# Entry point (stdio MCP server)
# ---------------------------------------------------------------------------

def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
