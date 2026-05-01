# CLAUDE.md — Logram Project Guidance

> Read by Claude Code at session start. Establishes project context and the rules to follow when modifying this codebase OR when instrumenting a downstream user codebase with Logram.

---

## What this project is

**Logram** is a Python SDK that records every step of an AI pipeline locally and replays unchanged steps from cache when you re-run. It is an iteration infrastructure and a "Time-Machine debugger" for LLM/VLM pipelines — NOT a monitoring tool, NOT a test framework, NOT an orchestrator.

Core primitives exposed from `logram/__init__.py`:
- `init(project, input_id, ...)` / `finalize(status)` / `flush()` — run lifecycle.
- `@trace(...)` — decorator that captures step inputs/outputs and the logic fingerprint.
- `@stateful(include=[...])` — class decorator declaring tracked instance attributes.
- `worker_init(run_id)` — propagates the run context across multiprocessing workers.
- `bind_session_run` / `with_session_run` — per-request binding for web servers.
- `set_run_id` / `current_run_id` / `current_input_id` — manual context control.

---

## Project structure (DO NOT REORGANIZE)

```
logram/
├── __init__.py          # public API surface — re-exports only
├── decorators.py        # @trace, @stateful, the wrapper machinery
├── oracle.py            # AST hashing, JIT global resolution, MRO traversal, callee Merkle tree
├── storage.py           # SQLite + WAL backend, background write thread, content-addressed blobs
├── serializer.py        # ensure_serializable / rehydrate_logram_output, type tagging
├── context.py           # ContextVar definitions (current_run_id, current_input_id, current_step_id)
├── versioning.py        # git-derived semantic version IDs
├── metrics.py           # token usage extraction, ROI computation
├── analysis.py          # logic divergence analysis (recursive callee diff)
├── mcp_server.py        # Model Context Protocol server for autonomous agents
├── cli.py               # Typer CLI (logram inspect / replay / diff / stats / mcp / ...)
└── server/              # local FastAPI dashboard (read-only)
```

---

## When working ON the Logram SDK itself

You are editing the engine. Apply the standard rules of this repo:
- Preserve the public API. Anything imported from `logram` (top-level) is contract.
- The decorator's zero-crash contract is non-negotiable: every tracing operation runs inside a total catch-all. If you change `_logic_async` / `_logic_sync` / `_logic_async_gen` / `_logic_sync_gen`, the user pipeline MUST keep running even if instrumentation throws.
- Storage writes happen on a background daemon thread. Never make a write path blocking.
- Oracle changes (in `oracle.py`) affect cache stability for every existing user. Treat AST hash changes as breaking.
- The MCP server (`mcp_server.py`) enforces three security gates on `run_surgical_replay` (path jail, circuit breaker, logic guard). Do not relax them without explicit reason.

---

## When instrumenting a downstream user codebase

The authoritative ruleset is **`LOGRAM_AGENT_RULES.md`** at the project root. Read it before any instrumentation work. The condensed Cursor version is in `.cursorrules`. The summary below is for quick reference.

### Prime Directive — Autonomous Judgment & Zero-Intrusion

You are a guest in the user's codebase. **Adapt Logram to the existing code; never refactor the code to fit Logram.** The configuration surface (`ignore_in_hash`, `vcr_key_fn`, `track_args`, `include_state`, `state_in_hash`, `__logram_trace_key__`) exists SPECIFICALLY so you can preserve user code shape.

Default reflexes:
- When in doubt, instrument LESS, not more.
- Exclude-first before refactoring signatures.
- The "do-nothing" option is always valid for any candidate function.
- Surface every non-obvious decision in the hand-off — silent decisions are the leading cause of "the cache is lying to me" complaints.
- Stop at the first solution that works. Do not stack improvements.

### Lifecycle skeleton

```python
import logram

async def main():
    logram.init(project="my_pipeline", input_id=doc_id)
    try:
        result = await pipeline(doc_id)
        await logram.finalize(status="success")
        return result
    except Exception:
        await logram.finalize(status="failed")
        raise
```

`init()` exactly once, before any traced function. `finalize()` awaited on BOTH paths. Never call `init()` mid-run or inside multiprocessing workers (use `worker_init` instead).

### Decoration rules (compressed)

- `@logram.trace()` is the **INNERMOST** decorator on stacks (under `@app.post`, `@retry`, etc.).
- Trace meaningful work boundaries (LLM calls, expensive transforms, fan-out steps). Not trivial helpers, dunders, or framework boilerplate.
- Sync / async / generator / async-generator — all native. Do not change the function shape.
- Exceptions MUST propagate. Never `try: ... except: return None` inside a traced function.
- State-mutating classes use `@logram.stateful(include=[load_bearing_fields_only])`.
- Custom argument classes need `__logram_trace_key__` returning a stable serializable dict (or be Pydantic / dataclass / have stable repr).

### Cost ladder for adapting cache stability (LOWEST tier first)

1. `@trace()` no params → 2. `ignore_in_hash` → 3. `vcr_key_fn` → 4. `track_args` → 5. `__logram_trace_key__` (additive, three lines) → 6. `@stateful` → 7. move inline imports → 8. **only with explicit user approval:** signature changes, class splits, module reorganization.

### Absolute prohibitions

1. NEVER edit / wrap / monkey-patch the Logram SDK source.
2. NEVER swallow exceptions inside `@trace`.
3. NEVER trace a function that calls `logram.init/finalize/flush`.
4. NEVER trace dunders.
5. NEVER hardcode `os.environ["LOGRAM_REPLAY"] = "true"` in user source.
6. NEVER commit `.logram/` or `.logram_assets/` (add both to `.gitignore`).
7. NEVER use `LOGRAM_FORCE_STEP` on a FAILED step (Logic Guard will abort).
8. NEVER restructure user code without explicit approval.

### Hand-off format

After any instrumentation, report:
1. **Traced** — `<file>:<symbol> — <reason>`.
2. **Deliberately NOT traced** — `<file>:<symbol> — <reason>`.
3. **Configuration knobs used** — every `ignore_in_hash` / `vcr_key_fn` / etc., with one-line rationale.
4. **Code changes beyond decorators** — list and justify each. If empty, say so.
5. **Open questions parked** — refactors considered but not performed.
6. **Verification commands** — one live run, then `LOGRAM_REPLAY=true python <entry>.py`.

### Diagnostic signals to watch for

| Log signal | Meaning | Action |
|---|---|---|
| `[PROBE 2][UNSTABLE_REPR] type=Foo` | Class `Foo` passed to `@trace` has unstable repr | Add `__logram_trace_key__` to `Foo` |
| `[Logram][oracle] callee budget exhausted` | Call graph >256 user functions | With user consent: `logram.oracle._CALLEE_BUDGET = 1024` |
| Replay always misses, no warnings | Inline imports / mid-run global mutation / unstable args | Audit imports; consider `ignore_in_hash` |
| `default_run` mixed entries in `logram inspect` | Multiprocessing without `worker_init` | Add `initializer=logram.worker_init` |

---

## Environment variables (user-facing controls — do not hardcode)

| Variable | Purpose |
|---|---|
| `LOGRAM_REPLAY=true` | Enable replay mode |
| `LOGRAM_FORCE_STEP=name1,name2` | Force named SUCCESS steps to re-execute live |
| `LOGRAM_FORCE_FROM=name` | Cascade live execution from this step onward |
| `LOGRAM_DB_PATH=/path/logram.db` | Override SQLite location |
| `LOGRAM_PROJECT_ROOT=/path` | Override project root for blob assets |
| `LOGRAM_INPUT_ID=...` | Override `input_id` for the current run |

---

## Dev commands (this repo)

```bash
pip install -e .                       # install in editable mode
logram --help                          # browse CLI
logram inspect <run_id>                # render run step tree
logram replay <script.py>              # surgical replay
logram diff <run_a> <run_b>            # full diff (code, globals, inputs, outputs)
logram mcp start                       # launch MCP server (stdio)
```

The package builds via `flit_core`. `pyproject.toml` declares the dependency surface — keep it minimal.

---

## When in doubt

- Working untraced code beats broken instrumented code.
- A `@trace` parameter beats a code refactor.
- Surfacing a question beats a silent decision.
- The user's existing convention beats any rule in this file.
- For edge cases, full rationale, and the complete refactoring playbook → **`LOGRAM_AGENT_RULES.md`**.
