# LOGRAM_AGENT_RULES.md

> **AUDIENCE:** Autonomous coding agents (Cursor, Claude Code, Copilot CLI, Codex, custom LLM agents).
> **PURPOSE:** Authoritative ruleset for installing, instrumenting, and writing Python code in a project that uses the **Logram SDK** (`logram-sdk >= 0.3.0`).
> **CONTRACT:** These rules are imperative. Treat MUST / MUST NOT / SHALL as RFC 2119 keywords. If a user instruction conflicts with a rule below, surface the conflict and ASK before proceeding.

---

## 0. AGENT BOOTSTRAP — READ BEFORE ANY ACTION

<rule id="0.1" priority="critical">
**You are operating on a USER codebase, not on the Logram SDK source.**
You MUST NOT edit, monkey-patch, subclass, or fork any module under the `logram/` package (`logram.decorators`, `logram.storage`, `logram.oracle`, `logram.serializer`, `logram.context`, `logram.mcp_server`, `logram.cli`).
The SDK is an opaque, stable interface. The only legal entry points are the symbols re-exported from `logram/__init__.py`:
`init`, `finalize`, `flush`, `set_run_id`, `bind_session_run`, `with_session_run`, `worker_init`, `trace`, `stateful`, `current_run_id`, `current_input_id`, `rehydrate_logram_output`, `get_semantic_version`.
</rule>

<rule id="0.2" priority="critical">
**Logram is a Data-Flow Tracker, not an APM, not a logger, not a test framework.**
Its sole purpose is to record SUCCESSFUL step outputs and replay them on subsequent runs when the underlying logic is unchanged. Do not propose Logram as a replacement for `logging`, `pytest`, `OpenTelemetry`, or `Sentry`.
</rule>

<rule id="0.3" priority="critical">
**The cache is sacred. Every refactor you perform MUST preserve cache stability.**
A "good" refactor leaves the AST hash, the JIT-resolved globals, and the callee Merkle tree of every previously-traced function unchanged unless the user explicitly requests a behavioral change. If you cannot guarantee cache stability, STOP and ask the user.
</rule>

<rule id="0.4" priority="high">
**Before instrumenting anything, run a discovery pass:**
1. Locate the pipeline entry point (typically a script with `if __name__ == "__main__":` or an async `main()`).
2. Identify the LLM / VLM / API call sites (`openai.*`, `anthropic.*`, `google.generativeai.*`, `litellm.*`, HTTP clients to model endpoints).
3. Identify the orchestration layer (LangChain `Runnable`, LlamaIndex `QueryEngine`, plain Python functions, FastAPI routes).
4. Identify class-based pipelines that accumulate state across method calls (`self.results`, `self.cache`, `self.page_map`, etc.).
5. Identify multiprocessing / Ray / Celery boundaries.

Report your findings to the user BEFORE editing files. Do not assume.
</rule>

---

## 0.5. PRIME DIRECTIVE — AUTONOMOUS JUDGMENT & ZERO-INTRUSION

> **This section overrides every other rule in this document when they conflict.**
> If a rule below says "MUST add `@stateful`" and applying it would force a destructive refactor of stable user code, the Prime Directive wins: you adapt Logram to the code, never the other way around.

<rule id="0.5" priority="critical">
**LOGRAM IS A GUEST IN THE USER'S CODEBASE. ACT LIKE ONE.**
The user wrote, debugged, and shipped their pipeline before you arrived. Their structure encodes constraints you cannot see (review history, team conventions, downstream consumers, framework idioms, migrations in flight). Your job is to slip Logram in around what already works — NOT to "modernize" the codebase, restructure modules, rename functions, change return types, or impose an architecture. Every byte of code you change without an explicit cache-stability or correctness reason is a violation of this directive.
</rule>

<rule id="0.6" priority="critical">
**ADAPT LOGRAM TO THE CODE. NEVER REFACTOR THE CODE TO FIT LOGRAM.**
Logram exposes a wide configuration surface SPECIFICALLY so you do not have to bend the user's code to make caching work. Reach for the configuration knob FIRST. Only refactor when no configuration option exists, AND the refactor is locally safe, AND you have surfaced it to the user.

| User-code shape | ❌ Wrong reflex (intrusive) | ✅ Right reflex (adaptive) |
|---|---|---|
| Function takes a `request_id` that bloats the cache key | Remove the parameter / restructure the signature | `@trace(ignore_in_hash=["request_id"])` |
| Class has 14 attributes, only 3 matter for replay | Split the class, extract a "state container" | `@stateful(include=["only", "the", "three"])` |
| Helper method on a stateful class doesn't read `self.*` | Move it to a module-level function | `@trace(state_in_hash=False)` |
| Method only depends on `self.page_map`, not the rest | Refactor the class hierarchy | `@trace(include_state=["page_map"])` |
| Function receives a giant 80KB image object | Change the function signature | `@trace(vcr_key_fn=lambda f, a, kw: ({"id": kw["tile"].id}, {}))` |
| Mutable in-place argument pattern | Rewrite to return a value | `@trace(track_args=["accumulator"])` |
| Custom class with unstable `repr()` | Convert to a Pydantic model | Add `__logram_trace_key__` (one method, three lines) |
| Pipeline uses `getattr(self, dyn)` dispatch | Replace dispatch with explicit `if/elif` chain | `@trace(vcr_key_fn=...)` exposing the discriminator |
| `logram view` shows unhelpful inputs | Change what the function accepts | `@trace(log_input_fn=lambda ...)` |
| Function returns a dict you cannot change downstream | Force a Pydantic model | Accept the dict — type rehydration is a bonus, not a requirement |

If the user's code is genuinely incompatible with caching even after exhausting these knobs, **DO NOT TRACE IT**. A function left untraced is a feature, not a failure.
</rule>

<rule id="0.7" priority="critical">
**WHEN IN DOUBT, INSTRUMENT LESS.**
A small, surgical instrumentation that caches the 3 most expensive steps is infinitely more valuable than an exhaustive instrumentation that decorates 30 functions and breaks one. The user can always extend coverage later. They cannot easily revert a destructive refactor that touched fifteen files.

Default coverage targets, in order of priority:
1. **The single most expensive step** (the LLM/VLM call). If you trace nothing else, trace this.
2. **Aggregation steps that consume many of (1)** (per-tile loops, per-document fan-out).
3. **Deterministic but slow transforms** (PDF parsing, embedding, large file I/O).

Stop there for the first pass. Hand off to the user. Let them tell you where to go deeper.
</rule>

<rule id="0.8" priority="critical">
**EXCLUDE-FIRST, INCLUDE-LATER.**
The configuration toolkit is asymmetric BY DESIGN: it is far easier and safer to *exclude* something from the cache key than to *re-add* it later. When uncertain whether an argument or state field affects the output, **exclude it from the hash and let the user opt back in**. The cost of an over-narrow key is a stale cache hit (visible immediately, fixable with a force-replay). The cost of an over-broad key is silent loss of every cache benefit (invisible, undermines the entire investment).

Default mental model: every argument is `ignore_in_hash` until proven load-bearing.
</rule>

<rule id="0.9" priority="high">
**THE "DO-NOTHING" OPTION IS ALWAYS ON THE TABLE.**
For each candidate target you considered tracing, you MUST be able to answer "why this function?". If the only answer is "because it exists" or "for completeness", **do not trace it**. Acceptable answers are exactly:
- "It calls a paid external API" (LLM, embeddings, third-party service).
- "It takes >500ms reliably and is called repeatedly across runs."
- "The user explicitly asked to instrument it."
- "It is a fan-out point whose downstream depends on its cached output."

Anything else: leave it alone. Tracing imposes Oracle introspection cost on every first call of a run. Tiny pure functions pay that cost without benefit.
</rule>

<rule id="0.10" priority="high">
**SURFACE EVERY NON-OBVIOUS DECISION TO THE USER. DO NOT NEGOTIATE WITH YOURSELF.**
Whenever you reach for an `ignore_in_hash`, a `vcr_key_fn`, an `exclude_state`, a class-level refactor, or a "skip this function" decision — write it in your hand-off summary with a one-line rationale. The user's trust in Logram depends on understanding what you cached and why. Silent decisions are the leading cause of "the cache is lying to me" bug reports.

Format every decision as: `<file>:<symbol> — <action> — <because>`.
</rule>

<rule id="0.11" priority="critical">
**REFACTORS HAVE A BLAST RADIUS. RESPECT IT.**
Before any structural change to user code (moving a function, splitting a class, changing a return type, modifying a signature), ask:
1. Is this change LOCAL (one function body) or NON-LOCAL (touches callers, imports, type stubs, tests)?
2. Could this break a test, a downstream import, a serialized format, or a public API?
3. Is the cache-stability gain worth the disruption — or can a `@trace` parameter achieve the same result?

If the change is non-local, **stop and propose it as a discrete suggestion**. Do not bundle it into "Logram instrumentation" — it is a separate refactor with its own review surface.
</rule>

---

## 1. INSTALLATION

<rule id="1.1" priority="critical">
**Add the dependency. Do not vendorize the SDK.**

```bash
pip install https://github.com/Mnemonic-Systems/logram.git
# or, in pyproject.toml:
#   dependencies = ["logram-sdk>=0.3.0"]
# or, in requirements.txt:
#   logram-sdk>=0.3.0
```

The package exposes two console scripts: `logram` and `lg`. Both point to `logram.cli:app`.
</rule>

<rule id="1.2" priority="high">
**Add `.logram/` and `.logram_assets/` to `.gitignore`.**

```gitignore
# Logram local trace store (DO NOT COMMIT)
.logram/
.logram_assets/
```

Why: `.logram/logram.db` is a local SQLite store. `.logram_assets/` contains content-addressed binary blobs (images, PDFs). Both are machine-local development artifacts. Committing them will leak data and bloat the repository.
</rule>

<rule id="1.3" priority="medium">
**Optional — register the MCP server with the user's IDE.**
Run this command ONLY if the user explicitly asks for MCP integration:

```bash
logram mcp install      # auto-links to detected coding agent
# or, to print the JSON config block:
logram mcp config
```

DO NOT modify `~/.cursor/mcp.json` or `~/.config/claude/mcp.json` by hand. Always defer to the `logram mcp install` command.
</rule>

---

## 2. THE INSTRUMENTATION CONTRACT — THREE LIFECYCLE PHASES

Every Logram-instrumented script follows the same skeleton. Memorize it.

```python
import logram

# 1. INIT — at the top of the entry point, before any traced function runs.
run_id = logram.init(
    project="my_pipeline",      # REQUIRED — string, used for filtering runs.
    input_id="doc_42",          # OPTIONAL but RECOMMENDED — stable per-input key.
    run_name=None,              # OPTIONAL — human label; auto-suffixed with timestamp.
    tags=None,                  # OPTIONAL — list[str], e.g. ["GOLDEN", "v2-prompt"].
)

# 2. EXECUTION — your @logram.trace-decorated functions run here.
result = await pipeline(...)

# 3. FINALIZE — at the end of the script, in a try/except. ALWAYS awaited.
await logram.finalize(status="success", metrics={"pages": 12})
```

<rule id="2.1" priority="critical">
**`logram.init()` MUST be called exactly once per pipeline run, BEFORE any `@trace`-decorated function executes.**
Calling `init()` mid-run resets the run_id and corrupts the trace tree. If the user's codebase has multiple entry points, instrument each one separately.
</rule>

<rule id="2.2" priority="critical">
**`await logram.finalize(status=...)` MUST be called at the end of the run, in both success and failure paths.**
Use `try/except/finally` or a context manager pattern. Without `finalize()`, the background write thread may not flush in time, and the run will appear UNFINISHED in `logram list`.

#### ✅ DO — Correct lifecycle wrapper
```python
async def main():
    logram.init(project="my_pipeline", input_id=doc_id)
    try:
        result = await run_pipeline(doc_id)
        await logram.finalize(status="success")
        return result
    except Exception:
        await logram.finalize(status="failed")
        raise
```

#### ❌ DO NOT — Missing finalize on error path
```python
async def main():
    logram.init(project="my_pipeline", input_id=doc_id)
    result = await run_pipeline(doc_id)        # ← if this raises, finalize never runs
    await logram.finalize(status="success")
    return result
```
</rule>

<rule id="2.3" priority="high">
**For sync-only scripts that cannot use `await`, run finalize via a small bridge:**

```python
import asyncio
import logram

logram.init(project="my_pipeline", input_id=doc_id)
try:
    result = run_pipeline(doc_id)              # sync
finally:
    asyncio.run(logram.finalize(status="success"))
```

DO NOT call `logram.finalize()` without `await` / `asyncio.run()`. It returns a coroutine; ignoring it leaks a never-awaited task and skips the final flush.
</rule>

---

## 3. THE `@logram.trace` DECORATOR — WHERE TO PUT IT

`@logram.trace()` is the primary instrumentation primitive. It captures a step's inputs, output, logic fingerprint, and globals. On replay, unchanged steps return their cached output without re-executing.

<rule id="3.1" priority="critical">
**Decorate the OUTERMOST function whose execution you want to cache as a single atomic unit.**
Trace the meaningful boundaries of work — a "step" in the user's mental model — not every helper.

#### ✅ TARGETS WORTH TRACING
- LLM / VLM call wrappers (`call_gemini`, `extract_with_gpt`, `embed`)
- Expensive deterministic transforms (`parse_pdf`, `tokenize_long_doc`, `vectorize_image`)
- I/O-bound or cost-bearing steps (`fetch_from_api`, `transcribe_audio`)
- Per-tile / per-document pipeline stages (`process_tile`, `extract_quantities`)

#### ❌ DO NOT TRACE
- Pure trivial helpers (`def _normalize(x): return x.strip().lower()`) — overhead exceeds benefit.
- Functions that return non-serializable resources (open files, sockets, DB connections, threads, locks).
- Functions whose output is intentionally non-deterministic and must run live every time (e.g. `random.choice`, `time.time` wrappers, UUID generators) — unless wrapped behind a deterministic seed.
- Anything inside the `logram` package itself.
- Test fixtures and `pytest` test functions.
</rule>

<rule id="3.2" priority="critical">
**`@logram.trace` MUST be the INNERMOST decorator (closest to the function definition) when stacked with framework decorators.**

The reason: framework decorators (`@app.post`, `@retry`, `@cache`) often replace the function with a wrapper object. If `@trace` is on top of them, it traces the wrapper — losing access to the real function's source, signature, and call graph. The Oracle's AST hash becomes worthless.

#### ✅ DO — `@trace` closest to the function
```python
@app.post("/analyze")           # ← framework decorator (outer)
@logram.trace()                 # ← Logram (inner — sees the real function)
async def analyze(payload: Payload) -> Result:
    ...
```

#### ❌ DO NOT — `@trace` on top of a framework decorator
```python
@logram.trace()                 # ← traces the FastAPI wrapper, not your code
@app.post("/analyze")
async def analyze(payload: Payload) -> Result:
    ...
```
</rule>

<rule id="3.3" priority="high">
**Trace methods on classes the same way you trace functions. The Oracle is MRO-aware.**

```python
class DocumentPipeline:
    @logram.trace()
    async def process_tile(self, tile: ImageTile) -> dict:
        ...

    @logram.trace()
    async def aggregate(self, tiles: list[dict]) -> Result:
        ...
```

When a method calls `self.helper(x)`, the Oracle resolves `helper` through `cls.__mro__` and includes its source in the parent's logic hash. You do NOT need to trace `helper` for the dependency to be tracked — but tracing it makes its output independently cacheable.
</rule>

<rule id="3.4" priority="high">
**Sync, async, generators, async generators — all four are supported natively. Do NOT rewrite the function shape.**

```python
@logram.trace()
def sync_step(x): ...                 # ✅ supported

@logram.trace()
async def async_step(x): ...          # ✅ supported

@logram.trace()
def stream(x):                        # ✅ supported (generator)
    yield ...

@logram.trace()
async def astream(x):                 # ✅ supported (async generator)
    async for chunk in upstream():
        yield chunk
```

DO NOT wrap an async function in `asyncio.run()` to "make it sync" so you can trace it. The decorator inspects the function and dispatches correctly.
</rule>

---

## 4. STATEFUL PIPELINES — `@logram.stateful`

<rule id="4.1" priority="critical">
**If a class accumulates mutable state across `@trace`-decorated method calls, you MUST decorate the class with `@logram.stateful(include=[...])` listing the load-bearing attributes.**

Without `@stateful`, replay will return the cached output of `process_tile` but will NOT restore the mutations to `self.results` / `self.page_map`. Downstream steps will see an empty accumulator and produce wrong results — silently.

#### ✅ DO — Stateful class fully declared
```python
@logram.stateful(include=["results", "page_map", "ocr_cache"])
class DocumentPipeline:
    def __init__(self):
        self.results: dict = {}
        self.page_map: dict = {}
        self.ocr_cache: dict = {}

    @logram.trace()
    async def process_tile(self, tile: ImageTile) -> dict:
        out = await call_vlm(tile)
        self.results[tile.tile_id] = out      # mutation captured & restored on replay
        return out
```

#### ❌ DO NOT — Mutate state without declaring it
```python
class DocumentPipeline:                       # ← missing @stateful
    def __init__(self):
        self.results = {}

    @logram.trace()
    async def process_tile(self, tile):
        out = await call_vlm(tile)
        self.results[tile.tile_id] = out      # ← mutation LOST on replay
        return out
```
</rule>

<rule id="4.2" priority="high">
**`include=[...]` MUST list ONLY attributes that are read or written by `@trace`-decorated methods.**
Listing transient or huge fields (`self._http_client`, `self._semaphore`, `self.raw_pdf_bytes`) will bloat the cache key and cause needless cache misses. List the SEMANTIC state, not the implementation detail.
</rule>

<rule id="4.3" priority="medium">
**Per-method state scoping with `include_state` / `exclude_state` / `state_in_hash=False`:**

- `@logram.trace(include_state=["page_map"])` — restrict the hash to one field.
- `@logram.trace(exclude_state=["font_registry"])` — drop one field from the default set.
- `@logram.trace(state_in_hash=False)` — declare the method as state-pure (helper utilities only).

Use these to fine-tune cache behavior on methods whose actual state dependency is narrower than the class-level `include` list.
</rule>

---

## 5. CUSTOM OBJECT IDENTITY — `__logram_trace_key__`

<rule id="5.1" priority="critical">
**Any custom class that is passed as an ARGUMENT to a `@trace`-decorated function MUST implement `__logram_trace_key__` UNLESS it is a Pydantic model, a dataclass, or has a stable content-based `__repr__`.**

Why: Logram serializes arguments to build the VCR cache key. The default fallback for unknown objects is `repr(obj)`, which often returns `<MyClass object at 0x7f3a2c>`. Memory addresses change every run → cache key drifts → replay never hits.

The PROBE 2 logger emits `[PROBE 2][UNSTABLE_REPR]` warnings when this happens. If you see them, fix the class.

#### ✅ DO — Stable identity protocol
```python
class ImageTile:
    def __init__(self, tile_id: str, page: int, image_bytes: bytes, bbox: tuple):
        self.tile_id = tile_id
        self.page = page
        self.image_bytes = image_bytes
        self.bbox = bbox

    def __logram_trace_key__(self) -> dict:
        # Return a STABLE, SERIALIZABLE dict that uniquely identifies this object.
        # NO memory addresses. NO timestamps. NO non-deterministic values.
        return {"tile_id": self.tile_id, "page": self.page, "bbox": self.bbox}
```

#### ❌ DO NOT — Include volatile fields in the key
```python
class ImageTile:
    def __logram_trace_key__(self):
        return {
            "tile_id": self.tile_id,
            "loaded_at": time.time(),     # ← changes every run → cache always misses
            "object_id": id(self),        # ← changes every run → cache always misses
        }
```
</rule>

<rule id="5.2" priority="medium">
**`__logram_trace_log__` is OPTIONAL — implement only if you want a richer DISPLAY representation in `logram view` without affecting the cache key.**

```python
class ImageTile:
    def __logram_trace_key__(self):
        return {"tile_id": self.tile_id}              # cache key (minimal)

    def __logram_trace_log__(self):
        return {                                       # richer display
            "tile_id": self.tile_id,
            "page": self.page,
            "bbox": self.bbox,
            "image_size_kb": len(self.image_bytes) // 1024,
        }
```
</rule>

---

## 6. RETURN TYPES — TYPED OBJECTS = PERFECT REHYDRATION

<rule id="6.1" priority="high">
**Prefer Pydantic models or `@dataclass` types as return values from `@trace`-decorated functions.**

Logram tags typed objects with their class metadata at capture time and rehydrates the exact instance on replay. Plain dicts work but downstream code must handle dict access instead of attribute access.

#### ✅ DO — Refactor toward typed outputs
```python
from pydantic import BaseModel

class ExtractionResult(BaseModel):
    quantities: list[float]
    units: list[str]
    confidence: float

@logram.trace()
def extract_quantities(page: Page) -> ExtractionResult:
    ...
    return ExtractionResult(quantities=qs, units=us, confidence=c)
```

#### ⚠️ ACCEPTABLE — Plain dicts (works, but loses type reconstruction)
```python
@logram.trace()
def extract_quantities(page):
    return {"quantities": qs, "units": us, "confidence": c}
```

#### ❌ DO NOT — Return non-serializable resources
```python
@logram.trace()
def open_pdf(path):
    return open(path, "rb")              # ← file handle. On replay → str(file_obj). BROKEN.

@logram.trace()
def get_db():
    return psycopg2.connect(...)          # ← DB connection. NEVER cacheable.
```
</rule>

<rule id="6.2" priority="medium">
**If a function MUST return a non-serializable resource, do NOT trace it. Wrap the surrounding logic instead.**

#### Before (wrong)
```python
@logram.trace()
def load_image(path):
    return PIL.Image.open(path)           # PIL.Image is non-trivially serializable
```

#### After (correct)
```python
def load_image(path):                     # ← untraced — returns the live PIL object
    return PIL.Image.open(path)

@logram.trace()
def describe_image(path: str) -> str:     # ← traced — returns plain text
    img = load_image(path)
    return vlm.describe(img)
```
</rule>

---

## 7. CONSTANTS, PROMPTS, AND MODULE-LEVEL GLOBALS

<rule id="7.1" priority="critical">
**Define prompts and configuration constants at MODULE LEVEL. The Oracle captures their runtime value automatically — naming convention is irrelevant.**

```python
# All four are captured by the Oracle. No special syntax required.
SYSTEM_PROMPT = "You are an extraction assistant..."
temperature = 0.7
modelName = "gpt-4o"
PROMPT_VARIANTS = ["a", "b"]

@logram.trace()
def extract(text: str):
    return llm(SYSTEM_PROMPT, text, temperature=temperature, model=modelName)
```

Editing `temperature = 0.7` to `0.9` correctly invalidates the cache for `extract()` and every step that calls it.
</rule>

<rule id="7.2" priority="critical">
**MOVE INLINE IMPORTS TO THE TOP OF THE MODULE.** Imports inside a function body create LOCAL bindings that the Oracle treats as opaque and does NOT capture.

#### ❌ BEFORE — Constants invisible to the Oracle
```python
def call_llm(prompt):
    import openai                          # ← local binding
    return openai.chat.completions.create(
        model=openai.MODEL_DEFAULT,        # ← __local__:openai.MODEL_DEFAULT — FILTERED
        messages=[{"role": "user", "content": prompt}],
    )
```

#### ✅ AFTER — Constants tracked correctly
```python
import openai                              # ← module-level

def call_llm(prompt):
    return openai.chat.completions.create(
        model=openai.MODEL_DEFAULT,        # ← captured by JIT global resolution
        messages=[{"role": "user", "content": prompt}],
    )
```

If `ruff` / `pylint` complains about `E402`, that confirms the fix.
</rule>

<rule id="7.3" priority="high">
**DO NOT mutate module-level globals INSIDE a `@trace`-decorated function during a single run.**
The Oracle snapshots globals at FIRST call within a run. Mid-run mutations are invisible to subsequent steps in the SAME run (they will be detected across runs, but not within).

#### ❌ DO NOT
```python
PROMPT_CACHE: dict = {}

@logram.trace()
def get_prompt(key):
    if key not in PROMPT_CACHE:
        PROMPT_CACHE[key] = build_prompt(key)   # ← mid-run mutation, invisible mid-run
    return PROMPT_CACHE[key]
```

#### ✅ DO — Move mutable accumulators into a `@stateful` class
```python
@logram.stateful(include=["prompt_cache"])
class PromptManager:
    def __init__(self):
        self.prompt_cache: dict = {}

    @logram.trace()
    def get_prompt(self, key):
        if key not in self.prompt_cache:
            self.prompt_cache[key] = build_prompt(key)
        return self.prompt_cache[key]
```
</rule>

---

## 8. ADVANCED `@trace` PARAMETERS — WHEN TO REACH FOR THEM

Use these ONLY when the default behavior produces wrong cache hits or wrong cache misses. Do not apply prophylactically.

| Parameter | When to use |
|---|---|
| `ignore_in_hash=["timestamp", "request_id"]` | Argument is observability-only, has zero effect on output. Including it would force a cache miss every run. |
| `vcr_key_fn=lambda f, a, kw: (..., ...)` | Default key is too broad (large object, lossy compaction) OR control flow depends on dynamic dispatch the Oracle cannot resolve statically. Must return `(args_repr, kwargs_repr)`. |
| `log_input_fn=lambda f, a, kw: {...}` | `logram view` displays unhelpful argument representations. Affects DISPLAY only, not cache key. |
| `compact_inputs=False` | Need full-fidelity input in `logram view` for diagnostic reasons. Trade-off: bigger SQLite rows. |
| `track_args=["accumulator"]` | The function mutates a mutable argument in-place (return is `None` or unused) and downstream code depends on the mutation. |
| `state_in_hash=False` | Method on a `@stateful` class that does NOT read `self.*` — pure helper. |
| `include_state=[...]` / `exclude_state=[...]` | Method only depends on a subset of the class's `@stateful` fields. |

<rule id="8.1" priority="medium">
**`vcr_key_fn` MUST return `(args_dict, kwargs_dict)` — a 2-tuple of serializable dicts. Returning anything else raises and Logram falls back to default keying.**

```python
@logram.trace(
    vcr_key_fn=lambda func, args, kwargs: (
        {"tile_id": kwargs["tile"].tile_id, "page": kwargs["tile"].page},
        {},
    )
)
async def process_tile(self, tile: ImageTile) -> dict:
    ...
```
</rule>

<rule id="8.2" priority="high">
**For dynamic dispatch (`getattr(self, f"_handle_{type}")`), make the dynamic discriminator EXPLICIT in `vcr_key_fn`.**

```python
@logram.trace(
    vcr_key_fn=lambda fn, args, kwargs: (args, {**kwargs, "_dispatch": args[1].type})
)
async def dispatch(self, payload):
    method = getattr(self, f"_handle_{payload.type}")
    return await method(payload)
```
</rule>

---

## 8.5. THE EXCLUSION TOOLKIT — TRACING SELECTIVELY (READ THIS BEFORE REFACTORING)

> Direct application of the Prime Directive (§0.5–0.11). When the user's code does not naturally fit the cache, your FIRST move is to narrow what enters the hash. Refactoring the user's code is a LAST RESORT.

### 8.5.A — The Decision Tree

```
A function is a good caching target, but…
│
├── ❓ Some arguments are noise (request_id, timestamps, tracing context)
│   └── ✅ @trace(ignore_in_hash=["request_id", "timestamp", "trace_ctx"])
│
├── ❓ Some arguments are huge or have unstable repr
│   └── ✅ @trace(vcr_key_fn=lambda f, a, kw: ({"id": kw["obj"].id}, {}))
│
├── ❓ Method on a stateful class but doesn't read self.*
│   └── ✅ @trace(state_in_hash=False)
│
├── ❓ Method only depends on a few state fields
│   └── ✅ @trace(include_state=["page_map"])  # OR exclude_state=["irrelevant"]
│
├── ❓ Function mutates a passed-in container in place
│   └── ✅ @trace(track_args=["accumulator"])
│
├── ❓ Custom argument class has unstable repr
│   └── ✅ Implement __logram_trace_key__ (NON-INVASIVE — adds one method)
│
├── ❓ Stored input view in `logram view` is unhelpful
│   └── ✅ @trace(log_input_fn=lambda f, a, kw: {...summary...})
│       # Affects DISPLAY only, not cache key. Pure visual fix, zero risk.
│
├── ❓ Function genuinely cannot be cached deterministically
│   └── ✅ DO NOT TRACE IT. Period. Move on.
│
└── ❓ Refactoring the user's code would solve it elegantly
    └── ⚠️ STOP. Surface the refactor as a SEPARATE suggestion to the user.
        Do not bundle it into "instrumentation".
```

### 8.5.B — Refactor cost ladder (cheapest → most invasive)

| Tier | Action | Invasiveness | Use when |
|---|---|---|---|
| **0** | Add `@trace()` with no parameters | Zero — one decorator line | Default for clean targets |
| **1** | Add `@trace(ignore_in_hash=[...])` | Zero — one decorator parameter | Function has noise arguments |
| **2** | Add `@trace(vcr_key_fn=...)` | Minimal — one lambda | Default key is wrong shape |
| **3** | Add `@trace(track_args=[...])` | Minimal — one parameter | In-place mutation pattern |
| **4** | Add `__logram_trace_key__` to a custom class | Low — one method, no behavior change | Class passed to traced fn has unstable repr |
| **5** | Add `@stateful(include=[...])` to a class | Low — one decorator on the class | Class accumulates state across traced calls |
| **6** | Move inline `import` to module top | Low — but touches imports | Function uses constants from inline-imported module |
| **7** | Convert dict return to dataclass/Pydantic | Medium — affects callers | User explicitly wants typed rehydration |
| **8** | Split a class, change a signature, restructure modules | HIGH — breaks public surface | NEVER without explicit user approval |

**Rule:** Always operate at the LOWEST tier that solves the problem. Going from tier 1 to tier 7 because "it would be cleaner" is a Prime Directive violation.

### 8.5.C — When to leave a function untraced (and how to explain it)

A function is a BAD trace target — leave it alone, document the choice — when ANY of:

- It is non-deterministic by design (`uuid.uuid4()`, `random.*`, `datetime.now()`) AND the non-determinism is the desired behavior.
- It returns a non-serializable resource (open file, socket, DB connection, threading primitive, GUI widget).
- It is a thin wrapper (1-3 lines) around a single call — overhead exceeds benefit.
- It is a `__init__`, `__repr__`, `__hash__`, `__eq__`, or any dunder.
- It is a `pytest` fixture or test function.
- It is part of the framework code (FastAPI dependency, SQLAlchemy event listener, signal handler).
- It is called >10,000 times per run with sub-millisecond bodies — Oracle overhead becomes the bottleneck.
- The user has a strong existing convention (decorators, base class, mixin) that you would have to fight.

In your hand-off summary, list each of these as `<file>:<symbol> — NOT TRACED — <reason>`. The user must know what you DIDN'T touch as much as what you did.

### 8.5.D — Concrete example: progressive narrowing instead of refactoring

The user's code:
```python
class DocumentPipeline:
    def __init__(self):
        self.results = {}
        self.page_map = {}
        self.font_registry = {}
        self.metrics = {"started_at": time.time(), "step_count": 0}
        self._http_client = httpx.AsyncClient()
        self._semaphore = asyncio.Semaphore(10)
        self._lock = asyncio.Lock()

    async def extract(self, page: Page, request_id: str, trace_ctx: dict):
        # … reads self.page_map only
```

#### ❌ Wrong — destructive refactor
"I'll split this class into `DocumentState` (the data) and `DocumentRunner` (the I/O), remove `request_id` and `trace_ctx` from the signature, and convert `metrics` into a Pydantic model."

#### ✅ Right — adaptive instrumentation, zero structural change
```python
@logram.stateful(include=["results", "page_map", "font_registry"])
#                          ^^^ excluded: metrics (volatile), _http_client,
#                              _semaphore, _lock (runtime resources)
class DocumentPipeline:
    def __init__(self):
        # … unchanged

    @logram.trace(
        ignore_in_hash=["request_id", "trace_ctx"],   # noise args
        include_state=["page_map"],                    # only field this method reads
    )
    async def extract(self, page: Page, request_id: str, trace_ctx: dict):
        # body unchanged
```

The user's class shape, signatures, and runtime resources are preserved exactly. Logram adapts; the code does not move.

---

## 9. DISTRIBUTED EXECUTION — MULTIPROCESSING / RAY / CELERY

<rule id="9.1" priority="critical">
**ContextVars do NOT propagate across process boundaries. Every worker MUST be initialized with `logram.worker_init` to inherit the parent's `run_id`.**

#### ✅ DO — `ProcessPoolExecutor`
```python
import logram
from concurrent.futures import ProcessPoolExecutor

run_id = logram.init(project="my_pipeline", input_id="doc_42")

with ProcessPoolExecutor(
    initializer=logram.worker_init,
    initargs=(run_id,),
) as pool:
    results = list(pool.map(process_tile, tiles))

await logram.finalize(status="success")
```

#### ✅ DO — `multiprocessing.Pool`
```python
import multiprocessing as mp
mp.set_start_method("spawn", force=True)     # ← Linux: avoid fork (see 9.2)

with mp.Pool(initializer=logram.worker_init, initargs=(run_id,)) as pool:
    results = pool.map(process_tile, tiles)
```

#### ❌ DO NOT — Call `logram.init()` inside workers
```python
def worker(tile):
    logram.init(project="my_pipeline")        # ← creates a NEW run per worker. WRONG.
    ...
```
</rule>

<rule id="9.2" priority="high">
**On Linux, set the multiprocessing start method to `spawn` or `forkserver` BEFORE creating any pool.** The default `fork` inherits the parent's open SQLite connection, which leads to silent corruption.

```python
import multiprocessing
multiprocessing.set_start_method("spawn", force=True)
```
</rule>

<rule id="9.3" priority="medium">
**Celery integration: register `worker_init` as the `worker_process_init` signal handler.**

```python
from celery.signals import worker_process_init

@worker_process_init.connect
def init_logram(**kwargs):
    run_id = os.environ.get("LOGRAM_RUN_ID")
    if run_id:
        logram.worker_init(run_id)
```

The orchestrator MUST set `LOGRAM_RUN_ID` in the worker environment before dispatching.
</rule>

---

## 10. WEB SERVERS — FASTAPI, FLASK, ETC.

<rule id="10.1" priority="high">
**Each request handler that runs a pipeline MUST bind a per-request run_id. Use `bind_session_run` or `@with_session_run`.**

#### ✅ DO — Decorator approach (preferred)
```python
from fastapi import FastAPI, Depends
import logram

app = FastAPI()

@app.post("/analyze")
@logram.with_session_run(prefix="analyze", session_id_attr="id")
async def analyze(session: Session = Depends(get_session)):
    logram.init(project="api_analyze", input_id=session.doc_id)
    try:
        result = await run_pipeline(session)
        await logram.finalize(status="success")
        return result
    except Exception:
        await logram.finalize(status="failed")
        raise
```

#### ✅ DO — Manual binding
```python
@app.post("/analyze")
async def analyze(session: Session = Depends(get_session)):
    logram.bind_session_run(session, prefix="analyze")
    logram.init(project="api_analyze", input_id=session.doc_id)
    ...
```

#### ❌ DO NOT — Single global `init()` at startup
```python
logram.init(project="api_analyze")           # ← all requests collide on the same run_id
app = FastAPI()
@app.post("/analyze")
async def analyze(...): ...                  # ← traces from all requests interleave
```
</rule>

---

## 11. STREAMING / GENERATORS

<rule id="11.1" priority="high">
**`@trace` on a generator function captures yielded chunks via shadow accumulation. The cache is saved ONLY if the stream is fully consumed.**

```python
@logram.trace()
async def stream_llm(prompt: str):
    async for chunk in client.chat(prompt, stream=True):
        yield chunk

# Caller MUST exhaust the iterator for the cache to be written:
async for part in stream_llm("Hello"):       # ✅ full consumption
    process(part)
```

If the consumer breaks early (`async for x in gen: if cond: break`), no cache entry is created — replay will run the stream live. This is correct behavior; do not try to "fix" it.
</rule>

<rule id="11.2" priority="medium">
**Yielded chunks MUST be serializable.** Strings, dicts, dataclasses, Pydantic models — all fine. Open file handles, sockets, or generators-of-generators — not fine.
</rule>

---

## 12. ENVIRONMENT VARIABLES — REPLAY CONTROL

<rule id="12.1" priority="medium">
**The agent SHOULD propose these env vars in user-facing instructions, but MUST NOT hardcode them in the source.**

| Variable | Purpose |
|---|---|
| `LOGRAM_REPLAY=true` | Enable replay mode. Cached SUCCESS steps return from cache; everything else runs live. |
| `LOGRAM_FORCE_STEP=step_name[,step2,...]` | Force named SUCCESS steps to re-execute live. Invalidates their cache entry. |
| `LOGRAM_FORCE_FROM=step_name` | Cascade live execution from this step onward. |
| `LOGRAM_DB_PATH=/path/logram.db` | Override SQLite store location (CI, monorepo, shared store). |
| `LOGRAM_PROJECT_ROOT=/path` | Override project root for blob asset storage (Docker, monorepo). |
| `LOGRAM_INPUT_ID=...` | Override `input_id` for the current run (used by `logram test`). |

#### ✅ DO — Tell the user the command to run
> "After your changes, run: `LOGRAM_REPLAY=true python my_pipeline.py` to validate via surgical replay."

#### ❌ DO NOT — Bake env-var manipulation into application code
```python
os.environ["LOGRAM_REPLAY"] = "true"         # ← NEVER do this in user source
import logram
```
</rule>

---

## 13. WHAT NEVER TO DO — HARD PROHIBITIONS

<rule id="13.1" priority="critical">
**❌ NEVER edit, monkey-patch, or wrap the Logram SDK source.**
Includes: `logram.decorators._logic_async`, `TraceStorage`, `BlobManager`, `compute_logic_fingerprint`, the Oracle's `_CALLEE_BUDGET`, etc. The only legal `_CALLEE_BUDGET` modification is the documented startup override (`logram.oracle._CALLEE_BUDGET = 1024`) — and only when the user explicitly asks for it.
</rule>

<rule id="13.2" priority="critical">
**❌ NEVER catch and swallow exceptions inside a `@trace`-decorated function in a way that converts a real failure into a fake success.**

Logram caches ONLY `SUCCESS` outcomes. A function that returns `None` or `{"error": ...}` instead of raising will be cached as a successful outcome — and the cached failure will replay forever.

#### ❌ DO NOT
```python
@logram.trace()
async def call_vlm(tile):
    try:
        return await vlm.generate(tile)
    except Exception:
        return None                          # ← cached as SUCCESS=None forever
```

#### ✅ DO — Let exceptions propagate; Logram records failure correctly
```python
@logram.trace()
async def call_vlm(tile):
    return await vlm.generate(tile)          # ← raises → status=FAILED, no cache entry
```
</rule>

<rule id="13.3" priority="critical">
**❌ NEVER trace a function that calls `logram.init()`, `logram.finalize()`, or `logram.flush()` internally.** These are lifecycle primitives, not steps.
</rule>

<rule id="13.4" priority="high">
**❌ NEVER add `logram` to a function's signature, type stubs, or Pydantic models.** The decorator preserves `__name__`, `__qualname__`, `__doc__`, and signature via `functools.wraps`. There is nothing to declare.
</rule>

<rule id="13.5" priority="high">
**❌ NEVER seed PRNGs, set timestamps, or inject UUIDs INSIDE a `@trace`-decorated function expecting determinism on replay.** Cached outputs are byte-for-byte the same as the live run; if the live run produced `random.random() = 0.4127`, the replay returns `0.4127`. But if you re-run live (cache miss), you get a new value. If determinism matters, pass the seed/UUID as an explicit argument and put it in `ignore_in_hash` only if it truly shouldn't affect the output.
</rule>

<rule id="13.6" priority="high">
**❌ NEVER use `LOGRAM_FORCE_STEP` on a step that is currently `FAILED`.** The MCP Logic Guard will abort. Failed steps already run live on every replay — forcing them is redundant and triggers a safety check.
</rule>

<rule id="13.7" priority="medium">
**❌ NEVER commit `.logram/` or `.logram_assets/` to git.** Re-check `.gitignore` after instrumentation.
</rule>

<rule id="13.8" priority="medium">
**❌ NEVER trace `__init__`, `__repr__`, `__hash__`, `__eq__`, or any dunder method.** Tracing constructors leads to recursive serialization attempts on partially-built objects.
</rule>

---

## 14. REFACTORING PLAYBOOK — STEP-BY-STEP FOR INSTRUMENTING AN EXISTING CODEBASE

> **Read §0.5 (Prime Directive) and §8.5 (Exclusion Toolkit) before executing this playbook.** Every step below is subordinate to "Adapt to the code, don't refactor the code". If a step would force a non-local change, STOP and surface it as a separate proposal.

When the user says "add Logram to my pipeline", execute this sequence:

### PHASE A — Inventory (read-only)
1. Locate the entry point. Confirm with the user.
2. List candidate `@trace` targets (LLM calls, expensive transforms, per-item processors).
3. List classes that mutate state across method calls (`grep -n "self\.\w\+\s*=" src/`).
4. List custom classes passed as arguments to candidate targets — flag any without a stable `__repr__`.
5. List inline `import` statements inside function bodies.
6. Check for multiprocessing / Ray / Celery usage.
7. **Report this inventory to the user.** Wait for confirmation before editing.

### PHASE B — Lifecycle wiring
1. Add `import logram` to the entry point.
2. Insert `logram.init(project=..., input_id=...)` at the start of `main()`.
3. Wrap the body in `try/except`, ensuring `await logram.finalize(status=...)` runs on both paths.

### PHASE C — Decoration
1. Add `@logram.trace()` to each confirmed target. Place it as the INNERMOST decorator.
2. For each candidate class with mutable state: add `@logram.stateful(include=[...])` with the precise field list.
3. For each custom argument class: add `__logram_trace_key__` returning a stable dict.

### PHASE D — Adaptations required for cache stability (LOWEST-TIER FIRST)
1. **Try the configuration toolkit FIRST** (see §8.5 ladder). For each cache-stability concern, exhaust tiers 0–4 before considering tiers 5+.
2. Inline imports inside a `@trace`-decorated function: move to module top ONLY when those constants are needed by the cache. If the inline import is used for cycle-breaking or lazy loading, leave it and surface the trade-off to the user.
3. Type conversions (dict → Pydantic): SKIP unless the user explicitly requests typed rehydration. Plain dicts work; downstream code does not need rewriting.
4. For multiprocessing: add `worker_init` initializer. For Linux: set `spawn` start method. (These are non-negotiable for correctness, not stylistic.)
5. **Stop at the first solution that works.** Do not pile additional "improvements" once cache stability is achieved.

### PHASE E — Verification (you, the agent, MUST do this)
1. Run the pipeline once live: `python <entry_point>.py`. Confirm `.logram/logram.db` is created.
2. Run `logram inspect <run_id>` (or report what the user should see) to confirm the step tree matches expectations.
3. Run `LOGRAM_REPLAY=true python <entry_point>.py`. Confirm steps return from cache (look for `REPLAYED` in the output).
4. Inspect the logs for `[PROBE 2][UNSTABLE_REPR]` warnings → fix the flagged classes.
5. Inspect the logs for `[Logram][oracle] callee budget exhausted` → if seen, surface to the user.

### PHASE F — Hand-off (apply Rule 0.10 — surface every non-obvious decision)
Tell the user, in order:
- **What was decorated** — `<file>:<symbol> — TRACED — <reason>`.
- **What was deliberately NOT decorated** — `<file>:<symbol> — NOT TRACED — <reason>` (e.g. "trivial helper", "non-deterministic by design", "framework boilerplate").
- **What was refactored** — with diffs and a one-line "because" per change. If you touched anything beyond decorators / the lifecycle wrapper / `__logram_trace_key__` / `.gitignore`, justify it.
- **What configuration knobs you used** — `ignore_in_hash`, `vcr_key_fn`, `track_args`, `include_state`, `exclude_state`, `state_in_hash` — and why each was preferred over a code change.
- **What env vars to use for replay** (`LOGRAM_REPLAY=true`, optional `LOGRAM_FORCE_FROM=...`).
- **Open questions and trade-offs you parked** — anything you considered changing but left untouched, with a one-line rationale ("could split this class for cleaner state, but the existing public API has 4 callers — flagging for your decision").

---

## 15. CANONICAL "BEFORE / AFTER" — A FULL INSTRUMENTATION

### BEFORE — User's pipeline (uninstrumented)

```python
# pipeline.py
import asyncio
from openai import AsyncOpenAI

class InvoicePipeline:
    def __init__(self, doc_path):
        self.doc_path = doc_path
        self.client = AsyncOpenAI()
        self.results = {}
        self.page_map = {}

    async def call_vlm(self, tile):
        import json                                   # ← inline import (BAD)
        response = await self.client.chat.completions.create(
            model="gpt-4o", messages=[{"role": "user", "content": tile.text}]
        )
        return json.loads(response.choices[0].message.content)

    async def process_tile(self, tile):
        out = await self.call_vlm(tile)
        self.results[tile.tile_id] = out              # ← state mutation (UNTRACKED)
        return out

    async def run(self):
        tiles = load_tiles(self.doc_path)
        for tile in tiles:
            await self.process_tile(tile)
        return self.results

class ImageTile:                                       # ← no stable identity
    def __init__(self, tile_id, text, image_bytes):
        self.tile_id = tile_id
        self.text = text
        self.image_bytes = image_bytes

async def main():
    pipeline = InvoicePipeline("doc_42.pdf")
    return await pipeline.run()

if __name__ == "__main__":
    asyncio.run(main())
```

### AFTER — Logram-instrumented (correct)

```python
# pipeline.py
import asyncio
import json                                            # ✅ moved to module top
from openai import AsyncOpenAI
import logram

SYSTEM_PROMPT = "You are an extraction assistant..."   # ✅ module-level → tracked

@logram.stateful(include=["results", "page_map"])      # ✅ state declared
class InvoicePipeline:
    def __init__(self, doc_path):
        self.doc_path = doc_path
        self.client = AsyncOpenAI()                    # ← runtime resource (not in `include`)
        self.results: dict = {}
        self.page_map: dict = {}

    @logram.trace()
    async def call_vlm(self, tile):
        response = await self.client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": tile.text},
            ],
        )
        return json.loads(response.choices[0].message.content)

    @logram.trace()
    async def process_tile(self, tile):
        out = await self.call_vlm(tile)
        self.results[tile.tile_id] = out
        return out

    async def run(self):
        tiles = load_tiles(self.doc_path)
        for tile in tiles:
            await self.process_tile(tile)
        return self.results

class ImageTile:
    def __init__(self, tile_id, text, image_bytes):
        self.tile_id = tile_id
        self.text = text
        self.image_bytes = image_bytes

    def __logram_trace_key__(self):                    # ✅ stable identity
        return {"tile_id": self.tile_id}

async def main():
    logram.init(project="invoice_agent", input_id="doc_42")
    try:
        pipeline = InvoicePipeline("doc_42.pdf")
        result = await pipeline.run()
        await logram.finalize(status="success", metrics={"tiles": len(result)})
        return result
    except Exception:
        await logram.finalize(status="failed")
        raise

if __name__ == "__main__":
    asyncio.run(main())
```

### Diff summary the agent must report
- Added: `import logram`, `import json` (top-level), `SYSTEM_PROMPT` constant.
- Decorated: `InvoicePipeline` with `@logram.stateful(include=["results", "page_map"])`.
- Decorated: `call_vlm`, `process_tile` with `@logram.trace()`.
- Added: `__logram_trace_key__` to `ImageTile`.
- Wrapped: `main()` body in try/except with `logram.init` / `logram.finalize`.
- Removed: inline `import json` inside `call_vlm`.
- `.gitignore`: added `.logram/` and `.logram_assets/`.

---

## 16. FAILURE MODES THE AGENT MUST RECOGNIZE AND ACT ON

| Symptom | Cause | Fix |
|---|---|---|
| `[PROBE 2][UNSTABLE_REPR]` in logs | Custom class passed to `@trace` lacks stable identity | Add `__logram_trace_key__` |
| Replay always misses, no warnings | Inline imports OR mutated globals OR unstable args | Move imports to top; check `ignore_in_hash` |
| Replay returns wrong downstream data | Stateful class missing `@stateful` OR `include=` list incomplete | Add/expand `@stateful` |
| Tracing crashes the pipeline | Should be impossible (Logram has total catch-all) — REPORT TO USER as a SDK bug |
| `logram inspect` shows `default_run` mixed entries | Multiprocessing without `worker_init` | Add `initializer=logram.worker_init` |
| `[Logram][oracle] callee budget exhausted` | Call graph >256 user functions | `logram.oracle._CALLEE_BUDGET = 1024` (with user consent) |
| `LOGRAM_FORCE_STEP` aborted by Logic Guard | Forcing a `FAILED` step | Failed steps run live automatically — remove the env var |

---

## 17. SUMMARY OF MUST-OBEY RULES (PRINTABLE CHECKLIST)

### Prime Directive (judgment)
- [ ] Reached for a CONFIGURATION knob (§8.5) before any structural refactor.
- [ ] Every decorated function has a defensible "why this one?" answer (§0.9).
- [ ] Every NON-decorated function the user might have expected has a documented "why not?" (§0.10).
- [ ] No signature changes, class splits, module restructures, or rename cascades.
- [ ] Every non-trivial decision listed in the hand-off summary as `<file>:<symbol> — <action> — <reason>`.

### Lifecycle
- [ ] `logram.init()` exactly once, before any traced function.
- [ ] `await logram.finalize(...)` in BOTH success AND failure paths.

### Decoration
- [ ] `@logram.trace()` is the INNERMOST decorator on stacks.
- [ ] Trace boundaries of WORK (LLM calls, transforms), not trivial helpers.
- [ ] State-mutating classes use `@logram.stateful(include=[...])` listing ONLY load-bearing fields.
- [ ] Custom argument classes with unstable repr have `__logram_trace_key__` (additive, three lines).

### Cache stability
- [ ] Inline imports relevant to traced constants are at module top (others left alone if intentional).
- [ ] Module-level globals NOT mutated mid-run inside traced functions.
- [ ] Exceptions PROPAGATE out of traced functions; never swallowed.

### Distributed / web
- [ ] Multiprocessing pools use `initializer=logram.worker_init`.
- [ ] Linux multiprocessing uses `spawn` start method.
- [ ] FastAPI / web handlers bind a per-request run_id.

### Hygiene
- [ ] `.logram/` and `.logram_assets/` are in `.gitignore`.
- [ ] Logram SDK source is NEVER edited.
- [ ] Verified post-instrumentation with one live run + one `LOGRAM_REPLAY=true` run.

---

## 18. WHEN IN DOUBT — THE TWO LOAD-BEARING DOCTRINES

### Doctrine 1 — Cache stability is sacred (§0.3)
- Refactor that changes the AST for a non-behavioral reason (formatting, comments, docstrings): proceed; the structural hash is invariant.
- Refactor that changes the AST for a behavioral reason: explain to the user that the cache for that step (and its callers) will invalidate, and confirm before proceeding.
- Refactor that changes the value of a constant the function reads: same — cache invalidates, confirm.
- Refactor that moves logic from one function to another: invalidates BOTH callers and callees in the Merkle aggregation. CONFIRM before proceeding.

### Doctrine 2 — The user's code is sovereign (§0.5–0.11)
- If a rule in this document seems to demand a structural change, re-read §8.5. There is almost certainly a configuration knob that achieves the same goal without touching the code.
- If you find yourself writing more than ~10 lines of net new code per function instrumented, you have probably crossed from "instrumentation" into "rewrite". Stop and reassess.
- If the user asks for "full coverage" but the codebase fights it (dynamic dispatch everywhere, no stable types, heavy mutation), report this as a finding — do not paper over it with aggressive refactors. Logram works best as a partial, surgical instrumentation; insisting on totality is an anti-pattern.
- If you are about to disagree with an existing user convention (a base class, a registry pattern, a metaclass, a custom decorator stack), the convention wins. Find a way to integrate Logram around it, or report the incompatibility honestly.

**Logram earns its keep by being invisible to the codebase and indispensable to the developer. If you cannot achieve both, default to invisible.**

---

*End of LOGRAM_AGENT_RULES.md — version 1.0, targeting `logram-sdk >= 0.3.0`.*
