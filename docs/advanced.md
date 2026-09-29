# Advanced usage

[← README](../README.md) · [How it works](how-it-works.md) · [Limitations](limitations.md) · [CLI](cli.md) · **Advanced usage** · [Agent interface](mcp.md) · [Design](design.md) · [Benchmarks](../benchmarks/README.md)

## `vcr_key_fn`: custom cache key

By default, all named arguments enter the cache key. For custom objects this can cause **false misses**: fields that change between runs without affecting the output (a counter, an embedded timestamp) change the key every time. `vcr_key_fn` pins the key to the fields that define identity. It is also the only option for third-party classes you cannot modify.

```python
# Own class: key on identity fields only (args[0] = self, args[1] = tile)
@logram.trace(vcr_key_fn=lambda func, args, kwargs: (
    {"tile_id": args[1].tile_id, "page": args[1].page_number}, {}
))
async def process_tile(self, tile: ImageTile) -> dict:
    ...  # the 80 KB image_bytes are not part of the key

# Third-party class
from langchain.schema import Document

@logram.trace(vcr_key_fn=lambda func, args, kwargs: ({"page_content": args[0].page_content}, {}))
def extract_entities(doc: Document) -> list[str]: ...
```

The function receives `(func, args, kwargs)`, with positional arguments as-is (including `self`), and returns `(vcr_args, vcr_kwargs)`; a single return value is paired with `{}`.

## `ignore_in_hash`: exclude volatile arguments

Request IDs, timestamps and tracing contexts change on every call without affecting the output; leaving them in the key guarantees a miss.

```python
@logram.trace(ignore_in_hash=["timestamp", "request_id", "trace_ctx"])
async def classify(self, text: str, timestamp: float, request_id: str) -> str: ...
```

**Diagnostic:** if a step keeps running live in replay mode, enable debug logging (`logging.basicConfig(level=logging.DEBUG)`) and compare the `[PROBE 3][HashComponents]` lines of two runs. A field of `COMPONENT_args_repr` that differs between runs but should not matter belongs in `ignore_in_hash`.

## `log_input_fn`: custom input logging

Stores a focused or human-readable summary of the inputs instead of the default serialization. It affects display only, not the cache key (use `vcr_key_fn` for that).

```python
def _summarize_tile_input(func, args, kwargs):
    tile = kwargs.get("tile")
    return {"tile_id": tile.tile_id, "page": tile.page_number,
            "bbox": tile.bbox, "image_size_kb": len(tile.image_bytes) // 1024}

@logram.trace(log_input_fn=_summarize_tile_input)
async def process_tile(self, tile: ImageTile) -> dict: ...
```

## `@logram.stateful`, `state_in_hash`, `include_state`, `exclude_state`: state control

`@stateful(include=[...])` declares the attributes a class accumulates across traced calls; they are snapshotted before and after each call and restored on replay. By default the state enters the cache key, which is correct when the same arguments with different state can give different results.

```python
@logram.stateful(include=["results", "page_map", "ocr_cache", "font_registry"])
class DocumentPipeline:

    @logram.trace(include_state=["page_map"])       # only page_map enters this key
    async def resolve_page(self, page_id: int) -> dict: ...

    @logram.trace(exclude_state=["font_registry"])  # all fields except font_registry
    async def extract_text(self, tile: ImageTile) -> str: ...

    @logram.trace(state_in_hash=False)              # method neither reads nor writes tracked fields
    async def format_output(self, raw: dict) -> str: ...
```

`include_state` and `exclude_state` select the fields that are both part of the key and captured/restored for that method. `state_in_hash=False` turns state tracking off for the method entirely: its state changes are neither keyed nor restored on replay, so use it only on methods that do not touch the tracked attributes.

## `track_args`: in-place argument mutations

A step that fills a mutable argument and returns `None` would replay `None` without restoring the mutation, leaving downstream steps with wrong data. `track_args` stores and restores the delta, through the same `values_registry` as `@stateful`.

```python
@logram.trace(track_args=["accumulator"])
async def aggregate_results(self, items: list, accumulator: dict) -> None:
    for item in items:
        accumulator[item["id"]] = item["value"]
```

## `__logram_trace_key__` and `__logram_trace_log__`: object identity protocols

`__logram_trace_key__` controls the cache key of a custom object; `__logram_trace_log__` controls what is stored and displayed (`logram view`, `logram diff`) without affecting caching. If only the first is defined, it is used for both.

```python
class ImageTile:
    def __logram_trace_key__(self):
        return {"tile_id": self.tile_id, "page": self.page_number}

    def __logram_trace_log__(self):
        return {"tile_id": self.tile_id, "page": self.page_number, "bbox": self.bbox,
                "image_size_kb": len(self.image_bytes) // 1024, "grid_tag": self.grid_tag}
```

The key must be stable across runs (no addresses, no timestamps) and change when the object's logical identity changes.

## Run lifecycle: tags, `flush`, `finalize`

Tags categorize runs; `GOLDEN` is the tag behind `logram golden add` and `logram test`.

```python
run_id = logram.init(project="invoice_agent", run_name="batch_run",
                     input_id=document_id, tags=["GOLDEN", "staging", "v2-prompt"])
try:
    await run_pipeline()
    await logram.finalize(status="success", metrics={"pages": 12})
except Exception:
    await logram.finalize(status="failed")
    raise
```

Writes are asynchronous, so short scripts can exit before the queue is flushed: always call `finalize()`, which closes the run, computes metrics and waits for pending writes. `flush()` persists without closing, for checkpoints in long pipelines. Metrics passed to `finalize()` appear in `logram stats`.

## Web servers and external run IDs: `bind_session_run`, `with_session_run`, `set_run_id`

In a FastAPI-style server, each request can be traced as its own run with a stable ID derived from the session rather than a random UUID:

```python
@app.post("/analyze")
async def analyze(session: Session = Depends(get_session)):
    logram.bind_session_run(session, prefix="analyze", session_id_attr="id")
    result = await run_pipeline(session)
    await logram.finalize(status="success")
    return result

@logram.with_session_run(prefix="analyze", session_id_attr="id")
async def analyze(session: Session): ...

# Custom resolver for any session shape
logram.bind_session_run(session, resolver=lambda s: f"req_{s.correlation_id}_{s.user_id}")
```

When the run ID comes from an orchestrator or job queue:

```python
logram.set_run_id(f"job_{os.environ['JOB_ID']}", verbose=True)
```

## `LOGRAM_FORCE_STEP` and `LOGRAM_FORCE_FROM`: replay control

`LOGRAM_FORCE_STEP` invalidates the cached success of named steps so they run live. Do not use it on failed steps: they have no cache entry and already run live, and doing so triggers the MCP logic guard. `LOGRAM_FORCE_FROM` runs a step and everything downstream live, while still replaying everything upstream.

```bash
LOGRAM_REPLAY=true LOGRAM_FORCE_STEP=call_vlm python my_pipeline.py
LOGRAM_REPLAY=true LOGRAM_FORCE_FROM=extract_quantities python my_pipeline.py
```

| | `FORCE_STEP` | `FORCE_FROM` |
|---|---|---|
| Target step | Live | Live |
| Upstream steps | Replayed | Replayed |
| Downstream steps | Replayed | Live |
| Use when | The fix does not change the step's output for downstream steps | The fix changes data consumed downstream |

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `LOGRAM_REPLAY` | `""` | `"true"` enables replay: every step with a cached success replays |
| `LOGRAM_FORCE_STEP` | `""` | Comma-separated steps to force live; invalidates their cache |
| `LOGRAM_FORCE_FROM` | `""` | Step from which execution cascades live |
| `LOGRAM_DB_PATH` | `.logram/logram.db` | Database location (shared store, CI artifact path) |
| `LOGRAM_PROJECT_ROOT` | auto-detected | Root for blob storage, detected from `pyproject.toml` / `.git`; set it in Docker or monorepos |
| `LOGRAM_INPUT_ID` | `""` | Override the run's `input_id` (used by `logram test`) |
