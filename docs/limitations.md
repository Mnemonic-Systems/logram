# Guarantees, best practices and limitations

Logram tracks data flow, not system resources. The Oracle traces the code you write, not the code Python generates at runtime. The following rules make the difference between good and exact replays.

## 1. Return data, not resources

Avoid returning open files, sockets, database connections or non-yielding generators from a traced step. A non-serializable output is stored as `str(obj)`, so on replay the next step receives a string. Return dicts, Pydantic models or dataclasses.

**Streaming outputs** (generators and async generators) are supported natively. During a live run, Logram captures each yielded chunk without adding latency. The cache is written only if the stream is fully consumed (a premature `break` creates no entry), and on replay the stored chunks are re-yielded:

```python
@logram.trace
async def stream_llm(prompt: str):
    async for chunk in client.chat(prompt, stream=True):
        yield chunk

async for part in stream_llm("Hello"):   # identical usage, live or replayed
    print(part)
```

## 2. Typed outputs give exact rehydration

Serialization is tag-based: class metadata is embedded at capture time, so no type annotation is required on the traced function. Return Pydantic models or dataclasses to get typed objects back on replay; plain dicts work but lose type reconstruction.

```python
@logram.trace
def extract_quantities(page: Page) -> ExtractionResult:
    ...
    return ExtractionResult(tiles=tiles, totals=totals)
    # replay returns an ExtractionResult instance, not a dict
```

## 3. Give custom objects a stable identity

Arguments follow a different path from code. Primitives, containers, Pydantic models and dataclasses get content-based keys automatically (every element counts; long strings and bytes are hashed). Other classes fall back to `repr()`, which often contains a memory address that changes every run and defeats replay. Implement `__logram_trace_key__`:

```python
class ImageTile:
    def __logram_trace_key__(self):
        return {"tile_id": self.tile_id}
```

At runtime, Logram detects address-based reprs and warns: `[PROBE 2][UNSTABLE_REPR] type=ImageTile … Fix: implement __logram_trace_key__ on this class.`

## 4. Treat configuration as immutable within a run

Constants are snapshotted when a function is first called in a run. A global mutated *inside* a traced function during a run is detected on the next run, not by later steps of the same run. Pass dynamic values as arguments, or manage shared mutable state with `@stateful`.

## 5. Distributed execution

Context variables, including the current `run_id`, do not cross process boundaries. Without intervention, worker processes write to a shared default run. Use `logram.worker_init` as the pool initializer:

```python
run_id = logram.init(project="my_pipeline", input_id="doc_42")

with ProcessPoolExecutor(initializer=logram.worker_init, initargs=(run_id,)) as pool:
    results = list(pool.map(process_tile, tiles))
```

`worker_init` sets the context in each worker without calling `logram.init()` again; the SQLite store is shared on disk, and concurrent writers wait for each other. Workers flush their pending writes on exit. Both `spawn` and `fork` are supported (a forked child gets its own writer, and the parent never forks in the middle of a write); as with any multi-threaded Python program, `spawn` or `forkserver` remain the safer default.

## 6. What triggers a re-run

The Oracle errs on the side of re-execution rather than stale cache.

**Invariant (no false invalidation)**

- Comments, whitespace, blank lines, docstrings.
- `black` / `ruff` reformatting.
- Python minor-version upgrades (3.10 → 3.13).

**Invalidates correctly**

- Editing a function body (structural AST hash), including renaming a parameter or changing a type annotation.
- Changing a constant value (runtime global resolution), wherever it sits in a container.
- Modifying a callee at any depth of the user-space call graph (Merkle aggregation), including the function behind a `functools.partial`.
- Changing a method called through `self.method()` (MRO traversal), a class constant read through `self.CONSTANT`, or any method of a project class the step instantiates.
- Changing a global compiled regex, enum, date, dataclass or Pydantic instance, or the bound arguments of a `partial`.
- Adding or removing `eval`, `exec` or dynamic `getattr` (volatility markers).

The [mutation benchmark](../benchmarks/README.md) checks these guarantees against the actual output of each step: no stale replay on 111 behaviour-changing mutations, and no invalidation on 663 cosmetic edits.

## Known blind spots

### Imports inside a function body

```python
def fn(x):
    import math            # local binding
    return math.pi * x     # seen as __local__:math.pi, filtered
```

Constants from inline-imported modules are not captured as resolved globals. Editing the function itself still invalidates it; only changes to the imported module's constants are invisible. Move imports to the top of the file (as `ruff E402` also recommends).

### Arbitrary objects stored in globals

Builtins, compiled regexes, enums, dates, dataclass and Pydantic instances read from a global are captured by content. Any other object read as a whole (an instance of a plain class, a NumPy array, a client) is captured by type only:

```python
SETTINGS = PlainSettings(temperature=0.7)

def fn(x):
    return call_llm(x, SETTINGS)    # SETTINGS captured as "PlainSettings", not by value
```

Attribute reads are resolved (`SETTINGS.temperature` in the body is captured by value). Otherwise make the object a dataclass or Pydantic model, or pass it as an argument keyed by `__logram_trace_key__`.

### Objects held in attributes

Methods of an object created in the step, or called through `self`, are part of the call graph. An object created elsewhere and stored in an attribute is not: in `self.client.complete(...)` with `self.client` assigned in `__init__`, a change to the client class's code does not invalidate the step. Trace the client call itself, or key the step with `vcr_key_fn`.

### Anything outside your project's code

Only functions defined under the working directory are fingerprinted (run your pipeline from the project root). Upgrading a library, changing a model version on the provider side, or editing a prompt stored in a YAML or JSON file read at runtime does not change any fingerprint. Pass such data as step arguments (the file content, the model name) so it enters the cache key, or force the affected steps with `LOGRAM_FORCE_STEP`.

### Truly dynamic dispatch: `getattr(self, var_name)`, `globals()[key]`

The dispatch site is tracked through `<volatile:getattr_dyn>`, but the resolved callee depends on runtime data and is invisible to the Merkle tree. Make the dynamic element explicit in the cache key:

```python
@logram.trace(vcr_key_fn=lambda fn, args, kwargs: (args, kwargs, type(args[0]).__name__))
async def dispatch_step(self, payload):
    method = getattr(self, f"_handle_{payload.type}")
    return await method(payload)
```

### Very deep call graphs: more than 256 user-space callees

The callee Merkle tree is bounded at 256 unique user-space functions per traced step to keep hashing time predictable. Real pipelines reach 30–80. When the budget is hit, a `[Logram][oracle] callee budget exhausted` warning shows where it stopped. To raise it:

```python
import logram.oracle
logram.oracle._CALLEE_BUDGET = 1024
```
