# CLI reference

The CLI is built on [Typer](https://typer.tiangolo.com/) and [Rich](https://rich.readthedocs.io/): step trees, syntax-highlighted code, unified diffs and progress bars in the terminal.

| Command | Description |
|---|---|
| `logram list` | List runs with status, duration, relative time |
| `logram inspect <run_id>` | Step tree with status badges (accepts `last`, `last-failed`, `-1`, `-2`…) |
| `logram view <step_id>` | Inputs, output, error and blobs of one step |
| `logram recover <logic_hash>` | Runtime source code and globals for a fingerprint |
| `logram replay <script.py>` | Replay (`--force`, `--from`) |
| `logram diff <run_a> <run_b>` | Code, globals, inputs, outputs (`--code`, `--globals`, `--inputs`, `--outputs`) |
| `logram diff last` / `--ss` | Previous run → last run / last success → last run, same `input_id` |
| `logram golden add <run_id>` | Mark a run as a reference |
| `logram test <script.py>` | Regression test against reference runs |
| `logram restore <run_id>` | Print copy-pasteable code blocks to revert to a run |
| `logram stats` | Time, tokens and cost saved by replay |
| `logram open <step_id>` | Open a step's image blob in the system viewer |
| `logram clean` | Interactive cleanup of failed runs and orphan assets |
| `logram doctor` | Environment check: Python, database, MCP wiring, orphan blobs |
| `logram live` | Live step tree, polls every 500 ms (`--interval`) |
| `logram ui` | Local web dashboard API server |
| `logram mcp start` / `config` / `install` | Launch, print config for, or register the MCP server |

## Replay: `logram replay`

Reruns the pipeline with `LOGRAM_REPLAY=true`: unchanged steps replay, modified steps run live.

```bash
logram replay my_pipeline.py                            # standard replay
logram replay my_pipeline.py --force call_vlm           # force one step live (invalidates its cache)
logram replay my_pipeline.py --from extract_quantities  # run this step and everything downstream live
logram replay my_pipeline.py -f step_a -f step_b        # force several steps live
```

The CLI prints which steps are forced live, invalidates their cached rows before launching, streams the subprocess output, and ends with a success or failure badge and a hint for the next command.

## Diff: `logram diff`

Compares two runs across four dimensions and renders:

1. a summary table, one row per step, with `logic_hash`, `source`, `globals` and `callees` marked `identical`, `changed` or `same`;
2. unified diffs for every dimension that changed;
3. a callee tree for steps whose deep dependencies changed (callee, depth, what changed).

```bash
logram diff run_20260425_142211 run_20260425_150033   # full diff
logram diff last                                      # previous run → last run, same input_id
logram diff --ss                                      # last success → last run, same input_id
logram diff <run_a> <run_b> --code                    # source only
logram diff <run_a> <run_b> --globals                 # prompts and constants only
logram diff <run_a> <run_b> --inputs                  # runtime inputs only
logram diff <run_a> <run_b> --outputs                 # runtime outputs only
```

```text
diff  ·  run_a → run_b

step                logic_hash   source   globals   callees
extract_quantities  changed      same     same      build_prompt +1

callee tree  ·  extract_quantities  ·  2 node(s) changed
└── build_prompt  depth 1  globals:EXTRACTION_PROMPT_V3
    └── extract_quantities → build_prompt
```

## Investigation: `list`, `inspect`, `view`, `recover`

```bash
logram list                                      # all runs, newest first
logram list --project my_agent                   # filter by project
logram list --group-by-input                     # group by input_id
logram list --copy-field run_id --copy-index 1   # copy the first run_id to the clipboard

logram inspect last          # most recent run
logram inspect last-failed   # most recent failed run (alias: fail)
logram inspect -1            # most recent (-2 = second-to-last, …)
```

Tab completion (`logram --install-completion`) suggests real run IDs alongside the shorthands.

```text
my_agent  ·  2026-04-25 14:22:11  ·  success

my_agent
├── ✓ load_pdf            0.012s   SUCCESS
├── ✓ split_tiles         0.044s   SUCCESS
├── ⏭ call_vlm           [×150]   REPLAYED
├── ✗ extract_quantities  1.201s   FAILED
└── · aggregate_results   —        SKIPPED

Total: 1.26s   Live: 1.26s   Replayed: 150 steps
```

`logram view <step_id>` shows inputs, output and error as highlighted JSON, with binary outputs listed by hash, size and path. `logram recover <logic_hash>` shows the exact source and globals that were active for a fingerprint: what actually ran, not what the file says today.

## Regression: `golden`, `test`, `restore`

```bash
logram golden add run_20260425_142211
logram test my_pipeline.py
```

```text
golden test  ·  my_pipeline.py  ·  3 input(s)

input_id          baseline         new run          result     details
doc_invoice_42    run_20260420...  run_20260425...  ✓ SUCCESS  no regression
doc_invoice_87    run_20260420...  run_20260425...  ✗ FAILED   2 step(s) differ
doc_invoice_103   run_20260420...  run_20260425...  ✓ SUCCESS  no regression
```

`logram test` replays the script once per reference input, with `LOGRAM_INPUT_ID` set to that input (`logram.init()` uses it as the run's `input_id`; the script should load the matching document, e.g. `os.environ.get("LOGRAM_INPUT_ID", "doc_42")`). Every call of every step is compared, and the command exits with code 1 on any regression, so it can run in CI. `logram restore <run_id>` prints all source blocks and global snapshots of a run as copy-pasteable panels, to revert a function manually to a known-good state.

## Metrics: `stats`

```bash
logram stats                         # global
logram stats --project my_agent      # one project
logram stats <run_id>                # one run
logram stats --hourly-rate 150       # custom rate for the cost estimate
```

```text
stats  ·  global  ·  47 run(s)  ·  6 820 steps (6 521 replayed)

metric                  value
Resource time saved     2h 14m 33s
Total compute time      2h 21m 08s
Efficiency ratio        95.3%
Financial gain (est.)   33.60 €

tokens              value
Spent (live)        124 820
Saved (cache)       2 643 100
Bypass rate         95.5%
```

## Maintenance and UI: `clean`, `open`, `ui`, `doctor`, `live`

- `logram clean` lists failed runs and orphan blobs (assets no longer referenced by any step) and asks before deleting.
- `logram open <step_id>` opens a step's image blob in the system viewer, useful to see what a VLM actually received.
- `logram ui` launches a local read-only FastAPI server for the web dashboard (`--port`, `--no-open-browser`).
- `logram doctor` checks Python version, SDK installation, the `.logram/` directory and database, MCP wiring for Claude Code, Claude Desktop and Cursor, and pending cleanup.
- `logram live` shows a live-updating step tree of the running pipeline (`--interval`, minimum 100 ms); run it in a split terminal.

```text
logram doctor

  check                status     detail
  Python               ✓ ok       3.12.3
  Logram SDK           ✓ ok       0.3.0
  .logram/             ✓ ok       /path/to/.logram
  logram.db            ✓ ok       142 KB · 12 run(s)
  Claude Code MCP      ✓ ok       logram found in ~/.claude.json
  Claude Desktop MCP   – n/a      not supported on this platform
  Cursor MCP           ⚠ warn     found mcp.json but logram not wired
  Cleanup              ⚠ warn     3 failed run(s) · 1 orphan blob(s)
```
