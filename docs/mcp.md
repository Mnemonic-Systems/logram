# Agent interface (MCP)

[← README](../README.md) · [How it works](how-it-works.md) · [Limitations](limitations.md) · [CLI](cli.md) · [Advanced usage](advanced.md) · **Agent interface** · [Design](design.md) · [Benchmarks](../benchmarks/README.md)

Coding agents such as Claude and Cursor work from static source files. When a pipeline fails at step 97, the agent can read the code but not what actually ran. Logram's MCP server gives it access to the trace store, so it can:

- **Analyze**: identify failing runs, or read the exact runtime prompts of a successful baseline.
- **Navigate**: walk the recursive call graph to find where a prompt or helper is defined and called.
- **Experiment**: apply a change and validate it with a targeted replay, where only the changed steps call a model.
- **Certify**: run the updated pipeline against reference runs to check that a fix for one input did not break others.

## Setup

```bash
pip install "logram-sdk[mcp] @ git+https://github.com/Mnemonic-Systems/logram.git"
logram init          # writes the agent rule files below into your project
logram mcp install   # registers the MCP server with Claude Code, Cursor or Claude Desktop
logram mcp config    # or print the config block to add manually
```

| File written by `logram init` | Read by |
|---|---|
| `LOGRAM_AGENT_RULES.md` | Complete instrumentation rules (authoritative spec) |
| `.cursorrules` | Cursor, on every code generation |
| `CLAUDE.md` | Claude Code, at session start |

These files tell the agent how to instrument the pipeline, what to exclude from the cache and what never to touch. Commit them with your code.

Manual configuration (`~/.cursor/mcp.json` or Claude Desktop config):

```json
{
  "mcpServers": {
    "logram": {
      "command": "logram",
      "args": ["mcp", "start"]
    }
  }
}
```

## Tools

| Tool | When to use |
|---|---|
| `list_runs(project)` | First call in a debugging session; returns run IDs |
| `get_investigation_brief(run_id)` | Triage of a failed run: step, error, logic hash |
| `get_step_source(logic_hash)` | Exact code and prompts that ran at failure time |
| `analyze_logic_divergence(run_id_a, run_id_b)` | What changed between two runs, recursively |
| `compare_step_data(run_id_a, run_id_b, step_name)` | Diff runtime inputs and outputs when logic is identical |
| `run_surgical_replay(script_path)` | Validate a fix; only modified steps run live |
| `verify_against_golden_dataset(project, script_path)` | Check for regressions before closing a bug |

The tools that execute a script (`run_surgical_replay`, `verify_against_golden_dataset`) enforce these safety gates:

- **Path jail**: only `.py` files inside the current working directory. This stops an agent from running arbitrary files by mistake; it is not a sandbox, since any script in the project can do anything Python can.
- **Timeout**: the script is stopped after 15 minutes.
- **Circuit breaker** (`run_surgical_replay`): at most 5 replays per agent session, to bound API cost.
- **Logic guard** (`run_surgical_replay`): refuses `force_step` on a step whose last run failed. That step has no cache entry and already runs live, so forcing it only repeats the failure; the refusal does not consume the replay budget.
