# Design and positioning

[← README](../README.md) · [How it works](how-it-works.md) · [Limitations](limitations.md) · [CLI](cli.md) · [Advanced usage](advanced.md) · [Agent interface](mcp.md) · **Design** · [Benchmarks](../benchmarks/README.md)

## Motivation

Multi-step pipelines built on LLM or VLM calls are slow, costly and non-deterministic. Consider a pipeline that processes 150 image tiles through a VLM, where a logic error appears at step 97 (the figures below are an illustrative order of magnitude, not a measurement). Testing a fix today means:

1. **Waiting** about 4 minutes for steps 1–96 to re-execute.
2. **Paying** about $0.80 in API calls to reach the failure point again.
3. **Guessing**, since coding agents only see static source files, not what actually ran.

A fourth cost is less visible: every full re-run re-samples the upstream steps, so the effect of a change is **confounded** with sampling noise from steps that were never modified.

Freezing unchanged steps addresses all four.

| | Full re-run | Replay with Logram |
|---|---|---|
| Cost to validate a fix (150-tile VLM example, illustrative) | ~$0.80 | ~$0.004 |
| Feedback latency | ~4 min | ~2 s |
| Upstream outputs | Re-sampled on every run | Identical to the baseline |
| Agent context | Static source files | Runtime traces, diffs, targeted replay |

## What Logram is

A decorator-based SDK (`@logram.trace`) that records a pipeline's logic, data flow and state, **entirely locally**, and provides four capabilities on top of that record:

1. **Logic fingerprinting.** Before a step runs, Logram computes a fingerprint of its code from the abstract syntax tree, the runtime values of the constants it reads, and the functions it calls, recursively. Formatting, comments, docstrings and Python-version changes do not affect it; semantic changes do.
2. **Replay.** Unchanged steps are replayed from a local SQLite store in well under a millisecond each ([measured](../benchmarks/README.md)). Only modified logic, and the steps that consume its new output, execute live.
3. **Divergence analysis.** When two runs disagree, Logram walks the call graph of both and reports exactly what changed, down to a prompt constant three levels deep, with a unified diff.
4. **Agent interface.** A native MCP server exposes the trace store to coding agents (Claude, Cursor), which can inspect what ran, locate the cause of a divergence, apply a change, validate it by replay, and check it against reference runs.

## What Logram is not

- **Not a monitoring or observability tool.** It is not designed for production: no latency dashboards, alerting or anomaly detection. Use LangSmith, Langfuse or Datadog for that. They observe the past; Logram is for iterating on the next version.
- **Not an evaluation or benchmarking framework.** Its golden-run feature answers one question: *did my change break the baseline?* It is a regression guard, not an accuracy measurement system. For metrics across hundreds of documents, use RAGAS, DeepEval or a custom harness.
- **Not an orchestration framework.** There are no DAG objects, retry policies or schedulers. Logram wraps existing code, whether it uses LangChain, LlamaIndex, Prefect or plain Python functions.
- **Not a coding-assistant plugin.** The MCP server does not write code or suggest completions. It gives the agent ground truth about the runtime: real prompts, real outputs, real logic diffs.

## Related tools

Observability platforms (LangSmith, Langfuse) record what happened in production and evaluate it; orchestrators (Dagster, Prefect) schedule and retry pipelines; unit tests with mocks validate code against a frozen surface. Logram answers a different question: *how cheaply can a fix be validated against the real model, on the real inputs?* It is complementary to all three.

`pytest` validates a frozen surface and LangSmith records what happened; Logram lets you change one prompt and check the new behaviour against the real model, on the real inputs.

## Design rationale

Observability tools are built for production: they surface anomalies in deployed systems. That is essential for maintenance but does not help build a better system. Logram targets the development loop, where the bottleneck, both for improving performance and fixing logic, is the cost of verifying each change: the latency and API calls needed to re-execute successful upstream steps just to reach the logic being tuned.

Reducing that cost to seconds changes how pipelines get built:

- **Prototyping.** Several prompt variants or logic structures can be tested against the same historical context in minutes.
- **Controlled comparisons.** Sub-components are tuned on real historical inputs, with everything upstream held fixed.
- **Regression-free upgrades.** An improvement for one edge case is checked against all reference runs before it lands.
