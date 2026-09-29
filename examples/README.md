# Examples

## `invoice_pipeline.py`

A six-page extraction pipeline followed by a summary, built on a fake LLM client
that sleeps like a real API call and reports token usage. No API key needed.

```bash
python examples/invoice_pipeline.py                        # live run, ~4 s
LOGRAM_REPLAY=true python examples/invoice_pipeline.py     # every step replayed, ~20 ms
```

Now edit `SUMMARY_PROMPT` in the file and replay again:

```bash
LOGRAM_REPLAY=true python examples/invoice_pipeline.py     # only summarize runs live, ~0.6 s
logram inspect last                                         # step tree: 13 replayed, 2 live
logram diff last --globals                                  # the prompt change, before/after
logram stats                                                # time and tokens saved
```

Set `FAKE_LLM_LATENCY` (seconds, default `0.6`) to make the fake model slower or faster.
