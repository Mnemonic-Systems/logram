# Benchmarks

[← README](../README.md) · [How it works](../docs/how-it-works.md) · [Limitations](../docs/limitations.md)

Two questions matter for a replay cache: **is it sound** (does it ever serve a
stale result?) and **what does it cost**?

## Soundness: mutation benchmark

`mutation_soundness.py` applies every single-point mutation to
[`corpus.py`](corpus.py) (13 functions shaped like LLM-pipeline steps: prompt
templates, few-shot lists, scoring weights, regex parsing, helpers, methods,
closures, `functools.partial`, class constants, config objects). Mutations
change one constant (int, float, str, bool), one arithmetic operator or one
comparison operator. Cosmetic edits (reformatting, comments, docstrings) are
applied separately.

For every (mutant, step) pair, the benchmark runs the step on fixed inputs and
computes its fingerprint:

| output | fingerprint | verdict |
|---|---|---|
| changed | unchanged | **false hit**: replay would return a stale result |
| changed | changed | correct miss |
| unchanged | unchanged | correct hit |
| unchanged | changed | spurious miss: an unneeded rerun |

A reported false hit is always real (the output provably differs). Spurious
misses are an upper bound: a mutation can change behaviour on inputs the
benchmark does not try.

```bash
python benchmarks/mutation_soundness.py
```

| | v0.3.0 (before this work) | current |
|---|---|---|
| (mutant, step) pairs whose output changed | 111 | 111 |
| **false hits** | **18 (16.2%)** | **0** |
| spurious misses (upper bound) | 13 / 1280 (1.0%) | 20 / 1280 (1.6%) |
| cosmetic edits that keep every fingerprint | 647 / 663 | 663 / 663 |

The v0.3.0 false hits came from methods of objects created inside a step
(`Extractor().total(...)`, 12), `functools.partial` (4), a compiled regex held
in a global (1) and a few-shot list longer than the 16 items the fingerprint
used to look at (1). The cosmetic failures were docstrings.

**How the corpus was built, and what that implies.** The first version of the
corpus (9 steps) exposed the created-object and regex blind spots, which were
then fixed. Four more steps using patterns that had *not* been targeted
(`partial`, `lru_cache`, `SimpleNamespace` config, static/class methods) were
added afterwards as a held-out check; they exposed `partial`, fixed next. Zero
false hits on this corpus is therefore evidence, not proof: the corpus was
written by the same author as the oracle. `tests/test_benchmarks.py` runs the
benchmark in CI so that the result cannot silently regress.

**Known blind spots** (not exercised by the corpus, documented in the main
README): attributes holding objects created elsewhere (`self.client.call()`
with `self.client` set in `__init__`), truly dynamic dispatch
(`getattr(self, name)`), imports inside function bodies, data loaded from
files at runtime (prompts in YAML/JSON), and code outside the project
directory (a library upgrade does not change any fingerprint).

## Cost: `overhead.py`

```bash
python benchmarks/overhead.py
```

Median over 2,000 calls (cache keys: 200, or 10 for the largest), Python
3.13, Apple M-series laptop:

| measurement | median |
|---|---|
| untraced call | 0.04 µs |
| traced call, live (per call) | 50 µs |
| traced call, replayed (per call) | 174 µs |
| cold fingerprint, per step (13 corpus steps) | 0.20 ms |
| cache key: small dict | 6 µs |
| cache key: 10 KB prompt | 9 µs |
| cache key: list of 100 dicts | 163 µs |
| cache key: list of 10k dicts | 15.85 ms |
| cache key: 5 MB of bytes | 1.46 ms |

The fingerprint is computed once per function per run. Live-mode overhead is
dominated by building the cache key and serializing inputs; the SQLite write
happens on a background thread. A replayed step costs a SQLite lookup and the
rehydration of its output, which is three to four orders of magnitude below a
model call. The cache key visits every element of its arguments (that is what
makes it sound), so very large structured arguments cost milliseconds; a
`__logram_trace_key__` or `vcr_key_fn` keeps that constant.
