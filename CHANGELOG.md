# Changelog

All notable changes to this project. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[semantic versioning](https://semver.org/) (pre-1.0: minor versions may change behaviour).

## [Unreleased]

### Added

- `logram --version`.

### Fixed

- Install instructions: Logram is not published on PyPI (a different project
  named `logram` is), so extras are installed from the repository. The README
  now starts with a runnable example.

## [0.4.0] - 2026-09-29

Correctness release: replay no longer serves stale results, and several ways
of losing recorded data are fixed. Existing `.logram/` stores stay readable,
but fingerprints changed, so every step runs live once after upgrading.

### Fixed

- **Stale replays.** On the new mutation benchmark, 18 of 111 behaviour
  changes were still served from the cache; none are now. Causes:
  - globals and arguments were only inspected up to 16 list items, 24 dict
    keys and a few levels of nesting;
  - globals read inside lambdas and nested functions were ignored;
  - compiled regexes, enums, dates, dataclass and Pydantic instances held in
    globals were captured by type only;
  - `functools.partial` hid its bound arguments and the function it wraps;
  - methods of objects created inside a step, and `self.CONSTANT` reads in
    methods, were not part of the fingerprint;
  - objects exposing an `id` or `name` field were keyed on that field alone,
    and `__logram_trace_log__` changed the cache key.
- Replay batches written concurrently (e.g. a process pool in replay mode)
  failed with "database is locked" and were dropped silently.
- Forked worker processes lost pending steps on Python 3.10–3.12 and could
  hang on SQLite locks inherited from the parent.
- `logram clean` deleted blobs still referenced by step inputs or
  `@stateful` state.
- `logram test` compared only the last call of each step and always exited
  with 0, so the MCP `verify_against_golden_dataset` tool always passed.
- MCP: `verify_against_golden_dataset` had no path jail, neither script tool
  had a timeout, and the logic guard compared two different kinds of hash.
- `LOGRAM_DB_PATH` was ignored by the SDK, and `LOGRAM_INPUT_ID` by `init()`.
- Importing `logram` created `.logram/` and `.logram_assets/`.
- `@logram.trace` without parentheses returned a function instead of calling it.
- `logram inspect -2` (and other negative offsets) failed to parse.
- `logram inspect` and `logram live` counted nested steps twice in totals.
- `logram doctor` reported the SDK as not installed.
- Docstrings, and fields added by newer Python versions to the AST, changed
  the fingerprint; it is now identical on Python 3.10 to 3.13.

### Added

- Mutation benchmark for fingerprint soundness and an overhead benchmark
  (`benchmarks/`); the soundness benchmark also runs as a test.
- Test suite (82 tests) and CI: ruff, mypy, Python 3.10–3.13 on Linux and
  macOS, and an install of the built wheel without extras.
- `examples/invoice_pipeline.py`, runnable without an API key.
- `LICENSE` (MIT), `py.typed`, package classifiers.
- `docs/` pages for the details previously in the README.

### Changed

- The MCP server and the dashboard API are optional extras (`mcp`, `server`,
  `all`), e.g. `pip install "logram-sdk[mcp] @ git+https://github.com/Mnemonic-Systems/logram.git"`.
- `logram test` exits with 1 on any regression.
- `logram diff last` and `--ss` show the baseline on the left (old → new).
- The CLI finds the store from the project root, like the SDK, so it works
  from subdirectories.
- A cached entry that cannot be restored now runs the step live instead of
  raising.
- The cache key visits every element of the arguments: very large structured
  arguments cost more to key (about 16 ms for 10,000 dicts); use
  `__logram_trace_key__` or `vcr_key_fn` to keep that constant.
- Internals: one step executor instead of four copies of the tracing logic,
  the CLI split into a package, all user-facing text in English.

### Deprecated

- `compact_inputs` is still accepted by `@trace` but has no effect: inputs
  are always logged in full and the cache key is always content-complete.

## [0.3.0] - 2026-05-01

First public release: `@trace` / `@stateful` with AST-based logic
fingerprints and local SQLite replay, the `logram` CLI (`inspect`, `diff`,
`replay`, `golden`, `test`, `stats`, `live`, `doctor`…), the MCP server for
coding agents, the read-only dashboard API, and agent rule files written by
`logram init`.

[Unreleased]: https://github.com/Mnemonic-Systems/logram/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/Mnemonic-Systems/logram/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/Mnemonic-Systems/logram/releases/tag/v0.3.0
