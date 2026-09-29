"""Mutation benchmark for the logic fingerprint.

Every single-point mutation of ``corpus.py`` (a constant, an arithmetic or a
comparison operator) is applied in turn. For each mutant and each pipeline step
we observe two things:

* whether the step's **output** changed on a fixed set of inputs, and
* whether the step's **fingerprint** changed.

Output changed but fingerprint unchanged is a *false hit*: replay would serve a
stale result. Every false hit reported here is real, because a different
output proves the behaviour changed. The converse (fingerprint changed, output
unchanged) is a *spurious miss*: the step reruns although it did not need to.
That rate is an upper bound, since a behaviour change may not show on the
chosen inputs.

Cosmetic edits (reformatting, docstrings, comments) must keep every
fingerprint unchanged.

    python benchmarks/mutation_soundness.py [--json results.json]
"""

from __future__ import annotations

import argparse
import ast
import copy
import importlib.util
import json
import logging
import os
import sys
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any

from logram.decorators import _implementation_artifacts, clear_logic_snapshot_cache

CORPUS = Path(__file__).with_name("corpus.py")
MODULE_NAME = "corpus_under_test"

_BINOP_SWAP = {
    ast.Add: ast.Sub, ast.Sub: ast.Add, ast.Mult: ast.Add, ast.Div: ast.Mult,
    ast.FloorDiv: ast.Div, ast.Mod: ast.Mult, ast.Pow: ast.Mult,
}
_CMP_SWAP = {
    ast.Lt: ast.LtE, ast.LtE: ast.Lt, ast.Gt: ast.GtE, ast.GtE: ast.Gt, ast.Eq: ast.NotEq,
    ast.NotEq: ast.Eq, ast.In: ast.NotIn, ast.NotIn: ast.In, ast.Is: ast.IsNot, ast.IsNot: ast.Is,
}


# ---------------------------------------------------------------------------
# Mutants
# ---------------------------------------------------------------------------

def _excluded_ids(tree: ast.Module) -> set[int]:
    """Nodes that are not pipeline logic: docstrings and the STEPS input table."""
    excluded: set[int] = set()
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.ClassDef)) and body:
            first = body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
                excluded.add(id(first.value))
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "STEPS" for t in node.targets):
            excluded.update(id(n) for n in ast.walk(node))
    return excluded


def _mutate_node(node: ast.AST) -> str | None:
    """Mutate ``node`` in place; return the operator name, or None if not mutable."""
    if isinstance(node, ast.Constant):
        value = node.value
        if isinstance(value, bool):
            node.value = not value
            return "constant:bool"
        if isinstance(value, int):
            node.value = value + 1
            return "constant:int"
        if isinstance(value, float):
            node.value = value + 0.5
            return "constant:float"
        if isinstance(value, str):
            node.value = value + "!"
            return "constant:str"
        return None
    if isinstance(node, ast.BinOp) and type(node.op) in _BINOP_SWAP:
        node.op = _BINOP_SWAP[type(node.op)]()
        return "operator:arithmetic"
    if isinstance(node, ast.Compare) and len(node.ops) == 1 and type(node.ops[0]) in _CMP_SWAP:
        node.ops = [_CMP_SWAP[type(node.ops[0])]()]
        return "operator:comparison"
    return None


def semantic_mutants(source: str) -> list[tuple[str, str, int]]:
    """(operator, mutant source, line) for every single-point mutation."""
    tree = ast.parse(source)
    excluded = _excluded_ids(tree)
    candidates = [i for i, n in enumerate(ast.walk(tree)) if id(n) not in excluded]
    mutants = []
    for index in candidates:
        mutant = copy.deepcopy(tree)
        node = list(ast.walk(mutant))[index]
        operator = _mutate_node(node)
        if operator is not None:
            mutants.append((operator, ast.unparse(mutant), getattr(node, "lineno", 0)))
    return mutants


def cosmetic_edits(source: str) -> list[tuple[str, str]]:
    """Edits that change the text but not the behaviour."""
    tree = ast.parse(source)
    edits = [("cosmetic:reformat", ast.unparse(tree))]
    lines = source.splitlines()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            body_line = node.body[0].lineno - 1
            indent = " " * node.body[0].col_offset
            commented = lines[:body_line] + [f"{indent}# note: see the extraction spec"] + lines[body_line:]
            edits.append(("cosmetic:comment", "\n".join(commented) + "\n"))
            documented = copy.deepcopy(tree)
            fn = next(n for n in ast.walk(documented) if isinstance(n, ast.FunctionDef) and n.lineno == node.lineno)
            first = fn.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
                first.value.value = "Rewritten documentation."
            else:
                fn.body.insert(0, ast.Expr(ast.Constant("Added documentation.")))
            edits.append(("cosmetic:docstring", ast.unparse(documented)))
    return edits


# ---------------------------------------------------------------------------
# Observation
# ---------------------------------------------------------------------------

def observe(source: str, workdir: Path, steps: dict[str, list[tuple[Any, ...]]]) -> dict[str, tuple[str, list[str]]]:
    """Load ``source`` as a fresh module; return {step: (fingerprint, outputs)}."""
    # One directory per variant: same module name (it is part of the
    # fingerprint), distinct file (so inspect never reads a stale source).
    variant_dir = Path(tempfile.mkdtemp(dir=workdir))
    path = variant_dir / f"{MODULE_NAME}.py"
    path.write_text(source)
    spec = importlib.util.spec_from_file_location(MODULE_NAME, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[MODULE_NAME] = module
    try:
        spec.loader.exec_module(module)
    except Exception as exc:  # the mutant does not even import
        return {name: (f"import-error:{type(exc).__name__}", []) for name in steps}

    clear_logic_snapshot_cache()
    observed = {}
    for name, inputs in steps.items():
        func = getattr(module, name)
        _, fingerprint, _ = _implementation_artifacts(func)
        outputs = []
        for args in inputs:
            try:
                outputs.append(repr(func(*copy.deepcopy(args))))
            except Exception as exc:
                outputs.append(f"raise {type(exc).__name__}")
        observed[name] = (fingerprint, outputs)
    return observed


def run() -> dict[str, Any]:
    source = CORPUS.read_text()
    corpus_ns: dict[str, Any] = {}
    exec(compile(source, str(CORPUS), "exec"), corpus_ns)
    steps: dict[str, list[tuple[Any, ...]]] = corpus_ns["STEPS"]

    workdir = Path(tempfile.mkdtemp(prefix="logram_mutation_"))
    os.chdir(workdir)  # the oracle only follows callees defined under the cwd
    baseline_source = ast.unparse(ast.parse(source))
    baseline = observe(baseline_source, workdir, steps)

    counts: Counter[tuple[str, str]] = Counter()
    false_hits = []
    for operator, mutant_source, line in semantic_mutants(source):
        mutant = observe(mutant_source, workdir, steps)
        for name, (fingerprint, outputs) in mutant.items():
            base_fp, base_out = baseline[name]
            behaviour_changed = outputs != base_out
            fp_changed = fingerprint != base_fp
            if behaviour_changed and not fp_changed:
                verdict = "false_hit"
                false_hits.append({"operator": operator, "line": line, "step": name})
            elif behaviour_changed:
                verdict = "correct_miss"
            elif fp_changed:
                verdict = "spurious_miss"
            else:
                verdict = "correct_hit"
            counts[(operator, verdict)] += 1

    cosmetic: Counter[tuple[str, str]] = Counter()
    original = observe(source, workdir, steps)
    for operator, edited_source in cosmetic_edits(source):
        edited = observe(edited_source, workdir, steps)
        for name, (fingerprint, _) in edited.items():
            cosmetic[(operator, "stable" if fingerprint == original[name][0] else "invalidated")] += 1

    return {
        "semantic": {f"{op}|{verdict}": n for (op, verdict), n in sorted(counts.items())},
        "cosmetic": {f"{op}|{verdict}": n for (op, verdict), n in sorted(cosmetic.items())},
        "false_hits": false_hits,
    }


def summarize(results: dict[str, Any]) -> str:
    sem: Counter[str] = Counter()
    for key, n in results["semantic"].items():
        sem[key.split("|")[1]] += n
    changed = sem["false_hit"] + sem["correct_miss"]
    unchanged = sem["correct_hit"] + sem["spurious_miss"]
    cos: Counter[str] = Counter()
    for key, n in results["cosmetic"].items():
        cos[key.split("|")[1]] += n
    lines = [
        "| metric | value |",
        "|---|---|",
        f"| (mutant, step) pairs where the output changed | {changed} |",
        f"| false hits (stale replay) | **{sem['false_hit']}** ({sem['false_hit'] / max(changed, 1):.1%}) |",
        f"| pairs where the output did not change | {unchanged} |",
        f"| spurious misses (unneeded rerun, upper bound) | {sem['spurious_miss']} ({sem['spurious_miss'] / max(unchanged, 1):.1%}) |",
        f"| cosmetic edits keeping every fingerprint | {cos['stable']}/{cos['stable'] + cos['invalidated']} |",
    ]
    by_step: Counter[str] = Counter(h["step"] for h in results["false_hits"])
    if by_step:
        lines += ["", "False hits by step: " + ", ".join(f"`{s}` {n}" for s, n in by_step.most_common())]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--json", type=Path, help="write the raw results to this file")
    args = parser.parse_args()
    json_path = args.json.resolve() if args.json else None  # run() changes the cwd
    logging.disable(logging.WARNING)
    results = run()
    if json_path:
        json_path.write_text(json.dumps(results, indent=2))
    print(summarize(results))


if __name__ == "__main__":
    main()
