"""Logic fingerprint: what must and must not change the hash."""

from __future__ import annotations

import pytest

from logram.oracle import clear_oracle_cache, compute_logic_fingerprint

from .conftest import fingerprint, fingerprint_of, load_module

BASE = """
def f(x):
    return x + 1
"""


# ---------------------------------------------------------------------------
# Invariance: edits that do not change behaviour keep the same fingerprint
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "variant",
    [
        pytest.param("def f(x):\n    # a comment\n    return x + 1\n", id="comment"),
        pytest.param("def f(x):\n\n    return   x+1\n", id="whitespace"),
        pytest.param('def f(x):\n    """Docstring."""\n    return x + 1\n', id="docstring"),
        pytest.param("def f(x):\n    return (\n        x\n        + 1\n    )\n", id="line-breaks"),
    ],
)
def test_non_semantic_edits_keep_fingerprint(variant: str) -> None:
    assert fingerprint_of(variant) == fingerprint_of(BASE)


def test_fingerprint_is_deterministic() -> None:
    mod = load_module(BASE)
    assert fingerprint(mod.f) == fingerprint(mod.f)


# ---------------------------------------------------------------------------
# Invalidation: every semantic change must produce a new fingerprint
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    ("before", "after"),
    [
        pytest.param(BASE, "def f(x):\n    return x + 2\n", id="body"),
        pytest.param(BASE, "def f(y):\n    return y + 1\n", id="parameter-rename"),
        pytest.param(BASE, "def f(x: int) -> int:\n    return x + 1\n", id="annotation"),
        pytest.param(
            "T = 0.7\ndef f():\n    return T\n",
            "T = 0.9\ndef f():\n    return T\n",
            id="global-constant",
        ),
        pytest.param(
            "P = [str(i) for i in range(20)]\ndef f():\n    return P\n",
            "P = [str(i) for i in range(19)] + ['changed']\ndef f():\n    return P\n",
            id="global-list-item-beyond-16",
        ),
        pytest.param(
            "D = {f'k{i:02d}': i for i in range(30)}\ndef f():\n    return D\n",
            "D = {**{f'k{i:02d}': i for i in range(30)}, 'k29': -1}\ndef f():\n    return D\n",
            id="global-dict-key-beyond-24",
        ),
        pytest.param(
            "N = [[[[[[[[1]]]]]]]]\ndef f():\n    return N\n",
            "N = [[[[[[[[2]]]]]]]]\ndef f():\n    return N\n",
            id="global-deeply-nested",
        ),
        pytest.param(
            "S = {str(i) for i in range(20)}\ndef f():\n    return S\n",
            "S = {str(i) for i in range(19)} | {'changed'}\ndef f():\n    return S\n",
            id="global-set-beyond-16",
        ),
        pytest.param(
            "W = 1\ndef f(xs):\n    return sorted(xs, key=lambda x: x * W)\n",
            "W = -1\ndef f(xs):\n    return sorted(xs, key=lambda x: x * W)\n",
            id="global-read-in-lambda",
        ),
        pytest.param(
            "W = 1\ndef f(x):\n    def inner(y):\n        return y * W\n    return inner(x)\n",
            "W = 2\ndef f(x):\n    def inner(y):\n        return y * W\n    return inner(x)\n",
            id="global-read-in-nested-function",
        ),
        pytest.param(
            "W = 1\ndef f(xs):\n    return [x * W for x in xs]\n",
            "W = 2\ndef f(xs):\n    return [x * W for x in xs]\n",
            id="global-read-in-comprehension",
        ),
        pytest.param(
            "def helper(x):\n    return x * 2\ndef f(x):\n    return helper(x) + 1\n",
            "def helper(x):\n    return x * 3\ndef f(x):\n    return helper(x) + 1\n",
            id="transitive-callee",
        ),
        pytest.param(
            "def helper(x):\n    return x * 2\ndef f(xs):\n    return list(map(lambda x: helper(x), xs))\n",
            "def helper(x):\n    return x * 3\ndef f(xs):\n    return list(map(lambda x: helper(x), xs))\n",
            id="callee-called-from-lambda",
        ),
        pytest.param(
            "class S:\n    temperature = 0.7\nSETTINGS = S()\ndef f():\n    return SETTINGS.temperature\n",
            "class S:\n    temperature = 0.9\nSETTINGS = S()\ndef f():\n    return SETTINGS.temperature\n",
            id="global-object-attribute-read",
        ),
        pytest.param(
            "import re\nP = re.compile(r'a+')\ndef f(s):\n    return P.findall(s)\n",
            "import re\nP = re.compile(r'b+')\ndef f(s):\n    return P.findall(s)\n",
            id="global-compiled-regex",
        ),
        pytest.param(
            "from dataclasses import dataclass\n@dataclass\nclass C:\n    t: float\nCFG = C(0.7)\ndef f():\n    return read(CFG)\ndef read(c):\n    return c.t\n",
            "from dataclasses import dataclass\n@dataclass\nclass C:\n    t: float\nCFG = C(0.9)\ndef f():\n    return read(CFG)\ndef read(c):\n    return c.t\n",
            id="global-dataclass-instance",
        ),
        pytest.param(
            "from pydantic import BaseModel\nclass C(BaseModel):\n    t: float\nCFG = C(t=0.7)\ndef f():\n    return read(CFG)\ndef read(c):\n    return c.t\n",
            "from pydantic import BaseModel\nclass C(BaseModel):\n    t: float\nCFG = C(t=0.9)\ndef f():\n    return read(CFG)\ndef read(c):\n    return c.t\n",
            id="global-pydantic-instance",
        ),
        pytest.param(
            "class E:\n    def total(self, x):\n        return x * 2\ndef f(x):\n    return E().total(x)\n",
            "class E:\n    def total(self, x):\n        return x * 3\ndef f(x):\n    return E().total(x)\n",
            id="method-of-object-created-in-step",
        ),
        pytest.param(
            "class E:\n    def total(self, x):\n        return x * 2\ndef f(x):\n    e = E()\n    return e.total(x)\n",
            "class E:\n    def total(self, x):\n        return x * 3\ndef f(x):\n    e = E()\n    return e.total(x)\n",
            id="method-of-object-in-local-variable",
        ),
        pytest.param(
            "class E:\n    RATE = 2\n    def total(self, x):\n        return x * self.RATE\ndef f(x):\n    return E().total(x)\n",
            "class E:\n    RATE = 3\n    def total(self, x):\n        return x * self.RATE\ndef f(x):\n    return E().total(x)\n",
            id="class-constant-read-through-self",
        ),
        pytest.param(
            "import functools\ndef _w(x, k):\n    return x * k\nW = functools.partial(_w, k=2)\ndef f(x):\n    return W(x)\n",
            "import functools\ndef _w(x, k):\n    return x * k\nW = functools.partial(_w, k=3)\ndef f(x):\n    return W(x)\n",
            id="partial-bound-argument",
        ),
        pytest.param(
            "import functools\ndef _w(x, k):\n    return x * k\nW = functools.partial(_w, k=2)\ndef f(x):\n    return W(x)\n",
            "import functools\ndef _w(x, k):\n    return x + k\nW = functools.partial(_w, k=2)\ndef f(x):\n    return W(x)\n",
            id="partial-wrapped-function-body",
        ),
        pytest.param(
            "def f(x, scale=2):\n    return x * scale\n",
            "def f(x, scale=3):\n    return x * scale\n",
            id="default-argument",
        ),
    ],
)
def test_semantic_edits_change_fingerprint(before: str, after: str) -> None:
    assert fingerprint_of(before) != fingerprint_of(after)


def test_method_change_invalidates_caller_through_mro() -> None:
    src = """
class A:
    def helper(self, x):
        return x * {factor}

    def parent(self, x):
        return self.helper(x) + 1
"""
    a1 = load_module(src.format(factor=2)).A.parent
    a2 = load_module(src.format(factor=3)).A.parent
    assert fingerprint(a1) != fingerprint(a2)


def test_class_constant_read_by_traced_method_invalidates() -> None:
    src = """
class Pipeline:
    THRESHOLD = {value}

    def keep(self, xs):
        return [x for x in xs if x >= self.THRESHOLD]
"""
    before = load_module(src.format(value=0.5)).Pipeline.keep
    after = load_module(src.format(value=0.8)).Pipeline.keep
    assert fingerprint(before) != fingerprint(after)


def test_closure_value_change_invalidates() -> None:
    mod = load_module(
        """
def make(k):
    def f(x):
        return x * k
    return f
"""
    )
    assert fingerprint(mod.make(2)) != fingerprint(mod.make(3))


def test_runtime_mutation_of_global_invalidates() -> None:
    mod = load_module("CONFIG = {'temperature': 0.7}\ndef f():\n    return CONFIG['temperature']\n")
    before = fingerprint(mod.f)
    mod.CONFIG["temperature"] = 0.9
    assert fingerprint(mod.f) != before


def test_unrelated_global_does_not_invalidate() -> None:
    before = fingerprint_of("UNUSED = 1\ndef f(x):\n    return x + 1\n")
    after = fingerprint_of("UNUSED = 2\ndef f(x):\n    return x + 1\n")
    assert before == after


# ---------------------------------------------------------------------------
# Volatility markers
# ---------------------------------------------------------------------------

def _markers(source: str, name: str = "f") -> list[str]:
    clear_oracle_cache()
    snapshot, _ = compute_logic_fingerprint(getattr(load_module(source), name))
    return snapshot["volatile_markers"]


def test_eval_is_marked_volatile_deterministically() -> None:
    src = "def f(s):\n    return eval(s)\n"
    assert _markers(src) == ["<volatile:eval>"]
    assert fingerprint_of(src) == fingerprint_of(src)


def test_literal_getattr_is_safe_and_dynamic_getattr_is_volatile() -> None:
    assert _markers("def f(o):\n    return getattr(o, 'name')\n") == []
    assert _markers("def f(o, k):\n    return getattr(o, k)\n") == ["<volatile:getattr_dyn>"]


def test_eval_inside_lambda_is_marked_volatile() -> None:
    assert _markers("def f(xs):\n    return list(map(lambda s: eval(s), xs))\n") == ["<volatile:eval>"]


def test_shadowed_builtin_is_not_volatile() -> None:
    assert _markers("def f(eval, x):\n    return eval(x)\n") == []


def test_mutual_recursion_terminates() -> None:
    mod = load_module(
        """
def a(n):
    return b(n - 1) if n > 0 else 0

def b(n):
    return a(n - 1) if n > 0 else 0
"""
    )
    snapshot, _ = compute_logic_fingerprint(mod.a)
    assert "b" in snapshot["called_functions"]


# ---------------------------------------------------------------------------
# Cross-version stability: this digest must be identical on every supported
# Python version. CI runs this file on 3.10 → 3.13.
# ---------------------------------------------------------------------------

GOLDEN_SOURCE = '''
PROMPT = "Extract the quantities"
LIMITS = {"max_tokens": 256, "stops": ["\\n\\n", "###"]}


def helper(value: str) -> str:
    """Helper docstring."""
    return value.strip().lower()


def f(items: list[str], *, threshold: float = 0.5) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        if (key := helper(item)) and len(key) > threshold:
            counts[key] = counts.get(key, 0) + 1
    label = f"{PROMPT}: {len(counts)!r:>4}"
    match LIMITS:
        case {"max_tokens": int(n)} if n > 0:
            counts[label] = n
        case _:
            pass
    try:
        total = sum(counts.values())
    except (TypeError, ValueError) as exc:
        raise RuntimeError("bad counts") from exc
    return {k: v for k, v in counts.items() if v <= total}
'''

GOLDEN_STRUCTURAL_HASH = "4ab178a1c461b98815f4015718d39f6412690887af34e81f4a07db2ccabfdb96"
GOLDEN_FINGERPRINT = "5c687f5fb1df58d58d3b30ae504fd89602238da2dd89b71d822daf7ad1e3ae2a"


def test_structural_hash_is_stable_across_python_versions() -> None:
    import ast

    from logram.oracle import _hash_canonical_ast

    module = ast.parse(GOLDEN_SOURCE)
    fn = next(n for n in module.body if isinstance(n, ast.FunctionDef) and n.name == "f")
    assert _hash_canonical_ast(fn) == GOLDEN_STRUCTURAL_HASH


def test_full_fingerprint_is_stable_across_python_versions() -> None:
    mod = load_module(GOLDEN_SOURCE, name="logram_golden_module")
    assert fingerprint(mod.f) == GOLDEN_FINGERPRINT
