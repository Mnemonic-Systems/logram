# ruff: noqa: BLE001
# pylint: disable=broad-exception-caught
"""
Logic Oracle — version-stable, AST-only logic fingerprinting.

Replaces the old bytecode-based hash that was unstable across Python minor versions.
The contract exported to ``decorators.py``:

    snapshot, registry = compute_logic_fingerprint(func)

The snapshot is a deterministic, JSON-serializable dict whose SHA256 is the
implementation fingerprint. The registry maps callee hashes to their own
snapshots (Merkle aggregation).

Design pillars
==============
1. **Cross-version stability** — we hash the *canonicalized AST structure*,
   not ``ast.unparse`` output (whose formatting varies across Python releases)
   nor ``co_code`` (whose layout shifts on every minor upgrade).
2. **Deterministic volatility** — calls to ``eval`` / ``exec`` / ``compile`` /
   ``__import__`` / ``globals()`` and *non-literal* ``getattr`` produce stable
   string markers (e.g. ``<volatile:eval>``). They never inject a time-based
   nonce — that would defeat the cache for an entire function instead of just
   marking the dynamic site.
3. **Literal-aware ``getattr``** — ``getattr(self, "x")`` is treated as a
   plain attribute read (SAFE), while ``getattr(self, dyn)`` is volatile.
4. **MRO method resolution** — ``self.helper()`` and ``cls.helper()`` are
   resolved through ``func.__qualname__`` → enclosing class → ``cls.__mro__``,
   unwrapping ``@classmethod`` / ``@staticmethod`` / ``@property`` descriptors.
5. **Cycle-safe, user-space-only recursion** — visited-set prevents infinite
   loops; a 256-node budget bounds runtime; stdlib/site-packages calls are
   not descended into.
6. **No memory addresses in hashes** — non-primitive values are summarized by
   ``type.__qualname__`` only, never via ``repr()`` (which leaks ``0x...`` ids).
"""

from __future__ import annotations

import ast
import hashlib
import inspect
import json
import logging
import os
import sys
import sysconfig
import textwrap
import types
import weakref
from typing import Any, Callable, Iterable

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------------

_MAX_VALUE_DEPTH = 6
_MAX_LIST_ITEMS = 16
_MAX_DICT_ITEMS = 24
_MAX_STR_LEN = 220
_CALLEE_BUDGET = 256

# Functions that make code provably dynamic — cache must err on volatile.
_STRICT_VOLATILE_BUILTINS = frozenset({
    "eval", "exec", "compile", "__import__",
    "globals", "locals", "vars",
})

# Conditionally volatile: SAFE with a string-literal name, volatile otherwise.
_LITERAL_AWARE_BUILTINS = frozenset({"getattr", "setattr", "hasattr", "delattr"})

# Builtins whose *call* is irrelevant for hashing (no recursion target either).
_BENIGN_BUILTINS = frozenset({
    "print", "len", "range", "enumerate", "zip", "map", "filter",
    "sorted", "reversed", "sum", "min", "max", "abs", "round",
    "list", "dict", "set", "tuple", "frozenset", "bool", "int",
    "float", "str", "bytes", "bytearray", "type", "isinstance",
    "issubclass", "id", "hash", "iter", "next", "all", "any",
    "repr", "format", "open",
})

# AST fields that encode source position only — strip before hashing.
_AST_FIELDS_IGNORE = frozenset({
    "lineno", "col_offset", "end_lineno", "end_col_offset",
})

# Module-level memoization. Keyed by id(unwrapped_func). Cleared at af.init().
_FUNCTION_HASH_CACHE: dict[int, str] = {}
_FUNCTION_SNAPSHOT_CACHE: weakref.WeakValueDictionary[int, dict] = weakref.WeakValueDictionary()
_AST_TREE_CACHE: weakref.WeakValueDictionary[int, ast.AST] = weakref.WeakValueDictionary()


def clear_oracle_cache() -> None:
    """Drop all per-run memoization. Called from ``decorators.clear_logic_snapshot_cache``."""
    _FUNCTION_HASH_CACHE.clear()
    _FUNCTION_SNAPSHOT_CACHE.clear()
    _AST_TREE_CACHE.clear()


# ---------------------------------------------------------------------------
# User-space detection
# ---------------------------------------------------------------------------

def _stdlib_paths() -> tuple[str, ...]:
    paths: list[str] = []
    try:
        paths.append(os.path.realpath(sysconfig.get_paths()["stdlib"]))
    except Exception:
        pass
    try:
        paths.append(os.path.realpath(sysconfig.get_paths()["platstdlib"]))
    except Exception:
        pass
    for entry in sys.path:
        if not entry:
            continue
        real = os.path.realpath(entry)
        if "site-packages" in real or "dist-packages" in real:
            paths.append(real)
    return tuple(p for p in paths if p)


_STDLIB_PATHS: tuple[str, ...] = _stdlib_paths()


def _is_user_space_callable(value: Any) -> bool:
    """True iff value is a callable whose source lives in the project (not stdlib/site-packages)."""
    try:
        code = getattr(value, "__code__", None)
        if not isinstance(code, types.CodeType):
            return False
        filename = os.path.realpath(code.co_filename)
        if not filename or filename.startswith("<"):
            return False
        for stdlib_path in _STDLIB_PATHS:
            if filename.startswith(stdlib_path):
                return False
        return filename.startswith(os.path.realpath(os.getcwd()))
    except Exception:
        return False


def _unwrap_descriptor(value: Any) -> Any:
    """Unwrap classmethod/staticmethod/property to the raw function."""
    if isinstance(value, (classmethod, staticmethod)):
        return value.__func__
    if isinstance(value, property):
        return value.fget
    return value


def _fully_unwrap(func: Any) -> Any:
    """``inspect.unwrap`` + descriptor unwrap. Idempotent."""
    try:
        unwrapped = inspect.unwrap(func)
    except Exception:
        unwrapped = func
    return _unwrap_descriptor(unwrapped)


# ---------------------------------------------------------------------------
# Source acquisition
# ---------------------------------------------------------------------------

def _get_function_source(func: Any) -> str | None:
    try:
        raw = inspect.getsource(func)
        if raw and raw.strip():
            return textwrap.dedent(raw)
    except (OSError, TypeError):
        pass
    return None


def _parse_function_ast(func: Any) -> ast.AST | None:
    fid = id(func)
    cached = _AST_TREE_CACHE.get(fid)
    if cached is not None:
        return cached

    src = _get_function_source(func)
    if src is None:
        return None
    try:
        module = ast.parse(src)
    except SyntaxError:
        return None

    # The source we got back may be a single function def → grab the function node.
    fn_node: ast.AST | None = None
    for top in module.body:
        if isinstance(top, (ast.FunctionDef, ast.AsyncFunctionDef)):
            fn_node = top
            break
    if fn_node is None:
        # Could be a method whose source returned just the def; fall back to module.
        fn_node = module

    try:
        _AST_TREE_CACHE[fid] = fn_node  # type: ignore[assignment]
    except TypeError:
        pass
    return fn_node


# ---------------------------------------------------------------------------
# Scope analysis (shadowing detection)
# ---------------------------------------------------------------------------

class _ScopeAnalyzer(ast.NodeVisitor):
    """Collect names that are local to the target function's top-level scope.

    Critical: we do NOT recurse into nested ``FunctionDef`` / ``AsyncFunctionDef``
    / ``Lambda`` — they have their own scopes and their internal ``Name(Load)``
    references are not parent globals.
    """

    def __init__(self) -> None:
        self.locals: set[str] = set()
        self.globals_declared: set[str] = set()
        self.nonlocals_declared: set[str] = set()

    # — opaque scopes
    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.locals.add(node.name)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.locals.add(node.name)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.locals.add(node.name)

    def visit_Lambda(self, node: ast.Lambda) -> None:  # noqa: ARG002
        return

    # — scope declarations
    def visit_Global(self, node: ast.Global) -> None:
        self.globals_declared.update(node.names)

    def visit_Nonlocal(self, node: ast.Nonlocal) -> None:
        self.nonlocals_declared.update(node.names)

    # — bindings
    def visit_arg(self, node: ast.arg) -> None:
        self.locals.add(node.arg)

    def visit_Assign(self, node: ast.Assign) -> None:
        for tgt in node.targets:
            self._bind(tgt)
        self.generic_visit(node)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:
        self._bind(node.target)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        self._bind(node.target)
        self.generic_visit(node)

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
        self._bind(node.target)
        self.generic_visit(node)

    def visit_For(self, node: ast.For) -> None:
        self._bind(node.target)
        self.generic_visit(node)

    def visit_AsyncFor(self, node: ast.AsyncFor) -> None:
        self._bind(node.target)
        self.generic_visit(node)

    def visit_With(self, node: ast.With) -> None:
        for item in node.items:
            if item.optional_vars is not None:
                self._bind(item.optional_vars)
        self.generic_visit(node)

    def visit_AsyncWith(self, node: ast.AsyncWith) -> None:
        for item in node.items:
            if item.optional_vars is not None:
                self._bind(item.optional_vars)
        self.generic_visit(node)

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        if node.name:
            self.locals.add(node.name)
        self.generic_visit(node)

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self.locals.add(alias.asname or alias.name.split(".")[0])

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        for alias in node.names:
            self.locals.add(alias.asname or alias.name)

    def visit_comprehension(self, node: ast.comprehension) -> None:
        self._bind(node.target)
        self.generic_visit(node)

    def _bind(self, target: ast.AST) -> None:
        if isinstance(target, ast.Name):
            self.locals.add(target.id)
        elif isinstance(target, (ast.Tuple, ast.List)):
            for el in target.elts:
                self._bind(el)
        elif isinstance(target, ast.Starred):
            self._bind(target.value)


def _analyze_scope(fn_node: ast.AST) -> _ScopeAnalyzer:
    analyzer = _ScopeAnalyzer()
    body: Iterable[ast.AST]
    if isinstance(fn_node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        # Args first (so they're in locals before body bindings are evaluated).
        for arg_grp in (fn_node.args.posonlyargs, fn_node.args.args, fn_node.args.kwonlyargs):
            for a in arg_grp:
                analyzer.locals.add(a.arg)
        if fn_node.args.vararg is not None:
            analyzer.locals.add(fn_node.args.vararg.arg)
        if fn_node.args.kwarg is not None:
            analyzer.locals.add(fn_node.args.kwarg.arg)
        body = fn_node.body
    else:
        body = getattr(fn_node, "body", [])
    for stmt in body:
        analyzer.visit(stmt)
    # globals/nonlocals override locals
    analyzer.locals -= analyzer.globals_declared
    analyzer.locals -= analyzer.nonlocals_declared
    return analyzer


# ---------------------------------------------------------------------------
# Dependency scavenger
# ---------------------------------------------------------------------------

class _ASTScavenger(ast.NodeVisitor):
    """Walk the function body and harvest dependencies.

    Outputs:
      - ``global_reads``      : names referenced in Load context not shadowed locally
      - ``attribute_chains``  : ``[(root, [attr1, attr2, ...]), ...]`` for ``a.b.c``
      - ``call_targets``      : raw AST nodes used as call targets, for callee resolution
      - ``volatile_markers``  : sorted list of deterministic volatility tags
    """

    def __init__(self, locals_set: set[str]) -> None:
        self.locals = locals_set
        self.global_reads: set[str] = set()
        self.attribute_chains: list[tuple[str, tuple[str, ...]]] = []
        self.call_targets: list[ast.AST] = []
        self._markers: set[str] = set()

    # Don't descend into nested function definitions: their scopes are independent.
    # Their structure is still hashed because the scavenger sees them via
    # ``_canonical_ast_node`` over the whole tree, which DOES descend.
    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: ARG002
        return

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:  # noqa: ARG002
        return

    def visit_Lambda(self, node: ast.Lambda) -> None:  # noqa: ARG002
        return

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: ARG002
        return

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Load) and node.id not in self.locals:
            self.global_reads.add(node.id)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        chain: list[str] = []
        cur: ast.AST = node
        while isinstance(cur, ast.Attribute):
            chain.append(cur.attr)
            cur = cur.value
        chain.reverse()
        if isinstance(cur, ast.Name):
            root = cur.id
            if root in self.locals:
                # local-rooted chains (e.g. self.config.PROMPT) are recorded under a
                # marker namespace so MRO/closure resolution can pick them up.
                self.attribute_chains.append((f"__local__:{root}", tuple(chain)))
            else:
                self.attribute_chains.append((root, tuple(chain)))
                self.global_reads.add(root)
        # Recurse into the value side so calls/attrs nested deeper are seen.
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        target = node.func
        # Volatility detection on the call site.
        if isinstance(target, ast.Name):
            name = target.id
            if name in _STRICT_VOLATILE_BUILTINS and name not in self.locals:
                self._markers.add(f"<volatile:{name}>")
            elif name in _LITERAL_AWARE_BUILTINS and name not in self.locals:
                if not self._is_literal_string_arg(node, 1):
                    self._markers.add(f"<volatile:{name}_dyn>")
                # else: SAFE — semantically equivalent to attribute access
        # Always record the call target for callee resolution downstream.
        self.call_targets.append(target)
        self.generic_visit(node)

    @staticmethod
    def _is_literal_string_arg(call: ast.Call, idx: int) -> bool:
        if idx >= len(call.args):
            return False
        arg = call.args[idx]
        return isinstance(arg, ast.Constant) and isinstance(arg.value, str)

    @property
    def volatile_markers(self) -> list[str]:
        return sorted(self._markers)


# ---------------------------------------------------------------------------
# Canonical AST hashing (cross-version stable structure hash)
# ---------------------------------------------------------------------------

def _canonical_ast_node(node: Any) -> Any:
    if isinstance(node, ast.AST):
        cls = type(node).__name__
        out: list[Any] = [cls]
        for fname in node._fields:
            if fname in _AST_FIELDS_IGNORE:
                continue
            try:
                fval = getattr(node, fname)
            except AttributeError:
                continue
            out.append([fname, _canonical_ast_node(fval)])
        return out
    if isinstance(node, list):
        return ["__list__", [_canonical_ast_node(x) for x in node]]
    if isinstance(node, tuple):
        return ["__tuple__", [_canonical_ast_node(x) for x in node]]
    if node is None or isinstance(node, bool) or isinstance(node, (int, float, str)):
        return ["__v__", node]
    if isinstance(node, bytes):
        return ["__b__", node.hex()]
    if isinstance(node, complex):
        return ["__c__", [node.real, node.imag]]
    if node is Ellipsis:
        return ["__ellipsis__"]
    return ["__opaque__", type(node).__qualname__]


def _hash_canonical_ast(fn_node: ast.AST) -> str:
    canonical = _canonical_ast_node(fn_node)
    payload = json.dumps(canonical, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


# ---------------------------------------------------------------------------
# Deep value snapshot — addresses-free, deterministic
# ---------------------------------------------------------------------------

def _deep_value_snapshot(value: Any, *, depth: int = 0) -> Any:
    if depth > _MAX_VALUE_DEPTH:
        return {"__truncated__": "max_depth"}
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        # JSON can't encode NaN/Inf safely → tag.
        if value != value or value in (float("inf"), float("-inf")):
            return {"__float__": repr(value)}
        return value
    if isinstance(value, str):
        if len(value) <= _MAX_STR_LEN:
            return value
        digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]
        return {"__str_blob__": digest, "len": len(value)}
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        digest = hashlib.sha256(raw).hexdigest()[:12]
        return {"__bytes_blob__": digest, "len": len(raw)}
    if isinstance(value, (list, tuple)):
        return [_deep_value_snapshot(v, depth=depth + 1) for v in list(value)[:_MAX_LIST_ITEMS]]
    if isinstance(value, (set, frozenset)):
        try:
            ordered = sorted(value, key=lambda x: repr(x))
        except Exception:
            ordered = list(value)
        return {"__set__": [_deep_value_snapshot(v, depth=depth + 1) for v in ordered[:_MAX_LIST_ITEMS]]}
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        try:
            items = sorted(value.items(), key=lambda kv: str(kv[0]))
        except Exception:
            items = list(value.items())
        for i, (k, v) in enumerate(items):
            if i >= _MAX_DICT_ITEMS:
                break
            out[str(k)] = _deep_value_snapshot(v, depth=depth + 1)
        return out
    if isinstance(value, types.ModuleType):
        return {"__module__": getattr(value, "__name__", "<unknown>")}
    if isinstance(value, type):
        return {"__class_ref__": f"{getattr(value, '__module__', '?')}.{getattr(value, '__qualname__', value.__name__)}"}
    if callable(value):
        # Functions used as defaults/closure values: identify by qualname only.
        return {
            "__callable__": f"{getattr(value, '__module__', '?')}.{getattr(value, '__qualname__', '?')}"
        }
    # Fallback: type identity ONLY. Never repr() (repr can leak 0x... addresses).
    cls = type(value)
    return {"__type__": f"{getattr(cls, '__module__', '?')}.{getattr(cls, '__qualname__', cls.__name__)}"}


# ---------------------------------------------------------------------------
# Callee resolution
# ---------------------------------------------------------------------------

def _resolve_class_for_method(func: Any) -> type | None:
    """Recover the class that defines ``func`` from ``__qualname__`` + globals."""
    qn = getattr(func, "__qualname__", "")
    if not qn or "." not in qn:
        return None
    # Strip the trailing method name; what's left may include nested class names
    # and the ``<locals>`` marker for closures defined inside other functions.
    parts = qn.rsplit(".", 1)[0].split(".")
    cleaned = [p for p in parts if p != "<locals>"]
    if not cleaned:
        return None
    g = getattr(func, "__globals__", {}) or {}
    cur: Any = g.get(cleaned[0])
    for part in cleaned[1:]:
        if cur is None:
            return None
        cur = getattr(cur, part, None)
    return cur if isinstance(cur, type) else None


def _resolve_via_mro(cls: type, name: str) -> Any:
    for klass in cls.__mro__:
        if name in klass.__dict__:
            return _unwrap_descriptor(klass.__dict__[name])
    return None


def _resolve_call_target(
    target: ast.AST,
    func: Any,
    g: dict[str, Any],
    bound_class: type | None,
) -> Any:
    if isinstance(target, ast.Name):
        return g.get(target.id)
    if isinstance(target, ast.Attribute):
        chain: list[str] = []
        cur: ast.AST = target
        while isinstance(cur, ast.Attribute):
            chain.append(cur.attr)
            cur = cur.value
        chain.reverse()
        if not isinstance(cur, ast.Name):
            return None
        root = cur.id
        if root in ("self", "cls") and bound_class is not None:
            method = _resolve_via_mro(bound_class, chain[0])
            for nxt in chain[1:]:
                if method is None:
                    return None
                method = getattr(method, nxt, None)
            return method
        base = g.get(root)
        for attr in chain:
            if base is None:
                return None
            base = getattr(base, attr, None)
        return base
    return None


# ---------------------------------------------------------------------------
# Globals / closures / defaults / annotations capture
# ---------------------------------------------------------------------------

def _resolve_globals_value(name: str, g: dict[str, Any]) -> Any:
    return g.get(name)


def _resolve_attribute_chain(root_value: Any, attrs: tuple[str, ...]) -> tuple[bool, Any]:
    cur = root_value
    for a in attrs:
        if cur is None:
            return False, None
        try:
            cur = getattr(cur, a)
        except Exception:
            return False, None
    return True, cur


def _capture_resolved_globals(
    scavenger: _ASTScavenger,
    g: dict[str, Any],
) -> dict[str, Any]:
    """Snapshot the *runtime values* of names the function actually reads."""
    out: dict[str, Any] = {}

    # Direct reads: foo, CONFIG, prompts (module reference itself).
    for name in sorted(scavenger.global_reads):
        if name not in g:
            continue
        val = g[name]
        if isinstance(val, types.ModuleType):
            # Modules are recorded only structurally; their attrs come via chains.
            out[name] = {"__module__": getattr(val, "__name__", "<unknown>")}
            continue
        if callable(val) and not isinstance(val, type):
            out[name] = {
                "__callable__": f"{getattr(val, '__module__', '?')}.{getattr(val, '__qualname__', '?')}"
            }
            continue
        out[name] = _deep_value_snapshot(val)

    # Attribute chains: prompts.MY_PROMPT, config.settings.X.
    for root, attrs in scavenger.attribute_chains:
        if root.startswith("__local__:") or not attrs:
            continue
        if root not in g:
            continue
        ok, value = _resolve_attribute_chain(g[root], attrs)
        if not ok:
            continue
        # Skip callables here — they go through the callee resolver.
        if callable(value) and not isinstance(value, type):
            continue
        if isinstance(value, types.ModuleType):
            continue
        key = f"{root}.{'.'.join(attrs)}"
        out[key] = _deep_value_snapshot(value)
    return out


def _capture_closures(func: Any) -> dict[str, Any]:
    code = getattr(func, "__code__", None)
    cells = getattr(func, "__closure__", None)
    if not isinstance(code, types.CodeType) or not cells:
        return {}
    out: dict[str, Any] = {}
    for name, cell in zip(code.co_freevars, cells):
        try:
            content = cell.cell_contents
        except ValueError:
            out[name] = {"__cell__": "empty"}
            continue
        out[name] = _deep_value_snapshot(content)
    return out


def _capture_defaults(func: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    defaults = getattr(func, "__defaults__", None) or ()
    out["positional"] = [_deep_value_snapshot(d) for d in defaults]
    kwdefaults = getattr(func, "__kwdefaults__", None) or {}
    out["keyword"] = {
        k: _deep_value_snapshot(v)
        for k, v in sorted(kwdefaults.items(), key=lambda kv: kv[0])
    }
    return out


def _capture_annotations(func: Any) -> dict[str, str]:
    """Stringify annotations defensively — never trigger forward-ref resolution."""
    raw = getattr(func, "__annotations__", None) or {}
    out: dict[str, str] = {}
    for k, v in sorted(raw.items(), key=lambda kv: kv[0]):
        try:
            if isinstance(v, str):
                out[k] = v
            elif isinstance(v, type):
                out[k] = f"{v.__module__}.{v.__qualname__}"
            else:
                out[k] = repr(v)
        except Exception:
            out[k] = "<unrepresentable>"
    return out


# ---------------------------------------------------------------------------
# Recursive Merkle aggregation
# ---------------------------------------------------------------------------

def _build_function_snapshot(
    func: Any,
    *,
    visited: set[int],
    registry_out: dict[str, Any],
    budget: list[int],
) -> tuple[dict[str, Any], str]:
    """Compute snapshot + hash for one function, recursing into user-space callees.

    Cycle-safe: uses ``visited`` (set of id()) to detect already-in-progress nodes.
    Budget-bounded: ``budget[0]`` decremented per node; below zero short-circuits.
    Memoized: hits ``_FUNCTION_HASH_CACHE`` keyed by ``id(unwrapped)``.
    """
    unwrapped = _fully_unwrap(func)
    fid = id(unwrapped)

    cached_hash = _FUNCTION_HASH_CACHE.get(fid)
    cached_snap = _FUNCTION_SNAPSHOT_CACHE.get(fid)
    if cached_hash is not None and cached_snap is not None:
        return cached_snap, cached_hash

    if fid in visited:
        # Cycle → use qualname-only stub.
        qn = getattr(unwrapped, "__qualname__", "<unknown>")
        stub = {"name": qn, "__cycle__": True}
        return stub, hashlib.sha256(qn.encode("utf-8")).hexdigest()

    if budget[0] <= 0:
        qn = getattr(unwrapped, "__qualname__", "<unknown>")
        log.warning(
            "[Logram][oracle] callee budget exhausted at %s (limit=%d). "
            "Deeper callees will not be hashed individually; their changes will "
            "still be detected through source-level edits to ancestors but not "
            "through transitive constant mutations. Increase oracle._CALLEE_BUDGET "
            "if this is a real production graph.",
            qn,
            _CALLEE_BUDGET,
        )
        stub = {"name": qn, "__budget_exceeded__": True}
        return stub, hashlib.sha256(("BUDGET:" + qn).encode("utf-8")).hexdigest()
    budget[0] -= 1

    visited.add(fid)
    try:
        snapshot = _compute_single_function_snapshot(unwrapped)
        # Recurse into callees discovered by the scavenger.
        children: dict[str, str] = {}
        callee_funcs: list[tuple[str, Any]] = snapshot.pop("__callees__", [])
        for qname, callee in callee_funcs:
            if not _is_user_space_callable(callee):
                continue
            child_snap, child_hash = _build_function_snapshot(
                callee, visited=visited, registry_out=registry_out, budget=budget
            )
            short = child_hash[:12]
            children[qname] = short
            registry_out[short] = child_snap
        snapshot["called_functions"] = children

        h = hashlib.sha256(
            json.dumps(snapshot, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()

        _FUNCTION_HASH_CACHE[fid] = h
        try:
            _FUNCTION_SNAPSHOT_CACHE[fid] = snapshot
        except TypeError:
            pass
        return snapshot, h
    finally:
        visited.discard(fid)


def _compute_single_function_snapshot(func: Any) -> dict[str, Any]:
    """Build a snapshot for one function (no recursion). Stores callees for later."""
    qualname = getattr(func, "__qualname__", getattr(func, "__name__", "<unknown>"))

    fn_node = _parse_function_ast(func)
    if fn_node is None:
        # Source unavailable — snapshot must still be deterministic.
        return {
            "name": qualname,
            "structural_hash": hashlib.sha256(("SOURCE_UNAVAILABLE:" + qualname).encode("utf-8")).hexdigest(),
            "source_normalized": "",
            "signature": _safe_signature(func),
            "resolved_globals": {},
            "defaults": _capture_defaults(func),
            "closures": _capture_closures(func),
            "annotations": _capture_annotations(func),
            "volatile_markers": ["<volatile:source_unavailable>"],
            "__callees__": [],
        }

    structural_hash = _hash_canonical_ast(fn_node)

    # ``source_normalized`` is included for human-readable persistence in the
    # storage layer (logic_registry table → MCP server display). It is the
    # output of ``ast.unparse``, whose formatting can drift slightly across
    # Python minor versions — so adding it to the snapshot re-introduces a
    # *minor* cross-version coupling on top of the strictly-stable
    # ``structural_hash``. The trade-off is intentional: registry rows must
    # show real source code, and a Python upgrade is a legitimate moment to
    # rebuild the cache via ``clear_logic_snapshot_cache()``.
    try:
        source_normalized = ast.unparse(fn_node)
    except Exception:
        source_normalized = ""

    scope = _analyze_scope(fn_node)
    scavenger = _ASTScavenger(scope.locals)
    if isinstance(fn_node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        for stmt in fn_node.body:
            scavenger.visit(stmt)
        # Decorators participate in semantics — visit them too.
        for deco in fn_node.decorator_list:
            scavenger.visit(deco)
    else:
        scavenger.visit(fn_node)

    g = getattr(func, "__globals__", {}) or {}
    resolved_globals = _capture_resolved_globals(scavenger, g)
    closures = _capture_closures(func)
    defaults = _capture_defaults(func)
    annotations = _capture_annotations(func)

    bound_cls = _resolve_class_for_method(func)
    callees: list[tuple[str, Any]] = []
    seen_qns: set[str] = set()
    for target in scavenger.call_targets:
        # Skip benign builtins early.
        if isinstance(target, ast.Name) and target.id in _BENIGN_BUILTINS and target.id not in scope.locals:
            continue
        if isinstance(target, ast.Name) and (
            target.id in _STRICT_VOLATILE_BUILTINS or target.id in _LITERAL_AWARE_BUILTINS
        ):
            # Already accounted for via volatile markers.
            continue
        callee = _resolve_call_target(target, func, g, bound_cls)
        if callee is None:
            continue
        callee = _fully_unwrap(callee)
        if not callable(callee):
            continue
        if not _is_user_space_callable(callee):
            continue
        qn = getattr(callee, "__qualname__", None) or getattr(callee, "__name__", None)
        if not qn or qn in seen_qns:
            continue
        seen_qns.add(qn)
        callees.append((qn, callee))

    return {
        "name": qualname,
        "structural_hash": structural_hash,
        "source_normalized": source_normalized,
        "signature": _safe_signature(func),
        "resolved_globals": resolved_globals,
        "defaults": defaults,
        "closures": closures,
        "annotations": annotations,
        "volatile_markers": scavenger.volatile_markers,
        "__callees__": callees,  # popped by caller before final hash
    }


def _safe_signature(func: Any) -> str:
    try:
        return str(inspect.signature(func))
    except (TypeError, ValueError):
        return "()"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_logic_fingerprint(func: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build a deterministic snapshot + callee registry for ``func``.

    Returns ``(snapshot, registry)``. The caller computes the final fingerprint by
    JSON-hashing ``snapshot`` (callee Merkle root is already embedded as
    ``called_functions``). The registry maps short callee hashes to their full
    sub-snapshots — used by storage to persist the dependency graph.
    """
    try:
        registry: dict[str, Any] = {}
        budget = [_CALLEE_BUDGET]
        snapshot, _root_hash = _build_function_snapshot(
            func, visited=set(), registry_out=registry, budget=budget,
        )
        return snapshot, registry
    except Exception as exc:
        log.warning(
            "[Logram][oracle] fingerprint failed for %s: %s",
            getattr(func, "__qualname__", getattr(func, "__name__", "<unknown>")),
            exc,
        )
        return {
            "name": getattr(func, "__qualname__", getattr(func, "__name__", "<unknown>")),
            "structural_hash": hashlib.sha256(
                f"ORACLE_ERROR:{exc!r}".encode("utf-8", errors="replace")
            ).hexdigest(),
            "signature": _safe_signature(func),
            "resolved_globals": {},
            "defaults": {"positional": [], "keyword": {}},
            "closures": {},
            "annotations": {},
            "called_functions": {},
            "volatile_markers": ["<volatile:oracle_error>"],
        }, {}


# ---------------------------------------------------------------------------
# Smoke test (run with: python -m logram.oracle)
# ---------------------------------------------------------------------------

def _smoke_test() -> int:  # pragma: no cover - exercised manually
    """In-process validation that the oracle satisfies its contract."""
    import importlib.util

    failures: list[str] = []

    def _hash(snap: dict[str, Any]) -> str:
        return hashlib.sha256(
            json.dumps(snap, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()

    def _make_module(name: str, source: str) -> types.ModuleType:
        spec = importlib.util.spec_from_loader(name, loader=None)
        mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
        # Source must live on disk for inspect.getsource → temp file.
        import tempfile
        f = tempfile.NamedTemporaryFile(
            "w", suffix=".py", delete=False, dir=os.getcwd(), prefix=f"_oracle_smoke_{name}_"
        )
        f.write(source)
        f.flush()
        f.close()
        mod.__file__ = f.name
        spec = importlib.util.spec_from_file_location(name, f.name)
        mod = importlib.util.module_from_spec(spec)  # type: ignore[assignment, arg-type]
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        return mod

    # --- Test 1: comments / whitespace are invisible to the hash -------------
    src_a = "def f(x):\n    # this is a comment\n    return x + 1\n"
    src_b = "def f(x):\n\n    return    x   +    1\n"
    m_a = _make_module("smoke_t1a", src_a)
    m_b = _make_module("smoke_t1b", src_b)
    clear_oracle_cache()
    snap_a, _ = compute_logic_fingerprint(m_a.f)
    clear_oracle_cache()
    snap_b, _ = compute_logic_fingerprint(m_b.f)
    if _hash(snap_a) != _hash(snap_b):
        failures.append(
            f"T1 comments/whitespace invariance: {_hash(snap_a)} != {_hash(snap_b)}"
        )

    # --- Test 2: mutating a global dict between runs changes the hash --------
    src_c = "CONFIG = {'temp': 0.7}\ndef g():\n    return CONFIG['temp']\n"
    m_c = _make_module("smoke_t2", src_c)
    clear_oracle_cache()
    snap_c1, _ = compute_logic_fingerprint(m_c.g)
    m_c.CONFIG["temp"] = 0.9
    clear_oracle_cache()
    snap_c2, _ = compute_logic_fingerprint(m_c.g)
    if _hash(snap_c1) == _hash(snap_c2):
        failures.append("T2 dict mutation: hash did not change after CONFIG['temp']=0.9")

    # --- Test 3: transitive sub-function change invalidates the parent ------
    src_d1 = (
        "def helper(x):\n    return x * 2\n"
        "def parent(x):\n    return helper(x) + 1\n"
    )
    src_d2 = (
        "def helper(x):\n    return x * 3\n"  # body changed
        "def parent(x):\n    return helper(x) + 1\n"
    )
    m_d1 = _make_module("smoke_t3a", src_d1)
    m_d2 = _make_module("smoke_t3b", src_d2)
    clear_oracle_cache()
    snap_p1, _ = compute_logic_fingerprint(m_d1.parent)
    clear_oracle_cache()
    snap_p2, _ = compute_logic_fingerprint(m_d2.parent)
    if _hash(snap_p1) == _hash(snap_p2):
        failures.append("T3 transitive callee change: parent hash did not change")

    # --- Test 4: getattr literal == SAFE; getattr dynamic == volatile -------
    src_e = (
        "def lit(o):\n    return getattr(o, 'name')\n"
        "def dyn(o, k):\n    return getattr(o, k)\n"
    )
    m_e = _make_module("smoke_t4", src_e)
    clear_oracle_cache()
    snap_lit, _ = compute_logic_fingerprint(m_e.lit)
    clear_oracle_cache()
    snap_dyn, _ = compute_logic_fingerprint(m_e.dyn)
    if snap_lit.get("volatile_markers"):
        failures.append(f"T4 literal getattr should be SAFE, got {snap_lit['volatile_markers']}")
    if "<volatile:getattr_dyn>" not in (snap_dyn.get("volatile_markers") or []):
        failures.append(f"T4 dynamic getattr should be volatile, got {snap_dyn.get('volatile_markers')}")

    # --- Test 5: deterministic markers (no time-based nonce) ----------------
    src_f = "def evil(s):\n    return eval(s)\n"
    m_f = _make_module("smoke_t5", src_f)
    clear_oracle_cache()
    snap_f1, _ = compute_logic_fingerprint(m_f.evil)
    clear_oracle_cache()
    snap_f2, _ = compute_logic_fingerprint(m_f.evil)
    if _hash(snap_f1) != _hash(snap_f2):
        failures.append("T5 deterministic volatility: same code → same hash even with eval()")
    if "<volatile:eval>" not in (snap_f1.get("volatile_markers") or []):
        failures.append("T5 eval should produce <volatile:eval> marker")

    # --- Test 6: self.method() is resolved via MRO --------------------------
    src_g1 = (
        "class A:\n"
        "    def helper(self, x):\n        return x * 2\n"
        "    def parent(self, x):\n        return self.helper(x) + 1\n"
    )
    src_g2 = (
        "class A:\n"
        "    def helper(self, x):\n        return x * 3\n"  # changed
        "    def parent(self, x):\n        return self.helper(x) + 1\n"
    )
    m_g1 = _make_module("smoke_t6a", src_g1)
    m_g2 = _make_module("smoke_t6b", src_g2)
    clear_oracle_cache()
    snap_a1, _ = compute_logic_fingerprint(m_g1.A.parent)
    clear_oracle_cache()
    snap_a2, _ = compute_logic_fingerprint(m_g2.A.parent)
    if _hash(snap_a1) == _hash(snap_a2):
        failures.append("T6 MRO method resolution: parent hash did not pick up helper change")
    if "A.helper" not in (snap_a1.get("called_functions") or {}):
        failures.append(f"T6 MRO: A.helper missing from called_functions; got {snap_a1.get('called_functions')}")

    # --- Test 7: cycle safety (mutual recursion does not hang) --------------
    src_h = (
        "def a(n):\n    return b(n - 1) if n > 0 else 0\n"
        "def b(n):\n    return a(n - 1) if n > 0 else 0\n"
    )
    m_h = _make_module("smoke_t7", src_h)
    clear_oracle_cache()
    snap_a, reg = compute_logic_fingerprint(m_h.a)
    if "called_functions" not in snap_a:
        failures.append("T7 cycle safety: snapshot missing called_functions")

    # --- Test 8: shadowing — argument named like a builtin doesn't poison hash
    src_i = (
        "def takes_eval(eval, x):\n    return eval(x)\n"  # `eval` is a parameter!
    )
    m_i = _make_module("smoke_t8", src_i)
    clear_oracle_cache()
    snap_i, _ = compute_logic_fingerprint(m_i.takes_eval)
    if snap_i.get("volatile_markers"):
        failures.append(
            f"T8 shadowing: shadowed `eval` should not produce volatility, got {snap_i['volatile_markers']}"
        )

    # --- Cleanup tempfiles ---------------------------------------------------
    for mod in (m_a, m_b, m_c, m_d1, m_d2, m_e, m_f, m_g1, m_g2, m_h, m_i):
        try:
            os.unlink(mod.__file__)
        except Exception:
            pass

    if failures:
        print("[oracle smoke] FAILURES:")
        for f in failures:
            print("  -", f)
        return 1
    print(f"[oracle smoke] all {8} tests passed")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(_smoke_test())
