# ruff: noqa: BLE001
# pylint: disable=broad-exception-caught

from __future__ import annotations

import functools
import hashlib
import inspect
import json
import linecache
import logging
import os
import time
import traceback
import types
import uuid
import weakref
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, is_dataclass
from dataclasses import fields as dataclass_fields
from typing import Any, Callable, Union, get_args, get_origin, get_type_hints

from .context import _is_forced_by_flow, current_input_id, current_run_id, current_step_id
from .oracle import clear_oracle_cache, compute_logic_fingerprint, snapshot_digest
from .serializer import BlobManager, ensure_serializable, rehydrate_logram_output
from .storage import _VCR_MISS, TraceStorage

log = logging.getLogger(__name__)

storage = TraceStorage()
blobs = BlobManager()

_LOGIC_SNAPSHOT_CACHE: weakref.WeakKeyDictionary[Any, tuple[dict[str, Any], str, dict[str, Any]]] = weakref.WeakKeyDictionary()

_MAX_STR_LEN = 220
_MAX_KEY_DEPTH = 64
_STATEFUL_CONFIG_ATTR = "__af_state_config__"


def _diag_preview(value: Any, max_len: int = 700) -> str:
    """Compact debug preview that never raises and avoids huge logs."""
    try:
        compact = _compact_value(value)
        safe = ensure_serializable(compact)
        rendered = json.dumps(safe, ensure_ascii=False, sort_keys=True)
    except Exception:
        try:
            rendered = repr(value)
        except Exception:
            rendered = "<unrepresentable>"
    if len(rendered) > max_len:
        return f"{rendered[:max_len]}...<truncated:{len(rendered) - max_len}>"
    return rendered


def _extract_usage_tokens(result: Any) -> tuple[int | None, int | None]:
    """Extract (prompt_tokens, completion_tokens) from common LLM response shapes.

    Supported shapes:
    - OpenAI-like: result.usage.prompt_tokens / completion_tokens
    - Gemini-like: result.usage_metadata.prompt_token_count / candidates_token_count
    - dict variants with keys above
    - Anthropic-like fallbacks: input_tokens / output_tokens
    """

    def _as_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            out = int(value)
        except (TypeError, ValueError):
            return None
        return out if out >= 0 else None

    def _get(node: Any, key: str) -> Any:
        if node is None:
            return None
        if isinstance(node, dict):
            return node.get(key)
        return getattr(node, key, None)

    usage = _get(result, "usage")
    usage_metadata = _get(result, "usage_metadata")
    usage_obj = usage_metadata if usage_metadata is not None else usage

    prompt_tokens = _as_int(
        _get(usage_obj, "prompt_tokens")
        or _get(usage_obj, "prompt_token_count")
        or _get(usage_obj, "input_tokens")
        or _get(result, "prompt_tokens")
        or _get(result, "prompt_token_count")
    )

    completion_tokens = _as_int(
        _get(usage_obj, "completion_tokens")
        or _get(usage_obj, "candidates_token_count")
        or _get(usage_obj, "output_tokens")
        or _get(result, "completion_tokens")
        or _get(result, "candidates_token_count")
    )

    # Last-resort derivation if only total tokens is available.
    total_tokens = _as_int(
        _get(usage_obj, "total_tokens")
        or _get(usage_obj, "total_token_count")
        or _get(result, "total_tokens")
        or _get(result, "total_token_count")
    )
    if completion_tokens is None and total_tokens is not None and prompt_tokens is not None:
        completion_tokens = max(0, total_tokens - prompt_tokens)

    return prompt_tokens, completion_tokens


def _sha12_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:12]


def _has_obj_hook(obj: Any, attr: str) -> bool:
    return hasattr(obj, attr) and callable(getattr(obj, attr))


def _safe_call_hook(obj: Any, attr: str) -> Any:
    try:
        return getattr(obj, attr)()
    except Exception as e:
        log.debug("[Logram] hook %s failed on %s: %s", attr, type(obj).__name__, e)
        return None


def _compact_value(value: Any, depth: int = 0, _seen: set[int] | None = None) -> Any:
    """Content-based identity of ``value`` for the cache key.

    Every element of every container is included: two arguments that differ
    anywhere must produce different keys. Only long strings and binary
    payloads are replaced by a content hash, which keeps the key small
    without losing information.
    """
    if depth > _MAX_KEY_DEPTH:
        return "<max_depth>"
    if _seen is None:
        _seen = set()

    if value is None or isinstance(value, (bool, int, float)):
        return value

    if isinstance(value, str):
        if len(value) <= _MAX_STR_LEN:
            return value
        return {"__af_str__": True, "len": len(value), "sha12": _sha12_bytes(value.encode("utf-8"))}

    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = bytes(value)
        return {"__af_bytes__": True, "len": len(raw), "sha12": _sha12_bytes(raw)}

    if _has_obj_hook(value, "__logram_trace_key__"):
        out = _safe_call_hook(value, "__logram_trace_key__")
        if out is not None:
            return _compact_value(out, depth + 1, _seen)

    if isinstance(value, dict) and value.get("__af_blob__") is True:
        blob_hash = str(value.get("hash", ""))
        blob_size = value.get("size", 0)
        return {"__af_bytes__": True, "len": blob_size, "sha12": blob_hash[:12]}

    if isinstance(value, (list, tuple, set, frozenset, dict)):
        if id(value) in _seen:
            return "<cycle>"
        _seen.add(id(value))
        try:
            if isinstance(value, dict):
                return {str(k): _compact_value(v, depth + 1, _seen) for k, v in value.items()}
            items = [_compact_value(x, depth + 1, _seen) for x in value]
            if isinstance(value, (set, frozenset)):
                items.sort(key=repr)
            return items
        finally:
            _seen.discard(id(value))

    if is_dataclass(value) and not isinstance(value, type):
        try:
            state = {f.name: getattr(value, f.name) for f in dataclass_fields(value)}
        except Exception:
            state = None
        if state is not None:
            return {"__af_obj__": type(value).__name__, "state": _compact_value(state, depth + 1, _seen)}

    if callable(getattr(value, "model_dump", None)) and not isinstance(value, type):
        state = _safe_call_hook(value, "model_dump")
        if isinstance(state, dict):
            return {"__af_obj__": type(value).__name__, "state": _compact_value(state, depth + 1, _seen)}

    # ── PROBE 2 ── Detect address-based repr → guaranteed VCR cache miss ────────
    try:
        r = repr(value)
    except Exception:
        r = f"<unrepresentable {type(value).__name__}>"
    if " at 0x" in r:
        log.warning(
            "[Logram][PROBE 2][UNSTABLE_REPR] type=%s id=%d repr_preview=%s "
            "— repr() contains a memory address. This arg produces a different "
            "vcr_args string on every run → VCR cache miss GUARANTEED. "
            "Fix: implement __logram_trace_key__ on this class.",
            type(value).__name__,
            id(value),
            r[:160],
        )
    # ── END PROBE 2 ─────────────────────────────────────────────────────────────
    if len(r) > _MAX_STR_LEN:
        return {"__af_obj__": type(value).__name__, "repr_sha12": _sha12_bytes(r.encode("utf-8", errors="replace"))}
    return {"__af_obj__": type(value).__name__, "repr": r}


def _log_view(value: Any) -> Any:
    """What gets stored and displayed for an argument (does not affect the cache key)."""
    for hook in ("__logram_trace_log__", "__logram_trace_key__"):
        if _has_obj_hook(value, hook):
            out = _safe_call_hook(value, hook)
            if out is not None:
                return out
    return value


def _coerce_int_str_dict_keys(value: Any) -> Any:
    """Recursively coerce integer-like string dict keys back to int (reverses JSON round-trip key coercion)."""
    if isinstance(value, dict):
        coerced: dict[Any, Any] = {}
        for k, v in value.items():
            new_k: Any = k
            if isinstance(k, str):
                try:
                    new_k = int(k)
                except (ValueError, TypeError):
                    pass
            coerced[new_k] = _coerce_int_str_dict_keys(v)
        return coerced
    if isinstance(value, list):
        return [_coerce_int_str_dict_keys(x) for x in value]
    return value


def _return_type_hint(func) -> Any:
    try:
        mod = inspect.getmodule(func)
        globalns = vars(mod) if mod else {}
        hints = get_type_hints(func, globalns=globalns)
        return hints.get("return")
    except Exception as e:
        log.debug("[Logram] get_type_hints failed for %s: %s", getattr(func, "__qualname__", func), e)
        return None


def _unwrap_optional(annotation: Any) -> Any:
    if annotation is None:
        return None
    origin = get_origin(annotation)
    args = get_args(annotation)

    if origin is Union:
        non_none = tuple(a for a in args if a is not type(None))
        if len(non_none) == 1:
            return _unwrap_optional(non_none[0])
        return annotation

    if origin is types.UnionType:
        non_none = tuple(a for a in args if a is not type(None))
        if len(non_none) == 1:
            return _unwrap_optional(non_none[0])
    return annotation


def _is_constructible_model(cls: Any) -> bool:
    return isinstance(cls, type) and (hasattr(cls, "model_validate") or hasattr(cls, "parse_obj"))


def _construct_model(model_cls: type, data: Any) -> Any:
    if hasattr(model_cls, "model_validate"):
        return model_cls.model_validate(data)
    return model_cls.parse_obj(data)


def _rehydrate_cached(func, cached_res: Any) -> Any:
    out = rehydrate_logram_output(cached_res)

    rt = _return_type_hint(func)
    if rt is None:
        return out

    rt = _unwrap_optional(rt)
    if rt is None:
        return out

    origin = get_origin(rt)
    type_args = get_args(rt)

    if origin is list and type_args:
        item_t = type_args[0]
        if isinstance(out, list) and _is_constructible_model(item_t):
            try:
                return [_construct_model(item_t, item) for item in out]
            except Exception:
                return out
        return out

    if _is_constructible_model(rt) and isinstance(out, dict):
        try:
            return _construct_model(rt, out)
        except Exception:
            return out

    return out


def _rehydrate_cached_gen(func, cached_res: Any) -> list[Any]:
    """Like _rehydrate_cached but for generator functions.

    Extracts the yield-item type from AsyncIterator[T], AsyncGenerator[T, S],
    Iterator[T], or Generator[T, S, R] and constructs each chunk from the stored
    list using the same Pydantic/dataclass coercion as _rehydrate_cached.
    Falls back to the raw list if the annotation is absent or incompatible.
    """
    import collections.abc as _abc

    out = rehydrate_logram_output(cached_res)
    items: list[Any] = out if isinstance(out, list) else [out]

    rt = _return_type_hint(func)
    if rt is None:
        return items

    rt = _unwrap_optional(rt)
    if rt is None:
        return items

    origin = get_origin(rt)
    type_args = get_args(rt)

    _gen_origins = (
        _abc.AsyncIterator,
        _abc.AsyncGenerator,
        _abc.Iterator,
        _abc.Generator,
    )
    if origin not in _gen_origins or not type_args:
        return items

    # First type arg is always the yield type for all four forms.
    item_t = type_args[0]
    if not _is_constructible_model(item_t):
        return items

    try:
        return [
            _construct_model(item_t, item) if isinstance(item, dict) else item
            for item in items
        ]
    except Exception:
        return items


def _bind_named_arguments(func: Any, args: tuple[Any, ...], kwargs: dict[str, Any], *, drop_self_cls: bool = True) -> dict[str, Any]:
    try:
        bound = inspect.signature(func).bind(*args, **kwargs)
        named = dict(bound.arguments)
    except Exception:
        try:
            bound = inspect.signature(func).bind_partial(*args, **kwargs)
            named = dict(bound.arguments)
        except Exception:
            named = {}

    if drop_self_cls:
        named.pop("self", None)
        named.pop("cls", None)
    return named


def _implementation_artifacts(func: Any) -> tuple[dict[str, Any], str, dict[str, Any]]:
    """Logic snapshot, fingerprint and callee registry of ``func``, memoized per run.

    A step called 150 times (one call per tile) is fingerprinted once. The cache
    is cleared by ``logram.init()`` so source or constant edits between runs are
    picked up.
    """
    unwrapped = inspect.unwrap(func)
    cached = _LOGIC_SNAPSHOT_CACHE.get(unwrapped)
    if cached is not None:
        return cached

    logic_snapshot, callee_registry = compute_logic_fingerprint(unwrapped)
    fingerprint = snapshot_digest(logic_snapshot)

    qualname = getattr(unwrapped, "__qualname__", getattr(unwrapped, "__name__", "unknown"))
    markers = logic_snapshot.get("volatile_markers")
    log.debug(
        "[Logram][PROBE 1][FingerprintComponents] func=%s impl_fingerprint=%s structural_hash=%s "
        "resolved_globals=%s volatile_markers=%s",
        qualname,
        fingerprint,
        str(logic_snapshot.get("structural_hash", ""))[:12],
        sorted(logic_snapshot.get("resolved_globals") or {}),
        markers,
    )
    if markers:
        log.info(
            "[Logram][PROBE 1][VOLATILE_MARKERS] func=%s markers=%s — dynamic constructs "
            "(eval/exec/dynamic getattr…). Markers are deterministic, so the cache still works "
            "as long as the source does not change.",
            qualname,
            markers,
        )

    result = (logic_snapshot, fingerprint, callee_registry)
    try:
        _LOGIC_SNAPSHOT_CACHE[unwrapped] = result
    except TypeError:
        pass  # not weak-referenceable: fingerprint again next call
    return result


def clear_logic_snapshot_cache() -> None:
    linecache.checkcache()  # Refresh on-disk source cache so edits are picked up on next run.
    _LOGIC_SNAPSHOT_CACHE.clear()
    clear_oracle_cache()


def _logical_args_for_vcr(
    func: Any,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    ignore_in_hash: list[str] | None = None,
) -> tuple[Any, Any]:
    named = _bind_named_arguments(func, args, kwargs, drop_self_cls=True)
    ignore_set = {str(x) for x in (ignore_in_hash or []) if str(x)}
    filtered_named = {k: v for k, v in named.items() if k not in ignore_set}
    compact_named = {k: _compact_value(v) for k, v in filtered_named.items()}
    return compact_named, {}


def _default_log_inputs(func: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
    named = _bind_named_arguments(func, args, kwargs, drop_self_cls=True)
    # Keep full inputs for trace serialization so binary payloads (bytes/image bytes)
    # can be intercepted by BlobManager and persisted into .logram_assets.
    return {k: _log_view(v) for k, v in named.items()}


def _prepare_step_ctx(
    func: Any,
    name: str | None,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    vcr_key_fn: Callable[..., Any] | None = None,
    log_input_fn: Callable[..., Any] | None = None,
    ignore_in_hash: list[str] | None = None,
) -> dict[str, Any]:
    vcr_args, vcr_kwargs = _logical_args_for_vcr(func, args, kwargs, ignore_in_hash=ignore_in_hash)
    log_inputs = _default_log_inputs(func, args, kwargs)

    if vcr_key_fn is not None:
        try:
            custom_key = vcr_key_fn(func, args, kwargs)
            if isinstance(custom_key, tuple) and len(custom_key) == 2:
                vcr_args, vcr_kwargs = custom_key
            else:
                vcr_args, vcr_kwargs = custom_key, {}
            ignore_set = {str(x) for x in (ignore_in_hash or []) if str(x)}
            if ignore_set and isinstance(vcr_args, dict):
                vcr_args = {k: v for k, v in vcr_args.items() if str(k) not in ignore_set}
            if ignore_set and isinstance(vcr_kwargs, dict):
                vcr_kwargs = {k: v for k, v in vcr_kwargs.items() if str(k) not in ignore_set}
        except Exception as e:
            log.warning("[Logram] vcr_key_fn failed for %s: %s", getattr(func, "__qualname__", func), e)

    if log_input_fn is not None:
        try:
            log_inputs = log_input_fn(func, args, kwargs)
        except Exception as e:
            log.warning("[Logram] log_input_fn failed for %s: %s", getattr(func, "__qualname__", func), e)

    logic_snapshot, impl_fp, callee_registry = _implementation_artifacts(func)
    func_name = name or func.__name__
    log.debug("[Logram] step=%s cache_key_args=%s", func_name, _diag_preview({"args": vcr_args, "kwargs": vcr_kwargs}))

    return {
        "run_id": current_run_id.get() or "default_run",
        "input_id": current_input_id.get() or "unknown_input",
        "parent_id": current_step_id.get(),
        "step_id": str(uuid.uuid4()),
        "func_name": func_name,
        "vcr_args": vcr_args,
        "vcr_kwargs": vcr_kwargs,
        "log_inputs": log_inputs,
        "implementation_fingerprint": impl_fp,
        "logic_snapshot": logic_snapshot,
        "callee_registry": callee_registry,
    }


def _bind_all_arguments(func: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        bound = inspect.signature(func).bind(*args, **kwargs)
        return dict(bound.arguments)
    except Exception:
        try:
            bound = inspect.signature(func).bind_partial(*args, **kwargs)
            return dict(bound.arguments)
        except Exception:
            return {}


def _capture_tracked_args_snapshot(bound_args: dict[str, Any], track_args: list[str]) -> dict[str, Any]:
    snapshot: dict[str, Any] = {}
    for arg_name in track_args:
        if arg_name not in bound_args:
            continue
        try:
            snapshot[arg_name] = ensure_serializable(bound_args[arg_name], blob_manager=blobs)
        except Exception as exc:
            log.warning("[Logram] track_args snapshot capture failed for %s: %s", arg_name, exc)
            snapshot[arg_name] = None
    return snapshot


def _apply_replayed_arg_value(target: Any, replayed_value: Any) -> bool:
    try:
        if isinstance(target, dict) and isinstance(replayed_value, dict):
            target.clear()
            target.update(replayed_value)
            return True

        if isinstance(target, list) and isinstance(replayed_value, list):
            target[:] = replayed_value
            return True

        if isinstance(target, set) and isinstance(replayed_value, (set, list, tuple)):
            target.clear()
            target.update(replayed_value)
            return True

        if isinstance(target, dict) and not isinstance(replayed_value, dict):
            return False

        if isinstance(replayed_value, dict):
            applied = False
            for k, v in replayed_value.items():
                try:
                    setattr(target, k, v)
                    applied = True
                except Exception:
                    continue
            return applied
    except Exception:
        return False
    return False


def _short_traceback(exc: BaseException) -> list[str]:
    try:
        tb_lines = traceback.format_exception(type(exc), exc, exc.__traceback__)
        if len(tb_lines) > 12:
            tb_lines = tb_lines[-12:]
        return [ln.rstrip("\n") for ln in tb_lines]
    except Exception:
        return [f"{type(exc).__name__}: {exc}"]


def stateful(include: list[str]):
    """Class decorator marking instance attributes to replay-restore on cache hit."""

    def _decorate(cls):
        try:
            include_fields = [str(x) for x in (include or []) if str(x)]
            setattr(cls, _STATEFUL_CONFIG_ATTR, tuple(include_fields))
        except Exception as exc:
            log.warning("[Logram] stateful decorator failed on %s: %s", cls, exc)
        return cls

    return _decorate


def _get_stateful_target(args: tuple[Any, ...]) -> tuple[Any | None, list[str]]:
    if not args:
        return None, []
    instance = args[0]
    try:
        config = getattr(type(instance), _STATEFUL_CONFIG_ATTR, None)
        if isinstance(config, (list, tuple)):
            fields = [str(x) for x in config if str(x)]
            return instance, fields
    except Exception:
        pass
    return None, []


def _capture_state_snapshot(instance: Any, include_fields: list[str]) -> dict[str, Any]:
    snapshot: dict[str, Any] = {}
    if instance is None or not include_fields:
        return snapshot
    for key in include_fields:
        try:
            raw = getattr(instance, key) if hasattr(instance, key) else None
            snapshot[key] = ensure_serializable(raw, blob_manager=blobs)
        except Exception as exc:
            log.warning("[Logram] state snapshot capture failed for %s.%s: %s", type(instance).__name__, key, exc)
            snapshot[key] = None
    return snapshot


def _compute_effective_state_fields(
    stateful_fields: list[str],
    include_state: list[str] | None,
    exclude_state: list[str] | None,
) -> list[str]:
    """Intersect @stateful fields with @trace-level include/exclude to get the fields actually captured."""
    base = list(include_state) if include_state is not None else list(stateful_fields)
    if exclude_state:
        exclude_set = set(exclude_state)
        base = [f for f in base if f not in exclude_set]
    return base


def _stable_json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        try:
            return json.dumps(str(value), ensure_ascii=False, sort_keys=True)
        except Exception:
            return "null"


def _compute_state_delta(
    before: dict[str, Any],
    after: dict[str, Any],
) -> tuple[dict[str, str], dict[str, Any]]:
    state_delta: dict[str, str] = {}
    state_values: dict[str, Any] = {}

    all_keys = sorted(set(before.keys()) | set(after.keys()))
    for key in all_keys:
        before_dump = _stable_json_dumps(before.get(key))
        after_value = after.get(key)
        after_dump = _stable_json_dumps(after_value)
        if before_dump == after_dump:
            continue
        value_hash = hashlib.sha256(after_dump.encode("utf-8", errors="replace")).hexdigest()
        state_delta[key] = value_hash
        state_values[value_hash] = after_value

    return state_delta, state_values


@dataclass(frozen=True)
class _TraceOptions:
    name: str | None
    ignore_in_hash: list[str] | None
    track_args: list[str] | None
    vcr_key_fn: Callable[..., Any] | None
    log_input_fn: Callable[..., Any] | None
    state_in_hash: bool
    include_state: list[str] | None
    exclude_state: list[str] | None


# Returned by the replay helpers when the cached entry cannot be used, so the
# caller falls back to a live call.
_LIVE: Any = object()


class _Step:
    """One call of a traced function: cache lookup, then replay or live recording.

    Every method here is exception-safe: Logram's own failures are logged and
    never reach the user's pipeline. Only the user's function may raise, and its
    exceptions propagate unchanged (see the ``_run_*`` executors).
    """

    def __init__(self, func: Any, opts: _TraceOptions, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        self.func = func
        self.ctx = _prepare_step_ctx(
            func,
            opts.name,
            args,
            kwargs,
            vcr_key_fn=opts.vcr_key_fn,
            log_input_fn=opts.log_input_fn,
            ignore_in_hash=opts.ignore_in_hash,
        )
        self.name: str = self.ctx["func_name"]

        self.instance, stateful_fields = _get_stateful_target(args)
        self.state_fields = (
            _compute_effective_state_fields(stateful_fields, opts.include_state, opts.exclude_state)
            if opts.state_in_hash and self.instance is not None
            else []
        )
        self.tracked_args = [str(x) for x in (opts.track_args or []) if str(x)]
        self.bound_args = _bind_all_arguments(func, args, kwargs)
        self.state_before = self._state_snapshot()
        self.args_before = self._tracked_args_snapshot()

        self.cache_key, self.cached, self.cached_state_delta, self.cached_args_delta = storage.get_vcr_hit(
            self.name,
            self.ctx["vcr_args"],
            self.ctx["vcr_kwargs"],
            implementation_fingerprint=self.ctx["implementation_fingerprint"],
            run_id=self.ctx["run_id"],
            run_input_id=self.ctx["input_id"],
            state_snapshot=self.state_before or None,
        )
        self.replay = self._should_replay()
        self._token: Any = None
        self.started_at = 0.0

    @classmethod
    def open(cls, func: Any, opts: _TraceOptions, args: tuple[Any, ...], kwargs: dict[str, Any]) -> _Step | None:
        """Prepare the step, or return None if tracing cannot be set up (call untraced)."""
        try:
            return cls(func, opts, args, kwargs)
        except Exception:
            log.warning("[Logram] tracing disabled for this call of %s", getattr(func, "__qualname__", func), exc_info=True)
            return None

    # -- snapshots ---------------------------------------------------------

    def _state_snapshot(self) -> dict[str, Any]:
        if not self.state_fields:
            return {}
        return _capture_state_snapshot(self.instance, self.state_fields)

    def _tracked_args_snapshot(self) -> dict[str, Any]:
        if not self.tracked_args:
            return {}
        return _capture_tracked_args_snapshot(self.bound_args, self.tracked_args)

    # -- replay ------------------------------------------------------------

    def _should_replay(self) -> bool:
        force_from = os.environ.get("LOGRAM_FORCE_FROM", "").strip()
        forced_steps = {p.strip() for p in os.environ.get("LOGRAM_FORCE_STEP", "").split(",") if p.strip()}
        already_cascading = _is_forced_by_flow.get()
        starts_cascade = bool(force_from) and self.name == force_from
        if starts_cascade and not already_cascading:
            # Every step after this one in the same context runs live too.
            _is_forced_by_flow.set(True)
        forced = self.name in forced_steps or already_cascading or starts_cascade
        return os.environ.get("LOGRAM_REPLAY") == "true" and self.cached is not _VCR_MISS and not forced

    def _restore_side_effects(self) -> None:
        """Put back the instance state and argument mutations the live call produced."""
        if self.instance is not None and isinstance(self.cached_state_delta, dict) and self.cached_state_delta:
            resolved = storage.get_state_values(self.cached_state_delta)
            if len(resolved) < len(self.cached_state_delta):
                log.debug(
                    "[Logram][PROBE 4] step=%s state values missing from values_registry: %s",
                    self.name,
                    sorted(set(self.cached_state_delta) - set(resolved)),
                )
            for attr_name, raw_value in resolved.items():
                setattr(self.instance, attr_name, _coerce_int_str_dict_keys(rehydrate_logram_output(raw_value)))

        if isinstance(self.cached_args_delta, dict) and self.cached_args_delta:
            for arg_name, raw_value in storage.get_state_values(self.cached_args_delta).items():
                if arg_name in self.bound_args:
                    _apply_replayed_arg_value(self.bound_args[arg_name], rehydrate_logram_output(raw_value))

    def replayed_value(self) -> Any:
        """Cached result of a regular function, or ``_LIVE`` if it cannot be replayed."""
        started_at = time.time()
        try:
            self._restore_side_effects()
            value = _rehydrate_cached(self.func, self.cached)
        except Exception:
            log.warning("[Logram] replay of %s failed, running it live", self.name, exc_info=True)
            return _LIVE
        self.record_replay(started_at, started_at)
        return value

    def replayed_chunks(self) -> Any:
        """Cached chunks of a generator, or ``_LIVE`` if they cannot be replayed."""
        try:
            self._restore_side_effects()
            return _rehydrate_cached_gen(self.func, self.cached)
        except Exception:
            log.warning("[Logram] replay of %s failed, running it live", self.name, exc_info=True)
            return _LIVE

    def record_replay(self, started_at: float, finished_at: float) -> None:
        try:
            prompt_tokens, completion_tokens = _extract_usage_tokens(self.cached)
            payload = {
                "step_id": self.ctx["step_id"],
                "parent_id": self.ctx["parent_id"],
                "name": self.name,
                "inputs": _safe_tree(self.ctx.get("log_inputs", {})),
                # Storage copies the output from the source row (same cache key).
                "output": None,
                "status": "REPLAYED",
                "duration": round(max(0.0, finished_at - started_at), 6),
                "timestamp": finished_at,
                "started_at": started_at,
                "finished_at": finished_at,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "is_replay": True,
                "replay_from_logic_hash": self.cache_key,
                "state_delta": self.cached_state_delta if isinstance(self.cached_state_delta, dict) else {},
                "args_delta": self.cached_args_delta if isinstance(self.cached_args_delta, dict) else {},
            }
            storage.save_step_sync(
                self.ctx["run_id"],
                payload,
                self.cache_key,
                self.ctx.get("logic_snapshot"),
                callee_registry=self.ctx.get("callee_registry"),
            )
            log.debug("[Logram] step=%s mode=REPLAY key=%s", self.name, self.cache_key)
        except Exception:
            log.warning("[Logram] could not record replayed step %s", self.name, exc_info=True)

    # -- live --------------------------------------------------------------

    def enter(self) -> None:
        self.started_at = time.time()
        self._token = current_step_id.set(self.ctx["step_id"])

    def exit(self) -> None:
        try:
            current_step_id.reset(self._token)
        except ValueError:
            pass  # a generator can be finished from another context

    def record_success(self, result: Any) -> None:
        finished_at = time.time()
        try:
            state_delta, state_values = _compute_state_delta(self.state_before, self._state_snapshot())
            args_delta, arg_values = _compute_state_delta(self.args_before, self._tracked_args_snapshot())
            payload = _build_success_payload(self.ctx, result, started_at=self.started_at, finished_at=finished_at)
            payload["state_delta"] = state_delta
            payload["args_delta"] = args_delta
            storage.save_step_sync(
                self.ctx["run_id"],
                payload,
                self.cache_key,
                self.ctx.get("logic_snapshot"),
                state_values=state_values or None,
                arg_values=arg_values or None,
                callee_registry=self.ctx.get("callee_registry"),
            )
            log.debug("[Logram] step=%s mode=LIVE key=%s", self.name, self.cache_key)
        except Exception:
            log.warning("[Logram] could not record step %s", self.name, exc_info=True)

    def record_failure(self, exc: BaseException) -> None:
        try:
            payload = _build_failed_payload(self.ctx, exc, started_at=self.started_at, finished_at=time.time())
            # No cache key: failed steps are never replayed.
            storage.save_step_sync(self.ctx["run_id"], payload, "", self.ctx.get("logic_snapshot"))
        except Exception:
            log.warning("[Logram] could not record failed step %s", self.name, exc_info=True)


def _run_sync(func: Any, opts: _TraceOptions, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
    step = _Step.open(func, opts, args, kwargs)
    if step is None:
        return func(*args, **kwargs)
    if step.replay:
        value = step.replayed_value()
        if value is not _LIVE:
            return value
    step.enter()
    try:
        result = func(*args, **kwargs)
    except Exception as exc:
        step.record_failure(exc)
        raise
    finally:
        step.exit()
    step.record_success(result)
    return result


async def _run_async(func: Any, opts: _TraceOptions, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
    step = _Step.open(func, opts, args, kwargs)
    if step is None:
        return await func(*args, **kwargs)
    if step.replay:
        value = step.replayed_value()
        if value is not _LIVE:
            return value
    step.enter()
    try:
        result = await func(*args, **kwargs)
    except Exception as exc:
        step.record_failure(exc)
        raise
    finally:
        step.exit()
    step.record_success(result)
    return result


def _run_sync_gen(func: Any, opts: _TraceOptions, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Iterator[Any]:
    """Chunks are yielded as they come and cached only once the stream is fully consumed."""
    step = _Step.open(func, opts, args, kwargs)
    if step is None:
        yield from func(*args, **kwargs)
        return
    if step.replay:
        started_at = time.time()
        chunks = step.replayed_chunks()
        if chunks is not _LIVE:
            yield from chunks
            step.record_replay(started_at, time.time())
            return
    step.enter()
    items: list[Any] = []
    try:
        for item in func(*args, **kwargs):
            items.append(item)
            yield item
    except Exception as exc:
        step.record_failure(exc)
        raise
    finally:
        step.exit()
    step.record_success(items)


async def _run_async_gen(
    func: Any, opts: _TraceOptions, args: tuple[Any, ...], kwargs: dict[str, Any]
) -> AsyncIterator[Any]:
    """Async counterpart of ``_run_sync_gen``."""
    step = _Step.open(func, opts, args, kwargs)
    if step is None:
        async for chunk in func(*args, **kwargs):
            yield chunk
        return
    if step.replay:
        started_at = time.time()
        chunks = step.replayed_chunks()
        if chunks is not _LIVE:
            for chunk in chunks:
                yield chunk
            step.record_replay(started_at, time.time())
            return
    step.enter()
    items: list[Any] = []
    try:
        async for chunk in func(*args, **kwargs):
            items.append(chunk)
            yield chunk
    except Exception as exc:
        step.record_failure(exc)
        raise
    finally:
        step.exit()
    step.record_success(items)


def trace(
    name: str | Callable[..., Any] | None = None,
    *,
    ignore_in_hash: list[str] | None = None,
    track_args: list[str] | None = None,
    vcr_key_fn: Callable[[Any, tuple[Any, ...], dict[str, Any]], tuple[Any, Any] | Any] | None = None,
    log_input_fn: Callable[[Any, tuple[Any, ...], dict[str, Any]], Any] | None = None,
    compact_inputs: bool = True,
    state_in_hash: bool = True,
    include_state: list[str] | None = None,
    exclude_state: list[str] | None = None,
) -> Any:
    """Record a function's calls and replay them from the store when nothing relevant changed.

    Usable bare (``@trace``) or configured (``@trace(name=..., ignore_in_hash=[...])``),
    on sync and async functions and generators. ``compact_inputs`` is accepted for
    backward compatibility and has no effect.
    """

    def decorator(func: Any) -> Any:
        opts = _TraceOptions(
            name=name if isinstance(name, str) else None,
            ignore_in_hash=ignore_in_hash,
            track_args=track_args,
            vcr_key_fn=vcr_key_fn,
            log_input_fn=log_input_fn,
            state_in_hash=state_in_hash,
            include_state=include_state,
            exclude_state=exclude_state,
        )

        if inspect.isasyncgenfunction(func):

            @functools.wraps(func)
            async def asyncgen_wrapper(*args: Any, **kwargs: Any) -> Any:
                async for chunk in _run_async_gen(func, opts, args, kwargs):
                    yield chunk

            return asyncgen_wrapper

        if inspect.isgeneratorfunction(func):

            @functools.wraps(func)
            def syncgen_wrapper(*args: Any, **kwargs: Any) -> Any:
                yield from _run_sync_gen(func, opts, args, kwargs)

            return syncgen_wrapper

        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                return await _run_async(func, opts, args, kwargs)

            return async_wrapper

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            return _run_sync(func, opts, args, kwargs)

        return sync_wrapper

    # Bare ``@trace`` form: the decorated function arrives as ``name``.
    if callable(name):
        return decorator(name)
    return decorator


def _safe_tree(data: Any) -> Any:
    try:
        return ensure_serializable(data, blob_manager=blobs)
    except Exception as e:
        log.warning("[Logram] Serialization error: %s", e)
        return {"error": "serialization_failed", "msg": str(e)}


def _build_success_payload(ctx, result, *, started_at: float, finished_at: float):
    duration = max(0.0, finished_at - started_at)
    prompt_tokens, completion_tokens = _extract_usage_tokens(result)
    return {
        "step_id": ctx["step_id"],
        "parent_id": ctx["parent_id"],
        "name": ctx["func_name"],
        "inputs": _safe_tree(ctx.get("log_inputs", {})),
        "output": _safe_tree(result),
        "status": "SUCCESS",
        "duration": round(duration, 6),
        "timestamp": finished_at,
        "started_at": started_at,
        "finished_at": finished_at,
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
    }


def _build_failed_payload(ctx, exc: BaseException, *, started_at: float, finished_at: float):
    duration = max(0.0, finished_at - started_at)
    return {
        "step_id": ctx["step_id"],
        "parent_id": ctx["parent_id"],
        "name": ctx["func_name"],
        "inputs": _safe_tree(ctx.get("log_inputs", {})),
        "output": None,
        "status": "FAILED",
        "duration": round(duration, 6),
        "timestamp": finished_at,
        "started_at": started_at,
        "finished_at": finished_at,
        "error": {
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": _short_traceback(exc),
        },
    }
