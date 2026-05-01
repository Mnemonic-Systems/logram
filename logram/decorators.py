# ruff: noqa: BLE001
# pylint: disable=broad-exception-caught

from __future__ import annotations

import functools
import hashlib
import inspect
import json
import linecache
import logging
import time
import traceback
import types
import uuid
import os
import weakref
from dataclasses import asdict, is_dataclass
from typing import Any, Callable, Union, get_args, get_origin, get_type_hints

from .context import _is_forced_by_flow, current_input_id, current_run_id, current_step_id
from .oracle import clear_oracle_cache, compute_logic_fingerprint
from .serializer import BlobManager, ensure_serializable, rehydrate_logram_output
from .storage import TraceStorage, _VCR_MISS

log = logging.getLogger(__name__)

storage = TraceStorage()
blobs = BlobManager()

_LOGIC_SNAPSHOT_CACHE: weakref.WeakKeyDictionary[Any, tuple[dict[str, Any], str, dict[str, Any]]] = weakref.WeakKeyDictionary()
_LAST_LOGIC_SNAPSHOT: weakref.WeakKeyDictionary[Any, dict[str, Any]] = weakref.WeakKeyDictionary()

_MAX_LIST_ITEMS = 16
_MAX_DICT_ITEMS = 24
_MAX_STR_LEN = 220
_MAX_MODULE_SCAN_DEPTH = 3
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


def _diag_type_shape(value: Any) -> Any:
    """Return a small structure describing top-level value types for diagnostics."""
    if isinstance(value, dict):
        return {
            "type": "dict",
            "size": len(value),
            "keys": [str(k) for k in list(value.keys())[:_MAX_DICT_ITEMS]],
            "value_types": {str(k): type(v).__name__ for k, v in list(value.items())[:_MAX_DICT_ITEMS]},
        }
    if isinstance(value, (list, tuple)):
        return {
            "type": type(value).__name__,
            "size": len(value),
            "item_types": [type(v).__name__ for v in list(value)[:_MAX_LIST_ITEMS]],
        }
    return {"type": type(value).__name__}


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


def _compact_value(value: Any, depth: int = 0) -> Any:
    if depth > 4:
        return "<max_depth>"

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
            return _compact_value(out, depth + 1)

    if _has_obj_hook(value, "__logram_trace_log__"):
        out = _safe_call_hook(value, "__logram_trace_log__")
        if out is not None:
            return _compact_value(out, depth + 1)

    if is_dataclass(value) and not isinstance(value, type):
        try:
            value = asdict(value)
        except Exception:
            return {"__af_obj__": type(value).__name__, "repr": repr(value)[:_MAX_STR_LEN]}

    if isinstance(value, (list, tuple)):
        return [_compact_value(x, depth + 1) for x in value[:_MAX_LIST_ITEMS]]

    if isinstance(value, dict) and value.get("__af_blob__") is True:
        blob_hash = str(value.get("hash", ""))
        blob_size = value.get("size", 0)
        return {"__af_bytes__": True, "len": blob_size, "sha12": blob_hash[:12]}

    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for i, (k, v) in enumerate(value.items()):
            if i >= _MAX_DICT_ITEMS:
                break
            out[str(k)] = _compact_value(v, depth + 1)
        return out

    d = getattr(value, "__dict__", None)
    if isinstance(d, dict):
        summary: dict[str, Any] = {"__af_obj__": type(value).__name__}
        for field in (
            "tile_id",
            "grid_tag",
            "page_number",
            "offset_x",
            "offset_y",
            "width",
            "height",
            "page_width",
            "page_height",
            "bbox",
            "centroid",
            "id",
            "name",
        ):
            if field in d:
                summary[field] = _compact_value(d[field], depth + 1)

        img_bytes = d.get("image_bytes")
        if isinstance(img_bytes, (bytes, bytearray, memoryview)):
            raw = bytes(img_bytes)
            summary["image"] = {"len": len(raw), "sha12": _sha12_bytes(raw)}

        if len(summary) > 1:
            return summary

    # ── PROBE 2 ── Detect address-based repr → guaranteed VCR cache miss ────────
    try:
        r = repr(value)
        if " at 0x" in r or "object at 0x" in r:
            log.warning(
                "[Logram][PROBE 2][UNSTABLE_REPR] type=%s id=%d repr_preview=%s "
                "— repr() contains a memory address. This arg produces a different "
                "vcr_args string on every run → VCR cache miss GUARANTEED. "
                "Fix: implement __logram_trace_key__ on this class.",
                type(value).__name__,
                id(value),
                r[:160],
            )
    except Exception:
        pass
    # ── END PROBE 2 ─────────────────────────────────────────────────────────────
    return {"__af_obj__": type(value).__name__, "repr": repr(value)[:_MAX_STR_LEN]}


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


def _stable_snapshot_hash(snapshot: dict[str, Any]) -> str:
    try:
        payload = json.dumps(snapshot, ensure_ascii=False, sort_keys=True).encode("utf-8")
    except Exception:
        payload = str(snapshot).encode("utf-8", errors="replace")
    return hashlib.sha256(payload).hexdigest()


def _get_logic_snapshot(func: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    """Compute the logic snapshot + callee registry for ``func``.

    Thin delegate to ``logram.oracle.compute_logic_fingerprint``. The oracle
    performs AST-based, version-stable structural hashing with deterministic
    volatility markers and MRO-aware method resolution.
    """
    try:
        unwrapped = inspect.unwrap(func)
    except Exception:
        unwrapped = func
    return compute_logic_fingerprint(unwrapped)


def _implementation_artifacts(func: Any) -> tuple[dict[str, Any], str, dict[str, Any]]:
    unwrapped = inspect.unwrap(func)

    # Per-run cache: same function called 150× (e.g. tile loop) → compute snapshot once.
    # Cache is cleared by clear_logic_snapshot_cache() at af.init(), so each new run
    # gets a fresh snapshot (picking up source / global constant changes).
    cached = _LOGIC_SNAPSHOT_CACHE.get(unwrapped)
    if cached is not None:
        return cached

    logic_snapshot, callee_registry = _get_logic_snapshot(unwrapped)
    fingerprint = _stable_snapshot_hash(logic_snapshot)

    try:
        current_globals = logic_snapshot.get("resolved_globals")
        cur_size = len(current_globals) if isinstance(current_globals, dict) else 0
        previous = _LAST_LOGIC_SNAPSHOT.get(unwrapped)
        if isinstance(previous, dict):
            prev_globals = previous.get("resolved_globals")
            prev_size = len(prev_globals) if isinstance(prev_globals, dict) else 0
            if prev_size >= 4 and cur_size <= max(1, prev_size // 2):
                log.warning(
                    "[Logram] Resolved globals shrank significantly for %s (%d -> %d). "
                    "Possible capture regression (module refresh/scope issue).",
                    getattr(unwrapped, "__qualname__", getattr(unwrapped, "__name__", "unknown")),
                    prev_size,
                    cur_size,
                )
        _LAST_LOGIC_SNAPSHOT[unwrapped] = logic_snapshot
    except Exception:
        pass

    try:
        resolved = logic_snapshot.get("resolved_globals") if isinstance(logic_snapshot, dict) else None
        resolved_keys = sorted(list(resolved.keys())) if isinstance(resolved, dict) else []
        structural_hash = (
            logic_snapshot.get("structural_hash") if isinstance(logic_snapshot, dict) else None
        )
        volatile_markers = (
            logic_snapshot.get("volatile_markers") if isinstance(logic_snapshot, dict) else None
        )
        log.debug(
            "[Logram][Diag B][Introspection] func=%s hash=%s snapshot_keys=%s resolved_globals_count=%d resolved_globals_keys=%s signature=%s structural_hash=%s volatile_markers=%s",
            getattr(unwrapped, "__qualname__", getattr(unwrapped, "__name__", "unknown")),
            fingerprint,
            sorted(list(logic_snapshot.keys())) if isinstance(logic_snapshot, dict) else [],
            len(resolved_keys),
            resolved_keys,
            logic_snapshot.get("signature") if isinstance(logic_snapshot, dict) else None,
            structural_hash,
            volatile_markers,
        )
        if isinstance(resolved, dict):
            log.debug(
                "[Logram][Diag B][Introspection] func=%s resolved_globals_preview=%s",
                getattr(unwrapped, "__qualname__", getattr(unwrapped, "__name__", "unknown")),
                _diag_preview(resolved),
            )
    except Exception:
        pass

    # ── PROBE 1 ── Fingerprint sub-component hashes for Run-1/Run-2 diff ──────
    try:
        func_label = getattr(unwrapped, "__qualname__", getattr(unwrapped, "__name__", "unknown"))
        struct_hash = str(logic_snapshot.get("structural_hash", "") if isinstance(logic_snapshot, dict) else "")
        g_raw = json.dumps(
            logic_snapshot.get("resolved_globals", {}) if isinstance(logic_snapshot, dict) else {},
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )
        g_sha12 = _sha12_bytes(g_raw.encode("utf-8", errors="replace"))
        markers = logic_snapshot.get("volatile_markers") if isinstance(logic_snapshot, dict) else None
        log.debug(
            "[Logram][PROBE 1][FingerprintComponents] func=%s "
            "impl_fingerprint=%s structural_hash=%s globals_sha12=%s "
            "volatile_markers=%s "
            ">>> If impl_fingerprint differs between Run-1 and Run-2, compare structural_hash and globals_sha12 to isolate which component changed.",
            func_label,
            fingerprint,
            struct_hash[:12] if struct_hash else "<empty>",
            g_sha12,
            markers,
        )
        if markers:
            log.info(
                "[Logram][PROBE 1][VOLATILE_MARKERS] func=%s markers=%s "
                "— code uses dynamic constructs (eval/exec/dyn-getattr/etc.). "
                "Markers are deterministic: identical code → identical hash, so the cache still works "
                "as long as the function source itself does not change.",
                func_label,
                markers,
            )
    except Exception:
        pass
    # ── END PROBE 1 ─────────────────────────────────────────────────────────────

    result = (logic_snapshot, fingerprint, callee_registry)
    try:
        _LOGIC_SNAPSHOT_CACHE[unwrapped] = result
    except Exception:
        pass
    return result


def clear_logic_snapshot_cache() -> None:
    linecache.checkcache()  # Refresh on-disk source cache so edits are picked up on next run.
    _LOGIC_SNAPSHOT_CACHE.clear()
    _LAST_LOGIC_SNAPSHOT.clear()
    clear_oracle_cache()


def _default_vcr_args_kwargs(func: Any, args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[Any, Any]:
    named = _bind_named_arguments(func, args, kwargs, drop_self_cls=True)
    compact_named = {k: _compact_value(v) for k, v in named.items()}
    return compact_named, {}


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
    return named


def _prepare_step_ctx(
    func,
    name,
    args,
    kwargs,
    vcr_key_fn=None,
    log_input_fn=None,
    compact_inputs=True,
    ignore_in_hash: list[str] | None = None,
):
    if compact_inputs:
        named_for_probe = _bind_named_arguments(func, args, kwargs, drop_self_cls=True)
        vcr_args, vcr_kwargs = _logical_args_for_vcr(func, args, kwargs, ignore_in_hash=ignore_in_hash)
        log_inputs = _default_log_inputs(func, args, kwargs)
    else:
        named = _bind_named_arguments(func, args, kwargs, drop_self_cls=True)
        named_for_probe = named
        vcr_args = named
        vcr_kwargs = {}
        log_inputs = named

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
    try:
        pdf_probe = named_for_probe.get("pdf_bytes") if isinstance(named_for_probe, dict) else None
        if isinstance(pdf_probe, (bytes, bytearray, memoryview)):
            pdf_raw = bytes(pdf_probe)
            pdf_sha256 = hashlib.sha256(pdf_raw).hexdigest()
            log.debug(
                "[Logram][Diag PDF][RawArg] func=%s arg=pdf_bytes len=%d sha256=%s",
                func_name,
                len(pdf_raw),
                pdf_sha256,
            )
            compact_probe = _compact_value(pdf_raw)
            log.debug(
                "[Logram][Diag PDF][CompactedArg] func=%s arg=pdf_bytes compacted=%s",
                func_name,
                _diag_preview(compact_probe),
            )

        logical_args = {"args": vcr_args, "kwargs": vcr_kwargs}
        log.debug(
            "[Logram][Diag A][Capture] func=%s logical_args_shape=%s logical_args_preview=%s log_inputs_shape=%s log_inputs_preview=%s",
            func_name,
            _diag_type_shape(logical_args),
            _diag_preview(logical_args),
            _diag_type_shape(log_inputs),
            _diag_preview(log_inputs),
        )
    except Exception:
        pass

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


def trace(
    name: str = None,
    *,
    ignore_in_hash: list[str] | None = None,
    track_args: list[str] | None = None,
    vcr_key_fn: Callable[[Any, tuple[Any, ...], dict[str, Any]], tuple[Any, Any] | Any] | None = None,
    log_input_fn: Callable[[Any, tuple[Any, ...], dict[str, Any]], Any] | None = None,
    compact_inputs: bool = True,
    state_in_hash: bool = True,
    include_state: list[str] | None = None,
    exclude_state: list[str] | None = None,
):
    def decorator(func):
        # Generators (sync or async) produce a stream object, not a concrete
        # serializable value. Tracing them would silently store str(generator)
        # and permanently disable caching. Warn once at decoration time and
        # return the original function untouched — zero overhead, zero surprise.
        if inspect.isasyncgenfunction(func):
            @functools.wraps(func)
            async def asyncgen_wrapper(*args, **kwargs):
                async for chunk in _logic_async_gen(
                    func, name, ignore_in_hash, vcr_key_fn, log_input_fn, compact_inputs,
                    state_in_hash, include_state, exclude_state, track_args,
                    *args, **kwargs,
                ):
                    yield chunk
            return asyncgen_wrapper

        if inspect.isgeneratorfunction(func):
            @functools.wraps(func)
            def syncgen_wrapper(*args, **kwargs):
                yield from _logic_sync_gen(
                    func, name, ignore_in_hash, vcr_key_fn, log_input_fn, compact_inputs,
                    state_in_hash, include_state, exclude_state, track_args,
                    *args, **kwargs,
                )
            return syncgen_wrapper

        is_async = inspect.iscoroutinefunction(func)

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            return await _logic_async(
                func,
                name,
                ignore_in_hash,
                track_args,
                vcr_key_fn,
                log_input_fn,
                compact_inputs,
                state_in_hash,
                include_state,
                exclude_state,
                *args,
                **kwargs,
            )

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            return _logic_sync(
                func,
                name,
                ignore_in_hash,
                track_args,
                vcr_key_fn,
                log_input_fn,
                compact_inputs,
                state_in_hash,
                include_state,
                exclude_state,
                *args,
                **kwargs,
            )

        return async_wrapper if is_async else sync_wrapper

    return decorator


async def _logic_async(func, name, ignore_in_hash, track_args, vcr_key_fn, log_input_fn, compact_inputs, state_in_hash, include_state, exclude_state, *args, **kwargs):
    try:
        ctx = _prepare_step_ctx(
            func,
            name,
            args,
            kwargs,
            vcr_key_fn=vcr_key_fn,
            log_input_fn=log_input_fn,
            compact_inputs=compact_inputs,
            ignore_in_hash=ignore_in_hash,
        )
    except Exception:
        # Tracing must never break user execution.
        return await func(*args, **kwargs)

    state_instance, state_include_fields = _get_stateful_target(args)
    effective_state_fields = (
        _compute_effective_state_fields(state_include_fields, include_state, exclude_state)
        if state_in_hash else []
    )
    tracked_arg_names = [str(x) for x in (track_args or []) if str(x)]
    bound_all_args = _bind_all_arguments(func, args, kwargs)
    state_before: dict[str, Any] = {}
    tracked_args_before: dict[str, Any] = {}
    if state_instance is not None and effective_state_fields:
        try:
            state_before = _capture_state_snapshot(state_instance, effective_state_fields)
            log.debug(
                "[Logram][Diag A][StateCapture] phase=before async func=%s effective_fields=%s snapshot_preview=%s",
                ctx["func_name"],
                effective_state_fields,
                _diag_preview(state_before),
            )
        except Exception as exc:
            log.warning("[Logram] state pre-snapshot failed (async) for %s: %s", ctx["func_name"], exc)

    if tracked_arg_names:
        try:
            tracked_args_before = _capture_tracked_args_snapshot(bound_all_args, tracked_arg_names)
            log.debug(
                "[Logram][Diag A][ArgCapture] phase=before async func=%s tracked_args=%s snapshot_preview=%s",
                ctx["func_name"],
                tracked_arg_names,
                _diag_preview(tracked_args_before),
            )
        except Exception as exc:
            log.warning("[Logram] track_args pre-snapshot failed (async) for %s: %s", ctx["func_name"], exc)

    vcr_hash, cached_res, replay_state_delta, replay_args_delta = storage.get_vcr_hit(
        ctx["func_name"],
        ctx["vcr_args"],
        ctx["vcr_kwargs"],
        implementation_fingerprint=ctx.get("implementation_fingerprint", ""),
        run_id=ctx.get("run_id"),
        run_input_id=ctx.get("input_id"),
        state_snapshot=state_before if (state_in_hash and state_before) else None,
    )

    force_from    = os.environ.get("LOGRAM_FORCE_FROM", "").strip()
    forced_steps  = {p.strip() for p in os.environ.get("LOGRAM_FORCE_STEP", "").split(",") if p.strip()}

    already_cascading  = _is_forced_by_flow.get()
    triggering_cascade = bool(force_from) and ctx["func_name"] == force_from
    force_this_step    = ctx["func_name"] in forced_steps or already_cascading or triggering_cascade

    if triggering_cascade and not already_cascading:
        _is_forced_by_flow.set(True)

    if os.environ.get("LOGRAM_REPLAY") == "true" and cached_res is not _VCR_MISS and not force_this_step:
        replay_started_at = time.time()
        replay_finished_at = replay_started_at
        prompt_tokens, completion_tokens = _extract_usage_tokens(cached_res)

        if state_instance is not None and isinstance(replay_state_delta, dict) and replay_state_delta:
            try:
                resolved_state = storage.get_state_values(replay_state_delta)
                # ── PROBE 4 ── State restoration per-attribute detail (async) ─────
                log.debug(
                    "[Logram][PROBE 4][StateRestoreBegin] async func=%s "
                    "delta_keys=%s resolved_count=%d missing_count=%d "
                    ">>> If resolved_count < delta key count, values_registry lookup failed (flush race on Run-1?).",
                    ctx["func_name"],
                    sorted(list(replay_state_delta.keys())),
                    len(resolved_state),
                    len(replay_state_delta) - len(resolved_state),
                )
                for attr_name, raw_value in resolved_state.items():
                    rehydrated_value = rehydrate_logram_output(raw_value)
                    rehydrated_value = _coerce_int_str_dict_keys(rehydrated_value)
                    # ── PROBE 4b ── Per-attribute rehydration detail ───────────────
                    log.debug(
                        "[Logram][PROBE 4][AttrRestore] async func=%s attr=%s "
                        "raw_type=%s rehydrated_type=%s rehydrated_preview=%s",
                        ctx["func_name"],
                        attr_name,
                        type(raw_value).__name__,
                        type(rehydrated_value).__name__,
                        _diag_preview(rehydrated_value),
                    )
                    setattr(state_instance, attr_name, rehydrated_value)
                # ── END PROBE 4 ──────────────────────────────────────────────────
                log.debug(
                    "[Logram][Diag E][ReplayState] async func=%s applied_fields=%s delta_keys=%s",
                    ctx["func_name"],
                    sorted(list(resolved_state.keys())),
                    sorted(list(replay_state_delta.keys())),
                )
            except Exception as exc:
                log.warning("[Logram] replay state restore failed (async) for %s: %s", ctx["func_name"], exc)

        if isinstance(replay_args_delta, dict) and replay_args_delta:
            try:
                resolved_args = storage.get_state_values(replay_args_delta)
                applied_args: list[str] = []
                for arg_name, raw_value in resolved_args.items():
                    if arg_name not in bound_all_args:
                        continue
                    rehydrated_value = rehydrate_logram_output(raw_value)
                    if _apply_replayed_arg_value(bound_all_args[arg_name], rehydrated_value):
                        applied_args.append(arg_name)
                log.debug(
                    "[Logram][Diag E][ReplayArgs] async func=%s applied_args=%s delta_keys=%s",
                    ctx["func_name"],
                    sorted(applied_args),
                    sorted(list(replay_args_delta.keys())),
                )
            except Exception as exc:
                log.warning("[Logram] replay tracked args restore failed (async) for %s: %s", ctx["func_name"], exc)

        try:
            replay_payload = {
                "step_id": ctx["step_id"],
                "parent_id": ctx["parent_id"],
                "name": ctx["func_name"],
                "inputs": _safe_tree(ctx.get("log_inputs", {})),
                # Data aliasing: output is copied in storage from the existing row via logic_hash.
                "output": None,
                "status": "REPLAYED",
                "duration": round(max(0.0, replay_finished_at - replay_started_at), 6),
                "timestamp": replay_finished_at,
                "started_at": replay_started_at,
                "finished_at": replay_finished_at,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "is_replay": True,
                "replay_from_logic_hash": vcr_hash,
                "state_delta": replay_state_delta if isinstance(replay_state_delta, dict) else {},
                "args_delta": replay_args_delta if isinstance(replay_args_delta, dict) else {},
            }
            log.debug(
                "[Logram][Diag E][Save] mode=REPLAY async func=%s run_id=%s step_id=%s vcr_hash=%s payload_preview=%s",
                ctx["func_name"],
                ctx["run_id"],
                ctx["step_id"],
                vcr_hash,
                _diag_preview(replay_payload),
            )
            await storage.save_step(
                ctx["run_id"],
                replay_payload,
                vcr_hash,
                ctx.get("logic_snapshot"),
                state_values=None,
                arg_values=None,
                callee_registry=ctx.get("callee_registry"),
            )
        except Exception as se:
            log.warning("[Logram] save async replay trace failed: %s", se)
        rehydrated = _rehydrate_cached(func, cached_res)
        log.debug(
            "[Logram][Diag E][Rehydrate] mode=REPLAY async func=%s vcr_hash=%s cached_type=%s cached_preview=%s rehydrated_type=%s rehydrated_preview=%s",
            ctx["func_name"],
            vcr_hash,
            type(cached_res).__name__,
            _diag_preview(cached_res),
            type(rehydrated).__name__,
            _diag_preview(rehydrated),
        )
        return rehydrated

    token = current_step_id.set(ctx["step_id"])
    started_at = time.time()
    try:
        result = await func(*args, **kwargs)
        finished_at = time.time()
        state_delta: dict[str, str] = {}
        state_values: dict[str, Any] = {}
        args_delta: dict[str, str] = {}
        arg_values: dict[str, Any] = {}
        if state_instance is not None and effective_state_fields:
            try:
                state_after = _capture_state_snapshot(state_instance, effective_state_fields)
                state_delta, state_values = _compute_state_delta(state_before, state_after)
                log.debug(
                    "[Logram][Diag E][StateDelta] async func=%s delta_keys=%s state_values_hashes=%s",
                    ctx["func_name"],
                    sorted(list(state_delta.keys())),
                    sorted(list(state_values.keys())),
                )
            except Exception as exc:
                log.warning("[Logram] state delta capture failed (async) for %s: %s", ctx["func_name"], exc)
        if tracked_arg_names:
            try:
                tracked_args_after = _capture_tracked_args_snapshot(bound_all_args, tracked_arg_names)
                args_delta, arg_values = _compute_state_delta(tracked_args_before, tracked_args_after)
                log.debug(
                    "[Logram][Diag E][ArgsDelta] async func=%s delta_keys=%s arg_values_hashes=%s",
                    ctx["func_name"],
                    sorted(list(args_delta.keys())),
                    sorted(list(arg_values.keys())),
                )
            except Exception as exc:
                log.warning("[Logram] track_args delta capture failed (async) for %s: %s", ctx["func_name"], exc)
        try:
            payload = _build_success_payload(ctx, result, started_at=started_at, finished_at=finished_at)
            payload["state_delta"] = state_delta
            payload["args_delta"] = args_delta
            log.debug(
                "[Logram][Diag E][Save] mode=LIVE async func=%s run_id=%s step_id=%s vcr_hash=%s payload_preview=%s",
                ctx["func_name"],
                ctx["run_id"],
                ctx["step_id"],
                vcr_hash,
                _diag_preview(payload),
            )
            await storage.save_step(
                ctx["run_id"],
                payload,
                vcr_hash,
                ctx.get("logic_snapshot"),
                state_values=state_values,
                arg_values=arg_values,
                callee_registry=ctx.get("callee_registry"),
            )
        except Exception as se:
            log.warning("[Logram] save async failed: %s", se)
        return result
    except Exception as exc:
        finished_at = time.time()
        try:
            err_payload = _build_failed_payload(ctx, exc, started_at=started_at, finished_at=finished_at)
            log.debug(
                "[Logram][Diag E][Save] mode=LIVE_FAILED async func=%s run_id=%s step_id=%s vcr_hash=<empty> payload_preview=%s",
                ctx["func_name"],
                ctx["run_id"],
                ctx["step_id"],
                _diag_preview(err_payload),
            )
            await storage.save_step(ctx["run_id"], err_payload, "", ctx.get("logic_snapshot"))
        except Exception as se:
            log.warning("[Logram] save async failure trace failed: %s", se)
        raise
    finally:
        current_step_id.reset(token)


async def _logic_async_gen(
    func, name, ignore_in_hash, vcr_key_fn, log_input_fn, compact_inputs,
    state_in_hash, include_state, exclude_state, track_args,
    *args, **kwargs,
):
    """Shadow-buffering wrapper for async generator functions.

    Feature-complete mirror of _logic_async for generators:
    LIVE  : yields chunks transparently while accumulating them; saves the
            complete list to the VCR cache only when the stream is fully consumed.
    REPLAY: re-yields stored chunks from the cache; skips the live call entirely.
    Partial consumption (GeneratorExit / early break) is never cached.
    State + track_args: captured before first yield and after full consumption.
    GeneratorExit → no state_delta / args_delta persisted.
    """
    try:
        ctx = _prepare_step_ctx(
            func, name, args, kwargs,
            vcr_key_fn=vcr_key_fn,
            log_input_fn=log_input_fn,
            compact_inputs=compact_inputs,
            ignore_in_hash=ignore_in_hash,
        )
    except Exception:
        async for chunk in func(*args, **kwargs):
            yield chunk
        return

    # ── State + tracked-args capture — before first yield ─────────────────────
    state_instance, state_include_fields = _get_stateful_target(args)
    effective_state_fields = (
        _compute_effective_state_fields(state_include_fields, include_state, exclude_state)
        if state_in_hash else []
    )
    tracked_arg_names = [str(x) for x in (track_args or []) if str(x)]
    bound_all_args = _bind_all_arguments(func, args, kwargs)
    state_before: dict[str, Any] = {}
    tracked_args_before: dict[str, Any] = {}
    if state_instance is not None and effective_state_fields:
        try:
            state_before = _capture_state_snapshot(state_instance, effective_state_fields)
            log.debug(
                "[Logram][Diag A][StateCapture] phase=before async_gen func=%s effective_fields=%s snapshot_preview=%s",
                ctx["func_name"], effective_state_fields, _diag_preview(state_before),
            )
        except Exception as exc:
            log.warning("[Logram] async gen state pre-snapshot failed for %s: %s", ctx["func_name"], exc)
    if tracked_arg_names:
        try:
            tracked_args_before = _capture_tracked_args_snapshot(bound_all_args, tracked_arg_names)
            log.debug(
                "[Logram][Diag A][ArgCapture] phase=before async_gen func=%s tracked_args=%s snapshot_preview=%s",
                ctx["func_name"], tracked_arg_names, _diag_preview(tracked_args_before),
            )
        except Exception as exc:
            log.warning("[Logram] async gen track_args pre-snapshot failed for %s: %s", ctx["func_name"], exc)

    vcr_hash, cached_res, replay_state_delta, replay_args_delta = storage.get_vcr_hit(
        ctx["func_name"],
        ctx["vcr_args"],
        ctx["vcr_kwargs"],
        implementation_fingerprint=ctx.get("implementation_fingerprint", ""),
        run_id=ctx.get("run_id"),
        run_input_id=ctx.get("input_id"),
        state_snapshot=state_before if (state_in_hash and state_before) else None,
    )

    force_from        = os.environ.get("LOGRAM_FORCE_FROM", "").strip()
    forced_steps      = {p.strip() for p in os.environ.get("LOGRAM_FORCE_STEP", "").split(",") if p.strip()}
    already_cascading = _is_forced_by_flow.get()
    trig_cascade      = bool(force_from) and ctx["func_name"] == force_from
    force_this_step   = ctx["func_name"] in forced_steps or already_cascading or trig_cascade
    if trig_cascade and not already_cascading:
        _is_forced_by_flow.set(True)

    if os.environ.get("LOGRAM_REPLAY") == "true" and cached_res is not _VCR_MISS and not force_this_step:
        # ── REPLAY: restore state + args, re-yield stored chunks ───────────────
        replay_started_at  = time.time()
        replay_finished_at = replay_started_at
        prompt_tokens, completion_tokens = _extract_usage_tokens(cached_res)

        if state_instance is not None and isinstance(replay_state_delta, dict) and replay_state_delta:
            try:
                resolved_state = storage.get_state_values(replay_state_delta)
                log.debug(
                    "[Logram][PROBE 4][StateRestoreBegin] async_gen func=%s "
                    "delta_keys=%s resolved_count=%d missing_count=%d "
                    ">>> If resolved_count < delta key count, values_registry lookup failed (flush race on Run-1?).",
                    ctx["func_name"], sorted(replay_state_delta.keys()),
                    len(resolved_state), len(replay_state_delta) - len(resolved_state),
                )
                for attr_name, raw_value in resolved_state.items():
                    rehydrated = rehydrate_logram_output(raw_value)
                    rehydrated = _coerce_int_str_dict_keys(rehydrated)
                    log.debug(
                        "[Logram][PROBE 4][AttrRestore] async_gen func=%s attr=%s "
                        "raw_type=%s rehydrated_type=%s rehydrated_preview=%s",
                        ctx["func_name"], attr_name,
                        type(raw_value).__name__, type(rehydrated).__name__, _diag_preview(rehydrated),
                    )
                    setattr(state_instance, attr_name, rehydrated)
                log.debug(
                    "[Logram][Diag E][ReplayState] async_gen func=%s applied_fields=%s delta_keys=%s",
                    ctx["func_name"],
                    sorted(resolved_state.keys()),
                    sorted(replay_state_delta.keys()),
                )
            except Exception as exc:
                log.warning("[Logram] async gen state restore failed for %s: %s", ctx["func_name"], exc)

        if isinstance(replay_args_delta, dict) and replay_args_delta:
            try:
                resolved_args = storage.get_state_values(replay_args_delta)
                applied_args: list[str] = []
                for arg_name, raw_value in resolved_args.items():
                    if arg_name not in bound_all_args:
                        continue
                    rehydrated = rehydrate_logram_output(raw_value)
                    if _apply_replayed_arg_value(bound_all_args[arg_name], rehydrated):
                        applied_args.append(arg_name)
                log.debug(
                    "[Logram][Diag E][ReplayArgs] async_gen func=%s applied_args=%s delta_keys=%s",
                    ctx["func_name"], sorted(applied_args), sorted(replay_args_delta.keys()),
                )
            except Exception as exc:
                log.warning("[Logram] async gen replay tracked args restore failed for %s: %s", ctx["func_name"], exc)

        stored = _rehydrate_cached_gen(func, cached_res)
        log.debug(
            "[Logram][Diag E][Rehydrate] mode=REPLAY async_gen func=%s vcr_hash=%s "
            "cached_type=%s cached_preview=%s rehydrated_type=%s rehydrated_preview=%s",
            ctx["func_name"], vcr_hash,
            type(cached_res).__name__, _diag_preview(cached_res),
            type(stored).__name__, _diag_preview(stored),
        )
        for chunk in (stored if isinstance(stored, list) else [stored]):
            yield chunk
        replay_finished_at = time.time()
        try:
            replay_payload = {
                "step_id":                ctx["step_id"],
                "parent_id":              ctx["parent_id"],
                "name":                   ctx["func_name"],
                "inputs":                 _safe_tree(ctx.get("log_inputs", {})),
                "output":                 None,
                "status":                 "REPLAYED",
                "duration":               round(max(0.0, replay_finished_at - replay_started_at), 6),
                "timestamp":              replay_finished_at,
                "started_at":             replay_started_at,
                "finished_at":            replay_finished_at,
                "prompt_tokens":          prompt_tokens,
                "completion_tokens":      completion_tokens,
                "is_replay":              True,
                "replay_from_logic_hash": vcr_hash,
                "state_delta":            replay_state_delta if isinstance(replay_state_delta, dict) else {},
                "args_delta":             replay_args_delta if isinstance(replay_args_delta, dict) else {},
            }
            log.debug(
                "[Logram][Diag E][Save] mode=REPLAY async_gen func=%s run_id=%s step_id=%s vcr_hash=%s payload_preview=%s",
                ctx["func_name"], ctx["run_id"], ctx["step_id"], vcr_hash, _diag_preview(replay_payload),
            )
            await storage.save_step(
                ctx["run_id"], replay_payload, vcr_hash,
                ctx.get("logic_snapshot"),
                state_values=None, arg_values=None,
                callee_registry=ctx.get("callee_registry"),
            )
        except Exception as se:
            log.warning("[Logram] save async gen replay trace failed: %s", se)
        return

    # ── LIVE: shadow-buffer ────────────────────────────────────────────────────
    token      = current_step_id.set(ctx["step_id"])
    started_at = time.time()
    chunks: list[Any] = []
    finished_at: float = started_at
    try:
        async for chunk in func(*args, **kwargs):
            chunks.append(chunk)
            yield chunk
        finished_at = time.time()
    except Exception as exc:
        finished_at = time.time()
        try:
            err_payload = _build_failed_payload(ctx, exc, started_at=started_at, finished_at=finished_at)
            log.debug(
                "[Logram][Diag E][Save] mode=LIVE_FAILED async_gen func=%s run_id=%s step_id=%s vcr_hash=<empty> payload_preview=%s",
                ctx["func_name"], ctx["run_id"], ctx["step_id"], _diag_preview(err_payload),
            )
            await storage.save_step(ctx["run_id"], err_payload, "", ctx.get("logic_snapshot"))
        except Exception as se:
            log.warning("[Logram] save async gen failed (error path): %s", se)
        raise
    finally:
        try:
            current_step_id.reset(token)
        except ValueError:
            pass  # GeneratorExit may resume in a different async Context

    # Stream fully consumed — capture state_after + args_after, then persist.
    state_delta: dict[str, str] = {}
    state_values: dict[str, Any] = {}
    args_delta: dict[str, str] = {}
    arg_values: dict[str, Any] = {}
    if state_instance is not None and effective_state_fields:
        try:
            state_after = _capture_state_snapshot(state_instance, effective_state_fields)
            state_delta, state_values = _compute_state_delta(state_before, state_after)
            log.debug(
                "[Logram][Diag E][StateDelta] async_gen func=%s delta_keys=%s state_values_hashes=%s",
                ctx["func_name"], sorted(state_delta.keys()), sorted(state_values.keys()),
            )
        except Exception as exc:
            log.warning("[Logram] async gen state post-snapshot failed for %s: %s", ctx["func_name"], exc)
    if tracked_arg_names:
        try:
            tracked_args_after = _capture_tracked_args_snapshot(bound_all_args, tracked_arg_names)
            args_delta, arg_values = _compute_state_delta(tracked_args_before, tracked_args_after)
            log.debug(
                "[Logram][Diag E][ArgsDelta] async_gen func=%s delta_keys=%s arg_values_hashes=%s",
                ctx["func_name"], sorted(args_delta.keys()), sorted(arg_values.keys()),
            )
        except Exception as exc:
            log.warning("[Logram] async gen track_args post-snapshot failed for %s: %s", ctx["func_name"], exc)

    try:
        payload = _build_success_payload(ctx, chunks, started_at=started_at, finished_at=finished_at)
        payload["state_delta"] = state_delta
        payload["args_delta"]  = args_delta
        log.debug(
            "[Logram][Diag E][Save] mode=LIVE async_gen func=%s run_id=%s step_id=%s vcr_hash=%s payload_preview=%s",
            ctx["func_name"], ctx["run_id"], ctx["step_id"], vcr_hash, _diag_preview(payload),
        )
        await storage.save_step(
            ctx["run_id"], payload, vcr_hash,
            ctx.get("logic_snapshot"),
            state_values=state_values or None,
            arg_values=arg_values or None,
            callee_registry=ctx.get("callee_registry"),
        )
    except Exception as se:
        log.warning("[Logram] save async gen failed (success path): %s", se)


def _logic_sync_gen(
    func, name, ignore_in_hash, vcr_key_fn, log_input_fn, compact_inputs,
    state_in_hash, include_state, exclude_state, track_args,
    *args, **kwargs,
):
    """Shadow-buffering wrapper for sync generator functions.

    Feature-complete mirror of _logic_sync for generators. Yields items
    transparently while collecting them; saves on full consumption only.
    State + track_args: captured before first yield and after full consumption.
    GeneratorExit (partial consumption) → no state_delta / args_delta persisted.
    """
    try:
        ctx = _prepare_step_ctx(
            func, name, args, kwargs,
            vcr_key_fn=vcr_key_fn,
            log_input_fn=log_input_fn,
            compact_inputs=compact_inputs,
            ignore_in_hash=ignore_in_hash,
        )
    except Exception:
        yield from func(*args, **kwargs)
        return

    # ── State + tracked-args capture — before first yield ─────────────────────
    state_instance, state_include_fields = _get_stateful_target(args)
    effective_state_fields = (
        _compute_effective_state_fields(state_include_fields, include_state, exclude_state)
        if state_in_hash else []
    )
    tracked_arg_names = [str(x) for x in (track_args or []) if str(x)]
    bound_all_args = _bind_all_arguments(func, args, kwargs)
    state_before: dict[str, Any] = {}
    tracked_args_before: dict[str, Any] = {}
    if state_instance is not None and effective_state_fields:
        try:
            state_before = _capture_state_snapshot(state_instance, effective_state_fields)
            log.debug(
                "[Logram][Diag A][StateCapture] phase=before sync_gen func=%s effective_fields=%s snapshot_preview=%s",
                ctx["func_name"], effective_state_fields, _diag_preview(state_before),
            )
        except Exception as exc:
            log.warning("[Logram] sync gen state pre-snapshot failed for %s: %s", ctx["func_name"], exc)
    if tracked_arg_names:
        try:
            tracked_args_before = _capture_tracked_args_snapshot(bound_all_args, tracked_arg_names)
            log.debug(
                "[Logram][Diag A][ArgCapture] phase=before sync_gen func=%s tracked_args=%s snapshot_preview=%s",
                ctx["func_name"], tracked_arg_names, _diag_preview(tracked_args_before),
            )
        except Exception as exc:
            log.warning("[Logram] sync gen track_args pre-snapshot failed for %s: %s", ctx["func_name"], exc)

    vcr_hash, cached_res, replay_state_delta, replay_args_delta = storage.get_vcr_hit(
        ctx["func_name"],
        ctx["vcr_args"],
        ctx["vcr_kwargs"],
        implementation_fingerprint=ctx.get("implementation_fingerprint", ""),
        run_id=ctx.get("run_id"),
        run_input_id=ctx.get("input_id"),
        state_snapshot=state_before if (state_in_hash and state_before) else None,
    )

    force_from        = os.environ.get("LOGRAM_FORCE_FROM", "").strip()
    forced_steps      = {p.strip() for p in os.environ.get("LOGRAM_FORCE_STEP", "").split(",") if p.strip()}
    already_cascading = _is_forced_by_flow.get()
    trig_cascade      = bool(force_from) and ctx["func_name"] == force_from
    force_this_step   = ctx["func_name"] in forced_steps or already_cascading or trig_cascade
    if trig_cascade and not already_cascading:
        _is_forced_by_flow.set(True)

    if os.environ.get("LOGRAM_REPLAY") == "true" and cached_res is not _VCR_MISS and not force_this_step:
        # ── REPLAY: restore state + args, re-yield stored items ────────────────
        replay_started_at  = time.time()
        replay_finished_at = replay_started_at
        prompt_tokens, completion_tokens = _extract_usage_tokens(cached_res)

        if state_instance is not None and isinstance(replay_state_delta, dict) and replay_state_delta:
            try:
                resolved_state = storage.get_state_values(replay_state_delta)
                log.debug(
                    "[Logram][PROBE 4][StateRestoreBegin] sync_gen func=%s "
                    "delta_keys=%s resolved_count=%d missing_count=%d "
                    ">>> If resolved_count < delta key count, values_registry lookup failed (flush race on Run-1?).",
                    ctx["func_name"], sorted(replay_state_delta.keys()),
                    len(resolved_state), len(replay_state_delta) - len(resolved_state),
                )
                for attr_name, raw_value in resolved_state.items():
                    rehydrated = rehydrate_logram_output(raw_value)
                    rehydrated = _coerce_int_str_dict_keys(rehydrated)
                    log.debug(
                        "[Logram][PROBE 4][AttrRestore] sync_gen func=%s attr=%s "
                        "raw_type=%s rehydrated_type=%s rehydrated_preview=%s",
                        ctx["func_name"], attr_name,
                        type(raw_value).__name__, type(rehydrated).__name__, _diag_preview(rehydrated),
                    )
                    setattr(state_instance, attr_name, rehydrated)
                log.debug(
                    "[Logram][Diag E][ReplayState] sync_gen func=%s applied_fields=%s delta_keys=%s",
                    ctx["func_name"],
                    sorted(resolved_state.keys()),
                    sorted(replay_state_delta.keys()),
                )
            except Exception as exc:
                log.warning("[Logram] sync gen state restore failed for %s: %s", ctx["func_name"], exc)

        if isinstance(replay_args_delta, dict) and replay_args_delta:
            try:
                resolved_args = storage.get_state_values(replay_args_delta)
                applied_args: list[str] = []
                for arg_name, raw_value in resolved_args.items():
                    if arg_name not in bound_all_args:
                        continue
                    rehydrated = rehydrate_logram_output(raw_value)
                    if _apply_replayed_arg_value(bound_all_args[arg_name], rehydrated):
                        applied_args.append(arg_name)
                log.debug(
                    "[Logram][Diag E][ReplayArgs] sync_gen func=%s applied_args=%s delta_keys=%s",
                    ctx["func_name"], sorted(applied_args), sorted(replay_args_delta.keys()),
                )
            except Exception as exc:
                log.warning("[Logram] sync gen replay tracked args restore failed for %s: %s", ctx["func_name"], exc)

        stored = _rehydrate_cached_gen(func, cached_res)
        log.debug(
            "[Logram][Diag E][Rehydrate] mode=REPLAY sync_gen func=%s vcr_hash=%s "
            "cached_type=%s cached_preview=%s rehydrated_type=%s rehydrated_preview=%s",
            ctx["func_name"], vcr_hash,
            type(cached_res).__name__, _diag_preview(cached_res),
            type(stored).__name__, _diag_preview(stored),
        )
        yield from (stored if isinstance(stored, list) else [stored])
        replay_finished_at = time.time()
        try:
            replay_payload = {
                "step_id":                ctx["step_id"],
                "parent_id":              ctx["parent_id"],
                "name":                   ctx["func_name"],
                "inputs":                 _safe_tree(ctx.get("log_inputs", {})),
                "output":                 None,
                "status":                 "REPLAYED",
                "duration":               round(max(0.0, replay_finished_at - replay_started_at), 6),
                "timestamp":              replay_finished_at,
                "started_at":             replay_started_at,
                "finished_at":            replay_finished_at,
                "prompt_tokens":          prompt_tokens,
                "completion_tokens":      completion_tokens,
                "is_replay":              True,
                "replay_from_logic_hash": vcr_hash,
                "state_delta":            replay_state_delta if isinstance(replay_state_delta, dict) else {},
                "args_delta":             replay_args_delta if isinstance(replay_args_delta, dict) else {},
            }
            log.debug(
                "[Logram][Diag E][Save] mode=REPLAY sync_gen func=%s run_id=%s step_id=%s vcr_hash=%s payload_preview=%s",
                ctx["func_name"], ctx["run_id"], ctx["step_id"], vcr_hash, _diag_preview(replay_payload),
            )
            storage.save_step_sync(
                ctx["run_id"], replay_payload, vcr_hash,
                ctx.get("logic_snapshot"),
                state_values=None, arg_values=None,
                callee_registry=ctx.get("callee_registry"),
            )
        except Exception as se:
            log.warning("[Logram] save sync gen replay trace failed: %s", se)
        return

    # ── LIVE: shadow-buffer ────────────────────────────────────────────────────
    token      = current_step_id.set(ctx["step_id"])
    started_at = time.time()
    items: list[Any] = []
    finished_at: float = started_at
    try:
        for item in func(*args, **kwargs):
            items.append(item)
            yield item
        finished_at = time.time()
    except Exception as exc:
        finished_at = time.time()
        try:
            err_payload = _build_failed_payload(ctx, exc, started_at=started_at, finished_at=finished_at)
            log.debug(
                "[Logram][Diag E][Save] mode=LIVE_FAILED sync_gen func=%s run_id=%s step_id=%s vcr_hash=<empty> payload_preview=%s",
                ctx["func_name"], ctx["run_id"], ctx["step_id"], _diag_preview(err_payload),
            )
            storage.save_step_sync(ctx["run_id"], err_payload, "", ctx.get("logic_snapshot"))
        except Exception as se:
            log.warning("[Logram] save sync gen failed (error path): %s", se)
        raise
    finally:
        try:
            current_step_id.reset(token)
        except ValueError:
            pass  # GeneratorExit may resume in a different Context

    # Stream fully consumed — capture state_after + args_after, then persist.
    state_delta: dict[str, str] = {}
    state_values: dict[str, Any] = {}
    args_delta: dict[str, str] = {}
    arg_values: dict[str, Any] = {}
    if state_instance is not None and effective_state_fields:
        try:
            state_after = _capture_state_snapshot(state_instance, effective_state_fields)
            state_delta, state_values = _compute_state_delta(state_before, state_after)
            log.debug(
                "[Logram][Diag E][StateDelta] sync_gen func=%s delta_keys=%s state_values_hashes=%s",
                ctx["func_name"], sorted(state_delta.keys()), sorted(state_values.keys()),
            )
        except Exception as exc:
            log.warning("[Logram] sync gen state post-snapshot failed for %s: %s", ctx["func_name"], exc)
    if tracked_arg_names:
        try:
            tracked_args_after = _capture_tracked_args_snapshot(bound_all_args, tracked_arg_names)
            args_delta, arg_values = _compute_state_delta(tracked_args_before, tracked_args_after)
            log.debug(
                "[Logram][Diag E][ArgsDelta] sync_gen func=%s delta_keys=%s arg_values_hashes=%s",
                ctx["func_name"], sorted(args_delta.keys()), sorted(arg_values.keys()),
            )
        except Exception as exc:
            log.warning("[Logram] sync gen track_args post-snapshot failed for %s: %s", ctx["func_name"], exc)

    try:
        payload = _build_success_payload(ctx, items, started_at=started_at, finished_at=finished_at)
        payload["state_delta"] = state_delta
        payload["args_delta"]  = args_delta
        log.debug(
            "[Logram][Diag E][Save] mode=LIVE sync_gen func=%s run_id=%s step_id=%s vcr_hash=%s payload_preview=%s",
            ctx["func_name"], ctx["run_id"], ctx["step_id"], vcr_hash, _diag_preview(payload),
        )
        storage.save_step_sync(
            ctx["run_id"], payload, vcr_hash,
            ctx.get("logic_snapshot"),
            state_values=state_values or None,
            arg_values=arg_values or None,
            callee_registry=ctx.get("callee_registry"),
        )
    except Exception as se:
        log.warning("[Logram] save sync gen failed (success path): %s", se)


def _logic_sync(func, name, ignore_in_hash, track_args, vcr_key_fn, log_input_fn, compact_inputs, state_in_hash, include_state, exclude_state, *args, **kwargs):
    try:
        ctx = _prepare_step_ctx(
            func,
            name,
            args,
            kwargs,
            vcr_key_fn=vcr_key_fn,
            log_input_fn=log_input_fn,
            compact_inputs=compact_inputs,
            ignore_in_hash=ignore_in_hash,
        )
    except Exception:
        return func(*args, **kwargs)

    state_instance, state_include_fields = _get_stateful_target(args)
    effective_state_fields = (
        _compute_effective_state_fields(state_include_fields, include_state, exclude_state)
        if state_in_hash else []
    )
    tracked_arg_names = [str(x) for x in (track_args or []) if str(x)]
    bound_all_args = _bind_all_arguments(func, args, kwargs)
    state_before: dict[str, Any] = {}
    tracked_args_before: dict[str, Any] = {}
    if state_instance is not None and effective_state_fields:
        try:
            state_before = _capture_state_snapshot(state_instance, effective_state_fields)
            log.debug(
                "[Logram][Diag A][StateCapture] phase=before sync func=%s effective_fields=%s snapshot_preview=%s",
                ctx["func_name"],
                effective_state_fields,
                _diag_preview(state_before),
            )
        except Exception as exc:
            log.warning("[Logram] state pre-snapshot failed (sync) for %s: %s", ctx["func_name"], exc)

    if tracked_arg_names:
        try:
            tracked_args_before = _capture_tracked_args_snapshot(bound_all_args, tracked_arg_names)
            log.debug(
                "[Logram][Diag A][ArgCapture] phase=before sync func=%s tracked_args=%s snapshot_preview=%s",
                ctx["func_name"],
                tracked_arg_names,
                _diag_preview(tracked_args_before),
            )
        except Exception as exc:
            log.warning("[Logram] track_args pre-snapshot failed (sync) for %s: %s", ctx["func_name"], exc)

    vcr_hash, cached_res, replay_state_delta, replay_args_delta = storage.get_vcr_hit(
        ctx["func_name"],
        ctx["vcr_args"],
        ctx["vcr_kwargs"],
        implementation_fingerprint=ctx.get("implementation_fingerprint", ""),
        run_id=ctx.get("run_id"),
        run_input_id=ctx.get("input_id"),
        state_snapshot=state_before if (state_in_hash and state_before) else None,
    )

    force_from    = os.environ.get("LOGRAM_FORCE_FROM", "").strip()
    forced_steps  = {p.strip() for p in os.environ.get("LOGRAM_FORCE_STEP", "").split(",") if p.strip()}

    already_cascading  = _is_forced_by_flow.get()
    triggering_cascade = bool(force_from) and ctx["func_name"] == force_from
    force_this_step    = ctx["func_name"] in forced_steps or already_cascading or triggering_cascade

    if triggering_cascade and not already_cascading:
        _is_forced_by_flow.set(True)

    if os.environ.get("LOGRAM_REPLAY") == "true" and cached_res is not _VCR_MISS and not force_this_step:
        replay_started_at = time.time()
        replay_finished_at = replay_started_at
        prompt_tokens, completion_tokens = _extract_usage_tokens(cached_res)

        if state_instance is not None and isinstance(replay_state_delta, dict) and replay_state_delta:
            try:
                resolved_state = storage.get_state_values(replay_state_delta)
                # ── PROBE 4 ── State restoration per-attribute detail (sync) ──────
                log.debug(
                    "[Logram][PROBE 4][StateRestoreBegin] sync func=%s "
                    "delta_keys=%s resolved_count=%d missing_count=%d "
                    ">>> If resolved_count < delta key count, values_registry lookup failed (flush race on Run-1?).",
                    ctx["func_name"],
                    sorted(list(replay_state_delta.keys())),
                    len(resolved_state),
                    len(replay_state_delta) - len(resolved_state),
                )
                for attr_name, raw_value in resolved_state.items():
                    rehydrated_value = rehydrate_logram_output(raw_value)
                    rehydrated_value = _coerce_int_str_dict_keys(rehydrated_value)
                    # ── PROBE 4b ── Per-attribute rehydration detail ───────────────
                    log.debug(
                        "[Logram][PROBE 4][AttrRestore] sync func=%s attr=%s "
                        "raw_type=%s rehydrated_type=%s rehydrated_preview=%s",
                        ctx["func_name"],
                        attr_name,
                        type(raw_value).__name__,
                        type(rehydrated_value).__name__,
                        _diag_preview(rehydrated_value),
                    )
                    setattr(state_instance, attr_name, rehydrated_value)
                # ── END PROBE 4 ──────────────────────────────────────────────────
                log.debug(
                    "[Logram][Diag E][ReplayState] sync func=%s applied_fields=%s delta_keys=%s",
                    ctx["func_name"],
                    sorted(list(resolved_state.keys())),
                    sorted(list(replay_state_delta.keys())),
                )
            except Exception as exc:
                log.warning("[Logram] replay state restore failed (sync) for %s: %s", ctx["func_name"], exc)

        if isinstance(replay_args_delta, dict) and replay_args_delta:
            try:
                resolved_args = storage.get_state_values(replay_args_delta)
                applied_args: list[str] = []
                for arg_name, raw_value in resolved_args.items():
                    if arg_name not in bound_all_args:
                        continue
                    rehydrated_value = rehydrate_logram_output(raw_value)
                    if _apply_replayed_arg_value(bound_all_args[arg_name], rehydrated_value):
                        applied_args.append(arg_name)
                log.debug(
                    "[Logram][Diag E][ReplayArgs] sync func=%s applied_args=%s delta_keys=%s",
                    ctx["func_name"],
                    sorted(applied_args),
                    sorted(list(replay_args_delta.keys())),
                )
            except Exception as exc:
                log.warning("[Logram] replay tracked args restore failed (sync) for %s: %s", ctx["func_name"], exc)

        try:
            replay_payload = {
                "step_id": ctx["step_id"],
                "parent_id": ctx["parent_id"],
                "name": ctx["func_name"],
                "inputs": _safe_tree(ctx.get("log_inputs", {})),
                # Data aliasing: output is copied in storage from the existing row via logic_hash.
                "output": None,
                "status": "REPLAYED",
                "duration": round(max(0.0, replay_finished_at - replay_started_at), 6),
                "timestamp": replay_finished_at,
                "started_at": replay_started_at,
                "finished_at": replay_finished_at,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "is_replay": True,
                "replay_from_logic_hash": vcr_hash,
                "state_delta": replay_state_delta if isinstance(replay_state_delta, dict) else {},
                "args_delta": replay_args_delta if isinstance(replay_args_delta, dict) else {},
            }
            log.debug(
                "[Logram][Diag E][Save] mode=REPLAY sync func=%s run_id=%s step_id=%s vcr_hash=%s payload_preview=%s",
                ctx["func_name"],
                ctx["run_id"],
                ctx["step_id"],
                vcr_hash,
                _diag_preview(replay_payload),
            )
            storage.save_step_sync(
                ctx["run_id"],
                replay_payload,
                vcr_hash,
                ctx.get("logic_snapshot"),
                state_values=None,
                arg_values=None,
                callee_registry=ctx.get("callee_registry"),
            )
        except Exception as se:
            log.warning("[Logram] save sync replay trace failed: %s", se)
        rehydrated = _rehydrate_cached(func, cached_res)
        log.debug(
            "[Logram][Diag E][Rehydrate] mode=REPLAY sync func=%s vcr_hash=%s cached_type=%s cached_preview=%s rehydrated_type=%s rehydrated_preview=%s",
            ctx["func_name"],
            vcr_hash,
            type(cached_res).__name__,
            _diag_preview(cached_res),
            type(rehydrated).__name__,
            _diag_preview(rehydrated),
        )
        return rehydrated

    token = current_step_id.set(ctx["step_id"])
    started_at = time.time()
    try:
        result = func(*args, **kwargs)
        finished_at = time.time()
        state_delta: dict[str, str] = {}
        state_values: dict[str, Any] = {}
        args_delta: dict[str, str] = {}
        arg_values: dict[str, Any] = {}
        if state_instance is not None and effective_state_fields:
            try:
                state_after = _capture_state_snapshot(state_instance, effective_state_fields)
                state_delta, state_values = _compute_state_delta(state_before, state_after)
                log.debug(
                    "[Logram][Diag E][StateDelta] sync func=%s delta_keys=%s state_values_hashes=%s",
                    ctx["func_name"],
                    sorted(list(state_delta.keys())),
                    sorted(list(state_values.keys())),
                )
            except Exception as exc:
                log.warning("[Logram] state delta capture failed (sync) for %s: %s", ctx["func_name"], exc)
        if tracked_arg_names:
            try:
                tracked_args_after = _capture_tracked_args_snapshot(bound_all_args, tracked_arg_names)
                args_delta, arg_values = _compute_state_delta(tracked_args_before, tracked_args_after)
                log.debug(
                    "[Logram][Diag E][ArgsDelta] sync func=%s delta_keys=%s arg_values_hashes=%s",
                    ctx["func_name"],
                    sorted(list(args_delta.keys())),
                    sorted(list(arg_values.keys())),
                )
            except Exception as exc:
                log.warning("[Logram] track_args delta capture failed (sync) for %s: %s", ctx["func_name"], exc)
        try:
            payload = _build_success_payload(ctx, result, started_at=started_at, finished_at=finished_at)
            payload["state_delta"] = state_delta
            payload["args_delta"] = args_delta
            log.debug(
                "[Logram][Diag E][Save] mode=LIVE sync func=%s run_id=%s step_id=%s vcr_hash=%s payload_preview=%s",
                ctx["func_name"],
                ctx["run_id"],
                ctx["step_id"],
                vcr_hash,
                _diag_preview(payload),
            )
            storage.save_step_sync(
                ctx["run_id"],
                payload,
                vcr_hash,
                ctx.get("logic_snapshot"),
                state_values=state_values,
                arg_values=arg_values,
                callee_registry=ctx.get("callee_registry"),
            )
        except Exception as se:
            log.warning("[Logram] save sync failed: %s", se)
        return result
    except Exception as exc:
        finished_at = time.time()
        try:
            err_payload = _build_failed_payload(ctx, exc, started_at=started_at, finished_at=finished_at)
            log.debug(
                "[Logram][Diag E][Save] mode=LIVE_FAILED sync func=%s run_id=%s step_id=%s vcr_hash=<empty> payload_preview=%s",
                ctx["func_name"],
                ctx["run_id"],
                ctx["step_id"],
                _diag_preview(err_payload),
            )
            storage.save_step_sync(ctx["run_id"], err_payload, "", ctx.get("logic_snapshot"))
        except Exception as se:
            log.warning("[Logram] save sync failure trace failed: %s", se)
        raise
    finally:
        current_step_id.reset(token)


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
