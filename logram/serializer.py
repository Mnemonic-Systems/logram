# ruff: noqa: BLE001
# pylint: disable=broad-exception-caught

"""Recursive conversion to JSON-safe trees (dict / list / str / int / float / bool / null)."""

from __future__ import annotations

import hashlib
import importlib
import logging
import math
import os
import threading
from dataclasses import fields as dc_fields, is_dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping, Optional, Set
from uuid import UUID

from pydantic import BaseModel

log = logging.getLogger(__name__)

_MAX_SERIALIZE_DEPTH = 400

# New generic hidden tags.
_AF_MODEL_KEY = "__af_model__"
_AF_MODULE_KEY = "__af_module__"
_AF_KIND_KEY = "__af_kind__"
_AF_STATE_KEY = "state"

# Legacy wrappers still supported for backward compatibility.
_AF_PYDANTIC_KEY = "__logram_pydantic__"
_AF_DATACLASS_KEY = "__logram_dataclass__"


def _detect_project_root() -> Path:
    """Best-effort project root detection for stable asset storage location."""
    env_root = os.environ.get("LOGRAM_PROJECT_ROOT")
    if env_root:
        try:
            return Path(env_root).expanduser().resolve()
        except Exception:
            pass

    markers = (".git", "pyproject.toml", "poetry.lock", "uv.lock", "package.json")
    candidates = [Path.cwd().resolve(), Path(__file__).resolve().parent]

    seen: set[Path] = set()
    for start in candidates:
        cur = start
        while cur not in seen:
            seen.add(cur)
            if any((cur / marker).exists() for marker in markers):
                return cur
            if cur.parent == cur:
                break
            cur = cur.parent

    return Path.cwd().resolve()


class BlobManager:
    def __init__(self, base_path: str | Path = ".logram_assets"):
        self.project_root = _detect_project_root()
        raw_base = Path(base_path)
        self.base_path = raw_base if raw_base.is_absolute() else (self.project_root / raw_base)
        self.base_path.mkdir(parents=True, exist_ok=True)
        if not os.access(self.base_path, os.W_OK):
            raise PermissionError(f"Logram assets directory is not writable: {self.base_path}")
        self._lock = threading.Lock()

    def save_blob(self, data: bytes, ext: str = "bin") -> dict[str, Any]:
        h = hashlib.sha256(data).hexdigest()
        ext_clean = str(ext or "bin").lstrip(".")
        filename = f"{h}.{ext_clean}"
        path = self.base_path / filename
        with self._lock:
            if not path.exists():
                path.write_bytes(data)

        try:
            relative_path = path.resolve().relative_to(self.project_root)
            path_str = str(relative_path)
        except Exception:
            path_str = str(path)

        return {
            "__af_blob__": True,
            "path": path_str,
            "hash": h,
            "size": len(data),
        }


def _is_pydantic_model(obj: Any) -> bool:
    if isinstance(obj, BaseModel):
        return True
    try:
        from pydantic.v1 import BaseModel as BaseModelV1  # type: ignore

        return isinstance(obj, BaseModelV1)
    except Exception:
        return False


def _model_to_dict(obj: Any) -> dict[str, Any]:
    if hasattr(obj, "model_dump"):
        try:
            return obj.model_dump(mode="python", round_trip=True)
        except TypeError:
            return obj.model_dump(mode="python")
    return obj.dict()


def _resolve_tagged_class(module_name: str, model_name: str) -> type | None:
    try:
        mod = importlib.import_module(module_name)
    except Exception as e:
        log.warning("[Logram] Cannot import module %r: %s", module_name, e)
        return None

    obj: Any = mod
    for part in model_name.split("."):
        obj = getattr(obj, part, None)
        if obj is None:
            log.warning(
                "[Logram] Missing attribute %r while resolving %s.%s",
                part,
                module_name,
                model_name,
            )
            return None
    return obj if isinstance(obj, type) else None


def _construct_pydantic(model_cls: type, data: Any) -> Any:
    if hasattr(model_cls, "model_validate"):
        return model_cls.model_validate(data)
    return model_cls.parse_obj(data)


def _is_new_model_tag(obj: Any) -> bool:
    return (
        isinstance(obj, dict)
        and isinstance(obj.get(_AF_MODEL_KEY), str)
        and isinstance(obj.get(_AF_MODULE_KEY), str)
        and isinstance(obj.get(_AF_KIND_KEY), str)
        and _AF_STATE_KEY in obj
    )


def _legacy_to_new(obj: Any) -> dict[str, Any] | None:
    if isinstance(obj, dict) and _AF_PYDANTIC_KEY in obj and isinstance(obj[_AF_PYDANTIC_KEY], dict):
        meta = obj[_AF_PYDANTIC_KEY]
        return {
            _AF_MODEL_KEY: meta.get("qualname", ""),
            _AF_MODULE_KEY: meta.get("module", ""),
            _AF_KIND_KEY: "pydantic",
            _AF_STATE_KEY: meta.get("state"),
        }
    if isinstance(obj, dict) and _AF_DATACLASS_KEY in obj and isinstance(obj[_AF_DATACLASS_KEY], dict):
        meta = obj[_AF_DATACLASS_KEY]
        return {
            _AF_MODEL_KEY: meta.get("qualname", ""),
            _AF_MODULE_KEY: meta.get("module", ""),
            _AF_KIND_KEY: "dataclass",
            _AF_STATE_KEY: meta.get("state"),
        }
    return None


def _coerce_dataclass_fields(cls: type, state: Any) -> Any:
    """Backward compat: convert plain-dict values to nested dataclasses using type hints.

    Old cache recorded with asdict() flattened nested DCs to plain dicts.
    This repairs list[SomeDC] and SomeDC fields when the stored value is a dict.
    """
    if not isinstance(state, dict):
        return state
    try:
        import typing
        hints = typing.get_type_hints(cls)
    except Exception:
        return state

    repaired = dict(state)
    for field_name, hint in hints.items():
        if field_name not in repaired:
            continue
        val = repaired[field_name]
        origin = getattr(hint, "__origin__", None)
        args = getattr(hint, "__args__", ())
        # list[SomeDC] where items are plain dicts (old format)
        if origin is list and args and is_dataclass(args[0]) and isinstance(val, list):
            item_cls = args[0]
            repaired[field_name] = [
                item_cls(**item) if isinstance(item, dict) and not _is_new_model_tag(item) else item
                for item in val
            ]
        # SomeDC directly (non-list)
        elif is_dataclass(hint) and isinstance(val, dict) and not _is_new_model_tag(val):
            repaired[field_name] = hint(**val)
    return repaired


def rehydrate_logram_output(obj: Any) -> Any:
    """
    Rebuild recursively tagged Pydantic/dataclass instances.

    Fallback chain:
    - best effort class reconstruction from __af_model__/__af_module__
    - if class import/validation fails: return rehydrated state
    - for unknown objects: return raw dict/list values
    """
    tagged = obj if _is_new_model_tag(obj) else _legacy_to_new(obj)
    if tagged is not None:
        module_name = tagged.get(_AF_MODULE_KEY)
        model_name = tagged.get(_AF_MODEL_KEY)
        kind = tagged.get(_AF_KIND_KEY)
        state = rehydrate_logram_output(tagged.get(_AF_STATE_KEY))

        if not isinstance(module_name, str) or not isinstance(model_name, str):
            return state

        cls = _resolve_tagged_class(module_name, model_name)
        if cls is None:
            return state

        try:
            if kind == "pydantic":
                return _construct_pydantic(cls, state)
            if kind == "dataclass" and is_dataclass(cls):
                state = _coerce_dataclass_fields(cls, state)
                return cls(**state)
        except Exception as e:
            log.warning(
                "[Logram] Rehydrate failed for %s.%s (%s): %s",
                module_name,
                model_name,
                kind,
                e,
            )
            return state

        return state

    if isinstance(obj, list):
        return [rehydrate_logram_output(x) for x in obj]
    if isinstance(obj, dict):
        return {k: rehydrate_logram_output(v) for k, v in obj.items()}
    return obj


def ensure_serializable(
    obj: Any,
    *,
    blob_manager: Optional[BlobManager] = None,
    _seen: Optional[Set[int]] = None,
    _depth: int = 0,
) -> Any:
    if _depth > _MAX_SERIALIZE_DEPTH:
        return "<max serialization depth>"
    if _seen is None:
        _seen = set()

    if obj is None or isinstance(obj, bool):
        return obj
    if isinstance(obj, str):
        return obj
    if isinstance(obj, int) and not isinstance(obj, bool):
        return obj
    if isinstance(obj, float):
        if math.isfinite(obj):
            return obj
        return str(obj)

    # Absolute priority for binary payloads, before any model transformation.
    if isinstance(obj, (bytes, bytearray, memoryview)):
        raw = bytes(obj)
        if blob_manager is not None:
            return blob_manager.save_blob(raw, "bin")
        return "<<Binary Data>>"

    if isinstance(obj, UUID):
        return str(obj)
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    if isinstance(obj, timedelta):
        return str(obj)
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, Enum):
        try:
            return obj.value
        except Exception:
            return str(obj)

    if _is_pydantic_model(obj):
        return {
            _AF_MODEL_KEY: obj.__class__.__qualname__,
            _AF_MODULE_KEY: obj.__class__.__module__,
            _AF_KIND_KEY: "pydantic",
            _AF_STATE_KEY: ensure_serializable(
                _model_to_dict(obj),
                blob_manager=blob_manager,
                _seen=_seen,
                _depth=_depth + 1,
            ),
        }

    if is_dataclass(obj) and not isinstance(obj, type):
        try:
            dumped = {f.name: getattr(obj, f.name) for f in dc_fields(obj)}
        except Exception:
            return str(obj)
        return {
            _AF_MODEL_KEY: obj.__class__.__qualname__,
            _AF_MODULE_KEY: obj.__class__.__module__,
            _AF_KIND_KEY: "dataclass",
            _AF_STATE_KEY: ensure_serializable(
                dumped,
                blob_manager=blob_manager,
                _seen=_seen,
                _depth=_depth + 1,
            ),
        }

    if isinstance(obj, MappingProxyType):
        obj = dict(obj)

    if isinstance(obj, Mapping):
        oid = id(obj)
        if oid in _seen:
            return "<circular reference>"
        _seen.add(oid)
        try:
            out: dict[str, Any] = {}
            for k, v in obj.items():
                sk = k if isinstance(k, str) else str(k)
                out[sk] = ensure_serializable(v, blob_manager=blob_manager, _seen=_seen, _depth=_depth + 1)
            return out
        finally:
            _seen.discard(oid)

    if isinstance(obj, (list, tuple)):
        oid = id(obj)
        if oid in _seen:
            return "<circular reference>"
        _seen.add(oid)
        try:
            return [
                ensure_serializable(x, blob_manager=blob_manager, _seen=_seen, _depth=_depth + 1)
                for x in obj
            ]
        finally:
            _seen.discard(oid)

    if isinstance(obj, (set, frozenset)):
        return [
            ensure_serializable(x, blob_manager=blob_manager, _seen=_seen, _depth=_depth + 1)
            for x in obj
        ]

    return str(obj)


def dumps_json_safe(obj: Any, *, blob_manager: Optional[BlobManager] = None) -> str:
    import json

    safe = ensure_serializable(obj, blob_manager=blob_manager)
    return json.dumps(safe, ensure_ascii=False)


def loads_json_copy(s: str) -> Any:
    import json

    return json.loads(s)
