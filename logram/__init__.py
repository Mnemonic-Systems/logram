from __future__ import annotations

import functools
import inspect
import os
import uuid
from datetime import datetime
from typing import Any, Callable

from .context import current_input_id, current_run_id
from .decorators import clear_logic_snapshot_cache, stateful, storage, trace
from .serializer import rehydrate_logram_output
from .versioning import get_semantic_version

__version__ = "0.2.0"


def set_run_id(run_id: str, *, verbose: bool = False) -> str:
    """Bind an explicit run id to the current async context."""
    current_run_id.set(run_id)
    if verbose:
        print(f"[LOGRAM] Session initialized: {run_id}")
        if os.environ.get("LOGRAM_REPLAY") == "true":
            print("[LOGRAM] REPLAY MODE enabled (Time-Travel)")
    return run_id


def init(
    project: str,
    run_name: str | None = None,
    input_id: str | None = None,
    tags: list[str] | None = None,
) -> str:
    """
    Initialize Logram lifecycle metadata and bind current run id.
    """
    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    rid = f"{run_name}_{ts}" if run_name else f"run_{ts}"
    input_key = input_id or "unknown_input"
    version_id = get_semantic_version()

    # Force fresh on-disk/introspection scan for each new run lifecycle.
    clear_logic_snapshot_cache()

    set_run_id(rid, verbose=True)
    current_input_id.set(input_key)
    storage.init_run(
        rid,
        project=project,
        input_id=input_key,
        version_id=version_id,
        tags=tags or [],
    )
    return rid


async def flush(timeout: float = 10.0) -> bool:
    """Force persistence of pending traces."""
    return await storage.flush(timeout=timeout)


async def finalize(status: str = "success", metrics: dict[str, Any] | None = None) -> bool:
    """
    Close current run, update index registry, and flush pending traces.
    """
    run_id = current_run_id.get() or "default_run"
    storage.finalize_run(run_id, status=status, metrics=metrics or {})
    return await storage.flush()


# Backward-compatible alias kept for existing imports.
async def flush_traces() -> None:
    await flush()


def bind_session_run(
    session: Any,
    *,
    prefix: str = "run",
    session_id_attr: str = "id",
    resolver: Callable[[Any], str] | None = None,
    verbose: bool = False,
) -> str:
    """
    Build and bind a stable run id from an arbitrary session-like object.

    Default format: ``{prefix}_{session.<session_id_attr>}``.
    """
    if resolver is not None:
        run_id = resolver(session)
    else:
        session_value = getattr(session, session_id_attr, None) if session is not None else None
        if session_value is None:
            session_value = uuid.uuid4().hex[:8]
        run_id = f"{prefix}_{session_value}"
    return set_run_id(run_id, verbose=verbose)


def worker_init(
    run_id: str,
    input_id: str | None = None,
    *,
    verbose: bool = False,
) -> None:
    """
    Initialize Logram context in a worker process spawned by multiprocessing.

    ContextVars do not propagate across process boundaries — each worker starts
    with run_id=None, which causes all traced steps to be written under
    'default_run' and mixed together. Call this as the pool initializer to bind
    the parent run_id to every worker.

    Usage::

        run_id = logram.init(project="my_pipeline", input_id="doc_42")

        with ProcessPoolExecutor(
            initializer=logram.worker_init,
            initargs=(run_id,),
        ) as pool:
            results = list(pool.map(process_tile, tiles))

    Notes:
    - Do NOT call logram.init() in workers — the run is already registered in
      the parent process and the DB is shared via SQLite WAL.
    - Works with ProcessPoolExecutor, multiprocessing.Pool, Ray, and Celery
      (pass as worker_init / task_prerun signal handler).
    - On Linux (fork start method), prefer 'spawn' or 'forkserver' to avoid
      inheriting open SQLite connections from the parent.
    """
    import atexit
    from .decorators import storage as _storage

    current_run_id.set(run_id)
    if input_id is not None:
        current_input_id.set(input_id)

    # The write thread inside each worker process is a daemon — it is killed
    # when the process exits, before it can flush the queue. Registering
    # flush_sync as an atexit handler ensures every enqueued step is written
    # to SQLite before the worker process terminates.
    atexit.register(_storage.flush_sync)

    if verbose:
        import os as _os
        print(f"[LOGRAM] Worker {_os.getpid()} initialized: run_id={run_id}")


def with_session_run(
    *,
    prefix: str = "run",
    session_arg: str = "session",
    session_id_attr: str = "id",
    resolver: Callable[[Any], str] | None = None,
    verbose: bool = False,
):
    """
    Decorator for sync/async entrypoints to auto-bind a stable run id from a
    session argument.
    """

    def decorator(func):
        sig = inspect.signature(func)
        is_async = inspect.iscoroutinefunction(func)

        def _bind_from_call(args, kwargs) -> str:
            bound = sig.bind_partial(*args, **kwargs)
            session = bound.arguments.get(session_arg)
            return bind_session_run(
                session,
                prefix=prefix,
                session_id_attr=session_id_attr,
                resolver=resolver,
                verbose=verbose,
            )

        if is_async:

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                _bind_from_call(args, kwargs)
                return await func(*args, **kwargs)

            return async_wrapper

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            _bind_from_call(args, kwargs)
            return func(*args, **kwargs)

        return sync_wrapper

    return decorator


__all__ = [
    "trace",
    "stateful",
    "init",
    "finalize",
    "flush",
    "set_run_id",
    "bind_session_run",
    "with_session_run",
    "worker_init",
    "current_run_id",
    "current_input_id",
    "flush_traces",
    "rehydrate_logram_output",
    "get_semantic_version",
]