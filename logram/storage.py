# ruff: noqa: BLE001
# pylint: disable=broad-exception-caught

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import queue
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from .metrics import compute_time_savings, compute_token_totals
from .serializer import _detect_project_root

_BATCH_MAX_ITEMS = 50
_BATCH_FLUSH_INTERVAL_SEC = 0.5
_DB_TIMEOUT_SEC = 30.0

log = logging.getLogger(__name__)

# Sentinel that distinguishes a true VCR cache miss from a cached function that
# legitimately returned None.  Callers must use `result is _VCR_MISS` (not
# `result is None`) to test for absence of a cached entry.
_VCR_MISS: object = object()


@dataclass(slots=True)
class _StepRequest:
    run_id: str
    step_data: dict[str, Any]
    vcr_hash: str
    logic_snapshot: dict[str, Any] | None = None
    state_values: dict[str, Any] | None = None
    arg_values: dict[str, Any] | None = None
    callee_registry: dict[str, Any] | None = None


@dataclass(slots=True)
class _RunUpdateRequest:
    run_id: str
    status: str
    metrics: dict[str, Any]


class _FlushRequest:
    __slots__ = ("done",)

    def __init__(self) -> None:
        self.done = threading.Event()


class _StopRequest:
    __slots__ = ()


class TraceStorage:
    """
    SQLite-backed Logram storage.

    DB path: .logram/logram.db
    - runs
    - steps
    - logic_registry
    """

    def __init__(self, filename: str = ".logram_traces.json") -> None:
        # Kept for backward compatibility with old constructor signature.
        self.legacy_path = Path(filename)

        self.base_dir = _detect_project_root() / ".logram"
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.base_dir / "logram.db"

        self._queue: queue.Queue[_StepRequest | _RunUpdateRequest | _FlushRequest | _StopRequest] = queue.Queue(maxsize=50_000)
        self._worker_started = False
        self._worker_stop = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None

        self._setup_lock = threading.RLock()
        self._db_ready = False
        self._replay_hint_shown = False

    def _replay_mode(self) -> bool:
        return os.environ.get("LOGRAM_REPLAY") == "true"

    def _check_and_warn_replay_available(self) -> None:
        if self._replay_hint_shown or self._replay_mode():
            return
        self._replay_hint_shown = True
        try:
            conn = sqlite3.connect(self.db_path, timeout=5.0)
            try:
                self._configure_connection(conn)
                row = conn.execute(
                    "SELECT COUNT(*) FROM steps WHERE status = 'SUCCESS'"
                ).fetchone()
                count = int(row[0]) if row else 0
            finally:
                conn.close()
            if count > 0:
                print(
                    f"[LOGRAM] {count} step(s) en cache — "
                    "relance avec 'logram replay <script>.py' pour activer le Time-Travel."
                )
        except Exception:
            pass

    def _diag_preview(self, value: Any, max_len: int = 700) -> str:
        try:
            rendered = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
        except Exception:
            try:
                rendered = repr(value)
            except Exception:
                rendered = "<unrepresentable>"
        if len(rendered) > max_len:
            return f"{rendered[:max_len]}...<truncated:{len(rendered) - max_len}>"
        return rendered

    def _configure_connection(self, conn: sqlite3.Connection) -> None:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA foreign_keys=ON;")

    def _ensure_db(self) -> None:
        if self._db_ready:
            return
        with self._setup_lock:
            if self._db_ready:
                return
            try:
                conn = sqlite3.connect(self.db_path, timeout=_DB_TIMEOUT_SEC)
                try:
                    self._configure_connection(conn)
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS runs (
                            run_id TEXT PRIMARY KEY,
                            project TEXT,
                            input_id TEXT,
                            version_id TEXT,
                            status TEXT,
                            tags TEXT,
                            metrics_json TEXT,
                            resource_time_saved REAL,
                            wait_time_saved REAL,
                            total_prompt_tokens INTEGER,
                            total_completion_tokens INTEGER,
                            created_at REAL,
                            updated_at REAL
                        )
                        """
                    )
                    run_cols = [r[1] for r in conn.execute("PRAGMA table_info(runs)").fetchall()]
                    if "resource_time_saved" not in run_cols:
                        conn.execute("ALTER TABLE runs ADD COLUMN resource_time_saved REAL")
                    if "wait_time_saved" not in run_cols:
                        conn.execute("ALTER TABLE runs ADD COLUMN wait_time_saved REAL")
                    if "total_prompt_tokens" not in run_cols:
                        conn.execute("ALTER TABLE runs ADD COLUMN total_prompt_tokens INTEGER")
                    if "total_completion_tokens" not in run_cols:
                        conn.execute("ALTER TABLE runs ADD COLUMN total_completion_tokens INTEGER")
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS logic_registry (
                            logic_hash TEXT PRIMARY KEY,
                            name TEXT,
                            source_code TEXT,
                            globals_json TEXT,
                            resolved_globals TEXT,
                            called_functions_json TEXT,
                            signature TEXT
                        )
                        """
                    )
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS values_registry (
                            value_hash TEXT PRIMARY KEY,
                            value_json TEXT
                        )
                        """
                    )
                    # Backward-compatible migration for existing DBs.
                    cols = [r[1] for r in conn.execute("PRAGMA table_info(logic_registry)").fetchall()]
                    if "resolved_globals" not in cols:
                        conn.execute("ALTER TABLE logic_registry ADD COLUMN resolved_globals TEXT")
                    if "called_functions_json" not in cols:
                        conn.execute("ALTER TABLE logic_registry ADD COLUMN called_functions_json TEXT")
                    conn.execute(
                        """
                        CREATE TABLE IF NOT EXISTS steps (
                            step_id TEXT PRIMARY KEY,
                            run_id TEXT,
                            parent_step_id TEXT,
                            name TEXT,
                            inputs_json TEXT,
                            output_json TEXT,
                            logic_hash TEXT,
                            status TEXT,
                            duration REAL,
                            started_at REAL,
                            finished_at REAL,
                            prompt_tokens INTEGER,
                            completion_tokens INTEGER,
                            error_json TEXT,
                            timestamp REAL,
                            FOREIGN KEY(run_id) REFERENCES runs(run_id),
                            FOREIGN KEY(logic_hash) REFERENCES logic_registry(logic_hash)
                        )
                        """
                    )
                    step_cols = [r[1] for r in conn.execute("PRAGMA table_info(steps)").fetchall()]
                    if "started_at" not in step_cols:
                        conn.execute("ALTER TABLE steps ADD COLUMN started_at REAL")
                    if "finished_at" not in step_cols:
                        conn.execute("ALTER TABLE steps ADD COLUMN finished_at REAL")
                    if "prompt_tokens" not in step_cols:
                        conn.execute("ALTER TABLE steps ADD COLUMN prompt_tokens INTEGER")
                    if "completion_tokens" not in step_cols:
                        conn.execute("ALTER TABLE steps ADD COLUMN completion_tokens INTEGER")
                    if "state_delta" not in step_cols:
                        conn.execute("ALTER TABLE steps ADD COLUMN state_delta TEXT")
                    if "args_delta" not in step_cols:
                        conn.execute("ALTER TABLE steps ADD COLUMN args_delta TEXT")
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_steps_logic_hash_status ON steps(logic_hash, status)")
                    conn.execute("CREATE INDEX IF NOT EXISTS idx_steps_run_id ON steps(run_id)")
                    conn.commit()
                    self._db_ready = True
                finally:
                    conn.close()
            except Exception:
                # Zero-crash contract: tracing infra must not break pipeline.
                self._db_ready = False

    def _ensure_worker(self) -> None:
        if self._worker_started and self._worker_thread and self._worker_thread.is_alive():
            return

        self._ensure_db()
        self._worker_stop.clear()
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True, name="logram-sqlite-writer")
        self._worker_thread.start()
        self._worker_started = True

    def _safe_json_dumps(self, value: Any) -> str:
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            try:
                return json.dumps(str(value), ensure_ascii=False)
            except Exception:
                return "null"

    def _safe_json_loads(self, value: str | None) -> Any:
        if value is None:
            return None
        try:
            return json.loads(value)
        except Exception:
            return None

    def _is_meaningful_logic_snapshot(self, logic_snapshot: dict[str, Any] | None) -> bool:
        if not isinstance(logic_snapshot, dict) or not logic_snapshot:
            return False
        # Oracle-era snapshots always carry a structural_hash; legacy snapshots used source_normalized.
        structural = str(logic_snapshot.get("structural_hash") or "").strip()
        src = str(logic_snapshot.get("source_normalized") or logic_snapshot.get("source") or "").strip()
        resolved = logic_snapshot.get("resolved_globals", logic_snapshot.get("globals"))
        called = logic_snapshot.get("called_functions")
        has_globals = isinstance(resolved, dict) and len(resolved) > 0
        has_called = isinstance(called, dict) and len(called) > 0
        return bool(structural) or bool(src) or has_globals or has_called

    def _ensure_run_exists(
        self,
        conn: sqlite3.Connection,
        run_id: str,
        *,
        project: str = "default",
        input_id: str = "unknown_input",
        version_id: str = "unknown_version",
        tags: Optional[list[str]] = None,
        status: str = "running",
    ) -> None:
        now = time.time()
        conn.execute(
            """
            INSERT INTO runs (run_id, project, input_id, version_id, status, tags, metrics_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                project=CASE
                    WHEN excluded.project IN ('', 'default') THEN runs.project
                    ELSE COALESCE(NULLIF(excluded.project, ''), runs.project)
                END,
                input_id=CASE
                    WHEN excluded.input_id IN ('', 'unknown_input') THEN runs.input_id
                    ELSE COALESCE(NULLIF(excluded.input_id, ''), runs.input_id)
                END,
                version_id=CASE
                    WHEN excluded.version_id IN ('', 'unknown_version') THEN runs.version_id
                    ELSE COALESCE(NULLIF(excluded.version_id, ''), runs.version_id)
                END,
                tags=CASE WHEN runs.tags IS NULL OR runs.tags = '' THEN excluded.tags ELSE runs.tags END,
                status=excluded.status,
                updated_at=excluded.updated_at
            """,
            (
                run_id,
                project,
                input_id,
                version_id,
                status,
                self._safe_json_dumps(tags or []),
                self._safe_json_dumps({}),
                now,
                now,
            ),
        )

    def init_run(
        self,
        run_id: str,
        *,
        project: str,
        input_id: str,
        version_id: str,
        tags: Optional[list[str]] = None,
    ) -> None:
        try:
            self._ensure_db()
            conn = sqlite3.connect(self.db_path, timeout=_DB_TIMEOUT_SEC)
            try:
                self._configure_connection(conn)
                self._ensure_run_exists(
                    conn,
                    run_id,
                    project=project,
                    input_id=input_id,
                    version_id=version_id,
                    tags=tags or [],
                    status="running",
                )
                conn.commit()
            finally:
                conn.close()

            self._ensure_worker()
        except Exception:
            return

        self._check_and_warn_replay_available()

    def finalize_run(self, run_id: str, *, status: str = "success", metrics: Optional[dict[str, Any]] = None) -> None:
        try:
            self._ensure_worker()
            req = _RunUpdateRequest(run_id=run_id, status=status, metrics=metrics or {})
            self._queue.put_nowait(req)
        except queue.Full:
            return
        except Exception:
            return

    def register_logic(self, logic_snapshot: dict[str, Any] | None) -> str:
        if not self._is_meaningful_logic_snapshot(logic_snapshot):
            return ""
        try:
            payload = json.dumps(logic_snapshot, ensure_ascii=False, sort_keys=True).encode("utf-8")
            return hashlib.sha256(payload).hexdigest()
        except Exception:
            return ""

    def get_vcr_hit(
        self,
        func_name: str,
        args: Any,
        kwargs: Any,
        implementation_fingerprint: str = "",
        run_id: str | None = None,
        run_input_id: str | None = None,
        state_snapshot: dict[str, Any] | None = None,
    ) -> tuple[str, Any, dict[str, str] | None, dict[str, str] | None]:
        if run_input_id is None and run_id:
            try:
                run_input_id = self.get_run_input_id(run_id)
            except Exception:
                run_input_id = None
        resolved_input_id = str(run_input_id or "unknown_input")
        state_part = ""
        if state_snapshot:
            try:
                state_part = f"-state={json.dumps(state_snapshot, ensure_ascii=False, sort_keys=True)}"
            except Exception:
                state_part = f"-state={state_snapshot!r}"
        key_input = f"{func_name}-{args}-{kwargs}-{implementation_fingerprint}-input_id={resolved_input_id}{state_part}"
        input_in_key = resolved_input_id in key_input
        log.debug(
            "[Logram][Diag Input/VCR] func=%s run_id=%s run_input_id=%r input_id_in_hash_preimage=%s",
            func_name,
            run_id,
            resolved_input_id,
            input_in_key,
        )
        log.debug(
            "[Logram][Diag C][VCR Hash] func=%s fingerprint_len=%d key_input_len=%d key_input_raw=%r",
            func_name,
            len(str(implementation_fingerprint or "")),
            len(key_input),
            key_input,
        )
        log.debug(
            "[Logram][Diag C][VCR Hash Preimage Exact] func=%s preimage_exact=%s",
            func_name,
            key_input,
        )
        # ── PROBE 3 ── Hash key component breakdown for Run-1/Run-2 diff ────────
        log.debug(
            "[Logram][PROBE 3][HashComponents] func=%s "
            "COMPONENT_func_name=%r "
            "COMPONENT_args_repr=%r "
            "COMPONENT_kwargs_repr=%r "
            "COMPONENT_impl_fingerprint=%s "
            ">>> Copy these 4 lines from Run-1 and Run-2 and diff them to find which component changed.",
            func_name,
            func_name,
            str(args)[:600],
            str(kwargs)[:200],
            implementation_fingerprint or "<empty>",
        )
        # ── END PROBE 3 ─────────────────────────────────────────────────────────
        logic_hash = hashlib.sha256(key_input.encode("utf-8", errors="replace")).hexdigest()
        log.debug("[Logram][Diag C][VCR Hash] func=%s computed_logic_hash=%s", func_name, logic_hash)

        if not self._replay_mode():
            log.debug("[Logram][Diag D][VCR Lookup] func=%s replay_mode=false => lookup_skipped", func_name)
            return logic_hash, _VCR_MISS, None, None

        try:
            self._ensure_db()
            conn = sqlite3.connect(self.db_path, timeout=_DB_TIMEOUT_SEC)
            try:
                self._configure_connection(conn)
                row = conn.execute(
                    "SELECT output_json, state_delta, args_delta FROM steps WHERE logic_hash = ? AND status IN ('SUCCESS', 'REPLAYED') ORDER BY timestamp DESC LIMIT 1",
                    (logic_hash,),
                ).fetchone()
            finally:
                conn.close()

            if not row:
                log.debug(
                    "[Logram][Diag D][VCR Lookup] func=%s logic_hash=%s result=MISS db_path=%s",
                    func_name,
                    logic_hash,
                    self.db_path,
                )
                # ── PROBE 5 ── MISS diagnostic: show all stored hashes for same func ─
                try:
                    conn2 = sqlite3.connect(self.db_path, timeout=_DB_TIMEOUT_SEC)
                    try:
                        self._configure_connection(conn2)
                        existing = conn2.execute(
                            "SELECT logic_hash, status, timestamp FROM steps WHERE name = ? ORDER BY timestamp DESC LIMIT 5",
                            (func_name,),
                        ).fetchall()
                    finally:
                        conn2.close()
                    if existing:
                        stored_hashes = [(r[0], r[1]) for r in existing]
                        log.warning(
                            "[Logram][PROBE 5][MISS_DETAIL] func=%s "
                            "searched_hash=%s "
                            "stored_hashes_for_same_func=%s "
                            ">>> If searched_hash != ANY stored hash → impl_fingerprint or vcr_args changed. "
                            "Compare PROBE 1 and PROBE 3 between Run-1 and Run-2 to find which component differs.",
                            func_name,
                            logic_hash,
                            stored_hashes,
                        )
                    else:
                        log.warning(
                            "[Logram][PROBE 5][MISS_DETAIL] func=%s searched_hash=%s "
                            "NO ROWS AT ALL for this func_name in steps table "
                            ">>> Run-1 data was never persisted (flush race, crash before flush, or wrong DB path).",
                            func_name,
                            logic_hash,
                        )
                except Exception as probe_exc:
                    log.debug("[Logram][PROBE 5] secondary lookup failed: %s", probe_exc)
                # ── END PROBE 5 ─────────────────────────────────────────────────
                return logic_hash, _VCR_MISS, None, None

            output_json = row[0]
            state_delta_raw = row[1] if len(row) > 1 else None
            args_delta_raw = row[2] if len(row) > 2 else None
            state_delta = self._safe_json_loads(state_delta_raw)
            args_delta = self._safe_json_loads(args_delta_raw)
            if not isinstance(state_delta, dict):
                state_delta = None
            if not isinstance(args_delta, dict):
                args_delta = None
            log.debug(
                "[Logram][Diag D][VCR Lookup] func=%s logic_hash=%s result=HIT output_json_len=%d output_json_preview=%s",
                func_name,
                logic_hash,
                len(str(output_json)) if output_json is not None else 0,
                self._diag_preview(output_json),
            )
            if state_delta is not None:
                log.debug(
                    "[Logram][Diag D][VCR Lookup] func=%s logic_hash=%s state_delta_keys=%s",
                    func_name,
                    logic_hash,
                    sorted(list(state_delta.keys())),
                )
            if args_delta is not None:
                log.debug(
                    "[Logram][Diag D][VCR Lookup] func=%s logic_hash=%s args_delta_keys=%s",
                    func_name,
                    logic_hash,
                    sorted(list(args_delta.keys())),
                )
            return logic_hash, self._safe_json_loads(output_json), state_delta, args_delta
        except Exception:
            log.debug(
                "[Logram][Diag D][VCR Lookup] func=%s logic_hash=%s result=MISS exception_raised",
                func_name,
                logic_hash,
                exc_info=True,
            )
            return logic_hash, _VCR_MISS, None, None

    def get_run_input_id(self, run_id: str | None) -> str | None:
        if not run_id:
            return None
        try:
            self._ensure_db()
            conn = sqlite3.connect(self.db_path, timeout=_DB_TIMEOUT_SEC)
            try:
                self._configure_connection(conn)
                row = conn.execute(
                    "SELECT input_id FROM runs WHERE run_id = ? LIMIT 1",
                    (run_id,),
                ).fetchone()
            finally:
                conn.close()
            if not row:
                return None
            value = row[0]
            return str(value) if value is not None else None
        except Exception:
            return None

    def get_state_values(self, state_delta: dict[str, str]) -> dict[str, Any]:
        """Resolve `{attr: value_hash}` into `{attr: value_json_decoded}` from values_registry."""
        if not isinstance(state_delta, dict) or not state_delta:
            return {}

        hash_values = sorted({str(v) for v in state_delta.values() if isinstance(v, str) and v})
        if not hash_values:
            return {}

        try:
            self._ensure_db()
            conn = sqlite3.connect(self.db_path, timeout=_DB_TIMEOUT_SEC)
            try:
                self._configure_connection(conn)
                placeholders = ",".join(["?"] * len(hash_values))
                rows = conn.execute(
                    f"SELECT value_hash, value_json FROM values_registry WHERE value_hash IN ({placeholders})",
                    tuple(hash_values),
                ).fetchall()
            finally:
                conn.close()
        except Exception:
            log.debug("[Logram][Diag E][ReplayState] get_state_values failed", exc_info=True)
            return {}

        by_hash: dict[str, Any] = {}
        for row in rows:
            try:
                vh = str(row[0])
                by_hash[vh] = self._safe_json_loads(row[1])
            except Exception:
                continue

        resolved: dict[str, Any] = {}
        missing: list[str] = []
        for attr, value_hash in state_delta.items():
            if not isinstance(value_hash, str):
                continue
            if value_hash in by_hash:
                resolved[str(attr)] = by_hash[value_hash]
            else:
                missing.append(value_hash)

        log.debug(
            "[Logram][Diag E][ReplayState] resolved_attrs=%s missing_hashes=%s",
            sorted(list(resolved.keys())),
            missing,
        )
        return resolved

    def _load_replay_source_step(self, conn: sqlite3.Connection, logic_hash: str) -> dict[str, Any] | None:
        if not logic_hash:
            return None
        try:
            row = conn.execute(
                """
                SELECT output_json, duration, started_at, finished_at, prompt_tokens, completion_tokens,
                       step_id, run_id
                FROM steps
                WHERE logic_hash = ? AND status IN ('SUCCESS', 'REPLAYED')
                ORDER BY timestamp DESC
                LIMIT 1
                """,
                (logic_hash,),
            ).fetchone()
            if not row:
                return None
            return {
                "output_json": str(row[0]) if row[0] is not None else None,
                "duration": float(row[1] or 0.0),
                "started_at": float(row[2]) if row[2] is not None else None,
                "finished_at": float(row[3]) if row[3] is not None else None,
                "prompt_tokens": int(row[4]) if row[4] is not None else None,
                "completion_tokens": int(row[5]) if row[5] is not None else None,
                "step_id": str(row[6]) if row[6] is not None else None,
                "run_id": str(row[7]) if row[7] is not None else None,
            }
        except Exception:
            return None

    def _copy_callee_subtree(
        self,
        conn: sqlite3.Connection,
        source_step_id: str,
        source_run_id: str,
        new_parent_step_id: str,
        new_run_id: str,
        base_ts: float,
    ) -> None:
        """Copy child step rows from source_run into new_run as REPLAYED rows.

        Only called when the parent step is REPLAYED, meaning its Merkle hash
        matched — so the entire callee subtree is provably unchanged.
        """
        id_map: dict[str, str] = {source_step_id: new_parent_step_id}
        frontier: list[str] = [source_step_id]
        while frontier:
            placeholders = ",".join("?" * len(frontier))
            rows = conn.execute(
                f"""
                SELECT step_id, parent_step_id, name, inputs_json, output_json,
                       logic_hash, duration, started_at, finished_at,
                       prompt_tokens, completion_tokens, state_delta, args_delta,
                       error_json
                FROM steps
                WHERE parent_step_id IN ({placeholders}) AND run_id = ?
                ORDER BY timestamp
                """,
                (*frontier, source_run_id),
            ).fetchall()
            frontier = []
            for row in rows:
                (old_id, old_parent, name, inputs_json, output_json,
                 logic_hash, duration, started_at, finished_at,
                 prompt_tokens, completion_tokens, state_delta, args_delta,
                 error_json) = row
                new_id = str(uuid.uuid4())
                id_map[old_id] = new_id
                frontier.append(old_id)
                conn.execute(
                    """
                    INSERT OR IGNORE INTO steps (
                        step_id, run_id, parent_step_id, name, inputs_json, output_json,
                        logic_hash, status, duration, started_at, finished_at,
                        prompt_tokens, completion_tokens, state_delta, args_delta,
                        error_json, timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'REPLAYED', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        new_id,
                        new_run_id,
                        id_map.get(old_parent, old_parent),
                        name,
                        inputs_json,
                        output_json,
                        logic_hash,
                        float(duration or 0.0),
                        float(started_at) if started_at is not None else base_ts,
                        float(finished_at) if finished_at is not None else base_ts,
                        int(prompt_tokens) if prompt_tokens is not None else None,
                        int(completion_tokens) if completion_tokens is not None else None,
                        state_delta,
                        args_delta,
                        error_json,
                        base_ts,
                    ),
                )
        log.debug(
            "[Logram][Diag E][Persist] callee_subtree_copied source_step_id=%s source_run_id=%s new_parent=%s new_run_id=%s nodes=%d",
            source_step_id,
            source_run_id,
            new_parent_step_id,
            new_run_id,
            len(id_map) - 1,
        )

    def save_step_sync(
        self,
        run_id: str,
        step_data: dict[str, Any],
        vcr_hash: str,
        logic_snapshot: dict[str, Any] | None = None,
        state_values: dict[str, Any] | None = None,
        arg_values: dict[str, Any] | None = None,
        callee_registry: dict[str, Any] | None = None,
    ) -> None:
        try:
            self._ensure_worker()
            req = _StepRequest(
                run_id=run_id,
                step_data=step_data,
                vcr_hash=vcr_hash,
                logic_snapshot=logic_snapshot,
                state_values=state_values,
                arg_values=arg_values,
                callee_registry=callee_registry,
            )
            self._queue.put_nowait(req)
            log.debug(
                "[Logram][Diag E][Save] enqueue run_id=%s step_id=%s name=%s status=%s vcr_hash=%s logic_snapshot_keys=%s queue_size=%d step_preview=%s",
                run_id,
                (step_data or {}).get("step_id") if isinstance(step_data, dict) else None,
                (step_data or {}).get("name") if isinstance(step_data, dict) else None,
                (step_data or {}).get("status") if isinstance(step_data, dict) else None,
                vcr_hash,
                sorted(list((logic_snapshot or {}).keys())) if isinstance(logic_snapshot, dict) else [],
                self._queue.qsize(),
                self._diag_preview(step_data),
            )
        except queue.Full:
            log.debug("[Logram][Diag E][Save] enqueue_failed reason=queue_full run_id=%s", run_id)
            return
        except Exception:
            log.debug("[Logram][Diag E][Save] enqueue_failed reason=exception run_id=%s", run_id, exc_info=True)
            return

    async def save_step(
        self,
        run_id: str,
        step_data: dict[str, Any],
        vcr_hash: str,
        logic_snapshot: dict[str, Any] | None = None,
        state_values: dict[str, Any] | None = None,
        arg_values: dict[str, Any] | None = None,
        callee_registry: dict[str, Any] | None = None,
    ) -> None:
        self.save_step_sync(
            run_id=run_id,
            step_data=step_data,
            vcr_hash=vcr_hash,
            logic_snapshot=logic_snapshot,
            state_values=state_values,
            arg_values=arg_values,
            callee_registry=callee_registry,
        )

    def _write_batch(
        self,
        conn: sqlite3.Connection,
        step_batch: list[_StepRequest],
        run_updates: dict[str, _RunUpdateRequest],
    ) -> None:
        if not step_batch and not run_updates:
            return

        now = time.time()
        try:
            conn.execute("BEGIN")

            for req in step_batch:
                def _as_int_or_none(value: Any) -> int | None:
                    if value is None:
                        return None
                    try:
                        out = int(value)
                    except (TypeError, ValueError):
                        return None
                    return out if out >= 0 else None

                step = req.step_data if isinstance(req.step_data, dict) else {}
                step_id = str(step.get("step_id") or hashlib.sha1(f"{req.run_id}-{now}-{id(req)}".encode()).hexdigest())
                parent_step_id = step.get("parent_id")
                name = str(step.get("name") or "unknown_step")
                status = str(step.get("status") or "UNKNOWN")
                duration = float(step.get("duration") or 0.0)
                timestamp = float(step.get("timestamp") or time.time())
                started_at_raw = step.get("started_at")
                finished_at_raw = step.get("finished_at")

                started_at = float(started_at_raw) if started_at_raw is not None else max(0.0, timestamp - max(0.0, duration))
                finished_at = float(finished_at_raw) if finished_at_raw is not None else timestamp
                if finished_at < started_at:
                    finished_at = started_at
                prompt_tokens = _as_int_or_none(step.get("prompt_tokens"))
                completion_tokens = _as_int_or_none(step.get("completion_tokens"))

                inputs_json = self._safe_json_dumps(step.get("inputs"))
                error_json = self._safe_json_dumps(step.get("error"))
                state_delta_json = self._safe_json_dumps(step.get("state_delta"))
                args_delta_json = self._safe_json_dumps(step.get("args_delta"))

                computed_logic_hash = self.register_logic(req.logic_snapshot)
                logic_hash = req.vcr_hash or computed_logic_hash
                if status == "REPLAYED":
                    # Replay rows must alias the exact source logic_hash used for cache lookup.
                    logic_hash = str(req.vcr_hash or computed_logic_hash or "")
                    if req.vcr_hash and computed_logic_hash and req.vcr_hash != computed_logic_hash:
                        log.debug(
                            "[Logram][Diag E][Persist] replay_logic_hash_mismatch step_id=%s run_id=%s provided_hash=%s computed_hash=%s",
                            step_id,
                            req.run_id,
                            req.vcr_hash,
                            computed_logic_hash,
                        )
                log.debug(
                    "[Logram][Diag E][Persist] preparing run_id=%s step_id=%s name=%s status=%s logic_hash=%s has_logic_snapshot=%s",
                    req.run_id,
                    step_id,
                    name,
                    status,
                    logic_hash,
                    bool(req.logic_snapshot),
                )

                if isinstance(req.state_values, dict) and req.state_values:
                    for value_hash, raw_value in req.state_values.items():
                        try:
                            conn.execute(
                                """
                                INSERT OR IGNORE INTO values_registry (value_hash, value_json)
                                VALUES (?, ?)
                                """,
                                (str(value_hash), self._safe_json_dumps(raw_value)),
                            )
                        except Exception:
                            continue

                if isinstance(req.arg_values, dict) and req.arg_values:
                    for value_hash, raw_value in req.arg_values.items():
                        try:
                            conn.execute(
                                """
                                INSERT OR IGNORE INTO values_registry (value_hash, value_json)
                                VALUES (?, ?)
                                """,
                                (str(value_hash), self._safe_json_dumps(raw_value)),
                            )
                        except Exception:
                            continue

                # Data aliasing + duration recovery for replay rows.
                replay_source: dict[str, Any] | None = None
                if status == "REPLAYED":
                    replay_source = self._load_replay_source_step(conn, logic_hash)
                    if replay_source is not None:
                        output_json = replay_source["output_json"] if replay_source["output_json"] is not None else self._safe_json_dumps(step.get("output"))
                        source_duration = float(replay_source.get("duration") or 0.0)
                        if source_duration > 0:
                            duration = source_duration
                            # Keep replay anchored to current timeline while preserving original effort duration.
                            finished_at = float(finished_at or timestamp)
                            started_at = max(0.0, finished_at - duration)
                        source_prompt = replay_source.get("prompt_tokens")
                        source_completion = replay_source.get("completion_tokens")
                        if source_prompt is not None:
                            prompt_tokens = int(source_prompt)
                        if source_completion is not None:
                            completion_tokens = int(source_completion)
                        log.debug(
                            "[Logram][Diag E][Persist] replay_alias_applied step_id=%s logic_hash=%s source_duration=%s source_prompt_tokens=%s source_completion_tokens=%s",
                            step_id,
                            logic_hash,
                            replay_source.get("duration"),
                            replay_source.get("prompt_tokens"),
                            replay_source.get("completion_tokens"),
                        )
                    else:
                        output_json = self._safe_json_dumps(step.get("output"))
                        log.debug(
                            "[Logram][Diag E][Persist] replay_alias_missing step_id=%s logic_hash=%s fallback_output_preview=%s",
                            step_id,
                            logic_hash,
                            self._diag_preview(step.get("output")),
                        )
                else:
                    output_json = self._safe_json_dumps(step.get("output"))

                self._ensure_run_exists(conn, req.run_id, status="running")

                if logic_hash and self._is_meaningful_logic_snapshot(req.logic_snapshot):
                    source_code = req.logic_snapshot.get("source_normalized", req.logic_snapshot.get("source") or "")
                    globals_json = req.logic_snapshot.get("resolved_globals", req.logic_snapshot.get("globals") or {})
                    called_functions = req.logic_snapshot.get("called_functions") or {}
                    conn.execute(
                        """
                        INSERT INTO logic_registry (logic_hash, name, source_code, globals_json, resolved_globals, called_functions_json, signature)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(logic_hash) DO NOTHING
                        """,
                        (
                            logic_hash,
                            str(req.logic_snapshot.get("name") or "unknown"),
                            str(source_code),
                            self._safe_json_dumps(globals_json),
                            self._safe_json_dumps(globals_json),
                            self._safe_json_dumps(called_functions if isinstance(called_functions, dict) else {}),
                            str(req.logic_snapshot.get("signature") or ""),
                        ),
                    )

                # Upsert callee snapshots collected by recursive logic hashing.
                if isinstance(req.callee_registry, dict) and req.callee_registry:
                    for c_hash, c_snap in req.callee_registry.items():
                        try:
                            c_globals = c_snap.get("resolved_globals") or {}
                            c_called = c_snap.get("called_functions") or {}
                            conn.execute(
                                """
                                INSERT INTO logic_registry (logic_hash, name, source_code, globals_json, resolved_globals, called_functions_json, signature)
                                VALUES (?, ?, ?, ?, ?, ?, ?)
                                ON CONFLICT(logic_hash) DO NOTHING
                                """,
                                (
                                    str(c_hash),
                                    str(c_snap.get("name") or "unknown"),
                                    str(c_snap.get("source_normalized") or ""),
                                    self._safe_json_dumps(c_globals if isinstance(c_globals, dict) else {}),
                                    self._safe_json_dumps(c_globals if isinstance(c_globals, dict) else {}),
                                    self._safe_json_dumps(c_called if isinstance(c_called, dict) else {}),
                                    str(c_snap.get("signature") or ""),
                                ),
                            )
                        except Exception:
                            continue

                conn.execute(
                    """
                    INSERT OR REPLACE INTO steps (
                        step_id, run_id, parent_step_id, name, inputs_json, output_json, logic_hash,
                        status, duration, started_at, finished_at, prompt_tokens, completion_tokens, state_delta, args_delta, error_json, timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        step_id,
                        req.run_id,
                        str(parent_step_id) if parent_step_id is not None else None,
                        name,
                        inputs_json,
                        output_json,
                        logic_hash or None,
                        status,
                        duration,
                        started_at,
                        finished_at,
                        prompt_tokens,
                        completion_tokens,
                        state_delta_json,
                        args_delta_json,
                        error_json,
                        timestamp,
                    ),
                )
                log.debug(
                    "[Logram][Diag E][Persist] wrote_step run_id=%s step_id=%s status=%s logic_hash=%s inputs_json_len=%d output_json_len=%d state_delta_json_len=%d args_delta_json_len=%d error_json_len=%d",
                    req.run_id,
                    step_id,
                    status,
                    logic_hash,
                    len(inputs_json) if isinstance(inputs_json, str) else 0,
                    len(output_json) if isinstance(output_json, str) else 0,
                    len(state_delta_json) if isinstance(state_delta_json, str) else 0,
                    len(args_delta_json) if isinstance(args_delta_json, str) else 0,
                    len(error_json) if isinstance(error_json, str) else 0,
                )

                if status == "REPLAYED" and replay_source is not None:
                    src_step_id = replay_source.get("step_id")
                    src_run_id = replay_source.get("run_id")
                    if src_step_id and src_run_id and src_run_id != req.run_id:
                        try:
                            self._copy_callee_subtree(conn, src_step_id, src_run_id, step_id, req.run_id, timestamp)
                        except Exception as ce:
                            log.warning("[Logram] callee subtree copy failed for step %s: %s", step_id, ce)

                conn.execute(
                    "UPDATE runs SET updated_at = ?, status = COALESCE(status, 'running') WHERE run_id = ?",
                    (time.time(), req.run_id),
                )

            for run_id, upd in run_updates.items():
                self._ensure_run_exists(conn, run_id, status=upd.status)

                step_rows = conn.execute(
                    """
                    SELECT status, duration, started_at, finished_at, prompt_tokens, completion_tokens
                    FROM steps
                    WHERE run_id = ?
                    """,
                    (run_id,),
                ).fetchall()
                resource_time_saved, wait_time_saved = compute_time_savings(step_rows)
                total_prompt_tokens, total_completion_tokens = compute_token_totals(step_rows)

                conn.execute(
                    """
                    UPDATE runs
                    SET status = ?, metrics_json = ?, resource_time_saved = ?, wait_time_saved = ?,
                        total_prompt_tokens = ?, total_completion_tokens = ?, updated_at = ?
                    WHERE run_id = ?
                    """,
                    (
                        upd.status,
                        self._safe_json_dumps(upd.metrics or {}),
                        float(resource_time_saved),
                        float(wait_time_saved),
                        int(total_prompt_tokens),
                        int(total_completion_tokens),
                        now,
                        run_id,
                    ),
                )

            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass

    def _worker_loop(self) -> None:
        conn: sqlite3.Connection | None = None
        step_batch: list[_StepRequest] = []
        run_updates: dict[str, _RunUpdateRequest] = {}
        last_flush = time.monotonic()

        try:
            self._ensure_db()
            conn = sqlite3.connect(self.db_path, timeout=_DB_TIMEOUT_SEC)
            conn.row_factory = sqlite3.Row
            self._configure_connection(conn)
        except Exception:
            conn = None

        while not self._worker_stop.is_set():
            now = time.monotonic()
            pending = bool(step_batch or run_updates)
            timeout = max(0.0, _BATCH_FLUSH_INTERVAL_SEC - (now - last_flush)) if pending else _BATCH_FLUSH_INTERVAL_SEC

            item: _StepRequest | _RunUpdateRequest | _FlushRequest | _StopRequest | None = None
            try:
                item = self._queue.get(timeout=timeout)
            except queue.Empty:
                item = None

            if isinstance(item, _StopRequest):
                if conn is not None:
                    self._write_batch(conn, step_batch, run_updates)
                step_batch.clear()
                run_updates.clear()
                break

            if isinstance(item, _StepRequest):
                step_batch.append(item)
            elif isinstance(item, _RunUpdateRequest):
                run_updates[item.run_id] = item
            elif isinstance(item, _FlushRequest):
                if conn is not None:
                    self._write_batch(conn, step_batch, run_updates)
                step_batch.clear()
                run_updates.clear()
                last_flush = time.monotonic()
                item.done.set()
                continue

            should_flush_by_size = len(step_batch) >= _BATCH_MAX_ITEMS
            should_flush_by_time = (time.monotonic() - last_flush) >= _BATCH_FLUSH_INTERVAL_SEC and bool(step_batch or run_updates)

            if should_flush_by_size or should_flush_by_time:
                if conn is not None:
                    self._write_batch(conn, step_batch, run_updates)
                step_batch.clear()
                run_updates.clear()
                last_flush = time.monotonic()

        if conn is not None:
            try:
                self._write_batch(conn, step_batch, run_updates)
                conn.close()
            except Exception:
                pass

    def flush_sync(self, timeout: float = 10.0) -> bool:
        try:
            self._ensure_worker()
            req = _FlushRequest()
            self._queue.put(req, timeout=0.1)
            return req.done.wait(timeout=timeout)
        except Exception:
            return False

    async def flush(self, timeout: float = 10.0) -> bool:
        try:
            return await asyncio.to_thread(self.flush_sync, timeout)
        except Exception:
            return False
