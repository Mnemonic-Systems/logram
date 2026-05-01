from __future__ import annotations

import json
import logging
import mimetypes
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

import aiosqlite
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

from .models import (
    CompareChange,
    CompareResponse,
    DiffLogicSnapshot,
    DiffRecursiveFunction,
    DiffStepRow,
    DiffStepSide,
    GraphResponse,
    InputSummary,
    ProjectSummary,
    RunDetail,
    RunDiffResponse,
    RunDiffSummary,
    RunLineageEdge,
    RunLineageNode,
    RunLineageResponse,
    StatsResponse,
    StepDetail,
    StepGroup,
    StepMeta,
    StepNode,
)

LOG = logging.getLogger("logram.server")

BLOB_HASH_RE = re.compile(r"^[a-f0-9]{64}$")

DEFAULT_DB_PATH = Path(".logram") / "logram.db"
DEFAULT_ASSETS_DIR = Path(".logram_assets")


@dataclass(slots=True)
class LogramServerState:
    db_path: Path
    assets_dir: Path
    db_uri: str


class LogramDBError(RuntimeError):
    pass


def _json_loads(value: str | None) -> Any:
    if value is None:
        return None
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


def _json_norm(value: str | None) -> str:
    parsed = _json_loads(value)
    try:
        return json.dumps(parsed, ensure_ascii=False, sort_keys=True)
    except (TypeError, ValueError):
        return str(parsed)


def _sum_metric_keys(node: Any, keys: set[str]) -> float:
    total = 0.0
    if isinstance(node, dict):
        for key, value in node.items():
            if key.lower() in keys and isinstance(value, (int, float)):
                total += float(value)
            total += _sum_metric_keys(value, keys)
        return total
    if isinstance(node, list):
        for item in node:
            total += _sum_metric_keys(item, keys)
    return total


def _parse_called_functions(value: str | None) -> dict[str, str]:
    parsed = _json_loads(value)
    if not isinstance(parsed, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in parsed.items():
        if isinstance(k, str) and isinstance(v, str) and v:
            out[k] = v
    return out


def _infer_aliased_missing_step_names(
    *,
    source_steps: dict[str, aiosqlite.Row],
    source_by_id: dict[str, aiosqlite.Row],
    other_steps: dict[str, aiosqlite.Row],
) -> set[str]:
    """Treat nested steps as aliased when an ancestor exists with identical logic_hash."""
    missing = set(source_steps) - set(other_steps)
    if not missing:
        return set()

    aliased: set[str] = set()
    for name in missing:
        row = source_steps.get(name)
        if row is None:
            continue
        cursor = row
        hops = 0
        while cursor is not None and hops < 200:
            hops += 1
            ancestor_name = str(cursor["name"] or "")
            if ancestor_name and ancestor_name in other_steps:
                other = other_steps[ancestor_name]
                hash_src = cursor["logic_hash"]
                hash_other = other["logic_hash"]
                if hash_src and hash_src == hash_other:
                    aliased.add(name)
                    break
            parent_id = cursor["parent_step_id"]
            cursor = source_by_id.get(str(parent_id)) if parent_id else None
    return aliased


def _collect_called_qualnames(
    root_hash: str,
    called_by_hash: dict[str, dict[str, str]],
    *,
    limit: int = 2000,
) -> set[str]:
    """Recursively collect reachable callee qualnames for a logic_hash."""
    seen_hashes: set[str] = set()
    out_names: set[str] = set()
    stack: list[str] = [root_hash]

    while stack and len(seen_hashes) < limit:
        current = stack.pop()
        if not current or current in seen_hashes:
            continue
        seen_hashes.add(current)
        called = called_by_hash.get(current) or {}
        for qualname, child_hash in called.items():
            if isinstance(qualname, str) and qualname:
                out_names.add(qualname)
            if isinstance(child_hash, str) and child_hash and child_hash not in seen_hashes:
                stack.append(child_hash)
    return out_names


def _row_resolved_globals(row: aiosqlite.Row | dict[str, Any] | None) -> Any:
    if not row:
        return {}
    raw = row["resolved_globals"] if isinstance(row, aiosqlite.Row) else row.get("resolved_globals")
    return _json_loads(raw)


def _collect_recursive_function_payloads(
    *,
    root_hash: str,
    registry_by_hash: dict[str, aiosqlite.Row],
    limit: int = 2000,
) -> list[DiffRecursiveFunction]:
    out: list[DiffRecursiveFunction] = []
    seen_hashes: set[str] = set()
    stack: list[tuple[str, str]] = []

    root_row = registry_by_hash.get(root_hash)
    if not root_row:
        return out

    root_called = _parse_called_functions(root_row["called_functions_json"])
    for qualname, child_hash in root_called.items():
        if child_hash:
            stack.append((qualname, child_hash))

    while stack and len(seen_hashes) < limit:
        qualname, current_hash = stack.pop()
        if not current_hash or current_hash in seen_hashes:
            continue
        seen_hashes.add(current_hash)

        row = registry_by_hash.get(current_hash)
        source_code = row["source_code"] if row else None
        resolved_globals = _row_resolved_globals(row)

        out.append(
            DiffRecursiveFunction(
                qualname=qualname,
                logic_hash=current_hash,
                source_code=source_code,
                resolved_globals=resolved_globals,
            )
        )

        if not row:
            continue
        called = _parse_called_functions(row["called_functions_json"])
        for child_qualname, child_hash in called.items():
            if child_hash and child_hash not in seen_hashes:
                stack.append((child_qualname, child_hash))

    return out


def _logic_snapshot_from_registry(
    logic_hash: str | None,
    registry_by_hash: dict[str, aiosqlite.Row],
) -> DiffLogicSnapshot | None:
    if not logic_hash:
        return None
    row = registry_by_hash.get(logic_hash)
    if not row:
        return DiffLogicSnapshot(
            logic_hash=logic_hash,
            source_code=None,
            resolved_globals={},
            called_functions={},
            recursive_functions=[],
        )

    called_functions = _parse_called_functions(row["called_functions_json"])
    return DiffLogicSnapshot(
        logic_hash=logic_hash,
        source_code=row["source_code"],
        resolved_globals=_row_resolved_globals(row),
        called_functions=called_functions,
        recursive_functions=_collect_recursive_function_payloads(
            root_hash=logic_hash,
            registry_by_hash=registry_by_hash,
        ),
    )


def _tokenize_json(node: Any, out: set[str]) -> None:
    if node is None:
        out.add("null")
        return
    if isinstance(node, bool):
        out.add(f"bool:{str(node).lower()}")
        return
    if isinstance(node, (int, float)):
        out.add(f"num:{node}")
        return
    if isinstance(node, str):
        stripped = node.strip().lower()
        if stripped:
            out.add(f"str:{stripped}")
        return
    if isinstance(node, dict):
        for k, v in node.items():
            out.add(f"key:{str(k).lower()}")
            _tokenize_json(v, out)
        return
    if isinstance(node, list):
        for item in node:
            _tokenize_json(item, out)


def _json_similarity_score(a: Any, b: Any) -> float | None:
    if a is None or b is None:
        return None
    if a == b:
        return 100.0
    ta: set[str] = set()
    tb: set[str] = set()
    _tokenize_json(a, ta)
    _tokenize_json(b, tb)
    if not ta and not tb:
        return 100.0
    union = ta | tb
    if not union:
        return 100.0
    inter = ta & tb
    return round((len(inter) / len(union)) * 100.0, 2)


def _count_bbox_like(node: Any) -> int:
    if isinstance(node, dict):
        count = 0
        bbox = node.get("bbox")
        if isinstance(bbox, list) and len(bbox) == 4 and all(isinstance(n, (int, float)) for n in bbox):
            count += 1
        poly = node.get("polygon")
        if isinstance(poly, list) and len(poly) >= 3 and all(isinstance(p, list) and len(p) >= 2 for p in poly):
            count += 1
        for value in node.values():
            count += _count_bbox_like(value)
        return count
    if isinstance(node, list):
        return sum(_count_bbox_like(v) for v in node)
    return 0


def _extract_cost_usd(metrics_json: str | None) -> float:
    parsed = _json_loads(metrics_json)
    if isinstance(parsed, dict):
        direct = parsed.get("cost_usd")
        if isinstance(direct, (int, float)):
            return float(direct)
        return _sum_metric_keys(parsed, {"cost", "cost_usd", "usd"})
    return 0.0


def _build_sqlite_uri(db_path: Path) -> str:
    resolved = db_path.resolve()
    encoded = quote(str(resolved), safe="/")
    return f"file:{encoded}?mode=ro&cache=shared"


def _resolve_blob_path(assets_dir: Path, blob_hash: str) -> Path | None:
    if not BLOB_HASH_RE.fullmatch(blob_hash):
        raise HTTPException(status_code=400, detail="blob_hash invalide: format SHA-256 attendu")

    candidates = [
        assets_dir / blob_hash,
        assets_dir / f"{blob_hash}.bin",
        *sorted(assets_dir.glob(f"{blob_hash}.*")),
    ]

    assets_root = assets_dir.resolve()
    for candidate in candidates:
        if not candidate.exists() or not candidate.is_file():
            continue
        resolved = candidate.resolve()
        if not resolved.is_relative_to(assets_root):
            continue
        return resolved
    return None


def _detect_mime(path: Path) -> str:
    guessed, _ = mimetypes.guess_type(path.name)
    if guessed and guessed != "application/octet-stream":
        return guessed

    try:
        header = path.read_bytes()[:16]
    except OSError:
        return "application/octet-stream"

    if header.startswith(b"%PDF"):
        return "application/pdf"
    if header.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if header.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if header.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if header.startswith(b"RIFF") and header[8:12] == b"WEBP":
        return "image/webp"
    if header.startswith((b"II*\x00", b"MM\x00*")):
        return "image/tiff"
    if header.startswith(b"BM"):
        return "image/bmp"

    return "application/octet-stream"


async def _open_ro_connection(state: LogramServerState) -> aiosqlite.Connection:
    if not state.db_path.exists():
        raise FileNotFoundError(f"Base SQLite introuvable: {state.db_path}")

    try:
        conn = await aiosqlite.connect(state.db_uri, uri=True)
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA query_only=ON;")
        await conn.execute("PRAGMA busy_timeout=5000;")
        try:
            await conn.execute("PRAGMA journal_mode=WAL;")
        except aiosqlite.Error:
            # Read-only DB can reject WAL switch. Writer-side storage already enforces WAL.
            pass
        row = await (await conn.execute("PRAGMA journal_mode;")).fetchone()
        if row and str(row[0]).lower() != "wal":
            LOG.warning("SQLite journal_mode=%s (WAL recommandé)", row[0])
        return conn
    except aiosqlite.Error as exc:
        raise LogramDBError(str(exc)) from exc


def _run_row_to_model(row: aiosqlite.Row) -> RunDetail:
    resource_saved = float(row["resource_time_saved"] or 0.0)
    wait_saved = float(row["wait_time_saved"] or 0.0)
    roi = (wait_saved / resource_saved) if resource_saved > 0 else 0.0

    tags = _json_loads(row["tags"])
    if not isinstance(tags, list):
        tags = []

    metrics = _json_loads(row["metrics_json"])
    if not isinstance(metrics, dict):
        metrics = {}

    return RunDetail(
        run_id=str(row["run_id"]),
        project=row["project"],
        input_id=row["input_id"],
        version_id=row["version_id"],
        status=row["status"],
        tags=tags,
        metrics=metrics,
        resource_time_saved=resource_saved,
        wait_time_saved=wait_saved,
        roi_ratio=roi,
        total_prompt_tokens=int(row["total_prompt_tokens"] or 0),
        total_completion_tokens=int(row["total_completion_tokens"] or 0),
        created_at=float(row["created_at"]) if row["created_at"] is not None else None,
        updated_at=float(row["updated_at"]) if row["updated_at"] is not None else None,
    )


def create_app(*, db_path: Path | None = None, assets_dir: Path | None = None) -> FastAPI:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    db_path_resolved = (db_path or DEFAULT_DB_PATH).resolve()
    assets_dir_resolved = (assets_dir or DEFAULT_ASSETS_DIR).resolve()

    fastapi_app = FastAPI(title="Logram Dashboard Read API", version="1.0.0")
    fastapi_app.state.logram = LogramServerState(
        db_path=db_path_resolved,
        assets_dir=assets_dir_resolved,
        db_uri=_build_sqlite_uri(db_path_resolved),
    )

    fastapi_app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:3000", "http://127.0.0.1:3000", "http://localhost:3001", "http://127.0.0.1:3001"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @fastapi_app.middleware("http")
    async def request_logger(request: Request, call_next):  # type: ignore[override]
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            LOG.exception("%s %s -> 500 (%.1fms)", request.method, request.url.path, elapsed_ms)
            raise

        elapsed_ms = (time.perf_counter() - started) * 1000.0
        LOG.info("%s %s -> %s (%.1fms)", request.method, request.url.path, response.status_code, elapsed_ms)
        return response

    @fastapi_app.exception_handler(FileNotFoundError)
    async def file_not_found_handler(_: Request, exc: FileNotFoundError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @fastapi_app.exception_handler(LogramDBError)
    async def db_error_handler(_: Request, exc: LogramDBError) -> JSONResponse:
        return JSONResponse(status_code=500, content={"detail": f"Erreur SQLite: {exc}"})

    @fastapi_app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @fastapi_app.get("/api/projects", response_model=list[ProjectSummary])
    async def list_projects() -> list[ProjectSummary]:
        state: LogramServerState = fastapi_app.state.logram
        conn = await _open_ro_connection(state)
        try:
            cursor = await conn.execute(
                """
                SELECT
                    COALESCE(project, 'default') AS name,
                    COUNT(DISTINCT COALESCE(input_id, 'unknown_input')) AS document_count,
                    COUNT(*) AS run_count
                FROM runs
                GROUP BY COALESCE(project, 'default')
                ORDER BY run_count DESC, name ASC
                """
            )
            rows = await cursor.fetchall()
            return [
                ProjectSummary(
                    name=str(row["name"]),
                    document_count=int(row["document_count"] or 0),
                    run_count=int(row["run_count"] or 0),
                )
                for row in rows
            ]
        except aiosqlite.Error as exc:
            raise LogramDBError(str(exc)) from exc
        finally:
            await conn.close()

    @fastapi_app.get("/api/inputs", response_model=list[InputSummary])
    async def list_inputs(project: str | None = Query(None, min_length=1)) -> list[InputSummary]:
        state: LogramServerState = fastapi_app.state.logram
        conn = await _open_ro_connection(state)
        where_clause = ""
        params: tuple[Any, ...] = ()
        if project:
            where_clause = "WHERE project = ?"
            params = (project,)

        try:
            cursor = await conn.execute(
                f"""
                SELECT
                    COALESCE(project, 'default') AS project,
                    COALESCE(input_id, 'unknown_input') AS input_id,
                    COUNT(*) AS run_count,
                    MAX(created_at) AS last_run_timestamp
                FROM runs
                {where_clause}
                GROUP BY COALESCE(project, 'default'), COALESCE(input_id, 'unknown_input')
                ORDER BY last_run_timestamp DESC
                """,
                params,
            )
            rows = await cursor.fetchall()
            return [
                InputSummary(
                    project=row["project"],
                    input_id=str(row["input_id"]),
                    run_count=int(row["run_count"] or 0),
                    last_run_timestamp=float(row["last_run_timestamp"]) if row["last_run_timestamp"] is not None else None,
                )
                for row in rows
            ]
        except aiosqlite.Error as exc:
            raise LogramDBError(str(exc)) from exc
        finally:
            await conn.close()

    @fastapi_app.get("/api/runs", response_model=list[RunDetail])
    async def list_runs(
        input_id: str = Query(..., min_length=1),
        limit: int = Query(20, ge=1, le=200),
        offset: int = Query(0, ge=0),
    ) -> list[RunDetail]:
        state: LogramServerState = fastapi_app.state.logram
        conn = await _open_ro_connection(state)
        try:
            cursor = await conn.execute(
                """
                SELECT run_id, project, input_id, version_id, status, tags, metrics_json,
                       resource_time_saved, wait_time_saved,
                       total_prompt_tokens, total_completion_tokens,
                       created_at, updated_at
                FROM runs
                WHERE input_id = ?
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
                """,
                (input_id, int(limit), int(offset)),
            )
            rows = await cursor.fetchall()
            return [_run_row_to_model(row) for row in rows]
        except aiosqlite.Error as exc:
            raise LogramDBError(str(exc)) from exc
        finally:
            await conn.close()

    @fastapi_app.get("/api/runs/{run_id}/steps", response_model=list[StepMeta])
    async def run_steps(run_id: str) -> list[StepMeta]:
        state: LogramServerState = fastapi_app.state.logram
        conn = await _open_ro_connection(state)
        try:
            exists = await (await conn.execute("SELECT 1 FROM runs WHERE run_id = ? LIMIT 1", (run_id,))).fetchone()
            if not exists:
                raise HTTPException(status_code=404, detail=f"Run introuvable: {run_id}")

            cursor = await conn.execute(
                """
                SELECT step_id, parent_step_id, name, status, duration, logic_hash
                FROM steps
                WHERE run_id = ?
                ORDER BY timestamp ASC
                """,
                (run_id,),
            )
            rows = await cursor.fetchall()

            return [
                StepMeta(
                    id=str(row["step_id"]),
                    parent_id=row["parent_step_id"],
                    name=str(row["name"]),
                    status=row["status"],
                    duration=float(row["duration"] or 0.0),
                    logic_hash=row["logic_hash"],
                )
                for row in rows
            ]
        except aiosqlite.Error as exc:
            raise LogramDBError(str(exc)) from exc
        finally:
            await conn.close()

    @fastapi_app.get("/api/runs/{run_id}/graph", response_model=GraphResponse)
    async def run_graph(run_id: str, group: bool = Query(False)) -> GraphResponse:
        state: LogramServerState = fastapi_app.state.logram
        conn = await _open_ro_connection(state)
        try:
            exists = await (await conn.execute("SELECT 1 FROM runs WHERE run_id = ? LIMIT 1", (run_id,))).fetchone()
            if not exists:
                raise HTTPException(status_code=404, detail=f"Run introuvable: {run_id}")

            cursor = await conn.execute(
                """
                SELECT step_id, run_id, parent_step_id, name, status, duration, logic_hash, timestamp
                FROM steps
                WHERE run_id = ?
                ORDER BY timestamp ASC
                """,
                (run_id,),
            )
            rows = await cursor.fetchall()

            steps = [
                StepNode(
                    id=str(row["step_id"]),
                    run_id=str(row["run_id"]),
                    parent_step_id=row["parent_step_id"],
                    name=str(row["name"]),
                    status=row["status"],
                    duration=float(row["duration"] or 0.0),
                    logic_hash=row["logic_hash"],
                    timestamp=float(row["timestamp"]) if row["timestamp"] is not None else None,
                )
                for row in rows
            ]

            if not group:
                return GraphResponse(run_id=run_id, steps=steps, groups=[])

            grouped: dict[tuple[str | None, str], list[StepNode]] = {}
            for step in steps:
                key = (step.parent_step_id, step.name)
                grouped.setdefault(key, []).append(step)

            simplified_steps: list[StepNode] = []
            groups: list[StepGroup] = []

            for (parent_id, name), bucket in grouped.items():
                if len(bucket) <= 1:
                    simplified_steps.extend(bucket)
                    continue

                status_counts: dict[str, int] = {}
                for item in bucket:
                    status = str(item.status or "UNKNOWN")
                    status_counts[status] = status_counts.get(status, 0) + 1

                aggregate_status = "SUCCESS"
                if status_counts.get("FAILED", 0) > 0:
                    aggregate_status = "FAILED"
                elif status_counts.get("REPLAYED", 0) > 0:
                    aggregate_status = "REPLAYED"

                group_id = f"group::{parent_id or 'root'}::{name}"
                step_ids = [s.id for s in bucket]
                groups.append(
                    StepGroup(
                        id=group_id,
                        run_id=run_id,
                        parent_step_id=parent_id,
                        name=name,
                        status=aggregate_status,
                        duration=sum(s.duration for s in bucket),
                        logic_hash=bucket[0].logic_hash,
                        timestamp=min((s.timestamp or 0.0) for s in bucket),
                        step_ids=step_ids,
                        count=len(bucket),
                        status_counts=status_counts,
                    )
                )
                simplified_steps.append(
                    StepNode(
                        id=group_id,
                        run_id=run_id,
                        parent_step_id=parent_id,
                        name=name,
                        status=aggregate_status,
                        duration=sum(s.duration for s in bucket),
                        logic_hash=bucket[0].logic_hash,
                        timestamp=min((s.timestamp or 0.0) for s in bucket),
                    )
                )

            return GraphResponse(run_id=run_id, steps=simplified_steps, groups=groups)
        except aiosqlite.Error as exc:
            raise LogramDBError(str(exc)) from exc
        finally:
            await conn.close()

    @fastapi_app.get("/api/runs/{run_id}/lineage", response_model=RunLineageResponse)
    async def run_lineage(run_id: str) -> RunLineageResponse:
        state: LogramServerState = fastapi_app.state.logram
        conn = await _open_ro_connection(state)
        try:
            focus = await (
                await conn.execute(
                    "SELECT run_id, project, input_id FROM runs WHERE run_id = ? LIMIT 1",
                    (run_id,),
                )
            ).fetchone()
            if not focus:
                raise HTTPException(status_code=404, detail=f"Run introuvable: {run_id}")

            project = focus["project"]
            input_id = focus["input_id"]

            rows = await (
                await conn.execute(
                    """
                    SELECT run_id, project, input_id, version_id, status, created_at
                    FROM runs
                    WHERE project = ? AND input_id = ?
                    ORDER BY created_at ASC
                    """,
                    (project, input_id),
                )
            ).fetchall()

            run_ids = [str(r["run_id"]) for r in rows]
            replay_counts: dict[str, tuple[int, int]] = {}
            if run_ids:
                placeholders = ",".join("?" for _ in run_ids)
                replay_rows = await (
                    await conn.execute(
                        f"""
                        SELECT run_id,
                               SUM(CASE WHEN UPPER(COALESCE(status, '')) = 'REPLAYED' THEN 1 ELSE 0 END) AS replayed,
                               COUNT(*) AS total
                        FROM steps
                        WHERE run_id IN ({placeholders})
                        GROUP BY run_id
                        """,
                        tuple(run_ids),
                    )
                ).fetchall()
                replay_counts = {
                    str(r["run_id"]): (int(r["replayed"] or 0), int(r["total"] or 0)) for r in replay_rows
                }

            nodes: list[RunLineageNode] = []
            for row in rows:
                rid = str(row["run_id"])
                replayed, total = replay_counts.get(rid, (0, 0))
                replay_ratio = float(replayed) / float(total) if total > 0 else 0.0
                nodes.append(
                    RunLineageNode(
                        run_id=rid,
                        project=row["project"],
                        input_id=row["input_id"],
                        version_id=row["version_id"],
                        status=row["status"],
                        created_at=float(row["created_at"]) if row["created_at"] is not None else None,
                        replay_ratio=round(replay_ratio, 4),
                    )
                )

            edges: list[RunLineageEdge] = []
            for i in range(1, len(nodes)):
                prev = nodes[i - 1]
                cur = nodes[i]
                edge_type = "linear"
                if prev.version_id != cur.version_id and cur.replay_ratio > 0:
                    edge_type = "fork"
                edges.append(
                    RunLineageEdge(
                        source_run_id=prev.run_id,
                        target_run_id=cur.run_id,
                        edge_type=edge_type,
                    )
                )

            by_version: dict[str, list[str]] = {}
            for node in nodes:
                if node.version_id:
                    by_version.setdefault(node.version_id, []).append(node.run_id)
            for rid_list in by_version.values():
                if len(rid_list) < 2:
                    continue
                for i in range(1, len(rid_list)):
                    edges.append(
                        RunLineageEdge(
                            source_run_id=rid_list[i - 1],
                            target_run_id=rid_list[i],
                            edge_type="version-link",
                        )
                    )

            return RunLineageResponse(focus_run_id=run_id, nodes=nodes, edges=edges)
        except aiosqlite.Error as exc:
            raise LogramDBError(str(exc)) from exc
        finally:
            await conn.close()

    @fastapi_app.get("/api/steps/{step_id}", response_model=StepDetail)
    async def step_detail(step_id: str) -> StepDetail:
        state: LogramServerState = fastapi_app.state.logram
        conn = await _open_ro_connection(state)
        try:
            cursor = await conn.execute(
                """
                SELECT
                    s.step_id,
                    s.run_id,
                    s.parent_step_id,
                    s.name,
                    s.status,
                    s.duration,
                    s.timestamp,
                    s.started_at,
                    s.finished_at,
                    s.logic_hash,
                    s.prompt_tokens,
                    s.completion_tokens,
                    s.inputs_json,
                    s.output_json,
                    s.error_json,
                    lr.source_code,
                    COALESCE(lr.resolved_globals, lr.globals_json) AS resolved_globals
                FROM steps s
                LEFT JOIN logic_registry lr ON lr.logic_hash = s.logic_hash
                WHERE s.step_id = ?
                LIMIT 1
                """,
                (step_id,),
            )
            row = await cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail=f"Step introuvable: {step_id}")

            return StepDetail(
                step_id=str(row["step_id"]),
                run_id=str(row["run_id"]),
                parent_step_id=row["parent_step_id"],
                name=str(row["name"]),
                status=row["status"],
                duration=float(row["duration"] or 0.0),
                timestamp=float(row["timestamp"]) if row["timestamp"] is not None else None,
                started_at=float(row["started_at"]) if row["started_at"] is not None else None,
                finished_at=float(row["finished_at"]) if row["finished_at"] is not None else None,
                logic_hash=row["logic_hash"],
                prompt_tokens=int(row["prompt_tokens"] or 0),
                completion_tokens=int(row["completion_tokens"] or 0),
                inputs=_json_loads(row["inputs_json"]),
                output=_json_loads(row["output_json"]),
                error=_json_loads(row["error_json"]),
                source_code=row["source_code"],
                resolved_globals=_json_loads(row["resolved_globals"]),
            )
        except aiosqlite.Error as exc:
            raise LogramDBError(str(exc)) from exc
        finally:
            await conn.close()

    @fastapi_app.get("/api/assets/{blob_hash}")
    async def get_asset(blob_hash: str):
        state: LogramServerState = fastapi_app.state.logram
        blob_path = _resolve_blob_path(state.assets_dir, blob_hash)
        if blob_path is None:
            raise HTTPException(status_code=404, detail=f"Asset introuvable pour hash: {blob_hash}")

        media_type = _detect_mime(blob_path)
        return FileResponse(path=blob_path, media_type=media_type, filename=blob_path.name)

    @fastapi_app.get("/api/diff/{run_id_a}/{run_id_b}", response_model=RunDiffResponse)
    async def diff_runs(run_id_a: str, run_id_b: str) -> RunDiffResponse:
        state: LogramServerState = fastapi_app.state.logram
        conn = await _open_ro_connection(state)
        try:
            for rid in (run_id_a, run_id_b):
                row = await (
                    await conn.execute("SELECT 1 FROM runs WHERE run_id = ? LIMIT 1", (rid,))
                ).fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail=f"Run introuvable: {rid}")

            step_rows = await (
                await conn.execute(
                    """
                    SELECT
                        run_id,
                        step_id,
                        parent_step_id,
                        name,
                        status,
                        duration,
                        logic_hash,
                        inputs_json,
                        output_json,
                        error_json,
                        timestamp
                    FROM steps
                    WHERE run_id IN (?, ?)
                    ORDER BY timestamp ASC
                    """,
                    (run_id_a, run_id_b),
                )
            ).fetchall()

            run_metrics_rows = await (
                await conn.execute(
                    "SELECT run_id, metrics_json FROM runs WHERE run_id IN (?, ?)",
                    (run_id_a, run_id_b),
                )
            ).fetchall()
            metrics_by_run = {str(r["run_id"]): r["metrics_json"] for r in run_metrics_rows}

            logic_rows = await (
                await conn.execute(
                    """
                    SELECT
                        logic_hash,
                        source_code,
                        COALESCE(resolved_globals, globals_json) AS resolved_globals,
                        called_functions_json
                    FROM logic_registry
                    """
                )
            ).fetchall()
            registry_by_hash: dict[str, aiosqlite.Row] = {
                str(r["logic_hash"]): r for r in logic_rows
            }

            steps_a: dict[str, aiosqlite.Row] = {}
            steps_b: dict[str, aiosqlite.Row] = {}
            by_id_a: dict[str, aiosqlite.Row] = {}
            by_id_b: dict[str, aiosqlite.Row] = {}
            ordered_names: list[str] = []
            seen_names: set[str] = set()

            for row in step_rows:
                name = str(row["name"])
                if name not in seen_names:
                    seen_names.add(name)
                    ordered_names.append(name)

                if row["run_id"] == run_id_a:
                    steps_a[name] = row
                    by_id_a[str(row["step_id"])] = row
                else:
                    steps_b[name] = row
                    by_id_b[str(row["step_id"])] = row

            aliased_only_in_a = _infer_aliased_missing_step_names(
                source_steps=steps_a,
                source_by_id=by_id_a,
                other_steps=steps_b,
            )
            aliased_only_in_b = _infer_aliased_missing_step_names(
                source_steps=steps_b,
                source_by_id=by_id_b,
                other_steps=steps_a,
            )

            def _side_from_row(row: aiosqlite.Row | None, logic: DiffLogicSnapshot | None) -> DiffStepSide | None:
                if row is None:
                    return None
                return DiffStepSide(
                    step_id=str(row["step_id"]),
                    parent_step_id=row["parent_step_id"],
                    name=str(row["name"]),
                    status=row["status"],
                    duration=float(row["duration"] or 0.0),
                    logic_hash=row["logic_hash"],
                    inputs=_json_loads(row["inputs_json"]),
                    output=_json_loads(row["output_json"]),
                    error=_json_loads(row["error_json"]),
                    logic=logic,
                )

            rows: list[DiffStepRow] = []
            prompts_modified = 0
            bboxes_added = 0
            changed_steps = 0
            similarity_values: list[float] = []
            for name in ordered_names:
                if name in aliased_only_in_a or name in aliased_only_in_b:
                    continue

                row_a = steps_a.get(name)
                row_b = steps_b.get(name)

                hash_a = str(row_a["logic_hash"]) if row_a and row_a["logic_hash"] is not None else None
                hash_b = str(row_b["logic_hash"]) if row_b and row_b["logic_hash"] is not None else None

                identical = bool(hash_a and hash_b and hash_a == hash_b)
                output_a = _json_loads(row_a["output_json"]) if row_a is not None else None
                output_b = _json_loads(row_b["output_json"]) if row_b is not None else None
                output_changed = output_a != output_b
                similarity = _json_similarity_score(output_a, output_b)
                if similarity is not None:
                    similarity_values.append(similarity)

                if (hash_a != hash_b) and row_a is not None and row_b is not None:
                    prompts_modified += 1
                if output_changed and row_a is not None and row_b is not None:
                    bbox_a = _count_bbox_like(output_a)
                    bbox_b = _count_bbox_like(output_b)
                    bboxes_added += max(0, bbox_b - bbox_a)
                if (not identical) or output_changed or row_a is None or row_b is None:
                    changed_steps += 1

                if identical:
                    shared_logic = _logic_snapshot_from_registry(hash_a, registry_by_hash)
                    side_a = _side_from_row(row_a, shared_logic)
                    side_b = _side_from_row(row_b, shared_logic)
                else:
                    logic_a = _logic_snapshot_from_registry(hash_a, registry_by_hash)
                    logic_b = _logic_snapshot_from_registry(hash_b, registry_by_hash)
                    side_a = _side_from_row(row_a, logic_a)
                    side_b = _side_from_row(row_b, logic_b)

                rows.append(
                    DiffStepRow(
                        name=name,
                        a=side_a,
                        b=side_b,
                        identical=identical,
                        output_changed=output_changed,
                        output_similarity=similarity,
                    )
                )

            cost_a = _extract_cost_usd(metrics_by_run.get(run_id_a))
            cost_b = _extract_cost_usd(metrics_by_run.get(run_id_b))
            cost_delta_percent: float | None = None
            if abs(cost_a) > 1e-9:
                cost_delta_percent = round(((cost_b - cost_a) / cost_a) * 100.0, 2)

            similarity_score: float | None = None
            if similarity_values:
                similarity_score = round(sum(similarity_values) / len(similarity_values), 2)

            summary = RunDiffSummary(
                prompts_modified=prompts_modified,
                bboxes_added=bboxes_added,
                cost_delta_percent=cost_delta_percent,
                changed_steps=changed_steps,
                similarity_score=similarity_score,
            )

            return RunDiffResponse(
                run_id_a=run_id_a,
                run_id_b=run_id_b,
                steps=rows,
                summary=summary,
            )
        except aiosqlite.Error as exc:
            raise LogramDBError(str(exc)) from exc
        finally:
            await conn.close()

    @fastapi_app.get("/api/compare/{run_id_a}/{run_id_b}", response_model=CompareResponse)
    async def compare_runs(run_id_a: str, run_id_b: str) -> CompareResponse:
        state: LogramServerState = fastapi_app.state.logram
        conn = await _open_ro_connection(state)
        try:
            for rid in (run_id_a, run_id_b):
                row = await (await conn.execute("SELECT 1 FROM runs WHERE run_id = ? LIMIT 1", (rid,))).fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail=f"Run introuvable: {rid}")

            cursor = await conn.execute(
                """
                SELECT run_id, step_id, parent_step_id, name, logic_hash, output_json, timestamp
                FROM steps
                WHERE run_id IN (?, ?)
                ORDER BY timestamp ASC
                """,
                (run_id_a, run_id_b),
            )
            rows = await cursor.fetchall()

            steps_a: dict[str, aiosqlite.Row] = {}
            steps_b: dict[str, aiosqlite.Row] = {}
            by_id_a: dict[str, aiosqlite.Row] = {}
            by_id_b: dict[str, aiosqlite.Row] = {}
            for row in rows:
                target = steps_a if row["run_id"] == run_id_a else steps_b
                target_by_id = by_id_a if row["run_id"] == run_id_a else by_id_b
                target[str(row["name"])] = row
                target_by_id[str(row["step_id"])] = row

            names_a = set(steps_a)
            names_b = set(steps_b)
            shared = sorted(names_a & names_b)

            identical_steps: list[str] = []
            identical_step_set: set[str] = set()
            logic_hash_changed: list[CompareChange] = []
            output_changed: list[CompareChange] = []

            # Optional recursive logic expansion (when called_functions_json exists).
            called_by_hash: dict[str, dict[str, str]] = {}
            try:
                reg_rows = await (
                    await conn.execute(
                        """
                        SELECT logic_hash, called_functions_json
                        FROM logic_registry
                        """
                    )
                ).fetchall()
                for reg in reg_rows:
                    called_by_hash[str(reg["logic_hash"])] = _parse_called_functions(reg["called_functions_json"])
            except aiosqlite.Error:
                called_by_hash = {}

            for name in shared:
                row_a = steps_a[name]
                row_b = steps_b[name]
                hash_a = row_a["logic_hash"]
                hash_b = row_b["logic_hash"]
                out_a = _json_norm(row_a["output_json"])
                out_b = _json_norm(row_b["output_json"])

                if hash_a == hash_b and out_a == out_b:
                    if name not in identical_step_set:
                        identical_steps.append(name)
                        identical_step_set.add(name)

                    # If parent logic_hash is identical, recursive callees are identical too.
                    if isinstance(hash_a, str) and hash_a:
                        for qualname in sorted(_collect_called_qualnames(hash_a, called_by_hash)):
                            alias_label = f"{name}::{qualname}"
                            if alias_label not in identical_step_set:
                                identical_steps.append(alias_label)
                                identical_step_set.add(alias_label)
                    continue

                if hash_a != hash_b:
                    logic_hash_changed.append(
                        CompareChange(
                            step_name=name,
                            run_id_a_step_id=row_a["step_id"],
                            run_id_b_step_id=row_b["step_id"],
                            logic_hash_a=hash_a,
                            logic_hash_b=hash_b,
                        )
                    )

                if out_a != out_b:
                    output_changed.append(
                        CompareChange(
                            step_name=name,
                            run_id_a_step_id=row_a["step_id"],
                            run_id_b_step_id=row_b["step_id"],
                            logic_hash_a=hash_a,
                            logic_hash_b=hash_b,
                        )
                    )

            aliased_only_in_a = _infer_aliased_missing_step_names(
                source_steps=steps_a,
                source_by_id=by_id_a,
                other_steps=steps_b,
            )
            aliased_only_in_b = _infer_aliased_missing_step_names(
                source_steps=steps_b,
                source_by_id=by_id_b,
                other_steps=steps_a,
            )

            return CompareResponse(
                run_id_a=run_id_a,
                run_id_b=run_id_b,
                identical_steps=identical_steps,
                logic_hash_changed=logic_hash_changed,
                output_changed=output_changed,
                only_in_a=sorted((names_a - names_b) - aliased_only_in_a),
                only_in_b=sorted((names_b - names_a) - aliased_only_in_b),
            )
        except aiosqlite.Error as exc:
            raise LogramDBError(str(exc)) from exc
        finally:
            await conn.close()

    @fastapi_app.get("/api/stats", response_model=StatsResponse)
    async def stats(project: str | None = Query(None)) -> StatsResponse:
        state: LogramServerState = fastapi_app.state.logram
        conn = await _open_ro_connection(state)
        where_clause = ""
        params: tuple[Any, ...] = ()
        if project:
            where_clause = "WHERE project = ?"
            params = (project,)

        try:
            row = await (
                await conn.execute(
                    f"""
                    SELECT
                        COUNT(*) AS runs,
                        COALESCE(SUM(total_prompt_tokens), 0) AS total_prompt_tokens,
                        COALESCE(SUM(total_completion_tokens), 0) AS total_completion_tokens,
                        COALESCE(SUM(wait_time_saved), 0) AS wait_time_saved,
                        COALESCE(SUM(resource_time_saved), 0) AS resource_time_saved
                    FROM runs
                    {where_clause}
                    """,
                    params,
                )
            ).fetchone()

            metrics_rows = await (
                await conn.execute(
                    f"SELECT metrics_json FROM runs {where_clause}",
                    params,
                )
            ).fetchall()

            total_usd = 0.0
            for metrics_row in metrics_rows:
                metrics = _json_loads(metrics_row["metrics_json"])
                if metrics is None:
                    continue
                total_usd += _sum_metric_keys(
                    metrics,
                    {
                        "usd",
                        "cost_usd",
                        "total_usd",
                        "usd_spent",
                        "total_cost_usd",
                        "total_usd_spent",
                    },
                )

            runs = int(row["runs"] or 0) if row else 0
            prompt = int(row["total_prompt_tokens"] or 0) if row else 0
            completion = int(row["total_completion_tokens"] or 0) if row else 0
            wait_saved = float(row["wait_time_saved"] or 0.0) if row else 0.0
            resource_saved = float(row["resource_time_saved"] or 0.0) if row else 0.0
            ratio = (wait_saved / resource_saved) if resource_saved > 0 else 0.0

            return StatsResponse(
                project=project,
                runs=runs,
                total_prompt_tokens=prompt,
                total_completion_tokens=completion,
                total_tokens=prompt + completion,
                total_usd_spent=round(total_usd, 6),
                wait_time_saved=wait_saved,
                resource_time_saved=resource_saved,
                roi_wait_vs_resource=ratio,
            )
        except aiosqlite.Error as exc:
            raise LogramDBError(str(exc)) from exc
        finally:
            await conn.close()

    return fastapi_app


app = create_app()
