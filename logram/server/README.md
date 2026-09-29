# Logram dashboard API

Read-only FastAPI server behind the web dashboard. Install it with `pip install "logram-sdk[server] @ git+https://github.com/Mnemonic-Systems/logram.git"`.

- Serves the trace store (`.logram/logram.db`) **read-only**.
- Serves blobs from `.logram_assets/`, by content hash only.
- Provides the endpoints the dashboard needs: navigation, step graph, A/B comparison and stats.

## Endpoints

- `GET /health`
- `GET /api/projects`
- `GET /api/inputs?project=...`
- `GET /api/runs?input_id=...&limit=...&offset=...`
- `GET /api/runs/{run_id}/steps`
- `GET /api/runs/{run_id}/graph`
- `GET /api/runs/{run_id}/lineage`
- `GET /api/steps/{step_id}`
- `GET /api/assets/{blob_hash}`
- `GET /api/diff/{run_id_a}/{run_id_b}`
- `GET /api/compare/{run_id_a}/{run_id_b}`
- `GET /api/stats?project=...`

## Running it

```bash
logram ui            # API on http://127.0.0.1:8000, dashboard expected on http://localhost:3000
```

## Safety

- SQLite opened with `mode=ro` and `PRAGMA query_only=ON`.
- CORS limited to `localhost:3000` / `localhost:3001`.
- Every request is logged (method, path, status, latency).
- SQLite errors become clean `404` / `500` responses.
