# Logram Dashboard Read API

Serveur FastAPI de lecture pour le dashboard web.

## Objectif

- Exposer les données de `.logram/logram.db` **en lecture seule**.
- Servir les blobs depuis `.logram_assets/` de manière sécurisée.
- Fournir des endpoints rapides pour navigation, graphe React Flow, comparaison A/B et stats ROI.

## Endpoints

- `GET /api/projects`
- `GET /api/inputs?project=...`
- `GET /api/runs?input_id=...&limit=...&offset=...`
- `GET /api/runs/{run_id}/graph`
- `GET /api/steps/{step_id}`
- `GET /api/assets/{blob_hash}`
- `GET /api/compare/{run_id_a}/{run_id_b}`
- `GET /api/stats?project=...`

## Lancement

Depuis la CLI Logram:

- `af ui`

Par défaut:

- API: `http://127.0.0.1:8000`
- Dashboard web attendu: `http://localhost:3000`

## Robustesse

- Connexion SQLite `mode=ro` + `cache=shared`
- `PRAGMA query_only=ON`
- CORS activé pour `localhost:3000`
- Logs de chaque requête (méthode, path, status, latence)
- Erreurs SQL transformées en réponses HTTP propres (`404` / `500`)
