# Frontend — Log Parser UI

React + Vite + TypeScript app aligned with **`docs/BUILD-REQUIREMENTS.md`** (M1: parse UI, health, `POST /process`, optimized text preview; M2 starter: NL query tab).

## Quick start

```bash
cd frontend
cp .env.example .env
npm install
npm run dev
```

Open **http://localhost:5173** (Vite default). Ensure Docker backends are up so **Pipeline** (`http://localhost:8080`) and **Query** (`http://localhost:8081`) respond on `/health`.

## Environment

| Variable | Purpose |
|----------|---------|
| `VITE_PIPELINE_URL` | Pipeline API base (no trailing slash) |
| `VITE_QUERY_URL` | Query API base |

## Backend contract (v1)

Documented in **`swagger.yml`** (sync pipeline):

- `POST /process` — multipart field `file`, optional query `format=json|csv|xml|log|txt`; response may include `records_preview` (sample normalized rows)
- `POST /preview` — JSON `{ "text", "format?" }` — parse-only (no MinIO/Kafka/AI), size- and rate-limited
- `GET /health` — `{ "status": "ok", "service": "pipeline", ... }`

Query service (port 8081):

- `POST /query` — JSON `{ "query", "limit?", "time_range_hours?" }`
- `GET /health`, `GET /schema`

Reserved / not implemented on pipeline HTTP service today: `GET /queue`, review queue routes (see swagger notes).

## CORS

Pipeline and Query enable CORS for `http://localhost:5173` and `http://localhost:3000` by default (`CORS_ORIGINS` env override).

## Production build

```bash
npm run build
```

Static assets are emitted to `frontend/dist/`.
