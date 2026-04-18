# Frontend — Integration Guide

This folder is a placeholder for the frontend application.
The technology choice (React, Vue, Flask, Next.js, etc.) is left
to the frontend developer. This document covers everything you
need to know to connect any frontend to the backend.

---

## Backend services you connect to

The backend runs as three separate services, each on its own port.

### 1. Pipeline API — `http://localhost:8080`

Handles log file ingestion, parsing, normalization, and persistence to database.

| Method | Endpoint   | What it does                                         |
| ------ | ---------- | ---------------------------------------------------- |
| `POST` | `/process` | Upload a log file for processing (JSON/CSV/XML/LOG) |
| `GET`  | `/health`  | Liveness check                                       |

Full interactive docs (Swagger UI): **http://localhost:8080/docs**

**Example request:**

```bash
curl -X POST "http://localhost:8080/process" \
  -F "file=@logfile.json"
```

**Example response:**

```json
{
  "job_id": "59fce81a-9a58-488e-91f9-ebeeab1ac191",
  "file_name": "logfile.json",
  "file_format": "json",
  "status": "success",
  "events_processed": 42,
  "events_routed": {
    "logs.p0": 2,
    "logs.p1": 8,
    "logs.p2": 32
  },
  "events_in_review": 3,
  "errors": []
}
```

---

### 2. Query API — `http://localhost:8081`

Accepts natural language questions and returns structured data from TimescaleDB.

| Method | Endpoint  | What it does                                                  |
| ------ | --------- | ------------------------------------------------------------- |
| `POST` | `/query`  | Send a natural language question, get back SQL + query results |
| `GET`  | `/health` | Liveness check                                                |
| `GET`  | `/schema` | Return database schema documentation                          |

Full interactive docs (Swagger UI): **http://localhost:8081/docs**

**Example request:**

```json
POST /query
{
  "query": "Show me all critical errors from the last 24 hours",
  "limit": 100,
  "time_range_hours": 24
}
```

**Example response:**

```json
{
  "original_query": "Show me all critical errors from the last 24 hours",
  "generated_sql": "SELECT * FROM normalized_events WHERE timestamp > NOW() - INTERVAL '24 hours' ORDER BY timestamp DESC LIMIT 100",
  "rows": [
    {
      "job_id": "59fce81a-9a58-488e-91f9-ebeeab1ac191",
      "timestamp": "2026-04-18T05:52:09.267Z",
      "source": "machine_001",
      "event_type": "temperature_warning",
      "severity": "warning",
      "message": "Temperature exceeded 85C threshold",
      "ai_category": "thermal",
      "ai_root_cause": "Cooling system failure",
      "ai_recommended_action": "Inspect cooling system",
      "confidence_score": 0.92,
      "requires_review": false
    }
  ],
  "row_count": 1,
  "execution_time_ms": 45.23
}
```

---

### 3. WebSocket alerts — `ws://localhost:8083/ws/alerts`

**⚠️ Not yet implemented.** Placeholder for future real-time P0/P1 event streaming.

---

## Suggested pages

Based on the system's capabilities, these are the pages
the frontend should cover:

| Page          | Data source  | Key interactions                                        |
| ------------- | ------------ | ------------------------------------------------------- |
| **Upload**    | Pipeline API | Drag/drop or select log file, display processing result |
| **Analytics** | Query API    | Free-text query input, render results as table          |
| **Schema**    | Query API    | Browse database tables and columns (from `/schema`)     |

**Note:** Job tracking, live alerts, and review queue features are
architectural placeholders. The core pipeline processes logs end-to-end
but does not yet expose intermediate job status or webhook callbacks.

---

## Adding the frontend to Docker Compose

Once you have a working `Dockerfile` inside this folder,
uncomment the `frontend` service block in `docker-compose.yml`
and update the `build`, `ports`, and environment variables
to match your framework's dev server setup.

The environment variables to set are:

```
REACT_APP_PIPELINE_URL=http://localhost:8080      # or VITE_PIPELINE_URL, NEXT_PUBLIC_PIPELINE_URL
REACT_APP_QUERY_URL=http://localhost:8081         # or VITE_QUERY_URL, NEXT_PUBLIC_QUERY_URL
```

(Adjust variable prefixes based on your framework: `NEXT_PUBLIC_`, `VITE_`, `REACT_APP_`, etc.)

## CORS

All backend services have CORS enabled for `http://localhost:3000`
by default (the standard frontend dev server port).
If your frontend runs on a different port, update the
`CORS_ORIGINS` environment variable in `.env`.
