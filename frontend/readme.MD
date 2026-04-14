# Frontend — Integration Guide

This folder is a placeholder for the frontend application.
The technology choice (React, Vue, Flask, Next.js, etc.) is left
to the frontend developer. This document covers everything you
need to know to connect any frontend to the backend.

---

## Backend services you connect to

The backend runs as three separate services, each on its own port.

### 1. Pipeline API — `http://localhost:8080`

Handles log file ingestion, job tracking, and the human review queue.

| Method | Endpoint             | What it does                              |
| ------ | -------------------- | ----------------------------------------- |
| `POST` | `/logs/upload`       | Upload a raw log file for processing      |
| `GET`  | `/jobs/{id}`         | Poll the status of a submitted job        |
| `GET`  | `/queue`             | Fetch all items in the human review queue |
| `POST` | `/queue/{id}/review` | Submit a correction for a review item     |
| `GET`  | `/health`            | Liveness check                            |

Full interactive docs (Swagger UI): **http://localhost:8080/docs**

---

### 2. Query API — `http://localhost:8081`

Accepts natural language questions and returns structured data
for chart or table rendering.

| Method | Endpoint  | What it does                                             |
| ------ | --------- | -------------------------------------------------------- |
| `POST` | `/query`  | Send a natural language question, get back query results |
| `GET`  | `/health` | Liveness check                                           |

Full interactive docs (Swagger UI): **http://localhost:8081/docs**

**Example request:**

```json
POST /query
{
  "question": "Show me all critical errors from tool MCH_0001 in the last 24 hours"
}
```

**Example response:**

```json
{
  "sql": "SELECT * FROM log_events WHERE ...",
  "columns": ["event_time", "tool_id", "severity", "fault_code"],
  "rows": [...]
}
```

---

### 3. WebSocket alerts — `ws://localhost:8083/ws/alerts`

Real-time stream of P0 (critical) and P1 (standard error) events.
Connect once on page load and listen for incoming messages.

**Example message payload:**

```json
{
  "priority": "P0",
  "tool_id": "MCH_0001",
  "severity": "CRITICAL",
  "fault_code": "VACUUM_FAULT",
  "event_time": "2025-04-14T12:03:20Z",
  "message": "Vacuum fault detected on chamber C1"
}
```

**Connecting in JavaScript:**

```javascript
const ws = new WebSocket("ws://localhost:8083/ws/alerts");

ws.onmessage = (event) => {
  const alert = JSON.parse(event.data);
  console.log("New alert:", alert);
  // render to your alerts dashboard
};

ws.onclose = () => {
  // implement reconnect logic here
};
```

---

## Suggested pages

Based on the system's capabilities, these are the pages
the frontend should cover:

| Page             | Data source  | Key interactions                                      |
| ---------------- | ------------ | ----------------------------------------------------- |
| **Upload**       | Pipeline API | Drag/drop log file, show job progress, display result |
| **Live alerts**  | WebSocket    | Real-time feed of P0/P1 events with severity badges   |
| **Review queue** | Pipeline API | List low-confidence events, approve or correct them   |
| **Analytics**    | Query API    | Free-text query input, render results as chart/table  |

---

## Adding the frontend to Docker Compose

Once you have a working `Dockerfile` inside this folder,
uncomment the `frontend` service block in `docker-compose.yml`
and update the `build`, `ports`, and environment variables
to match your framework's dev server setup.

The three environment variables to set are:

```
PIPELINE_API_URL = http://localhost:8080
QUERY_API_URL    = http://localhost:8081
WS_ALERTS_URL    = ws://localhost:8083/ws/alerts
```

If your framework uses a different prefix convention
(e.g. `NEXT_PUBLIC_`, `VITE_`, `REACT_APP_`), rename them
accordingly — just make sure the values point to the same ports.

---

## CORS

All backend services have CORS enabled for `http://localhost:3000`
by default (the standard frontend dev server port).
If your frontend runs on a different port, update the
`CORS_ORIGINS` environment variable in `.env`.
