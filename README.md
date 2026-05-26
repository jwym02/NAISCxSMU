# Smart Tool Log Parser

### Winner — Micron AI Challenge, National AI Student Challenge 2026

## Author

Application entirely created by **Jovan Wang**.

An AI-powered log processing system for semiconductor manufacturing. It ingests equipment log files, classifies events using domain-grounded LLMs with few-shot RAG, detects statistical anomalies via Isolation Forest, routes by severity in real time, and learns from human feedback to improve over time.

---

## Features

### AI-Powered Log Classification

- **Domain Grounding** — LLM prompts are anchored with a comprehensive semiconductor fab equipment context covering 15+ equipment types (PECVD, Etch, Lithography, CMP, ALD, Ion Implant, etc.), their common fault modes, and correct category mappings. This prevents hallucination and ensures classifications align with real fab failure modes.

- **Few-Shot RAG (Retrieval-Augmented Generation)** — Previously human-approved events are stored in DynamoDB. On each new event, the system retrieves relevant approved examples from the same category, ranks them by machine similarity, and injects the best matches as few-shot examples into the LLM prompt. Cross-machine learning: an approval on one ETCH tool benefits all ETCH tools.

- **Isolation Forest Anomaly Detection** — A scikit-learn Isolation Forest model runs alongside the LLM, scoring every event on a 7-dimensional feature vector (severity, confidence, category, field presence, message length, review flag). Events above the anomaly threshold are escalated to the review queue even if the LLM is confident — the two systems act as independent checks.

- **Temporal Anomaly Detection** — For machines with multiple events in a batch, an LLM analyzes the time-series sequence for escalation patterns (monotonically increasing values, severity escalation, repeating fault codes at increasing frequency).

### Core Capabilities

- **Multi-format parsing** — Ingests JSON, XML, CSV, SYSLOG, and plain text log files
- **Smart priority routing** — Events auto-routed by severity via Kafka (P0 Critical, P1 Error, P2 Info, Dead Letter)
- **Real-time WebSocket alerts** — P0/P1 urgent events streamed to the dashboard instantly
- **Human-in-the-loop feedback** — Low-confidence events go to a review queue; reviewer corrections feed back into future AI predictions, improving accuracy over time
- **NL2SQL query engine** — Ask questions about logs in plain English; the system translates to SQL and queries TimescaleDB
- **Confidence scoring** — Events scored 0.0-1.0 based on AI confidence, field completeness, and rule agreement; below-threshold events automatically routed to human review

### Tech Stack

**Backend:** Python 3.11, FastAPI, Kafka, TimescaleDB, DynamoDB, Redis, MinIO, scikit-learn
**Frontend:** React, TypeScript, Vite, Recharts
**AI:** OpenRouter (Nvidia Nemotron), Isolation Forest (scikit-learn)
**Infrastructure:** Docker Compose (single command deployment)

---

## Getting Started

### Prerequisites

- Docker Desktop v24+ (with Compose v2 included)
  - Verify: `docker --version` and `docker compose version`

Python, databases, and all other dependencies run inside Docker — no local installation needed.

### 1. Setup Environment Variables

Create a `.env` file in the project root with your OpenRouter API key:

```
AI_KEY=your_openrouter_api_key
```

### 2. Start Everything

```bash
docker compose up -d
```

This downloads and starts all services (database, Kafka, Redis, MinIO, app containers). Takes 1-2 minutes on first run.

### 3. Verify Services

```bash
docker compose ps
```

You should see:

- **Infrastructure** (timescaledb, kafka, redis, minio) — "healthy" or "running"
- **Init containers** (kafka-init, dynamodb-init, minio-init) — "exited (0)" (this is normal)
- **App containers** (app-pipeline, app-consumer-hot, etc.) — "running"

### 4. Access the App

| Service           | URL                        | Purpose                    |
| ----------------- | -------------------------- | -------------------------- |
| **Frontend**      | http://localhost:3000      | Web dashboard              |
| **Pipeline API**  | http://localhost:8080/docs | Upload log files           |
| **Query API**     | http://localhost:8081/docs | Natural language queries   |
| **pgAdmin**       | http://localhost:5050      | Visual database browser    |
| **MinIO Console** | http://localhost:9001      | Browse uploaded files      |
| **Kafka UI**      | http://localhost:8090      | Message flow visualization |

### 5. Run a Demo

```bash
# Generate 100 realistic test logs
python generate_simulation_data.py

# Run two-phase streaming demo (uploads files, demonstrates human feedback loop)
python simulate_stream.py
```

---

## Quick Port Reference

| Port | Component                  |
| ---- | -------------------------- |
| 3000 | Frontend (React/Vite)      |
| 5050 | pgAdmin (Database Browser) |
| 5432 | TimescaleDB                |
| 6379 | Redis                      |
| 8000 | DynamoDB                   |
| 8080 | Pipeline API               |
| 8081 | Query API                  |
| 8083 | WebSocket Alerts           |
| 8090 | Kafka UI                   |
| 9001 | MinIO Console              |

---

## Understanding the Stack

### TimescaleDB — The Main Database

Stores all log events, review queue items, and review decisions. Organized as time-series hypertables with automatic retention policies.

- Tables: `log_events`, `normalized_events`, `review_queue_status`
- Location: `localhost:5432` (user: `logparser`, password: `logparser_secret`)
- Setup: Automatic via Docker

### DynamoDB — The Feedback Loop Storage

Stores normalization rules (field aliases), confidence boosts from previous approvals, and approved events for RAG few-shot learning. Does NOT store the review queue itself.

- Tables: `normalization-rules`, `approved-events`
- Access: http://localhost:8000

### MinIO — File Storage

S3-compatible object storage for raw and processed log files.

- Access: http://localhost:9001 (login: `minioadmin` / `minioadmin123`)
- Setup: Automatic via Docker

---

## Using pgAdmin

**Access:** http://localhost:5050 (Email: `admin@example.com`, Password: `admin`)

**First Time Setup:**

1. Click "Add New Server"
2. General tab — Name: `logparser-db`
3. Connection tab — Hostname: `timescaledb`, Port: `5432`, Database: `logparser_db`, Username: `logparser`, Password: `logparser_secret`

---

## Common Commands

```bash
# View logs (real-time)
docker compose logs -f

# View one service
docker compose logs -f app-pipeline

# Rebuild after code changes
docker compose up -d --build app-pipeline

# Access database CLI
docker exec -it timescaledb psql -U logparser -d logparser_db

# Stop (data preserved)
docker compose down

# Full reset (deletes all data)
docker compose down -v

# Reset script (wipes TimescaleDB but keeps DynamoDB feedback rules)
./reset.sh
```

---

## Troubleshooting

**"App containers can't connect to database/kafka"**
Use Docker service names inside containers, not `localhost`:

- Database: `timescaledb:5432` | Kafka: `kafka:29092` | Redis: `redis:6379` | MinIO: `minio:9000`

**"Something is taking forever to start"**
First startup is slow. Check `docker compose logs -f`. If stuck >3 minutes: `docker compose down && docker compose up -d`

**"Schema errors in database"**
Re-run the schema: `docker exec -i timescaledb psql -U logparser -d logparser_db < init/timescale/01_schema.sql`

**"Port already in use"**
Edit `docker-compose.yml` and change the host port (first number): `"5433:5432"`

**"I want to start fresh"**
`docker compose down -v && docker compose up -d`

---
