# Smart Tool Log Parser — Setup Guide

## What Is This Project?

This is a **log processing system** that:

- Takes log files from manufacturing tools (machines that make semiconductors)
- Reads and organizes the data
- Detects urgent events (fires, equipment failures)
- Stores everything in a database so engineers can search and analyze it

---

## Prerequisites

**Required:**

- Docker Desktop v24+ (with Compose v2 included)
  - Verify: `docker --version` and `docker compose version`

**NOT required:**

- Python installed on your machine (it runs inside Docker)
- Virtual environment (venv) — Docker handles this
- Any database software (TimescaleDB runs in Docker)

---

## 1. Setup Environment Variables

- Create your own .env` file (stores passwords and API keys)
- Add your AI / OpenRouter API key to this file

---

## 2. Start Everything (One Command!)

```bash
docker compose up -d
```

**What happens:**

- Docker downloads all the services (database, storage, etc.)
- Starts everything automatically in the background
- Takes 1-2 minutes on first run
- `-d` flag means "run in background"

---

## 3. Check If Everything Started

```bash
docker compose ps
```

**You should see:**

- ✅ **Infrastructure services** (timescaledb, kafka, redis, minio) — Status: "healthy" or "running"
- ✅ **Init containers** (kafka-init, dynamodb-init, minio-init) — Status: "exited (0)" — **This is normal!** They run once then stop.
- ✅ **App containers** (app-pipeline, app-consumer-hot, etc.) — Status: "running"

**If something shows "unhealthy" or "restarting":**

- Wait a bit longer (first startup can take 2-3 minutes)
- Check logs: `docker compose logs app-pipeline`

---

## Understanding the Stack (Simple Version)

### TimescaleDB — The Database

**What does it do?**

- Stores all your log data (like an Excel spreadsheet, but for databases)
- Organized to handle lots of time-stamped events (perfect for logs with timestamps)
- Keeps everything organized in tables

**Think of it like:**

- A filing cabinet on your computer
- Each log event gets filed and labeled by time
- You can search and sort through thousands of logs instantly

**Setup:**

- ✅ Automatic — When you run `docker compose up`, it creates the database for you
- ✅ Data is on your machine only — Your teammate's database is separate from yours
- ✅ No manual work needed — All tables and folders are created automatically
- Location: `localhost:5432` (password: `logparser_secret`)

---

### MinIO — The File Storage

**What does it do?**

- Stores your log files (like Google Drive or Dropbox, but on your computer)
- Keeps a backup copy of every file you upload
- Organized into "buckets" (like folders)

**Think of it like:**

- A cloud storage system, but running locally on your machine
- When you upload a log file, it gets saved here first before processing
- Two folders: one for raw files, one for processed files

**Setup:**

- ✅ Automatic — When you run `docker compose up`, the folders are created for you
- ✅ Data is on your machine only — Your teammate's storage is separate
- ✅ No manual work needed — Just run the command
- Access: http://localhost:9001 (username: `minioadmin`, password: `minioadmin123`)

---

### The Key Point

- **Both TimescaleDB and MinIO run inside Docker**
- **Each person on your team gets their own copy** when they run `docker compose up`
- **Teammates don't need to set them up manually** — it's all automatic
- **Their data stays on their machine** — not shared with you

---

## Service URLs (Where to Access Things)

| Service              | URL                           | What It Does                                                  |
| -------------------- | ----------------------------- | ------------------------------------------------------------- |
| **Pipeline API**     | http://localhost:8080/docs    | Upload log files here                                         |
| **Query API**        | http://localhost:8081/docs    | Ask questions about logs using natural language               |
| **WebSocket Alerts** | ws://localhost:8083/ws/alerts | Real-time notifications (P0/P1 urgent logs)                   |
| **Kafka UI**         | http://localhost:8090         | Visualize message flow (for debugging)                        |
| **MinIO Console**    | http://localhost:9001         | Browse uploaded files (login: `minioadmin` / `minioadmin123`) |
| **TimescaleDB**      | localhost:5432                | Database (user: `logparser` / password: `logparser_secret`)   |
| **DynamoDB**         | http://localhost:8000         | Database for low-confidence events (for data review)          |
| **Redis**            | localhost:6379                | Duplicate detection (caching)                                 |

**Quick tip:** Open pipeline and query APIs in your browser — they have interactive documentation!

---

## Common Commands (Cheat Sheet)

### Viewing Logs & Status

```bash
# See what all containers are doing (real-time)
docker compose logs -f

# See logs from one service only
docker compose logs -f app-pipeline

# Check what's running right now
docker compose ps
```

### Restarting After Code Changes

```bash
# Rebuild and restart the pipeline container
docker compose up -d --build app-pipeline

# Rebuild and restart ALL app containers
docker compose up -d --build app-pipeline app-consumer-hot app-consumer-cold app-consumer-deadletter app-query
```

### Accessing the Database

```bash
# Open a terminal inside the TimescaleDB container
docker exec -it timescaledb psql -U logparser -d logparser_db

# Run a quick query (example: count logs)
docker exec -it timescaledb psql -U logparser -d logparser_db \
  -c "SELECT priority, COUNT(*) FROM log_events GROUP BY priority;"
```

### Inspecting Kafka (Message Queue)

```bash
# List all topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Check if messages are flowing
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 --describe \
  --group consumer-group-hot
```

### Checking Redis (Duplicate Detection)

```bash
# See all keys stored
docker exec -it redis redis-cli keys "*"
```

### Stopping & Cleaning Up

```bash
# Stop everything (data is saved)
docker compose down

# Stop everything AND delete all data (fresh start)
docker compose down -v
```

---

## Placing Test Log Files

**Want to test the system?**

- Create a folder: `synthetic-logs/` in the project root
- Drop your test log files here (`.json`, `.xml`, `.csv`, `.log`, `.txt`, etc.)
- The pipeline reads from this folder automatically
- Files are not changed (read-only mount)

---

## Troubleshooting

### "App containers can't connect to database/kafka"

**Problem:** Connection refused error in logs

**Solution:** Use the Docker service names, NOT `localhost`

- ❌ Wrong: `localhost:5432`
- ✅ Right: `timescaledb:5432`

**Service names to use inside containers:**

- Database: `timescaledb:5432`
- Kafka: `kafka:29092`
- Redis: `redis:6379`
- MinIO: `minio:9000`

---

### "Something is taking forever to start"

**What to do:**

1. Check logs: `docker compose logs -f`
2. Wait a bit more (first startup is slow)
3. If stuck for more than 3 minutes, restart: `docker compose down && docker compose up -d`

---

### "Schema errors in database"

**If the database tables don't exist:**

```bash
docker exec -i timescaledb psql -U logparser -d logparser_db \
  < init/timescale/01_schema.sql
```

This runs the schema file again.

---

### "Port already in use" (e.g., port 5432 is taken)

**Problem:** Another app is using the same port

**Solution:** Edit `docker-compose.yml` and change the first number:

```yaml
# Change this line (5432 is already used):
ports:
  - "5432:5432"

# To this (use a different port on your machine):
ports:
  - "5433:5432"  # Now access database at localhost:5433
```

---

### "I want to start fresh with no data"

```bash
docker compose down -v
docker compose up -d
```

This deletes all data and starts clean.
