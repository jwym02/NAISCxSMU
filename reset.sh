#!/usr/bin/env bash
# reset.sh — Wipe and restart the full stack cleanly.
# Usage: ./reset.sh
set -e

echo "→ Stopping all services (graceful, 30s timeout)…"
docker compose down --timeout 30

echo "→ Removing TimescaleDB volume…"
# Docker names volumes as <project>_<volume> where project = lowercase dir name.
PROJ=$(basename "$PWD" | tr '[:upper:]' '[:lower:]' | tr -d '_' | tr -d '-')
docker volume rm "${PROJ}_timescale-data" 2>/dev/null \
  || docker volume rm naiscsxsmu_timescale-data 2>/dev/null \
  || echo "  (volume already removed or not found — continuing)"

echo "→ Starting stack…"
docker compose up -d

echo "→ Waiting for TimescaleDB to become healthy…"
TSDB_ID=$(docker compose ps -q timescaledb)
until [ "$(docker inspect --format='{{.State.Health.Status}}' "$TSDB_ID")" = "healthy" ]; do
  printf '.'; sleep 3
done
echo ""
echo "✓ Stack is up and TimescaleDB is healthy."
