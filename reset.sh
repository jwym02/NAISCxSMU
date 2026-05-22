#!/usr/bin/env bash
# reset.sh — Wipe and restart the full stack cleanly.
# Deletes TimescaleDB volume (fresh start), but keeps DynamoDB feedback rules.
# Usage: ./reset.sh
set -e

echo "→ Stopping all services (graceful, 30s timeout)…"
docker compose down --timeout 30

echo "→ Removing TimescaleDB volume (deletes all logs, review queue, review decisions)…"
# Try multiple volume name variations (accounts for different project naming)
REMOVED=false
for VOL in "naiscsxsmu_timescale-data" "naiscxsmu_timescale-data" "$(basename "$PWD" | tr '[:upper:]' '[:lower:]' | tr -d '_' | tr -d '-')_timescale-data"; do
  if docker volume rm "$VOL" 2>/dev/null; then
    echo "  ✓ Removed volume: $VOL"
    REMOVED=true
    break
  fi
done

if [ "$REMOVED" = false ]; then
  echo "  (volume not found or already removed — this is okay)"
fi

echo "→ Starting stack…"
docker compose up -d

echo "→ Waiting for TimescaleDB to become healthy…"
TSDB_ID=$(docker compose ps -q timescaledb)
TIMEOUT=120
ELAPSED=0
until [ "$(docker inspect --format='{{.State.Health.Status}}' "$TSDB_ID")" = "healthy" ]; do
  if [ $ELAPSED -ge $TIMEOUT ]; then
    echo ""
    echo "✗ TimescaleDB did not become healthy within ${TIMEOUT}s"
    echo "  Check logs with: docker compose logs timescaledb"
    exit 1
  fi
  printf '.'; sleep 3
  ELAPSED=$((ELAPSED + 3))
done
echo ""
echo "✓ Stack is up and TimescaleDB is healthy (waited ${ELAPSED}s)"
echo ""
echo "Summary:"
echo "  • TimescaleDB: RESET (fresh volume, all logs/queue/decisions wiped)"
echo "  • DynamoDB: KEPT (feedback rules preserved)"
echo "  • All services: RUNNING and healthy"
