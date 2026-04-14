# Consumes logs.p2
# Batches 100 records / 5 min window
# Bulk inserts into TimescaleDB
# Entry: python -m app.consumers.cold
# Container: app-consumer-cold (no port)