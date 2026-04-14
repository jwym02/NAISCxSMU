 # Consumes logs.p0 + logs.p1
# Writes to TimescaleDB immediately
# Pushes WebSocket alert to frontend
# Entry: python -m app.consumers.hot
# Container: app-consumer-hot (port 8083)