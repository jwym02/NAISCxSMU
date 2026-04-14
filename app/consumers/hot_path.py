# Consumes logs.p0 + logs.p1
# Writes to TimescaleDB immediately
# Pushes WebSocket alert to frontend
# Entry: python -m app.consumers.hot_path
# Container: app-consumer-hot (port 8083)
import uvicorn
from fastapi import FastAPI, WebSocket

app = FastAPI(title="Hot Consumer")


@app.get("/health")
def health():
    return {"status": "ok", "service": "consumer-hot"}


@app.websocket("/ws/alerts")
async def ws_alerts(websocket: WebSocket):
    await websocket.accept()
    await websocket.send_text("connected")
    await websocket.close()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8083)
