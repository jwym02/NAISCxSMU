# FastAPI app + router registration
import uvicorn
from fastapi import FastAPI

app = FastAPI(title="Query Service")


@app.get("/health")
def health():
    return {"status": "ok", "service": "query"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)
