# FastAPI app + router registration
import uvicorn
from fastapi import FastAPI
from dotenv import load_dotenv
load_dotenv()

app = FastAPI(title="Pipeline Service")


@app.get("/health")
def health():
    return {"status": "ok", "service": "pipeline"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
