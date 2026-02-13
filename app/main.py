from fastapi import FastAPI, HTTPException
from app.pipeline.runner import run_001_002_003

app = FastAPI(title="cash-ai-01", version="1.0.0")

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/v1/pipeline")
def pipeline(payload: dict):
    try:
        r = run_001_002_003(payload)
        return {"ok": True, "result": r.get("data"), "output": r.get("output")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
