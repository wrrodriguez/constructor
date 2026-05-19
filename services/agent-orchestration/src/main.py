# src/main.py
from fastapi import FastAPI
from src.config import settings

app = FastAPI(
    title="Constructor Agent Orchestration",
    version="0.1.0",
    docs_url="/docs" if settings.environment != "production" else None,
)


@app.get("/health")
async def health():
    return {"status": "ok"}
