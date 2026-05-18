# src/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.config import settings


def create_app() -> FastAPI:
    app = FastAPI(
        title="Constructor Platform API",
        version="0.1.0",
        docs_url="/docs" if settings.environment != "production" else None,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:4200"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    return app


app = create_app()


@app.get("/health")
async def health():
    return {"status": "ok"}
