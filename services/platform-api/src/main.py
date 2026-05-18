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
        allow_origins=settings.cors_allowed_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    from src.auth.router import router as auth_router
    app.include_router(auth_router)

    return app


app = create_app()


@app.get("/health")
async def health():
    return {"status": "ok"}
