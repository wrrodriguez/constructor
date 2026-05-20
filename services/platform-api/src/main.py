# src/main.py
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    from src.executions.redis_consumer import start_result_consumer
    task = asyncio.create_task(start_result_consumer())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def create_app() -> FastAPI:
    app = FastAPI(
        title="Constructor Platform API",
        version="0.1.0",
        docs_url="/docs" if settings.environment != "production" else None,
        lifespan=lifespan,
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

    from src.tenants.router import router as tenants_router
    from src.users.router import router as users_router
    app.include_router(tenants_router)
    app.include_router(users_router)

    from src.workflows.router import router as workflows_router
    app.include_router(workflows_router)

    from src.executions.router import router as executions_router
    app.include_router(executions_router)

    from src.webhooks.router import router as webhooks_router
    app.include_router(webhooks_router)

    return app


app = create_app()


@app.get("/health")
async def health():
    return {"status": "ok"}
