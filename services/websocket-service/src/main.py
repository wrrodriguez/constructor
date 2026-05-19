# src/main.py
import logging
import socketio
from fastapi import FastAPI
from sqlalchemy import text
from src.config import settings
from src.auth import verify_token
from src.database import AsyncSessionFactory
from src.socket_manager import sio

logger = logging.getLogger(__name__)


async def _get_execution_tenant(execution_id: str) -> str | None:
    """Consulta DB para verificar a qué tenant pertenece la ejecución."""
    async with AsyncSessionFactory() as db:
        result = await db.execute(
            text("SELECT tenant_id FROM process_executions WHERE id = :id"),
            {"id": execution_id},
        )
        row = result.fetchone()
        return str(row[0]) if row else None


@sio.event
async def connect(sid, environ, auth):
    if not auth or "token" not in auth:
        raise ConnectionRefusedError("Missing token")
    try:
        payload = verify_token(auth["token"])
    except ValueError as exc:
        raise ConnectionRefusedError(str(exc))
    async with sio.session(sid) as session:
        session["tenant_id"] = payload["tenant_id"]
        session["user_id"] = payload["sub"]
    logger.info("Client %s connected (tenant=%s)", sid, payload["tenant_id"])


@sio.event
async def disconnect(sid):
    logger.info("Client %s disconnected", sid)


@sio.event
async def join_execution(sid, data):
    execution_id = data.get("execution_id") if isinstance(data, dict) else None
    if not execution_id:
        await sio.emit("error", {"code": 400, "message": "execution_id required"}, to=sid)
        return

    async with sio.session(sid) as session:
        tenant_id = session.get("tenant_id")

    execution_tenant = await _get_execution_tenant(execution_id)
    if execution_tenant is None or execution_tenant != tenant_id:
        await sio.emit("error", {"code": 403, "message": "Forbidden"}, to=sid)
        return

    sio.enter_room(sid, execution_id)
    logger.info("Client %s joined room %s", sid, execution_id)


@sio.event
async def leave_execution(sid, data):
    execution_id = data.get("execution_id") if isinstance(data, dict) else None
    if execution_id:
        sio.leave_room(sid, execution_id)
        logger.info("Client %s left room %s", sid, execution_id)


_fastapi = FastAPI(
    title="Constructor WebSocket Service",
    version="0.1.0",
    docs_url="/docs" if settings.environment != "production" else None,
)


@_fastapi.get("/health")
async def health():
    return {"status": "ok"}


# Socket.IO maneja /socket.io/*, FastAPI maneja el resto
app = socketio.ASGIApp(sio, other_asgi_app=_fastapi)
