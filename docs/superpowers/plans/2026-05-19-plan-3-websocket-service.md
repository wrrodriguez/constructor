# Constructor Platform — Plan 3: WebSocket Service

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Construir el WebSocket Service y la publicación de eventos en Platform API para entregar actualizaciones de ejecuciones en tiempo real al frontend Angular vía Socket.IO.

**Architecture:** Platform API publica cambios de estado en el stream `constructor:execution.status.changed`. El WebSocket Service (FastAPI + python-socketio) consume ese stream y hace `sio.emit()` a rooms indexadas por `execution_id`. Los clientes se autentican con JWT en el handshake y se suscriben a rooms específicas con verificación de ownership en PostgreSQL.

**Tech Stack:** Python 3.12, FastAPI, python-socketio 5.x, PyJWT[cryptography], SQLAlchemy 2.0 async, redis[asyncio], pydantic-settings, pytest, testcontainers, cryptography

---

## Estructura de archivos

### Nuevo servicio
```
services/websocket-service/
├── src/
│   ├── __init__.py
│   ├── config.py              — Settings: database_url, redis_url, jwt_public_key_path, port
│   ├── events.py              — ExecutionStatusEvent (Pydantic)
│   ├── auth.py                — verify_token(token) → dict — usa PyJWT RS256
│   ├── database.py            — create_async_engine, AsyncSessionFactory
│   ├── socket_manager.py      — sio = AsyncServer(async_mode="asgi")
│   ├── main.py                — handlers @sio.event + FastAPI /health + app = ASGIApp(sio, fastapi)
│   ├── worker.py              — asyncio.gather(uvicorn, consumer)
│   └── consumer/
│       ├── __init__.py
│       └── redis_consumer.py  — StatusConsumer: xreadgroup → sio.emit()
├── tests/
│   ├── __init__.py
│   ├── conftest.py            — RSA keygen + env vars + TestContainers Redis fixtures
│   ├── test_auth.py           — verify_token: válido/expirado/inválido/sin claims
│   ├── test_socket.py         — connect, join_execution, tenant isolation
│   └── test_consumer.py       — consumer publica → sio.emit() con args correctos
├── pyproject.toml
├── Dockerfile
└── .env.example
```

### Cambios en servicios existentes
```
services/platform-api/src/executions/executor.py   — agregar _publish_status_change() + 5 llamadas
services/platform-api/tests/test_executor.py       — nuevo test verifica publicación Redis
docker-compose.yml                                 — agregar servicio websocket-service puerto 8002
```

---

## Task 1: Platform API — Publicar eventos de estado

**Files:**
- Modify: `services/platform-api/src/executions/executor.py`
- Test: `services/platform-api/tests/test_executor.py`

- [ ] **Step 1: Escribir el test que falla**

Agregar al final de `services/platform-api/tests/test_executor.py`:

```python
@pytest.mark.asyncio
async def test_executor_publishes_status_changed_events():
    """execute_workflow() publica en constructor:execution.status.changed en cada transición."""
    from src.executions.executor import execute_workflow

    mock_redis = AsyncMock()
    mock_redis.xadd = AsyncMock()
    mock_dispatch = AsyncMock(return_value={"text": "result"})

    with patch("src.executions.executor.dispatch_agent_step", mock_dispatch):
        with patch("src.executions.executor.get_redis", AsyncMock(return_value=mock_redis)):
            mock_execution = MagicMock()
            mock_execution.id = uuid.uuid4()
            mock_execution.tenant_id = uuid.uuid4()
            mock_execution.context = {}
            mock_execution.status = "pending"
            mock_execution.current_step_id = None

            mock_definition = MagicMock()
            mock_definition.steps = [
                {
                    "id": "s1",
                    "type": "agent",
                    "config": {"task_type": "default", "prompt": "analyze"},
                    "next": None,
                    "timeout_seconds": 60,
                }
            ]

            async def mock_get(model, pk):
                if "ProcessExecution" in str(model):
                    return mock_execution
                return mock_definition

            mock_db = AsyncMock(spec=AsyncSession)
            mock_db.get = AsyncMock(side_effect=mock_get)
            mock_db.commit = AsyncMock()

            await execute_workflow(
                mock_execution.id, uuid.uuid4(), mock_execution.tenant_id, mock_db,
            )

    # running + step_start + completed = al menos 3 publicaciones
    assert mock_redis.xadd.call_count >= 3
    streams = [call.args[0] for call in mock_redis.xadd.call_args_list]
    assert all(s == "constructor:execution.status.changed" for s in streams)
    # El último evento debe tener status=completed
    import json
    last_data = json.loads(mock_redis.xadd.call_args_list[-1].args[1]["data"])
    assert last_data["status"] == "completed"
```

- [ ] **Step 2: Correr el test — debe fallar**

```bash
cd services/platform-api
python -m pytest tests/test_executor.py::test_executor_publishes_status_changed_events -v
```

Esperado: `FAILED — AssertionError` (xadd nunca es llamado)

- [ ] **Step 3: Implementar `_publish_status_change` en executor.py**

Reemplazar el contenido completo de `services/platform-api/src/executions/executor.py`:

```python
# src/executions/executor.py
import asyncio
import json
import uuid
import logging
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from src.workflows.models import ProcessExecution
from src.executions.condition import evaluate_condition
from src.executions.transform import apply_transform
from src.executions.events import AgentTask
from src.executions.dispatcher import register_pending
from src.database import get_redis

logger = logging.getLogger(__name__)

TASK_STREAM = "constructor:agent.task.created"
STATUS_CHANGED_STREAM = "constructor:execution.status.changed"


async def _publish_status_change(execution: ProcessExecution) -> None:
    """Publica cambio de estado al stream para el WebSocket Service. No lanza excepciones."""
    try:
        redis = await get_redis()
        event = {
            "execution_id": str(execution.id),
            "tenant_id": str(execution.tenant_id),
            "status": execution.status,
            "current_step_id": execution.current_step_id,
            "context": execution.context,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await redis.xadd(STATUS_CHANGED_STREAM, {"data": json.dumps(event)})
    except Exception as exc:
        logger.warning("Failed to publish execution status change: %s", exc)


async def dispatch_agent_step(
    execution: ProcessExecution,
    step: dict,
) -> dict:
    """Publish AgentTask to Redis Streams and await the result via Future."""
    task = AgentTask(
        task_id=uuid.uuid4(),
        execution_id=execution.id,
        tenant_id=execution.tenant_id,
        step_id=step["id"],
        task_type=step["config"].get("task_type", "default"),
        prompt=step["config"].get("prompt", ""),
        context=execution.context,
        tools_allowed=step["config"].get("tools_allowed", []),
        model_override=step["config"].get("model_override"),
        timeout_seconds=step.get("timeout_seconds", 300),
        max_iterations=step["config"].get("max_iterations", 20),
    )
    future = register_pending(execution.id)
    redis = await get_redis()
    await redis.xadd(TASK_STREAM, {"data": task.model_dump_json()})
    return await asyncio.wait_for(future, timeout=task.timeout_seconds)


async def execute_workflow(
    execution_id: uuid.UUID,
    process_id: uuid.UUID,
    tenant_id: uuid.UUID,
    db: AsyncSession,
) -> None:
    """Main workflow execution loop. Runs as an asyncio.Task."""
    from src.workflows.models import ProcessDefinition

    execution = await db.get(ProcessExecution, execution_id)
    definition = await db.get(ProcessDefinition, process_id)

    if not execution or not definition:
        logger.error("Execution or definition not found: %s / %s", execution_id, process_id)
        return

    steps_map: dict[str, dict] = {s["id"]: s for s in definition.steps}
    current_step_id: str | None = definition.steps[0]["id"] if definition.steps else None

    execution.status = "running"
    execution.started_at = datetime.now(timezone.utc)
    await db.commit()
    await _publish_status_change(execution)

    try:
        while current_step_id:
            step = steps_map.get(current_step_id)
            if not step:
                raise ValueError(f"Step '{current_step_id}' not in definition")

            execution.current_step_id = current_step_id
            await db.commit()
            await _publish_status_change(execution)

            match step["type"]:
                case "condition":
                    current_step_id = evaluate_condition(step["config"], execution.context)
                    continue
                case "transform":
                    execution.context = apply_transform(step["config"], execution.context)
                case "agent":
                    result = await dispatch_agent_step(execution, step)
                    execution.context = {**execution.context, step["id"]: result}
                case _:
                    pass  # stub: notify, wait, human_approval — avanza al siguiente paso

            current_step_id = step.get("next")
            await db.commit()

        execution.status = "completed"
        execution.current_step_id = None
        execution.completed_at = datetime.now(timezone.utc)
        await db.commit()
        await _publish_status_change(execution)

    except asyncio.TimeoutError:
        execution.status = "failed"
        execution.context = {**execution.context, "_error": f"Timeout on step '{execution.current_step_id}'"}
        await db.commit()
        await _publish_status_change(execution)
    except Exception as exc:
        logger.error("Workflow execution failed: %s", exc)
        execution.status = "failed"
        execution.context = {**execution.context, "_error": str(exc)}
        await db.commit()
        await _publish_status_change(execution)
```

- [ ] **Step 4: Correr el test — debe pasar**

```bash
cd services/platform-api
python -m pytest tests/test_executor.py -v
```

Esperado: `PASSED` en todos (incluido el nuevo)

- [ ] **Step 5: Correr suite completa de platform-api**

```bash
python -m pytest tests/ -q
```

Esperado: `36 passed` (o más con el nuevo test)

- [ ] **Step 6: Commit**

```bash
cd services/platform-api
git add src/executions/executor.py tests/test_executor.py
git commit -m "feat: publish execution status events to Redis stream for WebSocket Service"
```

---

## Task 2: WebSocket Service — Scaffolding

**Files:**
- Create: `services/websocket-service/pyproject.toml`
- Create: `services/websocket-service/Dockerfile`
- Create: `services/websocket-service/.env.example`
- Create: `services/websocket-service/src/__init__.py`
- Create: `services/websocket-service/src/config.py`
- Create: `services/websocket-service/src/events.py`
- Create: `services/websocket-service/src/database.py`
- Create: `services/websocket-service/src/socket_manager.py`
- Create: `services/websocket-service/tests/__init__.py`

- [ ] **Step 1: Crear estructura de directorios**

```bash
mkdir -p services/websocket-service/src/consumer
mkdir -p services/websocket-service/tests
touch services/websocket-service/src/__init__.py
touch services/websocket-service/src/consumer/__init__.py
touch services/websocket-service/tests/__init__.py
```

- [ ] **Step 2: Crear pyproject.toml**

`services/websocket-service/pyproject.toml`:

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "constructor-websocket-service"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "fastapi>=0.115.0",
    "uvicorn[standard]>=0.30.0",
    "python-socketio[asyncio]>=5.11.0",
    "pyjwt[cryptography]>=2.8.0",
    "redis[asyncio]>=5.0.0",
    "sqlalchemy[asyncio]>=2.0.0",
    "asyncpg>=0.29.0",
    "pydantic>=2.7.0",
    "pydantic-settings>=2.3.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.23.0",
    "pytest-cov>=5.0.0",
    "testcontainers[redis]>=4.5.0",
    "cryptography>=42.0.0",
]

[tool.hatch.build.targets.wheel]
packages = ["src"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "session"
asyncio_default_test_loop_scope = "session"
testpaths = ["tests"]

[tool.ruff]
line-length = 100
```

- [ ] **Step 3: Crear Dockerfile**

`services/websocket-service/Dockerfile`:

```dockerfile
FROM python:3.12-slim AS base
WORKDIR /app
RUN pip install hatch

FROM base AS development
COPY pyproject.toml .
RUN pip install -e ".[dev]"
COPY . .

FROM base AS production
COPY pyproject.toml .
RUN pip install .
COPY src ./src
CMD ["python", "-m", "src.worker"]
```

- [ ] **Step 4: Crear .env.example**

`services/websocket-service/.env.example`:

```env
DATABASE_URL=postgresql+asyncpg://constructor:constructor_dev@localhost:5432/constructor_dev
REDIS_URL=redis://:redis_dev_password@localhost:6379/0
JWT_PUBLIC_KEY_PATH=../../keys/public.pem
ENVIRONMENT=development
PORT=8002
```

- [ ] **Step 5: Crear src/config.py**

`services/websocket-service/src/config.py`:

```python
# src/config.py
from functools import cached_property
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    redis_url: str
    jwt_public_key_path: Path
    environment: str = "development"
    port: int = 8002

    @cached_property
    def jwt_public_key(self) -> str:
        return self.jwt_public_key_path.read_text()


settings = Settings()
```

- [ ] **Step 6: Crear src/events.py**

`services/websocket-service/src/events.py`:

```python
# src/events.py
import uuid
from datetime import datetime
from typing import Literal
from pydantic import BaseModel


class ExecutionStatusEvent(BaseModel):
    execution_id: uuid.UUID
    tenant_id: uuid.UUID
    status: Literal["pending", "running", "completed", "failed", "cancelled"]
    current_step_id: str | None
    context: dict
    timestamp: datetime
```

- [ ] **Step 7: Crear src/database.py**

`services/websocket-service/src/database.py`:

```python
# src/database.py
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from src.config import settings

engine = create_async_engine(settings.database_url, echo=False)
AsyncSessionFactory = async_sessionmaker(engine, expire_on_commit=False)
```

- [ ] **Step 8: Crear src/socket_manager.py**

`services/websocket-service/src/socket_manager.py`:

```python
# src/socket_manager.py
import socketio

sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")
```

- [ ] **Step 9: Verificar que los imports funcionan**

```bash
cd services/websocket-service
pip install -e ".[dev]"
python -c "from src.socket_manager import sio; print('OK:', sio)"
```

Esperado: `OK: <socketio.asyncio_server.AsyncServer object at 0x...>`

- [ ] **Step 10: Commit**

```bash
git add services/websocket-service/
git commit -m "feat: websocket-service project scaffolding"
```

---

## Task 3: Auth Module (TDD)

**Files:**
- Create: `services/websocket-service/src/auth.py`
- Create: `services/websocket-service/tests/conftest.py`
- Create: `services/websocket-service/tests/test_auth.py`

- [ ] **Step 1: Crear tests/conftest.py**

`services/websocket-service/tests/conftest.py`:

```python
# tests/conftest.py
import os
import tempfile

# Generar RSA key pair antes de importar cualquier módulo src
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization

_private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_PRIVATE_PEM = _private_key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.TraditionalOpenSSL,
    encryption_algorithm=serialization.NoEncryption(),
)
_PUBLIC_PEM = _private_key.public_key().public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo,
)

# Escribir clave pública a archivo temporal
_pub_key_file = tempfile.NamedTemporaryFile(suffix=".pem", delete=False)
_pub_key_file.write(_PUBLIC_PEM)
_pub_key_file.close()

# Configurar env vars antes de que pydantic-settings lea el entorno
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/test")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("JWT_PUBLIC_KEY_PATH", _pub_key_file.name)

import pytest
import pytest_asyncio
import jwt
from datetime import datetime, timedelta, timezone


def make_token(tenant_id: str, user_id: str, expired: bool = False, missing_tenant: bool = False) -> str:
    exp = datetime.now(timezone.utc) + (
        timedelta(minutes=-1) if expired else timedelta(minutes=15)
    )
    payload: dict = {"sub": user_id, "exp": exp}
    if not missing_tenant:
        payload["tenant_id"] = tenant_id
    return jwt.encode(payload, _PRIVATE_PEM, algorithm="RS256")


@pytest.fixture
def valid_token() -> str:
    return make_token("tenant-111", "user-222")


@pytest.fixture
def expired_token() -> str:
    return make_token("tenant-111", "user-222", expired=True)


@pytest.fixture
def token_missing_tenant() -> str:
    return make_token("tenant-111", "user-222", missing_tenant=True)


@pytest.fixture(scope="module")
def redis_container():
    from testcontainers.redis import RedisContainer
    with RedisContainer("redis:7-alpine") as r:
        yield r


@pytest_asyncio.fixture(scope="module")
async def redis_client(redis_container):
    from redis.asyncio import Redis
    client = Redis(
        host=redis_container.get_container_host_ip(),
        port=int(redis_container.get_exposed_port(6379)),
        decode_responses=True,
    )
    yield client
    await client.aclose()
```

- [ ] **Step 2: Escribir tests/test_auth.py — deben fallar**

`services/websocket-service/tests/test_auth.py`:

```python
# tests/test_auth.py
import pytest
from src.auth import verify_token


def test_valid_token_returns_payload(valid_token):
    payload = verify_token(valid_token)
    assert payload["sub"] == "user-222"
    assert payload["tenant_id"] == "tenant-111"


def test_expired_token_raises(expired_token):
    with pytest.raises(ValueError, match="expired"):
        verify_token(expired_token)


def test_invalid_signature_raises():
    with pytest.raises(ValueError, match="Invalid"):
        verify_token("not.a.valid.token")


def test_missing_tenant_id_raises(token_missing_tenant):
    with pytest.raises(ValueError):
        verify_token(token_missing_tenant)
```

- [ ] **Step 3: Correr los tests — deben fallar**

```bash
cd services/websocket-service
python -m pytest tests/test_auth.py -v
```

Esperado: `ImportError: cannot import name 'verify_token' from 'src.auth'`

- [ ] **Step 4: Implementar src/auth.py**

`services/websocket-service/src/auth.py`:

```python
# src/auth.py
from typing import Any
import jwt
from jwt import ExpiredSignatureError, InvalidTokenError
from src.config import settings

ALGORITHM = "RS256"


def verify_token(token: str) -> dict[str, Any]:
    """Verifica JWT con clave pública RSA. Lanza ValueError si el token es inválido."""
    try:
        return jwt.decode(
            token,
            settings.jwt_public_key,
            algorithms=[ALGORITHM],
            options={"require": ["exp", "sub", "tenant_id"]},
        )
    except ExpiredSignatureError:
        raise ValueError("Token expired")
    except InvalidTokenError as e:
        raise ValueError(f"Invalid token: {e}")
```

- [ ] **Step 5: Correr los tests — deben pasar**

```bash
python -m pytest tests/test_auth.py -v
```

Esperado: `4 passed`

- [ ] **Step 6: Commit**

```bash
git add src/auth.py tests/conftest.py tests/test_auth.py
git commit -m "feat: JWT auth module for websocket-service"
```

---

## Task 4: Socket.IO Handlers (TDD)

**Files:**
- Create: `services/websocket-service/src/main.py`
- Create: `services/websocket-service/tests/test_socket.py`

- [ ] **Step 1: Escribir tests/test_socket.py — deben fallar**

`services/websocket-service/tests/test_socket.py`:

```python
# tests/test_socket.py
import pytest
import socketio
from unittest.mock import patch, AsyncMock


# Importar main dispara el registro de handlers en sio
import src.main  # noqa: F401
from src.socket_manager import sio


@pytest.mark.asyncio
async def test_connect_valid_token_accepted(valid_token):
    client = socketio.AsyncTestClient(sio)
    await client.connect(auth={"token": valid_token})
    assert client.is_connected()
    await client.disconnect()


@pytest.mark.asyncio
async def test_connect_invalid_token_rejected():
    client = socketio.AsyncTestClient(sio)
    await client.connect(auth={"token": "not-a-token"})
    assert not client.is_connected()


@pytest.mark.asyncio
async def test_connect_expired_token_rejected(expired_token):
    client = socketio.AsyncTestClient(sio)
    await client.connect(auth={"token": expired_token})
    assert not client.is_connected()


@pytest.mark.asyncio
async def test_connect_missing_token_rejected():
    client = socketio.AsyncTestClient(sio)
    await client.connect()
    assert not client.is_connected()


@pytest.mark.asyncio
async def test_join_execution_own_tenant(valid_token):
    with patch("src.main._get_execution_tenant", new=AsyncMock(return_value="tenant-111")):
        client = socketio.AsyncTestClient(sio)
        await client.connect(auth={"token": valid_token})
        await client.emit("join_execution", {"execution_id": "exec-abc"})
        received = await client.get_received()
        # No debe haber evento de error
        assert not any(e["name"] == "error" for e in received)
        await client.disconnect()


@pytest.mark.asyncio
async def test_join_execution_other_tenant_rejected(valid_token):
    # La ejecución pertenece a otro tenant
    with patch("src.main._get_execution_tenant", new=AsyncMock(return_value="tenant-OTHER")):
        client = socketio.AsyncTestClient(sio)
        await client.connect(auth={"token": valid_token})
        await client.emit("join_execution", {"execution_id": "exec-abc"})
        received = await client.get_received()
        errors = [e for e in received if e["name"] == "error"]
        assert len(errors) == 1
        assert errors[0]["args"][0]["code"] == 403
        await client.disconnect()


@pytest.mark.asyncio
async def test_join_execution_not_found_rejected(valid_token):
    with patch("src.main._get_execution_tenant", new=AsyncMock(return_value=None)):
        client = socketio.AsyncTestClient(sio)
        await client.connect(auth={"token": valid_token})
        await client.emit("join_execution", {"execution_id": "exec-nonexistent"})
        received = await client.get_received()
        errors = [e for e in received if e["name"] == "error"]
        assert len(errors) == 1
        assert errors[0]["args"][0]["code"] == 403
        await client.disconnect()


@pytest.mark.asyncio
async def test_two_clients_same_room_both_receive(valid_token):
    with patch("src.main._get_execution_tenant", new=AsyncMock(return_value="tenant-111")):
        client_a = socketio.AsyncTestClient(sio)
        client_b = socketio.AsyncTestClient(sio)
        await client_a.connect(auth={"token": valid_token})
        await client_b.connect(auth={"token": valid_token})

        await client_a.emit("join_execution", {"execution_id": "exec-shared"})
        await client_b.emit("join_execution", {"execution_id": "exec-shared"})

        # Emitir directamente desde el servidor a la room
        await sio.emit("execution_update", {"status": "completed"}, room="exec-shared")

        recv_a = await client_a.get_received()
        recv_b = await client_b.get_received()

        updates_a = [e for e in recv_a if e["name"] == "execution_update"]
        updates_b = [e for e in recv_b if e["name"] == "execution_update"]
        assert len(updates_a) == 1
        assert len(updates_b) == 1

        await client_a.disconnect()
        await client_b.disconnect()
```

- [ ] **Step 2: Correr los tests — deben fallar**

```bash
cd services/websocket-service
python -m pytest tests/test_socket.py -v
```

Esperado: `ImportError` o `ModuleNotFoundError` — `src.main` no existe aún

- [ ] **Step 3: Implementar src/main.py**

`services/websocket-service/src/main.py`:

```python
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
```

- [ ] **Step 4: Correr los tests — deben pasar**

```bash
python -m pytest tests/test_socket.py -v
```

Esperado: `7 passed`

- [ ] **Step 5: Commit**

```bash
git add src/main.py tests/test_socket.py
git commit -m "feat: socket.io handlers with JWT auth and tenant-scoped rooms"
```

---

## Task 5: Redis Consumer (TDD)

**Files:**
- Create: `services/websocket-service/src/consumer/redis_consumer.py`
- Create: `services/websocket-service/tests/test_consumer.py`

- [ ] **Step 1: Escribir tests/test_consumer.py — deben fallar**

`services/websocket-service/tests/test_consumer.py`:

```python
# tests/test_consumer.py
import asyncio
import json
import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch
from src.consumer.redis_consumer import StatusConsumer, STATUS_STREAM


@pytest.mark.asyncio
async def test_consumer_emits_execution_update_to_room(redis_client):
    """Publicar en el stream → sio.emit() con el evento correcto a la room."""
    event = {
        "execution_id": "3f4e7a10-0000-0000-0000-000000000001",
        "tenant_id": "aaaaaaaa-0000-0000-0000-000000000001",
        "status": "completed",
        "current_step_id": None,
        "context": {"result": "ok"},
        "timestamp": "2026-05-19T10:00:00+00:00",
    }
    await redis_client.xadd(STATUS_STREAM, {"data": json.dumps(event)})

    with patch("src.consumer.redis_consumer.sio") as mock_sio:
        mock_sio.emit = AsyncMock()
        consumer = StatusConsumer(redis_client)
        try:
            await asyncio.wait_for(consumer.start(), timeout=3)
        except asyncio.TimeoutError:
            pass

    mock_sio.emit.assert_called_once()
    call_args = mock_sio.emit.call_args
    assert call_args.args[0] == "execution_update"
    assert call_args.kwargs["room"] == "3f4e7a10-0000-0000-0000-000000000001"
    payload = call_args.args[1]
    assert payload["status"] == "completed"
    assert payload["execution_id"] == "3f4e7a10-0000-0000-0000-000000000001"


@pytest.mark.asyncio
async def test_consumer_acks_message_even_on_emit_failure(redis_client):
    """Si sio.emit falla, el mensaje igual se ackea para no bloquear el stream."""
    event = {
        "execution_id": "3f4e7a10-0000-0000-0000-000000000002",
        "tenant_id": "aaaaaaaa-0000-0000-0000-000000000001",
        "status": "failed",
        "current_step_id": None,
        "context": {},
        "timestamp": "2026-05-19T10:00:00+00:00",
    }
    await redis_client.xadd(STATUS_STREAM, {"data": json.dumps(event)})

    with patch("src.consumer.redis_consumer.sio") as mock_sio:
        mock_sio.emit = AsyncMock(side_effect=RuntimeError("emit failed"))
        consumer = StatusConsumer(redis_client)
        try:
            await asyncio.wait_for(consumer.start(), timeout=3)
        except asyncio.TimeoutError:
            pass

    # El stream no debe tener mensajes pendientes (todos ackados)
    pending = await redis_client.xpending(
        STATUS_STREAM, "websocket-service", "-", "+", 10
    )
    assert len(pending) == 0


@pytest.mark.asyncio
async def test_consumer_skips_malformed_message(redis_client):
    """Mensajes malformados se ackean y no detienen el consumer."""
    await redis_client.xadd(STATUS_STREAM, {"data": "not-valid-json"})

    with patch("src.consumer.redis_consumer.sio") as mock_sio:
        mock_sio.emit = AsyncMock()
        consumer = StatusConsumer(redis_client)
        try:
            await asyncio.wait_for(consumer.start(), timeout=3)
        except asyncio.TimeoutError:
            pass

    # sio.emit no fue llamado (mensaje ignorado)
    mock_sio.emit.assert_not_called()
```

- [ ] **Step 2: Correr los tests — deben fallar**

```bash
cd services/websocket-service
python -m pytest tests/test_consumer.py -v
```

Esperado: `ImportError: cannot import name 'StatusConsumer'`

- [ ] **Step 3: Implementar src/consumer/redis_consumer.py**

`services/websocket-service/src/consumer/redis_consumer.py`:

```python
# src/consumer/redis_consumer.py
import asyncio
import logging
from redis.asyncio import Redis
from src.events import ExecutionStatusEvent
from src.socket_manager import sio

logger = logging.getLogger(__name__)

STATUS_STREAM = "constructor:execution.status.changed"
CONSUMER_GROUP = "websocket-service"
CONSUMER_NAME = "ws-1"


class StatusConsumer:
    def __init__(self, redis: Redis):
        self._redis = redis

    async def start(self) -> None:
        try:
            await self._redis.xgroup_create(STATUS_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
        except Exception:
            pass  # group already exists

        while True:
            try:
                messages = await self._redis.xreadgroup(
                    CONSUMER_GROUP, CONSUMER_NAME,
                    {STATUS_STREAM: ">"},
                    count=10, block=2000,
                )
                for _stream, entries in (messages or []):
                    for entry_id, fields in entries:
                        await self._process(entry_id, fields)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Consumer error: %s", exc)
                await asyncio.sleep(1)

    async def _process(self, entry_id: str, fields: dict) -> None:
        try:
            event = ExecutionStatusEvent.model_validate_json(fields["data"])
            await sio.emit(
                "execution_update",
                event.model_dump(mode="json"),
                room=str(event.execution_id),
            )
        except Exception as exc:
            logger.error("Failed to process status event %s: %s", entry_id, exc)
        finally:
            await self._redis.xack(STATUS_STREAM, CONSUMER_GROUP, entry_id)
```

- [ ] **Step 4: Correr los tests — deben pasar**

```bash
python -m pytest tests/test_consumer.py -v
```

Esperado: `3 passed`

- [ ] **Step 5: Correr toda la suite del servicio**

```bash
python -m pytest tests/ -v
```

Esperado: `14 passed` (4 auth + 7 socket + 3 consumer)

- [ ] **Step 6: Commit**

```bash
git add src/consumer/ tests/test_consumer.py
git commit -m "feat: redis status consumer for websocket-service"
```

---

## Task 6: Worker + docker-compose

**Files:**
- Create: `services/websocket-service/src/worker.py`
- Modify: `docker-compose.yml`

- [ ] **Step 1: Crear src/worker.py**

`services/websocket-service/src/worker.py`:

```python
# src/worker.py
import asyncio
import logging
import uvicorn
from redis.asyncio import Redis
from src.config import settings
from src.main import app
from src.consumer.redis_consumer import StatusConsumer

logging.basicConfig(level=logging.INFO)


async def main() -> None:
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    consumer = StatusConsumer(redis)
    server = uvicorn.Server(
        uvicorn.Config(app, host="0.0.0.0", port=settings.port, log_level="info")  # nosec B104
    )
    await asyncio.gather(server.serve(), consumer.start())


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 2: Agregar websocket-service a docker-compose.yml**

En `docker-compose.yml`, agregar después del servicio `agent-orchestration`:

```yaml
  websocket-service:
    build:
      context: ./services/websocket-service
      target: development
    ports:
      - "8002:8002"
    environment:
      DATABASE_URL: postgresql+asyncpg://constructor:constructor_dev@postgres:5432/constructor_dev
      REDIS_URL: redis://:redis_dev_password@redis:6379/0
      JWT_PUBLIC_KEY_PATH: /app/keys/public.pem
      ENVIRONMENT: development
    volumes:
      - ./services/websocket-service:/app
      - ./keys:/app/keys
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    command: python -m src.worker
```

- [ ] **Step 3: Verificar que el build funciona**

```bash
cd constructor
docker compose build websocket-service
```

Esperado: `Image constructor-websocket-service Built`

- [ ] **Step 4: Levantar el servicio y verificar /health**

```bash
docker compose up -d websocket-service
sleep 5
curl http://localhost:8002/health
```

Esperado: `{"status":"ok"}`

- [ ] **Step 5: Verificar que los 5 containers están corriendo**

```bash
docker compose ps
```

Esperado: 5 servicios con estado `running` (postgres, redis, platform-api, agent-orchestration, websocket-service)

- [ ] **Step 6: Correr suite completa**

```bash
cd services/websocket-service
python -m pytest tests/ -q
```

Esperado: `14 passed`

- [ ] **Step 7: Commit final**

```bash
cd constructor
git add services/websocket-service/src/worker.py docker-compose.yml
git commit -m "feat: websocket-service worker and docker-compose integration"
```

---

## Self-review

**Cobertura del spec:**
- ✓ Nuevo servicio python-socketio
- ✓ Stream `constructor:execution.status.changed` publicado por Platform API
- ✓ JWT auth en el handshake (RS256, misma clave pública)
- ✓ Rooms por `execution_id` con verificación de ownership en DB
- ✓ Consumer Redis → `sio.emit("execution_update", room=execution_id)`
- ✓ `GET /health` endpoint
- ✓ docker-compose integrado
- ✓ Tests: auth, socket handlers, consumer

**Tipos consistentes:**
- `ExecutionStatusEvent` definido en `src/events.py` y usado en `redis_consumer.py`
- `verify_token()` en `src/auth.py` usado en `src/main.py`
- `sio` importado de `src/socket_manager.py` en `src/main.py` y `src/consumer/redis_consumer.py`
- `_get_execution_tenant()` definida y mockeada con el mismo nombre en todos los tests
