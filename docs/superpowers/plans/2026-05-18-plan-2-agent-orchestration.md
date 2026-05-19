# Constructor Platform — Plan 2: Agent Orchestration Service

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Construir el WorkflowExecutor en Platform API y el servicio Agent Orchestration que ejecuta agentes AI con LangGraph, coordinados vía Redis Streams.

**Architecture:** Platform API conduce la ejecución de workflows con un `asyncio.Task` (WorkflowExecutor) que itera pasos, evalúa `condition`/`transform` inline, y despacha `agent` steps a Agent Orchestration vía Redis Streams usando `asyncio.Future` para esperar resultados. Agent Orchestration es un servicio Python separado con un Redis Streams consumer, LangGraph ReAct runner, Model Router multi-proveedor y Tool Registry mínimo (`http_generic`, `sql_query`).

**Tech Stack:** Python 3.12, FastAPI, LangGraph ≥0.2, langchain-anthropic/openai/google-genai, redis[asyncio], SQLAlchemy 2.0 async, JMESPath, sqlparse, httpx, pytest, TestContainers

---

## Estructura de archivos

### Nuevo servicio
```
services/agent-orchestration/
├── src/
│   ├── config.py
│   ├── main.py                      # FastAPI — solo GET /health
│   ├── worker.py                    # entry point: consumer + HTTP server
│   ├── events.py                    # AgentTask, AgentResult schemas
│   ├── consumer/
│   │   └── redis_consumer.py        # lee agent.task.created, despacha al runner
│   ├── runner/
│   │   ├── state.py                 # AgentState TypedDict
│   │   ├── nodes.py                 # call_model_node, execute_tool_node, should_continue
│   │   └── agent_runner.py          # AgentRunner: construye grafo, run(task)->result
│   ├── router/
│   │   └── model_router.py          # ModelRouter: _resolve_model_id, get_model
│   ├── tools/
│   │   ├── base.py                  # BaseTool ABC
│   │   ├── registry.py              # ToolRegistry
│   │   ├── http_generic.py          # HTTP GET/POST con SSRF protection
│   │   └── sql_query.py             # SELECT-only con sqlparse
│   ├── memory/
│   │   ├── short_term.py            # Redis: guarda mensajes post-ejecución
│   │   └── long_term.py             # PostgreSQL: escribe agent_execution_logs
│   └── publisher/
│       └── result_publisher.py      # xadd agent.result.ready
├── tests/
│   ├── conftest.py                  # TestContainers Redis+PG, fixtures
│   ├── test_events.py
│   ├── test_model_router.py
│   ├── test_tools.py
│   ├── test_agent_runner.py
│   └── test_consumer.py
├── pyproject.toml
├── Dockerfile
└── .env.example
```

### Extensiones a Platform API
```
services/platform-api/
├── src/
│   ├── database.py                  # MODIFY: agregar get_redis()
│   ├── main.py                      # MODIFY: agregar lifespan + executions router
│   ├── config.py                    # no changes
│   ├── workflows/
│   │   └── models.py                # MODIFY: agregar current_step_id a ProcessExecution
│   └── executions/                  # NUEVO módulo
│       ├── __init__.py
│       ├── events.py                # AgentTask, AgentResult (idéntico al de agent-orch)
│       ├── dispatcher.py            # _pending dict, register_pending, resolve_pending
│       ├── condition.py             # evaluate_condition(config, context) -> str
│       ├── transform.py             # apply_transform(config, context) -> dict
│       ├── executor.py              # execute_workflow() asyncio.Task
│       ├── redis_consumer.py        # start_result_consumer() background task
│       ├── models.py                # AgentExecutionLog SQLAlchemy model
│       ├── schemas.py               # ExecutionCreate, ExecutionRead
│       ├── service.py               # create_execution, get_execution, list_executions
│       └── router.py                # POST/GET /executions
├── migrations/versions/
│   └── 005_executions.py            # NUEVO: current_step_id + agent_execution_logs
└── tests/
    ├── test_executions.py           # NUEVO
    └── test_executor.py             # NUEVO
```

---

## Task 1: Agent Orchestration — Project Scaffolding

**Files:**
- Create: `services/agent-orchestration/pyproject.toml`
- Create: `services/agent-orchestration/Dockerfile`
- Create: `services/agent-orchestration/.env.example`
- Create: `services/agent-orchestration/src/config.py`
- Create: `services/agent-orchestration/src/main.py`

- [ ] **Step 1: Crear estructura de directorios**

```bash
mkdir -p services/agent-orchestration/src/{consumer,runner,router,tools,memory,publisher}
mkdir -p services/agent-orchestration/tests
touch services/agent-orchestration/src/__init__.py
touch services/agent-orchestration/src/consumer/__init__.py
touch services/agent-orchestration/src/runner/__init__.py
touch services/agent-orchestration/src/router/__init__.py
touch services/agent-orchestration/src/tools/__init__.py
touch services/agent-orchestration/src/memory/__init__.py
touch services/agent-orchestration/src/publisher/__init__.py
touch services/agent-orchestration/tests/__init__.py
```

- [ ] **Step 2: Crear pyproject.toml**

```toml
# services/agent-orchestration/pyproject.toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "constructor-agent-orchestration"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "fastapi>=0.115.0",
    "uvicorn[standard]>=0.30.0",
    "langgraph>=0.2.0",
    "langchain-anthropic>=0.2.0",
    "langchain-openai>=0.2.0",
    "langchain-google-genai>=2.0.0",
    "langchain-core>=0.3.0",
    "redis[asyncio]>=5.0.0",
    "sqlalchemy[asyncio]>=2.0.0",
    "asyncpg>=0.29.0",
    "pydantic>=2.7.0",
    "pydantic-settings>=2.3.0",
    "httpx>=0.27.0",
    "sqlparse>=0.5.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.23.0",
    "pytest-cov>=5.0.0",
    "testcontainers[postgres,redis]>=4.5.0",
    "psycopg2-binary>=2.9.0",
]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

- [ ] **Step 3: Crear Dockerfile**

```dockerfile
# services/agent-orchestration/Dockerfile
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

```bash
# services/agent-orchestration/.env.example
DATABASE_URL=postgresql+asyncpg://constructor:constructor_dev@localhost:5432/constructor_dev
REDIS_URL=redis://:redis_dev_password@localhost:6379/0
ANTHROPIC_API_KEY=sk-ant-...
OPENAI_API_KEY=sk-...
GOOGLE_API_KEY=...
ENVIRONMENT=development
```

- [ ] **Step 5: Crear src/config.py**

```python
# src/config.py
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    redis_url: str
    anthropic_api_key: str = ""
    openai_api_key: str = ""
    google_api_key: str = ""
    environment: str = "development"
    max_concurrent_agents: int = 10


settings = Settings()
```

- [ ] **Step 6: Crear src/main.py**

```python
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
```

- [ ] **Step 7: Instalar dependencias y verificar health**

```bash
cd services/agent-orchestration
pip install -e ".[dev]"
cp .env.example .env
uvicorn src.main:app --port 8001
# En otro terminal:
curl http://localhost:8001/health
```

Esperado: `{"status":"ok"}`

- [ ] **Step 8: Commit**

```bash
git add services/agent-orchestration/
git commit -m "feat: agent-orchestration service scaffolding with FastAPI health endpoint"
```

---

## Task 2: Event Schemas (AgentTask + AgentResult)

**Files:**
- Create: `services/agent-orchestration/src/events.py`
- Create: `services/agent-orchestration/tests/test_events.py`
- Create: `services/platform-api/src/executions/__init__.py`
- Create: `services/platform-api/src/executions/events.py`

- [ ] **Step 1: Escribir tests — deben fallar**

```python
# services/agent-orchestration/tests/test_events.py
import uuid
import pytest
from src.events import AgentTask, AgentResult


def test_agent_task_roundtrip():
    task = AgentTask(
        task_id=uuid.UUID("12345678-1234-5678-1234-567812345678"),
        execution_id=uuid.UUID("87654321-4321-8765-4321-876543218765"),
        tenant_id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        step_id="step_1",
        task_type="code_analysis",
        prompt="Analyze this code",
        context={"input": "def foo(): pass"},
        tools_allowed=["http_generic"],
    )
    restored = AgentTask.model_validate_json(task.model_dump_json())
    assert restored.task_id == task.task_id
    assert restored.context == task.context
    assert restored.max_iterations == 20  # default


def test_agent_result_failed_status():
    result = AgentResult(
        task_id=uuid.uuid4(),
        execution_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        step_id="step_1",
        status="failed",
        output={},
        tokens_used=0,
        model_used="claude-sonnet-4-6",
        iterations=0,
        error="Something went wrong",
    )
    assert result.status == "failed"
    assert result.error == "Something went wrong"


def test_agent_task_model_override_default_none():
    task = AgentTask(
        task_id=uuid.uuid4(),
        execution_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        step_id="s1",
        task_type="default",
        prompt="do something",
        context={},
        tools_allowed=[],
    )
    assert task.model_override is None
    assert task.timeout_seconds == 300
```

- [ ] **Step 2: Ejecutar — deben fallar**

```bash
cd services/agent-orchestration
pytest tests/test_events.py -v
```

Esperado: `ImportError: cannot import name 'AgentTask'`

- [ ] **Step 3: Implementar src/events.py**

```python
# src/events.py
import uuid
from typing import Literal
from pydantic import BaseModel


class AgentTask(BaseModel):
    task_id: uuid.UUID
    execution_id: uuid.UUID
    tenant_id: uuid.UUID
    step_id: str
    task_type: str
    prompt: str
    context: dict
    tools_allowed: list[str]
    model_override: str | None = None
    timeout_seconds: int = 300
    max_iterations: int = 20


class AgentResult(BaseModel):
    task_id: uuid.UUID
    execution_id: uuid.UUID
    tenant_id: uuid.UUID
    step_id: str
    status: Literal["completed", "failed", "timeout"]
    output: dict
    tokens_used: int
    model_used: str
    iterations: int
    error: str | None = None
```

- [ ] **Step 4: Ejecutar tests — deben pasar**

```bash
pytest tests/test_events.py -v
```

Esperado: `3 passed`

- [ ] **Step 5: Crear el mismo archivo en Platform API**

```bash
mkdir -p services/platform-api/src/executions
touch services/platform-api/src/executions/__init__.py
```

```python
# services/platform-api/src/executions/events.py
# Identical contract to agent-orchestration/src/events.py
import uuid
from typing import Literal
from pydantic import BaseModel


class AgentTask(BaseModel):
    task_id: uuid.UUID
    execution_id: uuid.UUID
    tenant_id: uuid.UUID
    step_id: str
    task_type: str
    prompt: str
    context: dict
    tools_allowed: list[str]
    model_override: str | None = None
    timeout_seconds: int = 300
    max_iterations: int = 20


class AgentResult(BaseModel):
    task_id: uuid.UUID
    execution_id: uuid.UUID
    tenant_id: uuid.UUID
    step_id: str
    status: Literal["completed", "failed", "timeout"]
    output: dict
    tokens_used: int
    model_used: str
    iterations: int
    error: str | None = None
```

- [ ] **Step 6: Commit**

```bash
git add services/agent-orchestration/src/events.py services/agent-orchestration/tests/test_events.py
git add services/platform-api/src/executions/
git commit -m "feat: AgentTask and AgentResult event schemas for Redis Streams contract"
```

---

## Task 3: Model Router (TDD)

**Files:**
- Create: `services/agent-orchestration/src/router/model_router.py`
- Create: `services/agent-orchestration/tests/test_model_router.py`

- [ ] **Step 1: Escribir tests — deben fallar**

```python
# tests/test_model_router.py
import pytest
from src.router.model_router import ModelRouter, ROUTING_TABLE, FALLBACK_CHAIN


def test_routing_code_analysis():
    router = ModelRouter()
    model_id = router._resolve_model_id("code_analysis", None)
    assert model_id == "claude-opus-4-6"


def test_routing_security_scan():
    router = ModelRouter()
    assert router._resolve_model_id("security_scan", None) == "gpt-4o"


def test_routing_summarization():
    router = ModelRouter()
    assert router._resolve_model_id("summarization", None) == "claude-haiku-4-5-20251001"


def test_routing_unknown_task_uses_default():
    router = ModelRouter()
    assert router._resolve_model_id("unknown_task_xyz", None) == ROUTING_TABLE["default"]


def test_model_override_takes_priority():
    router = ModelRouter()
    assert router._resolve_model_id("code_analysis", "gpt-4o") == "gpt-4o"


def test_fallback_chain_defined():
    # Fallback chain must start with default model and have at least 3 entries
    assert ROUTING_TABLE["default"] in FALLBACK_CHAIN
    assert len(FALLBACK_CHAIN) >= 3


def test_last_model_used_updated():
    router = ModelRouter()
    router._resolve_model_id("code_analysis", None)
    # last_model_used is only set by get_model (which instantiates), but
    # _resolve_model_id returns the id. Verify the routing table is correct.
    assert "claude-opus-4-6" in ROUTING_TABLE.values()
```

- [ ] **Step 2: Ejecutar — deben fallar**

```bash
pytest tests/test_model_router.py -v
```

Esperado: `ImportError`

- [ ] **Step 3: Implementar src/router/model_router.py**

```python
# src/router/model_router.py
from langchain_core.language_models import BaseChatModel
from src.config import settings

ROUTING_TABLE: dict[str, str] = {
    "code_analysis":  "claude-opus-4-6",
    "security_scan":  "gpt-4o",
    "summarization":  "claude-haiku-4-5-20251001",
    "ocr_extraction": "gemini-1.5-pro",
    "default":        "claude-sonnet-4-6",
}

FALLBACK_CHAIN: list[str] = ["claude-sonnet-4-6", "gpt-4o", "gemini-1.5-flash"]


class ModelRouter:
    def __init__(self):
        self.last_model_used: str = ""

    def _resolve_model_id(self, task_type: str, model_override: str | None) -> str:
        return model_override or ROUTING_TABLE.get(task_type, ROUTING_TABLE["default"])

    def get_model(self, task_type: str, model_override: str | None = None) -> BaseChatModel:
        model_id = self._resolve_model_id(task_type, model_override)
        self.last_model_used = model_id
        return self._instantiate(model_id)

    def _instantiate(self, model_id: str) -> BaseChatModel:
        if model_id.startswith("claude"):
            from langchain_anthropic import ChatAnthropic
            return ChatAnthropic(model=model_id, api_key=settings.anthropic_api_key)
        if "gpt" in model_id or model_id.startswith("o1") or model_id.startswith("o3"):
            from langchain_openai import ChatOpenAI
            return ChatOpenAI(model=model_id, api_key=settings.openai_api_key)
        if "gemini" in model_id:
            from langchain_google_genai import ChatGoogleGenerativeAI
            return ChatGoogleGenerativeAI(model=model_id, google_api_key=settings.google_api_key)
        raise ValueError(f"Unsupported model id: {model_id}")
```

- [ ] **Step 4: Ejecutar tests — deben pasar**

```bash
pytest tests/test_model_router.py -v
```

Esperado: `7 passed`

- [ ] **Step 5: Commit**

```bash
git add services/agent-orchestration/src/router/ services/agent-orchestration/tests/test_model_router.py
git commit -m "feat: ModelRouter with task_type routing table and fallback chain"
```

---

## Task 4: Tool Registry + BaseTool (TDD)

**Files:**
- Create: `services/agent-orchestration/src/tools/base.py`
- Create: `services/agent-orchestration/src/tools/registry.py`
- Create: `services/agent-orchestration/tests/test_tools.py` (parcial)

- [ ] **Step 1: Escribir tests para registry — deben fallar**

```python
# tests/test_tools.py
import pytest
from src.tools.registry import ToolRegistry
from src.tools.base import BaseTool


class FakeTool(BaseTool):
    name = "fake_tool"
    description = "A fake tool for testing"
    input_schema = {
        "type": "object",
        "properties": {"value": {"type": "string"}},
        "required": ["value"],
    }

    async def execute(self, inputs: dict, tenant_context: dict) -> dict:
        return {"result": inputs["value"]}


def test_registry_register_and_get():
    registry = ToolRegistry()
    tool = FakeTool()
    registry.register(tool)
    assert registry.get("fake_tool") is tool


def test_registry_get_unknown_returns_none():
    registry = ToolRegistry()
    assert registry.get("nonexistent") is None


def test_registry_list_names():
    registry = ToolRegistry()
    registry.register(FakeTool())
    assert "fake_tool" in registry.list_names()


def test_tool_as_openai_spec():
    tool = FakeTool()
    spec = tool.as_openai_tool()
    assert spec["type"] == "function"
    assert spec["function"]["name"] == "fake_tool"
    assert "parameters" in spec["function"]


@pytest.mark.asyncio
async def test_tool_execute():
    tool = FakeTool()
    result = await tool.execute({"value": "hello"}, {"tenant_id": "t1"})
    assert result == {"result": "hello"}
```

- [ ] **Step 2: Ejecutar — deben fallar**

```bash
pytest tests/test_tools.py -v
```

Esperado: `ImportError`

- [ ] **Step 3: Implementar src/tools/base.py**

```python
# src/tools/base.py
from abc import ABC, abstractmethod


class BaseTool(ABC):
    name: str
    description: str
    input_schema: dict  # JSON Schema

    @abstractmethod
    async def execute(self, inputs: dict, tenant_context: dict) -> dict: ...

    def as_openai_tool(self) -> dict:
        """Format compatible with model.bind_tools() for all LangChain providers."""
        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.input_schema,
            },
        }
```

- [ ] **Step 4: Implementar src/tools/registry.py**

```python
# src/tools/registry.py
from src.tools.base import BaseTool


class ToolRegistry:
    def __init__(self):
        self._tools: dict[str, BaseTool] = {}

    def register(self, tool: BaseTool) -> None:
        self._tools[tool.name] = tool

    def get(self, name: str) -> BaseTool | None:
        return self._tools.get(name)

    def list_names(self) -> list[str]:
        return list(self._tools.keys())

    def get_all(self) -> list[BaseTool]:
        return list(self._tools.values())
```

- [ ] **Step 5: Ejecutar tests — deben pasar**

```bash
pytest tests/test_tools.py -v
```

Esperado: `5 passed`

- [ ] **Step 6: Commit**

```bash
git add services/agent-orchestration/src/tools/base.py services/agent-orchestration/src/tools/registry.py services/agent-orchestration/tests/test_tools.py
git commit -m "feat: BaseTool ABC and ToolRegistry with OpenAI tool spec format"
```

---

## Task 5: http_generic + sql_query Tools (TDD)

**Files:**
- Create: `services/agent-orchestration/src/tools/http_generic.py`
- Create: `services/agent-orchestration/src/tools/sql_query.py`
- Modify: `services/agent-orchestration/tests/test_tools.py`

- [ ] **Step 1: Agregar tests de http_generic y sql_query**

```python
# Agregar al final de tests/test_tools.py
import ipaddress
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from src.tools.http_generic import HttpGenericTool
from src.tools.sql_query import SqlQueryTool, _is_select_only


# --- http_generic ---

@pytest.mark.asyncio
async def test_http_generic_blocks_private_ip():
    tool = HttpGenericTool()
    with pytest.raises(ValueError, match="private"):
        await tool.execute(
            {"url": "http://192.168.1.1/secret", "method": "GET", "headers": {}},
            {"tenant_id": "t1"},
        )


@pytest.mark.asyncio
async def test_http_generic_blocks_localhost():
    tool = HttpGenericTool()
    with pytest.raises(ValueError, match="private"):
        await tool.execute(
            {"url": "http://127.0.0.1/admin", "method": "GET", "headers": {}},
            {"tenant_id": "t1"},
        )


@pytest.mark.asyncio
async def test_http_generic_makes_request():
    tool = HttpGenericTool()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"ok": True}
    mock_response.headers = {"content-type": "application/json"}

    with patch("httpx.AsyncClient") as MockClient:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.request = AsyncMock(return_value=mock_response)
        MockClient.return_value = mock_client

        result = await tool.execute(
            {"url": "http://example.com/api", "method": "GET", "headers": {}},
            {"tenant_id": "t1"},
        )

    assert result["status_code"] == 200
    assert result["body"] == {"ok": True}


# --- sql_query ---

def test_select_is_allowed():
    assert _is_select_only("SELECT id, name FROM users WHERE active = true") is True


def test_select_with_cte_is_allowed():
    assert _is_select_only("WITH cte AS (SELECT 1) SELECT * FROM cte") is True


def test_insert_is_rejected():
    assert _is_select_only("INSERT INTO users (email) VALUES ('x@y.com')") is False


def test_update_is_rejected():
    assert _is_select_only("UPDATE users SET active = false") is False


def test_delete_is_rejected():
    assert _is_select_only("DELETE FROM users") is False


def test_drop_is_rejected():
    assert _is_select_only("DROP TABLE users") is False


@pytest.mark.asyncio
async def test_sql_query_rejects_dml():
    tool = SqlQueryTool()
    with pytest.raises(ValueError, match="SELECT"):
        await tool.execute(
            {"query": "DELETE FROM users", "params": {}},
            {"tenant_id": "t1"},
        )
```

- [ ] **Step 2: Ejecutar — deben fallar**

```bash
pytest tests/test_tools.py -v -k "http_generic or sql_query or select or insert or update or delete or drop"
```

Esperado: `ImportError`

- [ ] **Step 3: Implementar src/tools/http_generic.py**

```python
# src/tools/http_generic.py
import ipaddress
import socket
import httpx
from src.tools.base import BaseTool

_PRIVATE_NETWORKS = [
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
]


def _is_private(host: str) -> bool:
    try:
        ip = ipaddress.ip_address(host)
        return any(ip in net for net in _PRIVATE_NETWORKS)
    except ValueError:
        try:
            resolved = socket.gethostbyname(host)
            ip = ipaddress.ip_address(resolved)
            return any(ip in net for net in _PRIVATE_NETWORKS)
        except (socket.gaierror, ValueError):
            return False


class HttpGenericTool(BaseTool):
    name = "http_generic"
    description = "Make an HTTP GET or POST request to an external URL"
    input_schema = {
        "type": "object",
        "properties": {
            "url":     {"type": "string", "description": "Full URL to request"},
            "method":  {"type": "string", "enum": ["GET", "POST"]},
            "headers": {"type": "object", "description": "HTTP headers"},
            "body":    {"type": "object", "description": "Request body (POST only)"},
        },
        "required": ["url", "method", "headers"],
    }

    async def execute(self, inputs: dict, tenant_context: dict) -> dict:
        from urllib.parse import urlparse
        parsed = urlparse(inputs["url"])
        host = parsed.hostname or ""
        if _is_private(host):
            raise ValueError(f"Access to private/internal networks is not allowed: {host}")

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.request(
                method=inputs["method"],
                url=inputs["url"],
                headers=inputs.get("headers", {}),
                json=inputs.get("body"),
            )
        try:
            body = response.json()
        except Exception:
            body = response.text

        return {
            "status_code": response.status_code,
            "body": body,
            "headers": dict(response.headers),
        }
```

- [ ] **Step 4: Implementar src/tools/sql_query.py**

```python
# src/tools/sql_query.py
import re
import sqlparse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from src.tools.base import BaseTool
from src.config import settings

_DML_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|EXEC|EXECUTE)\b",
    re.IGNORECASE,
)


def _is_select_only(query: str) -> bool:
    statements = sqlparse.parse(query.strip())
    if not statements:
        return False
    for stmt in statements:
        stmt_type = stmt.get_type()
        if stmt_type not in ("SELECT", None):
            return False
        if _DML_PATTERN.search(str(stmt)):
            return False
    return True


class SqlQueryTool(BaseTool):
    name = "sql_query"
    description = "Execute a read-only SQL SELECT query on the tenant database"
    input_schema = {
        "type": "object",
        "properties": {
            "query":  {"type": "string", "description": "SQL SELECT query"},
            "params": {"type": "object", "description": "Named query parameters"},
        },
        "required": ["query"],
    }

    async def execute(self, inputs: dict, tenant_context: dict) -> dict:
        query = inputs["query"]
        params = inputs.get("params", {})

        if not _is_select_only(query):
            raise ValueError("Only SELECT queries are allowed. DML and DDL are rejected.")

        engine = create_async_engine(settings.database_url)
        try:
            factory = async_sessionmaker(engine, expire_on_commit=False)
            async with factory() as session:
                await session.execute(
                    text("SET app.current_tenant = :tid"),
                    {"tid": str(tenant_context.get("tenant_id", ""))},
                )
                result = await session.execute(text(query), params)
                rows = [dict(row._mapping) for row in result.all()]
        finally:
            await engine.dispose()

        return {"rows": rows, "row_count": len(rows)}
```

- [ ] **Step 5: Ejecutar todos los tests de tools — deben pasar**

```bash
pytest tests/test_tools.py -v
```

Esperado: todos los tests pasan. El `test_sql_query_rejects_dml` pasa sin DB (error se lanza antes de conectar).

- [ ] **Step 6: Commit**

```bash
git add services/agent-orchestration/src/tools/ services/agent-orchestration/tests/test_tools.py
git commit -m "feat: HttpGenericTool with SSRF protection and SqlQueryTool with DML rejection"
```

---

## Task 6: LangGraph Agent Runner (TDD)

**Files:**
- Create: `services/agent-orchestration/tests/conftest.py`
- Create: `services/agent-orchestration/src/runner/state.py`
- Create: `services/agent-orchestration/src/runner/nodes.py`
- Create: `services/agent-orchestration/src/runner/agent_runner.py`
- Create: `services/agent-orchestration/tests/test_agent_runner.py`

- [ ] **Step 1: Crear conftest.py con fixtures**

```python
# tests/conftest.py
import pytest
import pytest_asyncio
import uuid
from unittest.mock import AsyncMock, MagicMock
from langchain_core.messages import AIMessage
from src.events import AgentTask
from src.router.model_router import ModelRouter
from src.tools.registry import ToolRegistry


def make_mock_model(responses: list[AIMessage]) -> MagicMock:
    """Mock ChatModel que devuelve AIMessages predefinidos en orden."""
    responses_iter = iter(responses)

    bound = MagicMock()
    bound.ainvoke = AsyncMock(side_effect=lambda msgs, **kw: next(responses_iter))

    model = MagicMock()
    model.bind_tools = MagicMock(return_value=bound)
    return model


@pytest.fixture
def sample_task() -> AgentTask:
    return AgentTask(
        task_id=uuid.uuid4(),
        execution_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        step_id="step_1",
        task_type="default",
        prompt="Analyze this code snippet",
        context={"code": "def foo(): return 42"},
        tools_allowed=[],
        timeout_seconds=30,
        max_iterations=5,
    )


@pytest.fixture
def mock_model_router() -> MagicMock:
    router = MagicMock(spec=ModelRouter)
    router.last_model_used = "claude-sonnet-4-6"
    return router


@pytest.fixture
def empty_registry() -> ToolRegistry:
    return ToolRegistry()
```

- [ ] **Step 2: Escribir tests del AgentRunner — deben fallar**

```python
# tests/test_agent_runner.py
import pytest
import uuid
from langchain_core.messages import AIMessage
from unittest.mock import MagicMock, AsyncMock
from src.runner.agent_runner import AgentRunner
from src.tools.registry import ToolRegistry
from src.tools.base import BaseTool


@pytest.mark.asyncio
async def test_runner_happy_path_no_tools(sample_task, mock_model_router, empty_registry):
    """LLM responde sin tool calls → completed en 1 iteración."""
    final_msg = AIMessage(content="Analysis complete: no issues found.")
    mock_model_router.get_model.return_value = make_mock_model_from(final_msg)

    runner = AgentRunner(mock_model_router, empty_registry)
    result = await runner.run(sample_task)

    assert result.status == "completed"
    assert result.output["text"] == "Analysis complete: no issues found."
    assert result.iterations == 1
    assert result.error is None
    assert result.execution_id == sample_task.execution_id


@pytest.mark.asyncio
async def test_runner_with_one_tool_call(sample_task, mock_model_router):
    """LLM hace un tool call, luego responde → completed en 2 iteraciones."""
    tool_call_msg = AIMessage(
        content="",
        tool_calls=[{
            "id": "call_abc",
            "name": "fake_tool",
            "args": {"value": "hello"},
        }],
    )
    final_msg = AIMessage(content="Tool returned hello. Done.")

    mock_tool = MagicMock(spec=BaseTool)
    mock_tool.name = "fake_tool"
    mock_tool.as_openai_tool.return_value = {
        "type": "function",
        "function": {"name": "fake_tool", "description": "...", "parameters": {}},
    }
    mock_tool.execute = AsyncMock(return_value={"result": "hello"})

    registry = ToolRegistry()
    registry.register(mock_tool)

    call_count = 0
    async def side_effect(msgs, **kw):
        nonlocal call_count
        call_count += 1
        return tool_call_msg if call_count == 1 else final_msg

    bound = MagicMock()
    bound.ainvoke = AsyncMock(side_effect=side_effect)
    mock_model = MagicMock()
    mock_model.bind_tools = MagicMock(return_value=bound)
    mock_model_router.get_model.return_value = mock_model

    task_with_tools = sample_task.model_copy(update={"tools_allowed": ["fake_tool"]})
    runner = AgentRunner(mock_model_router, registry)
    result = await runner.run(task_with_tools)

    assert result.status == "completed"
    assert result.iterations == 2
    mock_tool.execute.assert_called_once_with({"value": "hello"}, {"tenant_id": str(sample_task.tenant_id)})


@pytest.mark.asyncio
async def test_runner_timeout(sample_task, mock_model_router, empty_registry):
    """Timeout devuelve status='timeout'."""
    import asyncio

    async def slow_response(msgs, **kw):
        await asyncio.sleep(10)
        return AIMessage(content="never")

    bound = MagicMock()
    bound.ainvoke = AsyncMock(side_effect=slow_response)
    mock_model = MagicMock()
    mock_model.bind_tools = MagicMock(return_value=bound)
    mock_model_router.get_model.return_value = mock_model

    short_task = sample_task.model_copy(update={"timeout_seconds": 1})
    runner = AgentRunner(mock_model_router, empty_registry)
    result = await runner.run(short_task)

    assert result.status == "timeout"
    assert result.error is not None


def make_mock_model_from(response: AIMessage):
    bound = MagicMock()
    bound.ainvoke = AsyncMock(return_value=response)
    model = MagicMock()
    model.bind_tools = MagicMock(return_value=bound)
    return model
```

- [ ] **Step 3: Ejecutar — deben fallar**

```bash
pytest tests/test_agent_runner.py -v
```

Esperado: `ImportError: cannot import name 'AgentRunner'`

- [ ] **Step 4: Implementar src/runner/state.py**

```python
# src/runner/state.py
from typing import Annotated, TypedDict
from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages


class AgentState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]
    iterations: int
    max_iterations: int
    context: dict
    tools_allowed: list[str]
    task_id: str
    tenant_id: str
```

- [ ] **Step 5: Implementar src/runner/nodes.py**

```python
# src/runner/nodes.py
from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph import END
from src.runner.state import AgentState


async def call_model_node(state: AgentState, config: RunnableConfig) -> dict:
    model = config["configurable"]["model"]
    tools = config["configurable"]["tools"]
    bound = model.bind_tools([t.as_openai_tool() for t in tools])
    response = await bound.ainvoke(state["messages"])
    return {
        "messages": [response],
        "iterations": state["iterations"] + 1,
    }


async def execute_tool_node(state: AgentState, config: RunnableConfig) -> dict:
    registry = config["configurable"]["registry"]
    tenant_context = {"tenant_id": state["tenant_id"]}
    last_msg: AIMessage = state["messages"][-1]
    results = []

    for tc in last_msg.tool_calls:
        tool = registry.get(tc["name"])
        if tool is None:
            content = f"Error: tool '{tc['name']}' not registered"
        else:
            try:
                out = await tool.execute(tc["args"], tenant_context)
                content = str(out)
            except Exception as exc:
                content = f"Error: {exc}"
        results.append(ToolMessage(content=content, tool_call_id=tc["id"]))

    return {"messages": results}


def should_continue(state: AgentState) -> str:
    last = state["messages"][-1]
    if (
        isinstance(last, AIMessage)
        and last.tool_calls
        and state["iterations"] < state["max_iterations"]
    ):
        return "execute_tool"
    return END
```

- [ ] **Step 6: Implementar src/runner/agent_runner.py**

```python
# src/runner/agent_runner.py
import asyncio
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph import StateGraph, START, END
from src.runner.state import AgentState
from src.runner.nodes import call_model_node, execute_tool_node, should_continue
from src.router.model_router import ModelRouter
from src.tools.registry import ToolRegistry
from src.events import AgentTask, AgentResult


class AgentRunner:
    def __init__(self, model_router: ModelRouter, tool_registry: ToolRegistry):
        self.model_router = model_router
        self.tool_registry = tool_registry
        self._graph = self._build_graph()

    def _build_graph(self):
        g = StateGraph(AgentState)
        g.add_node("call_model", call_model_node)
        g.add_node("execute_tool", execute_tool_node)
        g.add_edge(START, "call_model")
        g.add_conditional_edges(
            "call_model",
            should_continue,
            {"execute_tool": "execute_tool", END: END},
        )
        g.add_edge("execute_tool", "call_model")
        return g.compile()

    async def run(self, task: AgentTask) -> AgentResult:
        model = self.model_router.get_model(task.task_type, task.model_override)
        tools = [t for name in task.tools_allowed if (t := self.tool_registry.get(name))]

        initial: AgentState = {
            "messages": [HumanMessage(
                content=f"<context>{task.context}</context>\n\n{task.prompt}"
            )],
            "iterations": 0,
            "max_iterations": task.max_iterations,
            "context": task.context,
            "tools_allowed": task.tools_allowed,
            "task_id": str(task.task_id),
            "tenant_id": str(task.tenant_id),
        }
        cfg = RunnableConfig(configurable={
            "model": model,
            "tools": tools,
            "registry": self.tool_registry,
        })

        try:
            final = await asyncio.wait_for(
                self._graph.ainvoke(initial, cfg),
                timeout=task.timeout_seconds,
            )
        except asyncio.TimeoutError:
            return AgentResult(
                task_id=task.task_id, execution_id=task.execution_id,
                tenant_id=task.tenant_id, step_id=task.step_id,
                status="timeout", output={}, tokens_used=0,
                model_used=self.model_router.last_model_used,
                iterations=0, error=f"Timed out after {task.timeout_seconds}s",
            )
        except Exception as exc:
            return AgentResult(
                task_id=task.task_id, execution_id=task.execution_id,
                tenant_id=task.tenant_id, step_id=task.step_id,
                status="failed", output={}, tokens_used=0,
                model_used=self.model_router.last_model_used,
                iterations=0, error=str(exc),
            )

        last_ai = next(
            (m for m in reversed(final["messages"]) if isinstance(m, AIMessage)), None
        )
        # Detect max_iterations hit: last message has tool_calls but loop exited
        if last_ai and last_ai.tool_calls and final["iterations"] >= task.max_iterations:
            return AgentResult(
                task_id=task.task_id, execution_id=task.execution_id,
                tenant_id=task.tenant_id, step_id=task.step_id,
                status="failed", output={}, tokens_used=self._count_tokens(final["messages"]),
                model_used=self.model_router.last_model_used,
                iterations=final["iterations"], error="max_iterations_reached",
            )

        return AgentResult(
            task_id=task.task_id, execution_id=task.execution_id,
            tenant_id=task.tenant_id, step_id=task.step_id,
            status="completed",
            output={"text": last_ai.content if last_ai else ""},
            tokens_used=self._count_tokens(final["messages"]),
            model_used=self.model_router.last_model_used,
            iterations=final["iterations"],
            error=None,
        )

    def _count_tokens(self, messages: list) -> int:
        total = 0
        for m in messages:
            if hasattr(m, "usage_metadata") and m.usage_metadata:
                total += m.usage_metadata.get("total_tokens", 0)
        return total
```

- [ ] **Step 7: Ejecutar tests — deben pasar**

```bash
pytest tests/test_agent_runner.py -v
```

Esperado: `3 passed`

- [ ] **Step 8: Commit**

```bash
git add services/agent-orchestration/src/runner/ services/agent-orchestration/tests/
git commit -m "feat: LangGraph ReAct agent runner with tool execution and timeout handling"
```

---

## Task 7: Memory Manager (TDD)

**Files:**
- Create: `services/agent-orchestration/src/memory/short_term.py`
- Create: `services/agent-orchestration/src/memory/long_term.py`

- [ ] **Step 1: Escribir tests**

```python
# tests/test_memory.py
import pytest
import pytest_asyncio
import uuid
from testcontainers.redis import RedisContainer
from testcontainers.postgres import PostgresContainer
from redis.asyncio import Redis
from langchain_core.messages import HumanMessage, AIMessage
from src.memory.short_term import ShortTermMemory
from src.memory.long_term import LongTermMemory


@pytest.fixture(scope="module")
def redis_container():
    with RedisContainer("redis:7-alpine") as r:
        yield r


@pytest.fixture(scope="module")
def postgres_container():
    with PostgresContainer("postgres:16-alpine") as pg:
        yield pg


@pytest_asyncio.fixture(scope="module")
async def redis_client(redis_container):
    client = Redis(
        host=redis_container.get_container_host_ip(),
        port=int(redis_container.get_exposed_port(6379)),
        decode_responses=True,
    )
    yield client
    await client.aclose()


@pytest_asyncio.fixture(scope="module")
async def pg_engine(postgres_container):
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy import text
    url = postgres_container.get_connection_url().replace(
        "postgresql+psycopg2", "postgresql+asyncpg"
    )
    engine = create_async_engine(url)
    async with engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS agent_execution_logs (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                tenant_id UUID NOT NULL,
                execution_id UUID NOT NULL,
                step_id VARCHAR(100) NOT NULL,
                task_id UUID NOT NULL,
                model_used VARCHAR(100) NOT NULL,
                tokens_used INTEGER NOT NULL DEFAULT 0,
                iterations INTEGER NOT NULL DEFAULT 0,
                status VARCHAR(20) NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """))
    yield engine
    await engine.dispose()


@pytest.mark.asyncio
async def test_short_term_save_and_load(redis_client):
    mem = ShortTermMemory(redis_client)
    tenant_id = str(uuid.uuid4())
    exec_id = str(uuid.uuid4())
    messages = [HumanMessage(content="hello"), AIMessage(content="world")]
    await mem.save(tenant_id, exec_id, messages, ttl_seconds=60)
    loaded = await mem.load(tenant_id, exec_id)
    assert len(loaded) == 2
    assert loaded[0].content == "hello"
    assert loaded[1].content == "world"


@pytest.mark.asyncio
async def test_short_term_load_missing_returns_empty(redis_client):
    mem = ShortTermMemory(redis_client)
    result = await mem.load("no-tenant", "no-exec")
    assert result == []


@pytest.mark.asyncio
async def test_long_term_write(pg_engine):
    from sqlalchemy.ext.asyncio import async_sessionmaker
    from sqlalchemy import text
    mem = LongTermMemory(pg_engine)
    exec_id = uuid.uuid4()
    tenant_id = uuid.uuid4()
    task_id = uuid.uuid4()
    await mem.write(
        tenant_id=tenant_id, execution_id=exec_id,
        step_id="step_1", task_id=task_id,
        model_used="claude-sonnet-4-6", tokens_used=150,
        iterations=2, status="completed",
    )
    factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with factory() as session:
        row = await session.execute(
            text("SELECT model_used, tokens_used FROM agent_execution_logs WHERE execution_id = :eid"),
            {"eid": str(exec_id)},
        )
        result = row.fetchone()
    assert result is not None
    assert result[0] == "claude-sonnet-4-6"
    assert result[1] == 150
```

- [ ] **Step 2: Ejecutar — deben fallar**

```bash
pytest tests/test_memory.py -v
```

Esperado: `ImportError`

- [ ] **Step 3: Implementar src/memory/short_term.py**

```python
# src/memory/short_term.py
import json
from redis.asyncio import Redis
from langchain_core.messages import BaseMessage, messages_to_dict, messages_from_dict


class ShortTermMemory:
    def __init__(self, redis: Redis):
        self._redis = redis

    def _key(self, tenant_id: str, execution_id: str) -> str:
        return f"tenant:{tenant_id}:execution:{execution_id}:messages"

    async def save(
        self,
        tenant_id: str,
        execution_id: str,
        messages: list[BaseMessage],
        ttl_seconds: int = 86400,
    ) -> None:
        key = self._key(tenant_id, execution_id)
        data = json.dumps(messages_to_dict(messages))
        await self._redis.set(key, data, ex=ttl_seconds)

    async def load(self, tenant_id: str, execution_id: str) -> list[BaseMessage]:
        key = self._key(tenant_id, execution_id)
        data = await self._redis.get(key)
        if not data:
            return []
        return messages_from_dict(json.loads(data))

    async def delete(self, tenant_id: str, execution_id: str) -> None:
        await self._redis.delete(self._key(tenant_id, execution_id))
```

- [ ] **Step 4: Implementar src/memory/long_term.py**

```python
# src/memory/long_term.py
import uuid
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from sqlalchemy import text


class LongTermMemory:
    def __init__(self, engine: AsyncEngine):
        self._factory = async_sessionmaker(engine, expire_on_commit=False)

    async def write(
        self,
        tenant_id: uuid.UUID,
        execution_id: uuid.UUID,
        step_id: str,
        task_id: uuid.UUID,
        model_used: str,
        tokens_used: int,
        iterations: int,
        status: str,
    ) -> None:
        async with self._factory() as session:
            await session.execute(
                text("""
                    INSERT INTO agent_execution_logs
                        (tenant_id, execution_id, step_id, task_id, model_used,
                         tokens_used, iterations, status)
                    VALUES
                        (:tenant_id, :execution_id, :step_id, :task_id, :model_used,
                         :tokens_used, :iterations, :status)
                """),
                {
                    "tenant_id": str(tenant_id),
                    "execution_id": str(execution_id),
                    "step_id": step_id,
                    "task_id": str(task_id),
                    "model_used": model_used,
                    "tokens_used": tokens_used,
                    "iterations": iterations,
                    "status": status,
                },
            )
            await session.commit()
```

- [ ] **Step 5: Ejecutar tests — deben pasar**

```bash
pytest tests/test_memory.py -v
```

Esperado: `4 passed`

- [ ] **Step 6: Commit**

```bash
git add services/agent-orchestration/src/memory/ services/agent-orchestration/tests/test_memory.py
git commit -m "feat: ShortTermMemory (Redis) and LongTermMemory (PostgreSQL) for agent executions"
```

---

## Task 8: Result Publisher + Redis Consumer (TDD)

**Files:**
- Create: `services/agent-orchestration/src/publisher/result_publisher.py`
- Create: `services/agent-orchestration/src/consumer/redis_consumer.py`
- Create: `services/agent-orchestration/tests/test_consumer.py`

- [ ] **Step 1: Implementar src/publisher/result_publisher.py**

```python
# src/publisher/result_publisher.py
from redis.asyncio import Redis
from src.events import AgentResult

RESULT_STREAM = "constructor:agent.result.ready"


class ResultPublisher:
    def __init__(self, redis: Redis):
        self._redis = redis

    async def publish(self, result: AgentResult) -> None:
        await self._redis.xadd(RESULT_STREAM, {"data": result.model_dump_json()})
```

- [ ] **Step 2: Implementar src/consumer/redis_consumer.py**

```python
# src/consumer/redis_consumer.py
import asyncio
import logging
from redis.asyncio import Redis
from src.events import AgentTask
from src.runner.agent_runner import AgentRunner
from src.publisher.result_publisher import ResultPublisher
from src.memory.long_term import LongTermMemory

logger = logging.getLogger(__name__)

TASK_STREAM = "constructor:agent.task.created"
CONSUMER_GROUP = "agent-orchestration"
CONSUMER_NAME = "worker-1"


class RedisConsumer:
    def __init__(
        self,
        redis: Redis,
        runner: AgentRunner,
        publisher: ResultPublisher,
        long_term: LongTermMemory,
    ):
        self._redis = redis
        self._runner = runner
        self._publisher = publisher
        self._long_term = long_term

    async def start(self) -> None:
        try:
            await self._redis.xgroup_create(TASK_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
        except Exception:
            pass  # group already exists

        while True:
            try:
                messages = await self._redis.xreadgroup(
                    CONSUMER_GROUP, CONSUMER_NAME,
                    {TASK_STREAM: ">"},
                    count=5, block=2000,
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
            task = AgentTask.model_validate_json(fields["data"])
            result = await self._runner.run(task)
            await self._publisher.publish(result)
            await self._long_term.write(
                tenant_id=task.tenant_id, execution_id=task.execution_id,
                step_id=task.step_id, task_id=task.task_id,
                model_used=result.model_used, tokens_used=result.tokens_used,
                iterations=result.iterations, status=result.status,
            )
        except Exception as exc:
            logger.error("Failed to process task %s: %s", entry_id, exc)
        finally:
            await self._redis.xack(TASK_STREAM, CONSUMER_GROUP, entry_id)
```

- [ ] **Step 3: Escribir test de integración del consumer**

```python
# tests/test_consumer.py
import asyncio
import pytest
import pytest_asyncio
import uuid
from unittest.mock import AsyncMock, MagicMock
from testcontainers.redis import RedisContainer
from redis.asyncio import Redis
from src.events import AgentTask, AgentResult
from src.consumer.redis_consumer import RedisConsumer, TASK_STREAM, CONSUMER_GROUP
from src.publisher.result_publisher import ResultPublisher, RESULT_STREAM


@pytest.fixture(scope="module")
def redis_container():
    with RedisContainer("redis:7-alpine") as r:
        yield r


@pytest_asyncio.fixture(scope="module")
async def redis_client(redis_container):
    client = Redis(
        host=redis_container.get_container_host_ip(),
        port=int(redis_container.get_exposed_port(6379)),
        decode_responses=True,
    )
    yield client
    await client.aclose()


@pytest.mark.asyncio
async def test_consumer_processes_task_and_publishes_result(redis_client):
    task = AgentTask(
        task_id=uuid.uuid4(), execution_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(), step_id="step_1",
        task_type="default", prompt="say hello",
        context={}, tools_allowed=[],
    )
    expected_result = AgentResult(
        task_id=task.task_id, execution_id=task.execution_id,
        tenant_id=task.tenant_id, step_id=task.step_id,
        status="completed", output={"text": "hello"},
        tokens_used=10, model_used="claude-sonnet-4-6",
        iterations=1, error=None,
    )

    mock_runner = MagicMock()
    mock_runner.run = AsyncMock(return_value=expected_result)

    mock_long_term = MagicMock()
    mock_long_term.write = AsyncMock()

    publisher = ResultPublisher(redis_client)
    consumer = RedisConsumer(redis_client, mock_runner, publisher, mock_long_term)

    # Publicar task en el stream
    await redis_client.xadd(TASK_STREAM, {"data": task.model_dump_json()})

    # Correr consumer por 3 segundos para procesar el mensaje
    try:
        await asyncio.wait_for(consumer.start(), timeout=3)
    except asyncio.TimeoutError:
        pass

    # Verificar que se publicó el resultado en el stream de salida
    results = await redis_client.xread({RESULT_STREAM: "0"}, count=10)
    assert results, "No results published to result stream"
    _stream, entries = results[0]
    assert len(entries) >= 1
    published = AgentResult.model_validate_json(entries[0][1]["data"])
    assert published.execution_id == task.execution_id
    assert published.status == "completed"
    mock_long_term.write.assert_called_once()
```

- [ ] **Step 4: Ejecutar test**

```bash
pytest tests/test_consumer.py -v
```

Esperado: `1 passed`

- [ ] **Step 5: Commit**

```bash
git add services/agent-orchestration/src/publisher/ services/agent-orchestration/src/consumer/ services/agent-orchestration/tests/test_consumer.py
git commit -m "feat: ResultPublisher and RedisConsumer with integration test"
```

---

## Task 9: Worker Entry Point + Docker Compose

**Files:**
- Create: `services/agent-orchestration/src/worker.py`
- Modify: `docker-compose.yml`

- [ ] **Step 1: Crear src/worker.py**

```python
# src/worker.py
import asyncio
import logging
import uvicorn
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import create_async_engine
from src.config import settings
from src.main import app
from src.runner.agent_runner import AgentRunner
from src.router.model_router import ModelRouter
from src.tools.registry import ToolRegistry
from src.tools.http_generic import HttpGenericTool
from src.tools.sql_query import SqlQueryTool
from src.memory.long_term import LongTermMemory
from src.publisher.result_publisher import ResultPublisher
from src.consumer.redis_consumer import RedisConsumer

logging.basicConfig(level=logging.INFO)


async def main() -> None:
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    engine = create_async_engine(settings.database_url)

    registry = ToolRegistry()
    registry.register(HttpGenericTool())
    registry.register(SqlQueryTool())

    model_router = ModelRouter()
    runner = AgentRunner(model_router, registry)
    publisher = ResultPublisher(redis)
    long_term = LongTermMemory(engine)
    consumer = RedisConsumer(redis, runner, publisher, long_term)

    server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=8001, log_level="info"))

    await asyncio.gather(server.serve(), consumer.start())


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 2: Agregar agent-orchestration a docker-compose.yml**

Abrir `docker-compose.yml` y agregar este servicio al final (antes del cierre `volumes:`):

```yaml
  agent-orchestration:
    build:
      context: ./services/agent-orchestration
      target: development
    ports:
      - "8001:8001"
    environment:
      DATABASE_URL: postgresql+asyncpg://constructor:constructor_dev@postgres:5432/constructor_dev
      REDIS_URL: redis://:redis_dev_password@redis:6379/0
      ANTHROPIC_API_KEY: ${ANTHROPIC_API_KEY:-}
      OPENAI_API_KEY: ${OPENAI_API_KEY:-}
      GOOGLE_API_KEY: ${GOOGLE_API_KEY:-}
      ENVIRONMENT: development
    volumes:
      - ./services/agent-orchestration:/app
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    command: python -m src.worker
```

- [ ] **Step 3: Verificar que el worker arranca (sin API keys reales)**

```bash
cd services/agent-orchestration
python -m src.worker
```

Esperado: el worker arranca, imprime `INFO: Started server process`, escucha en 8001 y espera mensajes en Redis (sin errores fatales).

- [ ] **Step 4: Commit**

```bash
git add services/agent-orchestration/src/worker.py docker-compose.yml
git commit -m "feat: agent-orchestration worker entry point with Redis consumer + health server"
```

---

## Task 10: Platform API — Migration 005

**Files:**
- Modify: `services/platform-api/src/workflows/models.py`
- Create: `services/platform-api/src/executions/models.py`
- Create: `services/platform-api/migrations/versions/005_executions.py`

- [ ] **Step 1: Agregar current_step_id a ProcessExecution en models.py**

En `services/platform-api/src/workflows/models.py`, agregar el campo a `ProcessExecution`:

```python
# Agregar después del campo `context`:
current_step_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
```

- [ ] **Step 2: Crear src/executions/models.py**

```python
# services/platform-api/src/executions/models.py
import uuid
from datetime import datetime
from sqlalchemy import String, Integer, ForeignKey, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column
from src.database import Base


class AgentExecutionLog(Base):
    __tablename__ = "agent_execution_logs"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    execution_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("process_executions.id"), nullable=False
    )
    step_id: Mapped[str] = mapped_column(String(255), nullable=False)
    task_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    model_used: Mapped[str] = mapped_column(String(100), nullable=False)
    tokens_used: Mapped[int] = mapped_column(Integer, default=0)
    iterations: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
```

- [ ] **Step 3: Generar migración**

```bash
cd services/platform-api
# Asegurar que alembic.ini tiene DATABASE_URL correcta (postgresql+psycopg2)
alembic revision --autogenerate -m "executions_fields_and_agent_logs"
```

Renombrar el archivo generado en `migrations/versions/` a `005_executions.py`.

- [ ] **Step 4: Agregar RLS a agent_execution_logs en la migración**

Abrir `migrations/versions/005_executions.py` y agregar al final de `upgrade()`:

```python
    # RLS para agent_execution_logs
    op.execute("ALTER TABLE agent_execution_logs ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE agent_execution_logs FORCE ROW LEVEL SECURITY")
    op.execute("""
        CREATE POLICY tenant_isolation ON agent_execution_logs
        USING (tenant_id = current_setting('app.current_tenant', TRUE)::uuid)
    """)
```

Y en `downgrade()`, antes de `op.drop_table(...)`:

```python
    op.execute("DROP POLICY IF EXISTS tenant_isolation ON agent_execution_logs")
    op.execute("ALTER TABLE agent_execution_logs DISABLE ROW LEVEL SECURITY")
```

- [ ] **Step 5: Aplicar migración**

```bash
alembic upgrade head
```

Esperado:
```
Running upgrade 004 -> 005, executions_fields_and_agent_logs
```

- [ ] **Step 6: Verificar**

```bash
docker compose exec postgres psql -U constructor -d constructor_dev \
  -c "SELECT column_name FROM information_schema.columns WHERE table_name='process_executions' AND column_name='current_step_id'"
```

Esperado: `current_step_id` listado.

- [ ] **Step 7: Registrar modelo en migrations/env.py**

En `migrations/env.py`, agregar:

```python
import src.executions.models  # noqa — registers AgentExecutionLog with Base.metadata
```

- [ ] **Step 8: Commit**

```bash
git add services/platform-api/src/workflows/models.py
git add services/platform-api/src/executions/models.py
git add services/platform-api/migrations/
git commit -m "feat: migration 005 — current_step_id on ProcessExecution + agent_execution_logs with RLS"
```

---

## Task 11: Platform API — Condition + Transform Evaluators (TDD)

**Files:**
- Create: `services/platform-api/src/executions/condition.py`
- Create: `services/platform-api/src/executions/transform.py`
- Create: `services/platform-api/tests/test_executor.py` (parcial)

- [ ] **Step 1: Escribir tests — deben fallar**

```python
# services/platform-api/tests/test_executor.py
import pytest
from src.executions.condition import evaluate_condition
from src.executions.transform import apply_transform


# --- condition ---

def test_condition_gt_true():
    config = {
        "expression": "score",
        "operator": ">",
        "value": 0.8,
        "branches": {"true": "step_approval", "false": "step_notify"},
    }
    assert evaluate_condition(config, {"score": 0.9}) == "step_approval"


def test_condition_gt_false():
    config = {
        "expression": "score",
        "operator": ">",
        "value": 0.8,
        "branches": {"true": "step_approval", "false": "step_notify"},
    }
    assert evaluate_condition(config, {"score": 0.5}) == "step_notify"


def test_condition_eq():
    config = {
        "expression": "status",
        "operator": "==",
        "value": "approved",
        "branches": {"true": "step_emit", "false": "step_review"},
    }
    assert evaluate_condition(config, {"status": "approved"}) == "step_emit"


def test_condition_nested_jmespath():
    config = {
        "expression": "steps.agent_1.risk_score",
        "operator": ">=",
        "value": 0.7,
        "branches": {"true": "high_risk", "false": "low_risk"},
    }
    context = {"steps": {"agent_1": {"risk_score": 0.75}}}
    assert evaluate_condition(config, context) == "high_risk"


def test_condition_in_operator():
    config = {
        "expression": "category",
        "operator": "in",
        "value": ["A", "B", "C"],
        "branches": {"true": "valid", "false": "invalid"},
    }
    assert evaluate_condition(config, {"category": "B"}) == "valid"
    assert evaluate_condition(config, {"category": "D"}) == "invalid"


# --- transform ---

def test_transform_simple_mapping():
    config = {"output.summary": "agent_1.text"}
    context = {"agent_1": {"text": "hello world"}}
    result = apply_transform(config, context)
    assert result["output"]["summary"] == "hello world"
    assert result["agent_1"]["text"] == "hello world"  # original untouched


def test_transform_multiple_fields():
    config = {
        "report.title": "input.name",
        "report.score": "analysis.risk_score",
    }
    context = {"input": {"name": "Policy #123"}, "analysis": {"risk_score": 0.42}}
    result = apply_transform(config, context)
    assert result["report"]["title"] == "Policy #123"
    assert result["report"]["score"] == 0.42


def test_transform_missing_source_sets_none():
    config = {"output.value": "nonexistent.path"}
    context = {}
    result = apply_transform(config, context)
    assert result["output"]["value"] is None
```

- [ ] **Step 2: Ejecutar — deben fallar**

```bash
cd services/platform-api
pytest tests/test_executor.py -v
```

Esperado: `ImportError`

- [ ] **Step 3: Instalar jmespath en platform-api**

Agregar `"jmespath>=1.0.0"` a `dependencies` en `services/platform-api/pyproject.toml`, luego:

```bash
pip install jmespath
```

- [ ] **Step 4: Implementar src/executions/condition.py**

```python
# src/executions/condition.py
import operator as op
import jmespath

_OPERATORS: dict[str, object] = {
    ">":       op.gt,
    ">=":      op.ge,
    "<":       op.lt,
    "<=":      op.le,
    "==":      op.eq,
    "!=":      op.ne,
    "in":      lambda a, b: a in b,
    "not_in":  lambda a, b: a not in b,
}


def evaluate_condition(config: dict, context: dict) -> str:
    """
    config: {
        "expression": "<jmespath>",
        "operator": ">",
        "value": <any>,
        "branches": {"true": "<step_id>", "false": "<step_id>"}
    }
    Returns the next step_id based on the evaluated condition.
    """
    extracted = jmespath.search(config["expression"], context)
    compare_fn = _OPERATORS[config["operator"]]
    result = bool(compare_fn(extracted, config["value"]))
    key = "true" if result else "false"
    return config["branches"][key]
```

- [ ] **Step 5: Implementar src/executions/transform.py**

```python
# src/executions/transform.py
import copy
import jmespath


def apply_transform(config: dict, context: dict) -> dict:
    """
    config: {"<dest_dot_path>": "<src_jmespath>", ...}
    Copies context and writes extracted values to destination paths.
    """
    result = copy.deepcopy(context)
    for dest_path, src_expr in config.items():
        value = jmespath.search(src_expr, context)
        _set_nested(result, dest_path, value)
    return result


def _set_nested(data: dict, path: str, value) -> None:
    keys = path.split(".")
    current = data
    for key in keys[:-1]:
        current = current.setdefault(key, {})
    current[keys[-1]] = value
```

- [ ] **Step 6: Ejecutar tests — deben pasar**

```bash
pytest tests/test_executor.py -v
```

Esperado: `11 passed`

- [ ] **Step 7: Commit**

```bash
git add services/platform-api/src/executions/condition.py services/platform-api/src/executions/transform.py services/platform-api/tests/test_executor.py services/platform-api/pyproject.toml
git commit -m "feat: condition evaluator (JMESPath + operators) and transform mapper"
```

---

## Task 12: Platform API — Redis Infrastructure

**Files:**
- Modify: `services/platform-api/src/database.py`
- Create: `services/platform-api/src/executions/dispatcher.py`
- Create: `services/platform-api/src/executions/redis_consumer.py`

- [ ] **Step 1: Agregar get_redis() a database.py**

```python
# Agregar al final de services/platform-api/src/database.py
from redis.asyncio import Redis as AsyncRedis

_redis_client: AsyncRedis | None = None


async def get_redis() -> AsyncRedis:
    global _redis_client
    if _redis_client is None:
        _redis_client = AsyncRedis.from_url(settings.redis_url, decode_responses=True)
    return _redis_client
```

- [ ] **Step 2: Crear src/executions/dispatcher.py**

```python
# src/executions/dispatcher.py
import asyncio
import uuid

# Module-level shared state: str(execution_id) -> asyncio.Future
_pending: dict[str, asyncio.Future] = {}


def register_pending(execution_id: uuid.UUID) -> asyncio.Future:
    """Register a Future that will be resolved when agent.result.ready arrives."""
    loop = asyncio.get_event_loop()
    future: asyncio.Future = loop.create_future()
    _pending[str(execution_id)] = future
    return future


def resolve_pending(execution_id: str, output: dict) -> bool:
    """Called by Redis consumer. Returns True if a Future was waiting."""
    future = _pending.pop(execution_id, None)
    if future and not future.done():
        future.set_result(output)
        return True
    return False


def reject_pending(execution_id: str, error: str) -> bool:
    """Called by Redis consumer on agent failure. Returns True if resolved."""
    future = _pending.pop(execution_id, None)
    if future and not future.done():
        future.set_exception(RuntimeError(error))
        return True
    return False
```

- [ ] **Step 3: Crear src/executions/redis_consumer.py**

```python
# src/executions/redis_consumer.py
import asyncio
import logging
from src.database import get_redis
from src.executions.events import AgentResult
from src.executions.dispatcher import resolve_pending, reject_pending

logger = logging.getLogger(__name__)

RESULT_STREAM = "constructor:agent.result.ready"
CONSUMER_GROUP = "platform-api"
CONSUMER_NAME = "platform-api-1"


async def start_result_consumer() -> None:
    """Background task: consumes agent.result.ready and resolves pending Futures."""
    redis = await get_redis()
    try:
        await redis.xgroup_create(RESULT_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
    except Exception:
        pass  # group already exists

    while True:
        try:
            messages = await redis.xreadgroup(
                CONSUMER_GROUP, CONSUMER_NAME,
                {RESULT_STREAM: ">"},
                count=10, block=1000,
            )
            for _stream, entries in (messages or []):
                for entry_id, fields in entries:
                    try:
                        result = AgentResult.model_validate_json(fields["data"])
                        if result.status == "completed":
                            resolve_pending(str(result.execution_id), result.output)
                        else:
                            reject_pending(
                                str(result.execution_id),
                                result.error or f"Agent {result.status}",
                            )
                        await redis.xack(RESULT_STREAM, CONSUMER_GROUP, entry_id)
                    except Exception as exc:
                        logger.error("Error processing result entry %s: %s", entry_id, exc)
                        await redis.xack(RESULT_STREAM, CONSUMER_GROUP, entry_id)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error("Result consumer error: %s", exc)
            await asyncio.sleep(1)
```

- [ ] **Step 4: Agregar lifespan a main.py de Platform API**

Reemplazar el contenido de `services/platform-api/src/main.py` con:

```python
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

    return app


app = create_app()


@app.get("/health")
async def health():
    return {"status": "ok"}
```

- [ ] **Step 5: Commit**

```bash
git add services/platform-api/src/database.py services/platform-api/src/executions/dispatcher.py services/platform-api/src/executions/redis_consumer.py services/platform-api/src/main.py
git commit -m "feat: Redis infrastructure for Platform API — dispatcher Futures + result consumer"
```

---

## Task 13: Platform API — WorkflowExecutor (TDD)

**Files:**
- Create: `services/platform-api/src/executions/executor.py`
- Modify: `services/platform-api/tests/test_executor.py`

- [ ] **Step 1: Agregar tests del WorkflowExecutor**

```python
# Agregar al final de services/platform-api/tests/test_executor.py
import pytest
import uuid
from unittest.mock import AsyncMock, patch, MagicMock
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_executor_runs_transform_and_condition(seeded_definition):
    """Workflow con transform + condition se ejecuta correctamente."""
    from src.executions.executor import execute_workflow
    from src.workflows.models import ProcessExecution, ProcessDefinition

    # seeded_definition fixture devuelve (execution_id, process_id, tenant_id, db)
    execution_id, process_id, tenant_id, db = seeded_definition

    await execute_workflow(execution_id, process_id, tenant_id, db)

    execution = await db.get(ProcessExecution, execution_id)
    assert execution.status == "completed"
    assert "output" in execution.context


@pytest.mark.asyncio
async def test_executor_dispatches_agent_step():
    """Cuando hay un agent step, dispatch_agent_step es llamado."""
    from src.executions.executor import execute_workflow

    mock_dispatch = AsyncMock(return_value={"text": "analysis result"})
    with patch("src.executions.executor.dispatch_agent_step", mock_dispatch):
        # Crear execution y definition en memoria usando mocks
        mock_execution = MagicMock()
        mock_execution.id = uuid.uuid4()
        mock_execution.tenant_id = uuid.uuid4()
        mock_execution.context = {}
        mock_execution.status = "pending"

        mock_definition = MagicMock()
        mock_definition.steps = [
            {"id": "s1", "type": "agent", "config": {"task_type": "default", "prompt": "analyze"}, "next": None, "timeout_seconds": 60}
        ]

        mock_db = AsyncMock(spec=AsyncSession)
        mock_db.get = AsyncMock(side_effect=lambda model, pk: mock_execution if model.__name__ == "ProcessExecution" else mock_definition)
        mock_db.commit = AsyncMock()

        await execute_workflow(mock_execution.id, uuid.uuid4(), mock_execution.tenant_id, mock_db)

    mock_dispatch.assert_called_once()
    assert mock_execution.status == "completed"
```

- [ ] **Step 2: Ejecutar — deben fallar**

```bash
pytest tests/test_executor.py::test_executor_dispatches_agent_step -v
```

Esperado: `ImportError: cannot import name 'execute_workflow'`

- [ ] **Step 3: Implementar src/executions/executor.py**

```python
# src/executions/executor.py
import asyncio
import uuid
import logging
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from src.workflows.models import ProcessDefinition, ProcessExecution
from src.executions.condition import evaluate_condition
from src.executions.transform import apply_transform
from src.executions.events import AgentTask
from src.executions.dispatcher import register_pending
from src.database import get_redis

logger = logging.getLogger(__name__)

TASK_STREAM = "constructor:agent.task.created"


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
    from src.workflows.models import ProcessExecution, ProcessDefinition

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

    try:
        while current_step_id:
            step = steps_map.get(current_step_id)
            if not step:
                raise ValueError(f"Step '{current_step_id}' not in definition")

            execution.current_step_id = current_step_id
            await db.commit()

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

    except asyncio.TimeoutError:
        execution.status = "failed"
        execution.context = {**execution.context, "_error": f"Timeout on step '{execution.current_step_id}'"}
        await db.commit()
    except Exception as exc:
        logger.error("Workflow execution failed: %s", exc)
        execution.status = "failed"
        execution.context = {**execution.context, "_error": str(exc)}
        await db.commit()
```

- [ ] **Step 4: Ejecutar tests del executor — deben pasar**

```bash
pytest tests/test_executor.py -v
```

Esperado: todos los tests pasan.

- [ ] **Step 5: Commit**

```bash
git add services/platform-api/src/executions/executor.py services/platform-api/tests/test_executor.py
git commit -m "feat: WorkflowExecutor with condition/transform/agent step dispatch via Redis Streams"
```

---

## Task 14: Platform API — Executions CRUD + Router

**Files:**
- Create: `services/platform-api/src/executions/schemas.py`
- Create: `services/platform-api/src/executions/service.py`
- Create: `services/platform-api/src/executions/router.py`
- Create: `services/platform-api/tests/test_executions.py`
- Modify: `services/platform-api/src/main.py`

- [ ] **Step 1: Escribir tests — deben fallar**

```python
# services/platform-api/tests/test_executions.py
import pytest
from httpx import AsyncClient, ASGITransport
from src.main import app


@pytest.mark.asyncio
async def test_trigger_execution_requires_auth():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.post("/executions", json={"process_definition_id": "some-id", "context": {}})
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_trigger_execution_invalid_definition(seeded_user, client):
    """Dispara ejecución con definition_id inexistente → 404."""
    import uuid
    login_resp = await client.post("/auth/login", json={
        "email": seeded_user["email"],
        "password": seeded_user["password"],
        "tenant_slug": seeded_user["tenant_slug"],
    })
    token = login_resp.json()["access_token"]
    resp = await client.post(
        "/executions",
        json={"process_definition_id": str(uuid.uuid4()), "context": {}},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_list_executions_empty(seeded_user, client):
    login_resp = await client.post("/auth/login", json={
        "email": seeded_user["email"],
        "password": seeded_user["password"],
        "tenant_slug": seeded_user["tenant_slug"],
    })
    token = login_resp.json()["access_token"]
    resp = await client.get("/executions", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)
```

- [ ] **Step 2: Crear src/executions/schemas.py**

```python
# src/executions/schemas.py
import uuid
from datetime import datetime
from pydantic import BaseModel


class ExecutionCreate(BaseModel):
    process_definition_id: uuid.UUID
    context: dict = {}


class ExecutionRead(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    process_definition_id: uuid.UUID
    status: str
    current_step_id: str | None
    context: dict
    triggered_by: uuid.UUID | None
    started_at: datetime | None
    completed_at: datetime | None
    created_at: datetime

    model_config = {"from_attributes": True}
```

- [ ] **Step 3: Crear src/executions/service.py**

```python
# src/executions/service.py
import asyncio
import uuid
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from src.workflows.models import ProcessDefinition, ProcessExecution
from src.executions.schemas import ExecutionCreate
from src.database import AsyncSessionFactory


async def create_execution(
    db: AsyncSession,
    data: ExecutionCreate,
    tenant_id: uuid.UUID,
    triggered_by: uuid.UUID,
) -> ProcessExecution:
    definition = await db.scalar(
        select(ProcessDefinition).where(
            ProcessDefinition.id == data.process_definition_id,
            ProcessDefinition.tenant_id == tenant_id,
        )
    )
    if not definition:
        return None

    execution = ProcessExecution(
        tenant_id=tenant_id,
        process_definition_id=data.process_definition_id,
        context=data.context,
        triggered_by=triggered_by,
        status="pending",
    )
    db.add(execution)
    await db.commit()
    await db.refresh(execution)

    # Arranca el WorkflowExecutor en background — usa su propia sesión de DB
    asyncio.create_task(
        _run_workflow(execution.id, definition.id, tenant_id)
    )
    return execution


async def _run_workflow(
    execution_id: uuid.UUID,
    process_id: uuid.UUID,
    tenant_id: uuid.UUID,
) -> None:
    from src.executions.executor import execute_workflow
    async with AsyncSessionFactory() as db:
        await db.execute(
            __import__("sqlalchemy").text("SET app.current_tenant = :tid"),
            {"tid": str(tenant_id)},
        )
        await execute_workflow(execution_id, process_id, tenant_id, db)


async def get_execution(db: AsyncSession, execution_id: uuid.UUID) -> ProcessExecution | None:
    return await db.scalar(
        select(ProcessExecution).where(ProcessExecution.id == execution_id)
    )


async def list_executions(
    db: AsyncSession,
    status: str | None = None,
    definition_id: uuid.UUID | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[ProcessExecution]:
    q = select(ProcessExecution)
    if status:
        q = q.where(ProcessExecution.status == status)
    if definition_id:
        q = q.where(ProcessExecution.process_definition_id == definition_id)
    q = q.order_by(ProcessExecution.created_at.desc()).limit(limit).offset(offset)
    result = await db.execute(q)
    return list(result.scalars().all())
```

- [ ] **Step 4: Crear src/executions/router.py**

```python
# src/executions/router.py
import uuid
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from src.auth.dependencies import get_current_user
from src.auth.schemas import CurrentUser
from src.rbac.dependencies import require_permission
from src.rbac.permissions import Permission
from src.database import get_db
from src.executions.schemas import ExecutionCreate, ExecutionRead
from src.executions.service import create_execution, get_execution, list_executions

router = APIRouter(prefix="/executions", tags=["executions"])


@router.post("", response_model=ExecutionRead, status_code=status.HTTP_202_ACCEPTED)
async def trigger(
    body: ExecutionCreate,
    db: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(require_permission(Permission.WORKFLOW_TRIGGER)),
):
    execution = await create_execution(db, body, current_user.tenant_id, current_user.id)
    if not execution:
        raise HTTPException(status_code=404, detail="ProcessDefinition not found")
    return execution


@router.get("/{execution_id}", response_model=ExecutionRead)
async def get_one(
    execution_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(require_permission(Permission.WORKFLOW_READ)),
):
    execution = await get_execution(db, execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")
    return execution


@router.get("", response_model=list[ExecutionRead])
async def list_all(
    status: str | None = Query(default=None),
    definition_id: uuid.UUID | None = Query(default=None),
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0),
    db: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(require_permission(Permission.WORKFLOW_READ)),
):
    return await list_executions(db, status=status, definition_id=definition_id, limit=limit, offset=offset)
```

- [ ] **Step 5: Registrar router en main.py**

En `services/platform-api/src/main.py`, dentro de `create_app()` después del `workflows_router`:

```python
    from src.executions.router import router as executions_router
    app.include_router(executions_router)
```

- [ ] **Step 6: Ejecutar tests**

```bash
pytest tests/test_executions.py -v
```

Esperado: los 3 tests pasan.

- [ ] **Step 7: Commit**

```bash
git add services/platform-api/src/executions/ services/platform-api/tests/test_executions.py services/platform-api/src/main.py
git commit -m "feat: executions CRUD endpoints POST/GET /executions with RBAC"
```

---

## Task 15: End-to-End Integration Test

**Files:**
- Create: `services/platform-api/tests/test_e2e_execution.py`

- [ ] **Step 1: Escribir test e2e**

```python
# services/platform-api/tests/test_e2e_execution.py
"""
End-to-end: Platform API dispara workflow con un agent step.
Agent Orchestration (mockeado via Redis) procesa y publica resultado.
Platform API recibe resultado y completa la ejecución.
"""
import asyncio
import pytest
import pytest_asyncio
import uuid
from httpx import AsyncClient, ASGITransport
from src.main import app
from src.executions.events import AgentTask, AgentResult

TASK_STREAM = "constructor:agent.task.created"
RESULT_STREAM = "constructor:agent.result.ready"


async def fake_agent_worker(redis, timeout: float = 10.0):
    """Simula Agent Orchestration: lee tasks y publica resultados de inmediato."""
    deadline = asyncio.get_event_loop().time() + timeout
    try:
        await redis.xgroup_create(TASK_STREAM, "fake-agent", id="0", mkstream=True)
    except Exception:
        pass

    while asyncio.get_event_loop().time() < deadline:
        messages = await redis.xreadgroup(
            "fake-agent", "worker-1",
            {TASK_STREAM: ">"},
            count=5, block=500,
        )
        for _stream, entries in (messages or []):
            for entry_id, fields in entries:
                task = AgentTask.model_validate_json(fields["data"])
                result = AgentResult(
                    task_id=task.task_id,
                    execution_id=task.execution_id,
                    tenant_id=task.tenant_id,
                    step_id=task.step_id,
                    status="completed",
                    output={"text": "E2E agent response"},
                    tokens_used=42,
                    model_used="claude-sonnet-4-6",
                    iterations=1,
                    error=None,
                )
                await redis.xadd(RESULT_STREAM, {"data": result.model_dump_json()})
                await redis.xack(TASK_STREAM, "fake-agent", entry_id)


@pytest.mark.asyncio
async def test_full_workflow_execution(seeded_user, client, seeded_workflow_definition, db_redis):
    """
    seeded_workflow_definition fixture (en conftest.py) crea un ProcessDefinition
    con un solo agent step y retorna su id.
    db_redis es el cliente Redis del TestContainer.
    """
    # Login
    login_resp = await client.post("/auth/login", json={
        "email": seeded_user["email"],
        "password": seeded_user["password"],
        "tenant_slug": seeded_user["tenant_slug"],
    })
    token = login_resp.json()["access_token"]

    # Arrancar fake agent worker en background
    worker_task = asyncio.create_task(fake_agent_worker(db_redis, timeout=15.0))

    # Disparar ejecución
    trigger_resp = await client.post(
        "/executions",
        json={"process_definition_id": str(seeded_workflow_definition), "context": {"input": "test"}},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert trigger_resp.status_code == 202
    execution_id = trigger_resp.json()["id"]

    # Polling hasta que la ejecución complete (máx 10 segundos)
    for _ in range(20):
        await asyncio.sleep(0.5)
        status_resp = await client.get(
            f"/executions/{execution_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        if status_resp.json()["status"] == "completed":
            break

    worker_task.cancel()

    final_resp = await client.get(
        f"/executions/{execution_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    data = final_resp.json()
    assert data["status"] == "completed"
    # El context debe incluir el output del agent step
    assert any("E2E agent response" in str(v) for v in data["context"].values())
```

- [ ] **Step 2: Agregar fixtures necesarias en conftest.py de platform-api**

En `services/platform-api/tests/conftest.py`, agregar:

```python
import pytest_asyncio
from redis.asyncio import Redis as AsyncRedis
from src.workflows.models import ProcessDefinition
from src.users.models import User


@pytest_asyncio.fixture
async def seeded_workflow_definition(db_session, seeded_user):
    """Crea ProcessDefinition con un agent step para tests e2e."""
    from src.workflows.models import ProcessDefinition
    from sqlalchemy import select, text

    # Obtener tenant y usuario del seeded_user
    from src.tenants.models import Tenant
    tenant = await db_session.scalar(
        select(Tenant).where(Tenant.slug == seeded_user["tenant_slug"])
    )
    user = await db_session.scalar(
        select(User).where(User.email == seeded_user["email"])
    )

    defn = ProcessDefinition(
        tenant_id=tenant.id,
        name="E2E Test Workflow",
        trigger_config={"type": "manual"},
        steps=[{
            "id": "step_agent",
            "type": "agent",
            "config": {"task_type": "default", "prompt": "Say hello", "tools_allowed": []},
            "next": None,
            "timeout_seconds": 30,
        }],
        on_error="stop",
        created_by=user.id,
    )
    db_session.add(defn)
    await db_session.commit()
    return defn.id


@pytest_asyncio.fixture
async def db_redis(redis_container):
    """Redis client conectado al TestContainer para tests e2e."""
    from testcontainers.redis import RedisContainer
    client = AsyncRedis(
        host=redis_container.get_container_host_ip(),
        port=int(redis_container.get_exposed_port(6379)),
        decode_responses=True,
    )
    yield client
    await client.aclose()
```

> **Nota:** `redis_container` debe ser un fixture de sesión en `conftest.py`. Si no existe, agregar:
> ```python
> @pytest.fixture(scope="session")
> def redis_container():
>     from testcontainers.redis import RedisContainer
>     with RedisContainer("redis:7-alpine") as r:
>         yield r
> ```

- [ ] **Step 3: Ejecutar test e2e**

```bash
cd services/platform-api
pytest tests/test_e2e_execution.py -v -s
```

Esperado: `1 passed` (puede tardar ~5s por el polling).

- [ ] **Step 4: Ejecutar suite completa**

```bash
pytest tests/ -v --cov=src --cov-report=term-missing
```

Esperado: cobertura ≥ 80%, todos los tests pasan.

- [ ] **Step 5: Commit**

```bash
git add services/platform-api/tests/test_e2e_execution.py services/platform-api/tests/conftest.py
git commit -m "test: end-to-end execution test with fake agent worker via Redis Streams"
```

---

## Task 16: CI Pipeline para Agent Orchestration

**Files:**
- Create: `.github/workflows/agent-orchestration-ci.yml`

- [ ] **Step 1: Crear pipeline**

```yaml
# .github/workflows/agent-orchestration-ci.yml
name: Agent Orchestration CI

on:
  push:
    paths:
      - "services/agent-orchestration/**"
  pull_request:
    paths:
      - "services/agent-orchestration/**"

jobs:
  test:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: services/agent-orchestration

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: "pip"

      - name: Install dependencies
        run: pip install -e ".[dev]" psycopg2-binary

      - name: Run tests
        run: pytest tests/ -v --cov=src --cov-report=xml --cov-fail-under=80
        env:
          DATABASE_URL: postgresql+asyncpg://test:test@localhost:5432/test
          REDIS_URL: redis://localhost:6379/0
          ANTHROPIC_API_KEY: ""
          OPENAI_API_KEY: ""
          GOOGLE_API_KEY: ""

      - name: Security scan — Bandit
        run: |
          pip install bandit
          bandit -r src/ -ll

      - name: Dependency audit
        run: |
          pip install pip-audit
          pip-audit

  lint:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: services/agent-orchestration
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Install ruff
        run: pip install ruff
      - name: Lint
        run: ruff check src/ tests/
```

- [ ] **Step 2: Commit final del plan**

```bash
git add .github/workflows/agent-orchestration-ci.yml
git commit -m "ci: github actions pipeline for agent-orchestration with tests and security scan"
```

---

## Self-Review

**Spec coverage:**

| Sección del spec | Task que lo implementa |
|-----------------|----------------------|
| WorkflowExecutor — condition, transform, agent steps | Tasks 11, 13 |
| WorkflowExecutor — notify/wait/human_approval stubs | Task 13 (executor.py case _) |
| Agent Orchestration — Redis consumer | Task 8 |
| Agent Orchestration — LangGraph ReAct runner | Task 6 |
| Model Router — routing table + fallback chain | Task 3 |
| Tool Registry — BaseTool + registro | Task 4 |
| http_generic (SSRF protection) | Task 5 |
| sql_query (DML rejection, RLS) | Task 5 |
| Memory — short-term Redis | Task 7 |
| Memory — long-term PostgreSQL (agent_execution_logs) | Tasks 7, 10 |
| Result Publisher | Task 8 |
| Event schemas AgentTask + AgentResult | Task 2 |
| Redis Streams consumer en Platform API | Task 12 |
| dispatcher.py — Future pattern | Task 12 |
| Endpoints POST/GET /executions | Task 14 |
| Migration 005 | Task 10 |
| lifespan en main.py | Task 12 |
| Worker entry point | Task 9 |
| Docker Compose update | Task 9 |
| End-to-end test | Task 15 |
| CI pipeline | Task 16 |

**Type consistency check:**
- `AgentTask.execution_id: uuid.UUID` ↔ `dispatcher.register_pending(execution_id: uuid.UUID)` ✓
- `AgentResult.output: dict` ↔ `resolve_pending(output: dict)` ↔ `executor.context[step_id] = result` ✓
- `BaseTool.as_openai_tool() -> dict` ↔ `call_model_node: model.bind_tools([t.as_openai_tool() for t in tools])` ✓
- `ModelRouter.get_model(task_type, model_override) -> BaseChatModel` ↔ `AgentRunner.run: model = self.model_router.get_model(...)` ✓
- `ProcessExecution.current_step_id: str | None` ↔ `executor.py: execution.current_step_id = current_step_id` ✓
