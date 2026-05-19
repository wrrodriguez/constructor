# Constructor Platform — Plan 2: Agent Orchestration Service

**Fecha:** 2026-05-18
**Estado:** Aprobado
**Autor:** kimn consulting SRL
**Depende de:** Plan 1 (Foundation & Platform API Core) — completado

---

## 1. Resumen

Plan 2 agrega el motor de ejecución real de workflows y el servicio de orquestación de agentes AI. Al finalizar este plan, un usuario puede disparar un `ProcessDefinition`, ver cómo sus pasos se ejecutan en secuencia (con condiciones, transformaciones y agentes AI reales), y obtener el resultado persistido en `ProcessExecution`.

**Scope de este plan:**
- `WorkflowExecutor` en Platform API — itera pasos, evalúa `condition` y `transform`, coordina con Agent Orchestration vía Redis Streams
- Nuevo servicio `agent-orchestration` — LangGraph ReAct loop, Model Router, Tool Registry mínimo, Memory Manager
- Contratos de eventos en Redis Streams (`agent.task.created`, `agent.result.ready`)
- Nuevos endpoints: `POST /executions`, `GET /executions/{id}`, `GET /executions`

**Fuera de scope en este plan:**
- Memoria semántica (Qdrant / RAG) — Plan 3 o posterior
- Tool Registry completo (GitHub, Jira, Slack, etc.) — Plan 5
- `human_approval`, `notify`, `wait` steps funcionales — stubs que avanzan al siguiente paso
- WebSocket streaming al frontend — Plan 3
- Frontend Angular — Plan 4

---

## 2. Arquitectura

### Flujo de ejecución

```
Usuario → POST /executions
              │
              ▼
    Platform API crea ProcessExecution (status: pending)
    Publica workflow.started en Redis Streams
    Arranca WorkflowExecutor (asyncio.Task)
              │
              ├─ condition → evalúa JMESPath sobre context → branching
              ├─ transform → aplica field mapping sobre context
              ├─ agent ──────────────────────────────────────────────►
              │    publica agent.task.created                          Agent Orchestration
              │    await Future (suspende Task)                          Redis consumer
              │                                                          AgentRunner (LangGraph)
              │    ◄─────────────────────────────────────────────────     ModelRouter
              │    agent.result.ready resuelve Future                     ToolRegistry
              │    context[step_id] = result                              MemoryManager
              │                                                           ResultPublisher
              ├─ notify / wait / human_approval → stub, avanza       ◄──────────────────
              │
              ▼
    ProcessExecution (status: completed | failed)
```

### Servicios involucrados

| Servicio | Rol en Plan 2 |
|----------|---------------|
| Platform API (existente) | WorkflowExecutor, nuevos endpoints /executions, Redis consumer |
| Agent Orchestration (nuevo) | Consume agent.task.created, ejecuta con LangGraph, publica agent.result.ready |
| PostgreSQL (existente) | ProcessExecution, AgentExecutionLog (nuevo) |
| Redis (existente) | Streams para eventos, short-term memory del agente |

---

## 3. Platform API — Extensiones

### 3.1 WorkflowExecutor

`asyncio.Task` que itera los pasos de un `ProcessDefinition` de forma lineal.

```python
# src/executions/executor.py (nuevo módulo)
async def execute_workflow(execution_id: UUID, db: AsyncSession):
    execution = await db.get(ProcessExecution, execution_id)
    definition = await db.get(ProcessDefinition, execution.process_definition_id)

    for step in definition.steps:
        execution.current_step_id = step["id"]
        await db.commit()

        match step["type"]:
            case "condition":
                next_step_id = evaluate_condition(step["config"], execution.context)
                # El executor salta al step con id == next_step_id
                # Los pasos restantes se filtran a partir de ese id
            case "transform":
                execution.context = apply_transform(step["config"], execution.context)
            case "agent":
                result = await dispatch_agent_step(execution, step)
                execution.context[step["id"]] = result
            case _:
                pass  # stub: avanza sin hacer nada

    execution.status = "completed"
    execution.completed_at = datetime.now(timezone.utc)
    await db.commit()
```

**Evaluación de `condition`:**
- Formato estructurado en `step.config`:
  ```json
  {
    "expression": "steps.step_1.risk_score",
    "operator": ">",
    "value": 0.8,
    "branches": { "true": "step_approval", "false": "step_notify" }
  }
  ```
- `expression` es una query JMESPath (extrae el valor del contexto)
- `operator` es uno de: `>`, `>=`, `<`, `<=`, `==`, `!=`, `in`, `not_in`
- La comparación se evalúa en Python puro, sin `eval()`. El resultado booleano se convierte a string key (`"true"` / `"false"`) para resolver el próximo step.
- Resultado: `step_id` del siguiente paso a ejecutar

**Evaluación de `transform`:**
- Mapping de campos con dot-notation
- Ejemplo config: `{"output.summary": "steps.agent_1.text", "output.score": "steps.agent_2.score"}`
- Implementación: función pura, sin dependencias externas

### 3.2 Coordinación con Agent Orchestration

**Publicar tarea:**
```python
# Publica en stream: tenant:{tenant_id}:agent.task.created
await redis.xadd(f"tenant:{tenant_id}:agent.task.created", agent_task.model_dump())
```

**Esperar resultado (Future pattern):**
```python
# Dict global en memoria: {execution_id: asyncio.Future}
_pending: dict[UUID, asyncio.Future] = {}

async def dispatch_agent_step(execution, step) -> dict:
    future = asyncio.get_event_loop().create_future()
    _pending[execution.id] = future
    await publish_agent_task(execution, step)
    return await asyncio.wait_for(future, timeout=step["timeout_seconds"])
```

**Consumer de resultados (corre en background al arrancar Platform API):**
```python
# Consume: tenant:{tenant_id}:agent.result.ready
# Cuando llega un resultado, resuelve el Future correspondiente
if execution_id in _pending:
    _pending[execution_id].set_result(result.output)
    del _pending[execution_id]
```

### 3.3 Nuevos endpoints

```
POST /executions
  body: { process_definition_id: UUID, context: dict }
  → crea ProcessExecution, arranca WorkflowExecutor, retorna 202 Accepted

GET /executions/{id}
  → retorna ProcessExecution con status, current_step_id, context

GET /executions
  → lista ejecuciones del tenant (paginado, filtros por status/definition_id)
```

**Permisos:**
- `POST /executions` → `Permission.WORKFLOW_TRIGGER`
- `GET /executions` → `Permission.WORKFLOW_READ`

### 3.4 Modelo de datos — extensiones

`ProcessExecution` recibe dos campos nuevos (migración 005):
- `current_step_id: str | None` — paso en ejecución actualmente
- `context: dict` — estado acumulado de la ejecución (outputs de cada paso)

Nueva tabla `agent_execution_logs` (migración 005):
```python
class AgentExecutionLog(Base):
    id: UUID
    tenant_id: UUID          # RLS
    execution_id: UUID       # FK ProcessExecution
    step_id: str
    task_id: UUID
    model_used: str
    tokens_used: int
    iterations: int
    status: str              # completed | failed | timeout
    created_at: datetime
```

---

## 4. Agent Orchestration Service

### 4.1 Estructura de archivos

```
services/agent-orchestration/
├── src/
│   ├── main.py              # FastAPI app mínima — solo GET /health
│   ├── config.py            # Settings: Redis URL, DB URL, API keys AI
│   ├── worker.py            # Entry point: arranca consumer loop + health server
│   ├── consumer/
│   │   └── redis_consumer.py    # Lee agent.task.created, despacha al AgentRunner
│   ├── runner/
│   │   ├── agent_runner.py      # Construye y ejecuta el grafo LangGraph
│   │   ├── state.py             # AgentState (TypedDict)
│   │   └── nodes.py             # Nodos: call_model, execute_tool
│   ├── router/
│   │   └── model_router.py      # Selecciona provider/model según task_type
│   ├── tools/
│   │   ├── registry.py          # ToolRegistry: registro y resolución de tools
│   │   ├── base.py              # BaseTool (ABC)
│   │   ├── http_generic.py      # HTTP GET/POST a URL arbitraria
│   │   └── sql_query.py         # SQL SELECT read-only
│   ├── memory/
│   │   ├── short_term.py        # Redis: contexto de ejecución actual
│   │   └── long_term.py         # PostgreSQL: historial de ejecuciones previas
│   └── publisher/
│       └── result_publisher.py  # Publica agent.result.ready en Redis Streams
├── tests/
│   ├── conftest.py              # TestContainers Redis + PostgreSQL, FakeChatModel
│   ├── test_agent_runner.py
│   ├── test_model_router.py
│   ├── test_tools.py
│   └── test_consumer.py
├── pyproject.toml
├── Dockerfile
└── .env.example
```

### 4.2 LangGraph Agent Runner

**Estado:**
```python
class AgentState(TypedDict):
    messages: Annotated[list[BaseMessage], add_messages]
    iterations: int
    context: dict          # contexto de la ejecución pasado por Platform API
    tools_allowed: list[str]
    task_id: str
    tenant_id: str
```

**Grafo:**
```python
graph = StateGraph(AgentState)
graph.add_node("call_model", call_model_node)
graph.add_node("execute_tool", execute_tool_node)
graph.add_edge(START, "call_model")
graph.add_conditional_edges(
    "call_model",
    should_continue,   # tiene tool_calls Y iterations < max_iterations
    {"execute_tool": "execute_tool", END: END}
)
graph.add_edge("execute_tool", "call_model")
compiled = graph.compile()
```

**Límites de seguridad:**
- `MAX_ITERATIONS = 20` (configurable por step)
- Timeout por ejecución: `step.timeout_seconds`
- Si se alcanza max_iterations: `status = "failed"`, `error = "max_iterations_reached"`

### 4.3 Model Router

```python
ROUTING_TABLE: dict[str, str] = {
    "code_analysis":   "claude-opus-4-6",
    "security_scan":   "gpt-4o",
    "summarization":   "claude-haiku-4-5-20251001",
    "ocr_extraction":  "gemini-1.5-pro",
    "default":         "claude-sonnet-4-6",
}

FALLBACK_CHAIN = ["claude-sonnet-4-6", "gpt-4o", "gemini-1.5-flash"]

# Lógica de selección (en orden de prioridad):
# 1. model_override en AgentTask → usar ese modelo directamente
# 2. ROUTING_TABLE[task_type] → modelo óptimo para el tipo de tarea
# 3. ROUTING_TABLE["default"] → fallback al modelo por defecto
# 4. Si el proveedor falla (exception) → siguiente en FALLBACK_CHAIN
```

**Providers soportados en Plan 2:**
- Anthropic: `langchain-anthropic` (`claude-opus-4-6`, `claude-sonnet-4-6`, `claude-haiku-4-5`)
- OpenAI: `langchain-openai` (`gpt-4o`, `o3-mini`)
- Google: `langchain-google-genai` (`gemini-1.5-pro`, `gemini-1.5-flash`)

Todos exponen `BaseChatModel` de LangChain — el router instancia el modelo correcto y lo pasa al AgentRunner sin que este sepa qué proveedor usa.

### 4.4 Tool Registry

```python
class BaseTool(ABC):
    name: str                # identificador único, ej: "http_generic"
    description: str         # descripción para el LLM
    input_schema: dict       # JSON Schema de los inputs

    @abstractmethod
    async def execute(self, inputs: dict, tenant_context: dict) -> dict: ...
```

**`http_generic`:**
- Inputs: `url`, `method` (GET|POST), `headers` (dict), `body` (dict opcional)
- Restricciones: URL debe matchear whitelist configurable por tenant; no permite acceso a IPs privadas (SSRF protection)
- Output: `{ status_code, body, headers }`

**`sql_query`:**
- Inputs: `query` (string), `params` (dict)
- Restricción estricta: la query es parseada, se rechaza cualquier statement que no sea `SELECT`. No se permite `INSERT`, `UPDATE`, `DELETE`, `DROP`, etc.
- Usa la conexión de DB del tenant con RLS activo
- Output: `{ rows: list[dict], row_count: int }`

**Registro:**
```python
registry = ToolRegistry()
registry.register(HttpGenericTool())
registry.register(SqlQueryTool())
# Agregar nuevas tools en el futuro: registry.register(GitHubTool())
```

### 4.5 Memory Manager

**Short-term (Redis):**
- Key: `tenant:{tenant_id}:execution:{execution_id}:memory`
- Almacena los últimos N mensajes del AgentState para no re-enviar todo el historial en cada iteración
- TTL: duración del timeout de la ejecución + 1 hora
- Implementación: Redis Hash, serializado como JSON

**Long-term (PostgreSQL):**
- Tabla `agent_execution_logs` (ver sección 3.4)
- Al finalizar cada ejecución de agente, se persiste el resumen: modelo usado, tokens, iteraciones, status
- Disponible para consulta vía `GET /executions/{id}`
- En el futuro (Plan 3+) se usa para RAG con Qdrant

### 4.6 Result Publisher

Al finalizar el AgentRunner (éxito o fallo):

```python
result = AgentResult(
    task_id=task.task_id,
    execution_id=task.execution_id,
    tenant_id=task.tenant_id,
    step_id=task.step_id,
    status="completed",   # o "failed" / "timeout"
    output=extract_output(final_state),
    tokens_used=count_tokens(final_state.messages),
    model_used=model_router.last_model_used,
    iterations=final_state.iterations,
    error=None,
)
await redis.xadd(
    f"tenant:{task.tenant_id}:agent.result.ready",
    result.model_dump_json()
)
```

---

## 5. Contratos de eventos Redis Streams

### `agent.task.created`

Stream: `tenant:{tenant_id}:agent.task.created`

```python
class AgentTask(BaseModel):
    task_id: UUID
    execution_id: UUID
    tenant_id: UUID
    step_id: str
    task_type: str           # "code_analysis" | "summarization" | "default" | etc.
    prompt: str              # prompt base del step config
    context: dict            # contexto acumulado hasta este paso
    tools_allowed: list[str] # subset del Tool Registry permitido para este step
    model_override: str | None
    timeout_seconds: int
    max_iterations: int = 20
```

### `agent.result.ready`

Stream: `tenant:{tenant_id}:agent.result.ready`

```python
class AgentResult(BaseModel):
    task_id: UUID
    execution_id: UUID
    tenant_id: UUID
    step_id: str
    status: Literal["completed", "failed", "timeout"]
    output: dict
    tokens_used: int
    model_used: str
    iterations: int
    error: str | None
```

**Consumer groups:**
- `platform-api` consume `agent.result.ready`
- `agent-orchestration` consume `agent.task.created`

---

## 6. Testing Strategy

### Agent Orchestration Service

| Test | Tipo | Descripción |
|------|------|-------------|
| `test_model_router.py` | Unit | Routing table correcta por task_type, fallback chain, model_override |
| `test_tools.py` | Unit | http_generic rechaza IPs privadas; sql_query rechaza DML |
| `test_agent_runner.py` | Integration | FakeChatModel que hace 2 tool calls y termina; verificar iteraciones, output, tokens |
| `test_consumer.py` | Integration | Publicar AgentTask en Redis (TestContainers), consumer lo procesa, verificar AgentResult en stream de salida |

### Platform API — extensiones

| Test | Tipo | Descripción |
|------|------|-------------|
| `test_executions.py` | Integration | POST /executions con definition que tiene condition + transform + agent stub; verificar context final |
| `test_executor.py` | Unit | WorkflowExecutor evalúa condition con JMESPath, transform aplica mapping correcto |
| End-to-end | Integration | Platform API dispara ejecución, Agent Orchestration (FakeChatModel) procesa, Platform API recibe resultado y completa la ejecución |

**Sin llamadas reales a APIs de AI en CI.** Todas las llamadas a Anthropic/OpenAI/Google se mockean con `FakeChatModel` de LangChain o `unittest.mock`.

**Cobertura mínima:** 80% en ambos servicios.

---

## 7. Dependencias nuevas

### Agent Orchestration Service (`pyproject.toml`)

```toml
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
    "jmespath>=1.0.0",
    "httpx>=0.27.0",    # para http_generic tool
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.23.0",
    "pytest-cov>=5.0.0",
    "testcontainers[postgres,redis]>=4.5.0",
    "langchain-core>=0.3.0",    # FakeChatModel
]
```

### Platform API — dependencias adicionales

```toml
# Agregar a dependencias existentes:
"jmespath>=1.0.0",   # evaluación de conditions
"redis[asyncio]>=5.0.0",  # ya presente, verificar versión
```

---

## 8. Seguridad

| Riesgo | Control |
|--------|---------|
| SSRF via http_generic | Whitelist de dominios por tenant; bloqueo de IPs privadas (10.x, 172.x, 192.168.x, 127.x) |
| SQL Injection via sql_query | Parser SQL rechaza todo lo que no sea SELECT; siempre usa prepared statements |
| Prompt Injection | Contexto externo pasa entre tags XML delimitadores: `<context>...</context>` en el system prompt; nunca concatenado directamente |
| Token runaway | Hard limit: max_iterations=20 por step, timeout_seconds por step, token budget por tenant/mes (fase 2) |
| Cross-tenant en Redis | Streams prefijados con `tenant:{tenant_id}:` — un consumer nunca lee de otro tenant |
| API keys de providers | En `.env` / AWS Secrets Manager; nunca en código ni logs |

---

## 9. Estructura de Docker Compose actualizada

```yaml
# Agregar a docker-compose.yml
agent-orchestration:
  build:
    context: ./services/agent-orchestration
    target: development
  environment:
    REDIS_URL: redis://:redis_dev_password@redis:6379/0
    DATABASE_URL: postgresql+asyncpg://constructor:constructor_dev@postgres:5432/constructor_dev
    ANTHROPIC_API_KEY: ${ANTHROPIC_API_KEY}
    OPENAI_API_KEY: ${OPENAI_API_KEY}
    GOOGLE_API_KEY: ${GOOGLE_API_KEY}
    ENVIRONMENT: development
  depends_on:
    postgres:
      condition: service_healthy
    redis:
      condition: service_healthy
  command: python -m src.worker
```

---

## 10. Cobertura del spec general

| Sección del spec principal | Cubierto en Plan 2 |
|---------------------------|-------------------|
| Agent Orchestration Service (Task Consumer, Model Router, Agent Runner, Tool Registry, Memory Manager, Result Publisher) | ✅ Completo |
| WorkflowExecutor — iterar pasos, condition, transform | ✅ Completo |
| WorkflowExecutor — agent step (LangGraph) | ✅ Completo |
| WorkflowExecutor — human_approval, notify, wait | ⬜ Stubs (Plan 4) |
| Event Bus Redis Streams | ✅ Contratos definidos e implementados |
| Memoria short-term (Redis) | ✅ |
| Memoria long-term (PostgreSQL) | ✅ |
| Memoria semántica (Qdrant / RAG) | ⬜ Plan 3+ |
| Tool Registry — http_generic, sql_query | ✅ |
| Tool Registry — GitHub, Jira, Slack, CI/CD, Email | ⬜ Plan 5 |
| WebSocket streaming al frontend | ⬜ Plan 3 |
| Frontend Angular | ⬜ Plan 4 |
