# Constructor Platform — Plan 3: WebSocket Service

**Fecha:** 2026-05-19
**Estado:** Aprobado
**Autor:** kimn consulting SRL

---

## 1. Objetivo

Construir el WebSocket Service: un cuarto microservicio que entrega actualizaciones de estado de ejecuciones en tiempo real al frontend Angular, consumiendo eventos de Redis Streams y distribuyéndolos a clientes autenticados vía Socket.IO.

---

## 2. Alcance

### Incluido en este plan

- Nuevo servicio `websocket-service` (Python 3.12, FastAPI, python-socketio)
- Publicación del evento `constructor:execution.status.changed` desde Platform API
- Autenticación JWT en el handshake WebSocket (clave pública RSA compartida)
- Rooms por `execution_id` con verificación de ownership por tenant
- Tests de autenticación, aislamiento multi-tenant y flujo consumer → cliente

### Excluido (fuera de alcance)

- Streaming de tokens (token-by-token) — requiere cambios en agent-orchestration, queda para v2
- Notificaciones push por rol (aprobaciones, alertas) — Plan 4 cuando exista el frontend
- Scaling horizontal con `AsyncRedisManager` — se deja preparado en arquitectura pero no se activa en v1
- Deployment AWS (Plan 5)

---

## 3. Arquitectura

### Componentes

```
[Angular SPA]
  │  WebSocket (Socket.IO)
  ▼
[WebSocket Service — puerto 8002]
  ├── FastAPI app (GET /health)
  ├── Socket.IO ASGI (namespace /executions)
  │     ├── on_connect: valida JWT, guarda tenant_id en session
  │     └── on_join_execution: verifica ownership, entra a room
  └── Redis Consumer (background task)
        └── lee constructor:execution.status.changed
              → sio.emit("execution_update", room=execution_id)

[Platform API — executor.py]
  └── publica en constructor:execution.status.changed
        en cada transición de estado de ProcessExecution
```

### Nuevo Redis Stream

**`constructor:execution.status.changed`**

Publicado por: Platform API (`executor.py`)
Consumido por: WebSocket Service (`redis_consumer.py`)
Consumer group: `websocket-service` / Consumer name: `ws-1`

Formato del mensaje:
```json
{
  "execution_id": "uuid",
  "tenant_id": "uuid",
  "status": "running | completed | failed | cancelled",
  "current_step_id": "step_id | null",
  "context": {}
}
```

Se publica en estas transiciones dentro de `execute_workflow()`:
- `pending → running` (al inicio)
- `running → completed` (al finalizar todos los pasos)
- `running → failed` (en `except`)
- Cada cambio de `current_step_id` (inicio de cada paso)

### Flujo completo

```
1. Cliente Angular conecta: ws://host:8002/socket.io/?token=<jwt>
2. WebSocket Service valida JWT con clave pública RSA
3. Si inválido → desconecta con código 401
4. Si válido → guarda tenant_id, user_id en socket.session
5. Cliente emite: join_execution({"execution_id": "uuid"})
6. Servidor verifica que execution pertenece al tenant del JWT (query DB)
7. Si ok → sio.enter_room(sid, execution_id)
8. Platform API ejecuta workflow → publica en Redis Stream
9. Redis Consumer recibe evento → sio.emit("execution_update", data, room=execution_id)
10. Solo clientes en esa room (del tenant correcto) reciben el evento
```

---

## 4. Estructura de archivos

### Nuevo servicio

```
services/websocket-service/
├── src/
│   ├── __init__.py
│   ├── config.py              — Settings: redis_url, database_url, jwt_public_key_path, port
│   ├── main.py                — FastAPI app + Socket.IO montado como ASGI middleware
│   ├── worker.py              — entry point: asyncio.gather(uvicorn.serve, consumer.start)
│   ├── auth.py                — verificar JWT con python-jose, extraer tenant_id/user_id
│   ├── socket_manager.py      — AsyncServer(async_mode="asgi"), instancia global `sio`
│   ├── events.py              — ExecutionStatusEvent schema (Pydantic)
│   └── consumer/
│       ├── __init__.py
│       └── redis_consumer.py  — xreadgroup en constructor:execution.status.changed
├── tests/
│   ├── __init__.py
│   ├── conftest.py            — TestContainers Redis + PG, fixtures sio test client
│   ├── test_auth.py           — JWT válido/inválido/expirado
│   ├── test_socket.py         — join room, tenant isolation, broadcast
│   └── test_consumer.py       — consumer → emit → cliente recibe
├── pyproject.toml
├── Dockerfile
└── .env.example
```

### Cambios en servicios existentes

**Platform API — `src/executions/executor.py`:**
- Agregar `xadd` a `constructor:execution.status.changed` en cada transición de estado dentro de `execute_workflow()`
- Helper interno `_publish_status_change(redis, execution)` para no repetir código

**`docker-compose.yml`:**
- Agregar servicio `websocket-service` en puerto `8002`
- Montar volumen `./keys:/app/keys` (acceso a clave pública RSA)

---

## 5. Contratos de API

### Socket.IO — namespace `/executions`

| Evento (cliente → servidor) | Payload | Descripción |
|-----------------------------|---------|-------------|
| `join_execution` | `{"execution_id": "uuid"}` | Suscribirse a updates de una ejecución |
| `leave_execution` | `{"execution_id": "uuid"}` | Desuscribirse |

| Evento (servidor → cliente) | Payload | Descripción |
|-----------------------------|---------|-------------|
| `execution_update` | `ExecutionStatusEvent` | Estado actualizado de la ejecución |
| `error` | `{"code": 401, "message": "..."}` | Error de autenticación o autorización |

### ExecutionStatusEvent

```python
class ExecutionStatusEvent(BaseModel):
    execution_id: uuid.UUID
    tenant_id: uuid.UUID
    status: Literal["pending", "running", "completed", "failed", "cancelled"]
    current_step_id: str | None
    context: dict
    timestamp: datetime
```

### HTTP

| Método | Path | Descripción |
|--------|------|-------------|
| `GET` | `/health` | Liveness check — retorna `{"status": "ok"}` |

---

## 6. Autenticación y autorización

### JWT en el handshake

El cliente Angular envía el JWT como query param en la URL de conexión:
```
ws://localhost:8002/socket.io/?token=<jwt>&EIO=4&transport=websocket
```

En el evento `connect`:
1. Se extrae `token` de `environ["HTTP_QUERY_STRING"]` o del header `Authorization`
2. Se verifica con `python-jose` usando la clave pública RSA en `config.jwt_public_key_path`
3. Algoritmo: `RS256` (mismo que Platform API)
4. Claims extraídos: `sub` (user_id), `tenant_id`, `exp`
5. Si la verificación falla → `raise ConnectionRefusedError("401")`

### Verificación de ownership

En el evento `join_execution`:
1. Se consulta PostgreSQL: `SELECT tenant_id FROM process_executions WHERE id = :execution_id`
2. Se compara con `tenant_id` del JWT
3. Si no coincide → `sio.emit("error", {"code": 403, "message": "Forbidden"}, to=sid)`
4. Si coincide → `sio.enter_room(sid, str(execution_id))`

---

## 7. Aislamiento multi-tenant

- Cada room se identifica por `execution_id` (UUID global único)
- El servidor verifica ownership antes de permitir el join
- El consumer filtra por `tenant_id` al emitir: solo emite al `execution_id`, nunca broadcast global
- Un cliente del tenant A que intente joinear una ejecución del tenant B recibe un error `403`

---

## 8. Decisiones de diseño

| Decisión | Alternativa | Razón |
|----------|-------------|-------|
| Socket.IO sobre WebSocket puro | FastAPI WebSocket nativo | Socket.IO maneja reconexión, heartbeats y rooms sin código extra |
| JWT en query param | Header Authorization | WebSocket upgrade HTTP no siempre permite headers custom en browsers |
| Verificación de ownership en DB | Solo confiar en JWT | El JWT no contiene el listado de executions del tenant — se necesita DB |
| Consumer group separado (`websocket-service`) | Compartir grupo con platform-api | Fan-out independiente; cada servicio procesa todos los mensajes del stream |
| `asyncio.gather` para server + consumer | Threading | Consistente con el patrón de agent-orchestration |

---

## 9. Testing

### test_auth.py

- JWT válido → conexión aceptada, `session` contiene `tenant_id` y `user_id`
- JWT expirado → conexión rechazada con código `401`
- JWT con firma inválida → conexión rechazada
- Sin token → conexión rechazada

### test_socket.py

- Cliente auténtico hace `join_execution` con su propia ejecución → entra a room, no recibe error
- Cliente auténtico hace `join_execution` con ejecución de otro tenant → recibe `error 403`
- Dos clientes del mismo tenant joinan la misma room → ambos reciben `execution_update`
- Cliente que no joinó ninguna room → no recibe eventos

### test_consumer.py (TestContainers Redis + PG)

- Se publica un `ExecutionStatusEvent` en `constructor:execution.status.changed`
- El consumer lo procesa en menos de 2 segundos
- El cliente Socket.IO conectado y en la room recibe el evento con todos los campos correctos
- Un cliente en otra room no recibe el evento

### Cambio en Platform API — test_executor.py

- Después de `execute_workflow()` se verifica que Redis contiene al menos un mensaje en `constructor:execution.status.changed` con `status=running` y otro con `status=completed` (o `failed`)

---

## 10. Stack tecnológico

| Componente | Tecnología |
|------------|-----------|
| Lenguaje | Python 3.12 |
| Framework HTTP | FastAPI 0.115+ |
| WebSocket protocol | python-socketio 5.x (AsyncServer, ASGIApp) |
| JWT | python-jose[cryptography] |
| Redis client | redis[asyncio] 5.x |
| DB access (ownership check) | SQLAlchemy 2.0 async + asyncpg |
| Pydantic | 2.7+ |
| Tests | pytest 8+, pytest-asyncio, python-socketio test client, testcontainers |
| Linting | Ruff, Bandit |
| Build | Hatch / hatchling |
