# Constructor

AI-powered process automation platform. Define workflows composed of AI agent steps, conditional logic, and human approvals — then execute them at scale with real-time status via WebSocket.

## Architecture

Event-driven microservices on Redis Streams, PostgreSQL for state, Socket.IO for real-time delivery.

```
Client
  │  REST                     ┌─────────────────────┐
  ├──────────────────────────▶│   Platform API :8000 │
  │                           │   FastAPI + Alembic  │
  │  WebSocket                │   PostgreSQL RLS     │
  └──────────────────────────▶│   Redis consumer     │
                              └──────────┬───────────┘
                                         │ constructor:agent.task.created
                                         ▼
                              ┌─────────────────────────┐
                              │  Agent Orchestration     │
                              │  :8001                   │
                              │  LangGraph + LangChain   │
                              │  Claude / GPT / Gemini   │
                              └──────────┬───────────────┘
                                         │ constructor:agent.result.ready
                                         ▼
                              ┌─────────────────────────┐
                              │   Platform API           │◀── resolves asyncio Future
                              │   (result consumer)      │    → HTTP 202 response
                              └──────────┬───────────────┘
                                         │ constructor:execution.status.changed
                                         ▼
                              ┌─────────────────────────┐
                              │  WebSocket Service :8002 │
                              │  python-socketio + Redis │
                              │  consumer groups         │
                              └─────────────────────────┘
                                         │ execution_update (Socket.IO)
                                         ▼
                                      Client
```

**Core flow:**
1. `POST /executions` creates a `ProcessExecution` and publishes to Redis Streams
2. Agent Orchestration picks up the task, runs a LangGraph loop, publishes the result
3. Platform API consumes the result, updates the DB, publishes a status event
4. WebSocket Service fans out the status event to every subscribed client in real time

## Services

| Service | Port | Purpose |
|---|---|---|
| `platform-api` | 8000 | REST API, workflow CRUD, auth, RBAC, audit log |
| `agent-orchestration` | 8001 | AI agent runner (LangGraph), tool execution |
| `websocket-service` | 8002 | Real-time updates via Socket.IO |
| `postgres` | 5432 | Primary data store with Row-Level Security |
| `redis` | 6379 | Event bus (Streams), caching |

## Quickstart

**1. Generate RSA keys for JWT signing:**

```bash
mkdir -p keys
openssl genrsa -out keys/private.pem 4096
openssl rsa -in keys/private.pem -pubout -out keys/public.pem
```

**2. Configure environment** (optional — defaults work for local dev):

```bash
# .env (optional)
ANTHROPIC_API_KEY=sk-ant-...   # leave unset to use stub model
OPENAI_API_KEY=sk-...
GOOGLE_API_KEY=AIzaSy...
USE_STUB_MODEL=true            # true = no API keys needed
```

**3. Start all services:**

```bash
docker compose up
```

Migrations run automatically on first boot. The stack is ready when you see `Application startup complete` on all three service logs.

**4. Seed a demo tenant and user:**

```bash
docker exec constructor-platform-api-1 python scripts/seed_demo.py
```

**5. Try it:**

```bash
# Login
TOKEN=$(curl -s -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@demo.com","password":"admin123","tenant_slug":"demo"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

# List workflows
curl -s http://localhost:8000/workflows -H "Authorization: Bearer $TOKEN" | python3 -m json.tool

# Execute a workflow (async, returns execution ID)
curl -s -X POST http://localhost:8000/executions \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"process_definition_id": "<workflow_id>", "context": {}}' \
  | python3 -m json.tool
```

**6. Connect WebSocket for real-time updates:**

```python
import asyncio, socketio

async def main():
    sio = socketio.AsyncSimpleClient()
    await sio.connect('http://localhost:8002', auth={'token': TOKEN})
    await sio.emit('join_execution', {'execution_id': EXEC_ID})
    while True:
        event, data = await sio.receive()
        if event == 'execution_update':
            print(data['status'], data.get('current_step_id'))
            if data['status'] in ('completed', 'failed'):
                break

asyncio.run(main())
```

## API

### Auth

```
POST /auth/login
  { "email", "password", "tenant_slug" }
  → { "access_token", "token_type", "expires_in" }
```

### Workflows

```
POST   /workflows           Create process definition
GET    /workflows           List workflows
GET    /workflows/:id       Get workflow
```

### Executions

```
POST   /executions          Trigger execution (202 Accepted)
GET    /executions/:id      Get execution status + output
GET    /executions          List executions (filter: status, definition_id, limit, offset)
```

### Tenants & Users

```
POST   /tenants             Create tenant (admin)
GET    /tenants             List tenants
POST   /users               Create user
GET    /users               List users
GET    /health              Health check (all services)
```

### WebSocket (Socket.IO)

```
# Client → Server
connect(auth={'token': JWT})           Authenticate connection
join_execution({'execution_id': UUID}) Subscribe to execution room
leave_execution({'execution_id': UUID})

# Server → Client
execution_update  { execution_id, status, current_step_id, context, timestamp }
error             { code, message }
```

## Running Tests

Each service has its own test suite using pytest + testcontainers.

```bash
# Platform API (unit + integration with real Postgres/Redis)
cd services/platform-api
pytest tests/ -v --cov=src --cov-fail-under=50

# Agent Orchestration (80% coverage minimum)
cd services/agent-orchestration
pytest tests/ -v --cov=src --cov-fail-under=80

# WebSocket Service
cd services/websocket-service
pytest tests/ -v --cov=src
```

Unit tests (no Docker required):

```bash
pytest tests/ -v -m "not integration"
```

## CI/CD

GitHub Actions runs on every push:

- **`platform-api-ci.yml`** — tests (50% coverage), Bandit security scan, pip-audit, Ruff
- **`agent-orchestration-ci.yml`** — tests (80% coverage), Bandit, pip-audit, Ruff

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.12 |
| API framework | FastAPI + uvicorn |
| ORM | SQLAlchemy 2.0 (async) + asyncpg |
| Migrations | Alembic |
| Event bus | Redis Streams (consumer groups) |
| AI agents | LangGraph + LangChain |
| AI models | Anthropic Claude, OpenAI GPT, Google Gemini |
| Real-time | python-socketio (Socket.IO) |
| Auth | JWT RS256 (PyJWT) + RBAC |
| Multi-tenancy | PostgreSQL Row-Level Security (RLS) |
| Testing | pytest + testcontainers |
| Linting | Ruff + Bandit |

## Project Structure

```
constructor/
├── services/
│   ├── platform-api/          # REST API, auth, workflows, executions
│   │   ├── src/
│   │   │   ├── auth/          # JWT, login, RBAC dependencies
│   │   │   ├── workflows/     # Process definition CRUD
│   │   │   ├── executions/    # Execution lifecycle + Redis dispatcher
│   │   │   ├── tenants/       # Tenant management
│   │   │   ├── users/         # User management
│   │   │   └── audit/         # Immutable audit log
│   │   ├── migrations/        # Alembic migrations
│   │   └── tests/
│   ├── agent-orchestration/   # LangGraph agent runner
│   │   ├── src/
│   │   │   ├── runner/        # LangGraph nodes, state, agent loop
│   │   │   ├── router/        # Model routing (Claude/GPT/Gemini/stub)
│   │   │   ├── tools/         # HTTP, SQL, extensible tool registry
│   │   │   ├── memory/        # Short-term + long-term agent memory
│   │   │   ├── consumer/      # Redis Streams consumer
│   │   │   └── publisher/     # Result publisher
│   │   └── tests/
│   └── websocket-service/     # Socket.IO real-time gateway
│       ├── src/
│       │   ├── consumer/      # Redis consumer → Socket.IO emit
│       │   ├── auth.py        # JWT verification
│       │   └── main.py        # Socket.IO event handlers
│       └── tests/
├── keys/                      # RSA key pair (git-ignored)
├── docker-compose.yml
└── .github/workflows/
```

## License

MIT
