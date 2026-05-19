# Constructor Platform — Plan 1: Foundation & Platform API Core

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Construir la base del monorepo, el esquema de base de datos con RLS multi-tenant, y el Platform API Service completo (auth JWT, RBAC, gestión de tenants/usuarios y Workflow Engine CRUD).

**Architecture:** Event-driven con servicios por dominio. Este plan construye el Platform API Service (Python FastAPI) que es el núcleo de negocio: gestiona auth, permisos, workflows y audit logs. Usa PostgreSQL con Row-Level Security enforced a nivel de motor para aislamiento de tenants. Redis se configura para cache/sessions pero el Event Bus se implementa en Plan 2.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy 2.0 (async), Pydantic v2, Alembic, python-jose (JWT RS256), passlib (bcrypt), pytest, pytest-asyncio, TestContainers, Docker Compose

---

## Descomposición del spec completo

Este es el **Plan 1 de 5**. Los planes posteriores dependen de este:

| Plan | Scope |
|------|-------|
| **Plan 1 (este)** | Foundation + Platform API (Auth, RBAC, Multi-tenancy, Workflow Engine CRUD) |
| Plan 2 | Agent Orchestration Service (LangGraph, Model Router, Tool Registry) |
| Plan 3 | WebSocket Service (Socket.IO, streaming de tokens) |
| Plan 4 | Frontend Angular (dashboards, workflow builder) |
| Plan 5 | Infrastructure as Code (Terraform, AWS ECS, CI/CD) |

---

## Estructura de archivos

```
constructor/
├── services/
│   └── platform-api/
│       ├── src/
│       │   ├── main.py                     # FastAPI app factory
│       │   ├── config.py                   # Settings con pydantic-settings
│       │   ├── database.py                 # Async engine, session factory, RLS injection
│       │   ├── auth/
│       │   │   ├── router.py               # POST /auth/login, /auth/refresh, /auth/logout
│       │   │   ├── service.py              # Lógica de auth: verificar password, emitir JWT
│       │   │   ├── jwt.py                  # Crear/validar JWT RS256
│       │   │   ├── dependencies.py         # get_current_user, require_auth
│       │   │   ├── models.py               # RefreshToken (SQLAlchemy)
│       │   │   └── schemas.py              # LoginRequest, TokenResponse, etc.
│       │   ├── tenants/
│       │   │   ├── router.py               # CRUD /tenants
│       │   │   ├── service.py              # Lógica de negocio
│       │   │   ├── models.py               # Tenant (SQLAlchemy)
│       │   │   └── schemas.py              # TenantCreate, TenantRead, etc.
│       │   ├── users/
│       │   │   ├── router.py               # CRUD /users
│       │   │   ├── service.py
│       │   │   ├── models.py               # User, UserRole (SQLAlchemy)
│       │   │   └── schemas.py              # UserCreate, UserRead, etc.
│       │   ├── rbac/
│       │   │   ├── permissions.py          # Enum de permisos + tabla Role
│       │   │   ├── models.py               # Role, UserRole (SQLAlchemy)
│       │   │   └── dependencies.py         # require_permission(Permission.X)
│       │   ├── workflows/
│       │   │   ├── router.py               # CRUD /workflows
│       │   │   ├── service.py
│       │   │   ├── models.py               # ProcessDefinition, ProcessExecution, ExecutionStep
│       │   │   ├── schemas.py
│       │   │   └── validator.py            # Valida estructura de steps JSON
│       │   └── audit/
│       │       ├── models.py               # AuditLog (SQLAlchemy)
│       │       ├── service.py              # log_action()
│       │       └── middleware.py           # Middleware que loguea requests
│       ├── migrations/
│       │   ├── env.py                      # Alembic env con RLS
│       │   └── versions/
│       │       ├── 001_initial_schema.py
│       │       └── 002_rls_policies.py
│       ├── tests/
│       │   ├── conftest.py                 # Fixtures: TestClient, DB con TestContainers
│       │   ├── test_auth.py
│       │   ├── test_tenants.py
│       │   ├── test_users.py
│       │   ├── test_rbac.py
│       │   └── test_workflows.py
│       ├── Dockerfile
│       ├── pyproject.toml
│       └── .env.example
├── docker-compose.yml                      # PostgreSQL + Redis para dev local
├── docker-compose.test.yml                 # PostgreSQL en puerto aleatorio para tests
└── .github/
    └── workflows/
        └── platform-api-ci.yml
```

---

## Task 1: Monorepo scaffolding + Docker Compose

**Files:**
- Create: `docker-compose.yml`
- Create: `docker-compose.test.yml`
- Create: `services/platform-api/pyproject.toml`
- Create: `services/platform-api/.env.example`
- Create: `services/platform-api/Dockerfile`

- [ ] **Step 1: Crear estructura de directorios**

```bash
mkdir -p services/platform-api/src/{auth,tenants,users,rbac,workflows,audit}
mkdir -p services/platform-api/tests
mkdir -p services/platform-api/migrations/versions
mkdir -p .github/workflows
```

- [ ] **Step 2: Crear docker-compose.yml**

```yaml
# docker-compose.yml
version: "3.9"
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: constructor
      POSTGRES_PASSWORD: constructor_dev
      POSTGRES_DB: constructor_dev
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U constructor"]
      interval: 5s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    command: redis-server --requirepass redis_dev_password
    healthcheck:
      test: ["CMD", "redis-cli", "-a", "redis_dev_password", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5

  platform-api:
    build:
      context: ./services/platform-api
      target: development
    ports:
      - "8000:8000"
    environment:
      DATABASE_URL: postgresql+asyncpg://constructor:constructor_dev@postgres:5432/constructor_dev
      REDIS_URL: redis://:redis_dev_password@redis:6379/0
      JWT_PRIVATE_KEY_PATH: /app/keys/private.pem
      JWT_PUBLIC_KEY_PATH: /app/keys/public.pem
      ENVIRONMENT: development
    volumes:
      - ./services/platform-api:/app
      - ./keys:/app/keys
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    command: uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload

volumes:
  postgres_data:
```

- [ ] **Step 3: Crear docker-compose.test.yml**

```yaml
# docker-compose.test.yml
version: "3.9"
services:
  postgres-test:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: constructor_test
      POSTGRES_PASSWORD: constructor_test
      POSTGRES_DB: constructor_test
    ports:
      - "5433:5432"
    tmpfs:
      - /var/lib/postgresql/data
```

- [ ] **Step 4: Crear pyproject.toml**

```toml
# services/platform-api/pyproject.toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "constructor-platform-api"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "fastapi>=0.115.0",
    "uvicorn[standard]>=0.30.0",
    "sqlalchemy[asyncio]>=2.0.0",
    "asyncpg>=0.29.0",
    "alembic>=1.13.0",
    "pydantic>=2.7.0",
    "pydantic-settings>=2.3.0",
    "python-jose[cryptography]>=3.3.0",
    "passlib[bcrypt]>=1.7.4",
    "redis[asyncio]>=5.0.0",
    "python-multipart>=0.0.9",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.23.0",
    "pytest-cov>=5.0.0",
    "httpx>=0.27.0",
    "testcontainers[postgres]>=4.5.0",
]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

- [ ] **Step 5: Crear .env.example**

```bash
# services/platform-api/.env.example
DATABASE_URL=postgresql+asyncpg://constructor:constructor_dev@localhost:5432/constructor_dev
REDIS_URL=redis://:redis_dev_password@localhost:6379/0
JWT_PRIVATE_KEY_PATH=./keys/private.pem
JWT_PUBLIC_KEY_PATH=./keys/public.pem
JWT_ACCESS_TOKEN_EXPIRE_MINUTES=15
JWT_REFRESH_TOKEN_EXPIRE_DAYS=7
ENVIRONMENT=development
```

- [ ] **Step 6: Crear Dockerfile**

```dockerfile
# services/platform-api/Dockerfile
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
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

- [ ] **Step 7: Generar par de claves RSA para JWT**

```bash
mkdir -p keys
openssl genrsa -out keys/private.pem 2048
openssl rsa -in keys/private.pem -pubout -out keys/public.pem
echo "keys/" >> .gitignore
```

- [ ] **Step 8: Verificar que Docker Compose levanta**

```bash
docker compose up postgres redis -d
docker compose ps
```

Esperado: ambos servicios en estado `healthy`.

- [ ] **Step 9: Commit**

```bash
git init
git add docker-compose.yml docker-compose.test.yml services/platform-api/pyproject.toml services/platform-api/.env.example services/platform-api/Dockerfile .gitignore
git commit -m "feat: monorepo scaffolding with docker compose and platform-api project"
```

---

## Task 2: Config, database y modelos base

**Files:**
- Create: `services/platform-api/src/config.py`
- Create: `services/platform-api/src/database.py`
- Create: `services/platform-api/src/main.py`

- [ ] **Step 1: Crear config.py**

```python
# src/config.py
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str
    redis_url: str
    jwt_private_key_path: Path
    jwt_public_key_path: Path
    jwt_access_token_expire_minutes: int = 15
    jwt_refresh_token_expire_days: int = 7
    environment: str = "development"

    @property
    def jwt_private_key(self) -> str:
        return self.jwt_private_key_path.read_text()

    @property
    def jwt_public_key(self) -> str:
        return self.jwt_public_key_path.read_text()


settings = Settings()
```

- [ ] **Step 2: Crear database.py con inyección de tenant en sesión**

```python
# src/database.py
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import event, text
from src.config import settings


engine = create_async_engine(settings.database_url, echo=False)
AsyncSessionFactory = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncSession:
    """FastAPI dependency: sesión de DB sin tenant (solo para auth)."""
    async with AsyncSessionFactory() as session:
        yield session


async def get_tenant_db(tenant_id: str) -> AsyncSession:
    """FastAPI dependency: sesión de DB con tenant_id inyectado en la sesión PostgreSQL.
    RLS usa este valor para filtrar todas las queries automáticamente.
    """
    async with AsyncSessionFactory() as session:
        await session.execute(
            text("SET app.current_tenant = :tenant_id"),
            {"tenant_id": tenant_id}
        )
        try:
            yield session
        finally:
            await session.execute(text("RESET app.current_tenant"))
```

- [ ] **Step 3: Crear main.py**

```python
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

    # Los routers se registran en tasks posteriores
    return app


app = create_app()


@app.get("/health")
async def health():
    return {"status": "ok"}
```

- [ ] **Step 4: Instalar dependencias y verificar que la app levanta**

```bash
cd services/platform-api
pip install -e ".[dev]"
cp .env.example .env
uvicorn src.main:app --reload
```

Esperado: `http://localhost:8000/health` retorna `{"status": "ok"}`.

- [ ] **Step 5: Commit**

```bash
git add services/platform-api/src/config.py services/platform-api/src/database.py services/platform-api/src/main.py
git commit -m "feat: fastapi app skeleton with config and async db session"
```

---

## Task 3: Modelos SQLAlchemy (Tenant, User, Role)

**Files:**
- Create: `services/platform-api/src/tenants/models.py`
- Create: `services/platform-api/src/users/models.py`
- Create: `services/platform-api/src/rbac/models.py`

- [ ] **Step 1: Crear tenants/models.py**

```python
# src/tenants/models.py
import uuid
from datetime import datetime
from sqlalchemy import String, DateTime, JSON, func
from sqlalchemy.orm import Mapped, mapped_column
from src.database import Base


class Tenant(Base):
    __tablename__ = "tenants"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    slug: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="active")
    config: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )
```

- [ ] **Step 2: Crear rbac/models.py**

```python
# src/rbac/models.py
import uuid
from sqlalchemy import String, ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column
from src.database import Base


class Role(Base):
    __tablename__ = "roles"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    description: Mapped[str] = mapped_column(String(255), default="")


class UserRole(Base):
    __tablename__ = "user_roles"
    __table_args__ = (
        UniqueConstraint("user_id", "tenant_id", "role_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    role_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("roles.id"), nullable=False)
```

- [ ] **Step 3: Crear users/models.py**

```python
# src/users/models.py
import uuid
from datetime import datetime
from sqlalchemy import String, Boolean, ForeignKey, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column, relationship
from src.database import Base


class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    email: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    hashed_password: Mapped[str] = mapped_column(String(255), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())


class RefreshToken(Base):
    __tablename__ = "refresh_tokens"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    token_hash: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    revoked: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
```

- [ ] **Step 4: Commit**

```bash
git add services/platform-api/src/
git commit -m "feat: sqlalchemy models for tenant, user, role and refresh token"
```

---

## Task 4: Migraciones Alembic + RLS policies

**Files:**
- Create: `services/platform-api/migrations/env.py`
- Create: `services/platform-api/migrations/versions/001_initial_schema.py`
- Create: `services/platform-api/migrations/versions/002_rls_policies.py`

- [ ] **Step 1: Inicializar Alembic**

```bash
cd services/platform-api
alembic init migrations
```

- [ ] **Step 2: Reemplazar migrations/env.py**

```python
# migrations/env.py
from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context
from src.database import Base
# Importar todos los modelos para que Alembic los detecte
import src.tenants.models
import src.users.models
import src.rbac.models

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(url=url, target_metadata=target_metadata, literal_binds=True)
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    # Usar URL síncrona para Alembic (reemplaza asyncpg por psycopg2)
    url = config.get_main_option("sqlalchemy.url").replace(
        "postgresql+asyncpg", "postgresql+psycopg2"
    )
    connectable = engine_from_config(
        {"sqlalchemy.url": url},
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
```

- [ ] **Step 3: Crear migración 001 — schema inicial**

```bash
alembic revision --autogenerate -m "initial_schema"
```

Renombrar el archivo generado a `001_initial_schema.py`. Verificar que incluye las tablas `tenants`, `users`, `roles`, `user_roles`, `refresh_tokens`.

- [ ] **Step 4: Crear migración 002 — RLS policies**

```bash
alembic revision -m "rls_policies"
```

Contenido de `002_rls_policies.py`:

```python
# migrations/versions/002_rls_policies.py
from alembic import op

revision = "002"
down_revision = "001"

TABLES_WITH_RLS = ["users", "user_roles", "refresh_tokens"]


def upgrade() -> None:
    # Habilitar RLS en tablas con tenant_id
    for table in TABLES_WITH_RLS:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
        op.execute(f"""
            CREATE POLICY tenant_isolation ON {table}
            USING (tenant_id = current_setting('app.current_tenant', TRUE)::uuid)
        """)

    # Seed de roles del sistema
    op.execute("""
        INSERT INTO roles (id, name, description) VALUES
        (gen_random_uuid(), 'super_admin', 'kimn internal super admin'),
        (gen_random_uuid(), 'tenant_admin', 'Full access within tenant'),
        (gen_random_uuid(), 'developer', 'Create and manage workflows and agents'),
        (gen_random_uuid(), 'project_manager', 'View and trigger workflows'),
        (gen_random_uuid(), 'analyst', 'View and approve workflows'),
        (gen_random_uuid(), 'viewer', 'Read-only access')
        ON CONFLICT (name) DO NOTHING
    """)


def downgrade() -> None:
    for table in TABLES_WITH_RLS:
        op.execute(f"DROP POLICY IF EXISTS tenant_isolation ON {table}")
        op.execute(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY")
```

- [ ] **Step 5: Instalar psycopg2 para Alembic y ejecutar migraciones**

```bash
pip install psycopg2-binary
alembic upgrade head
```

Esperado: `Running upgrade  -> 001, initial_schema` y `Running upgrade 001 -> 002, rls_policies`

- [ ] **Step 6: Verificar RLS en psql**

```bash
docker compose exec postgres psql -U constructor -d constructor_dev -c "\d users"
docker compose exec postgres psql -U constructor -d constructor_dev -c "SELECT policyname FROM pg_policies WHERE tablename='users'"
```

Esperado: política `tenant_isolation` listada para `users`.

- [ ] **Step 7: Commit**

```bash
git add services/platform-api/migrations/
git commit -m "feat: alembic migrations with initial schema and RLS tenant isolation policies"
```

---

## Task 5: JWT service + password hashing

**Files:**
- Create: `services/platform-api/src/auth/jwt.py`
- Create: `services/platform-api/src/auth/schemas.py`
- Create: `services/platform-api/tests/test_auth.py` (parcial)

- [ ] **Step 1: Escribir tests para JWT (TDD — deben fallar)**

```python
# tests/test_auth.py
import pytest
from datetime import timedelta
from src.auth.jwt import create_access_token, decode_access_token


def test_create_and_decode_access_token():
    payload = {"sub": "user-123", "tenant_id": "tenant-abc", "roles": ["developer"]}
    token = create_access_token(payload, expires_delta=timedelta(minutes=15))
    decoded = decode_access_token(token)
    assert decoded["sub"] == "user-123"
    assert decoded["tenant_id"] == "tenant-abc"
    assert decoded["roles"] == ["developer"]


def test_expired_token_raises():
    payload = {"sub": "user-123", "tenant_id": "tenant-abc", "roles": []}
    token = create_access_token(payload, expires_delta=timedelta(seconds=-1))
    with pytest.raises(Exception, match="expired"):
        decode_access_token(token)
```

- [ ] **Step 2: Ejecutar tests — deben fallar**

```bash
cd services/platform-api
pytest tests/test_auth.py -v
```

Esperado: `ImportError: cannot import name 'create_access_token'`

- [ ] **Step 3: Implementar auth/jwt.py**

```python
# src/auth/jwt.py
from datetime import datetime, timedelta, timezone
from typing import Any
from jose import jwt, JWTError, ExpiredSignatureError
from src.config import settings

ALGORITHM = "RS256"


def create_access_token(data: dict[str, Any], expires_delta: timedelta) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + expires_delta
    to_encode["exp"] = expire
    return jwt.encode(to_encode, settings.jwt_private_key, algorithm=ALGORITHM)


def decode_access_token(token: str) -> dict[str, Any]:
    try:
        return jwt.decode(token, settings.jwt_public_key, algorithms=[ALGORITHM])
    except ExpiredSignatureError:
        raise ValueError("Token expired")
    except JWTError as e:
        raise ValueError(f"Invalid token: {e}")
```

- [ ] **Step 4: Ejecutar tests — deben pasar**

```bash
pytest tests/test_auth.py -v
```

Esperado: `2 passed`

- [ ] **Step 5: Crear auth/schemas.py**

```python
# src/auth/schemas.py
from pydantic import BaseModel, EmailStr
import uuid


class LoginRequest(BaseModel):
    email: EmailStr
    password: str
    tenant_slug: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int  # segundos


class RefreshRequest(BaseModel):
    refresh_token: str


class CurrentUser(BaseModel):
    id: uuid.UUID
    email: str
    tenant_id: uuid.UUID
    roles: list[str]
```

- [ ] **Step 6: Commit**

```bash
git add services/platform-api/src/auth/ services/platform-api/tests/test_auth.py
git commit -m "feat: JWT RS256 create/decode with tests"
```

---

## Task 6: Auth service + endpoint de login

**Files:**
- Create: `services/platform-api/src/auth/service.py`
- Create: `services/platform-api/src/auth/router.py`
- Modify: `services/platform-api/src/main.py`

- [ ] **Step 1: Agregar tests de login a test_auth.py**

```python
# Agregar al final de tests/test_auth.py
import pytest
from httpx import AsyncClient, ASGITransport
from src.main import app


@pytest.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_login_invalid_credentials(client):
    response = await client.post("/auth/login", json={
        "email": "nobody@example.com",
        "password": "wrong",
        "tenant_slug": "nonexistent"
    })
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_login_returns_token(client, seeded_user):
    """seeded_user fixture se crea en conftest.py en Task 8."""
    response = await client.post("/auth/login", json={
        "email": seeded_user["email"],
        "password": seeded_user["password"],
        "tenant_slug": seeded_user["tenant_slug"]
    })
    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"
```

- [ ] **Step 2: Ejecutar — debe fallar**

```bash
pytest tests/test_auth.py::test_login_invalid_credentials -v
```

Esperado: `ImportError` o `404 Not Found` (router no registrado aún).

- [ ] **Step 3: Implementar auth/service.py**

```python
# src/auth/service.py
import uuid
import hashlib
from datetime import timedelta, datetime, timezone
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from passlib.context import CryptContext
from src.users.models import User, RefreshToken
from src.tenants.models import Tenant
from src.rbac.models import UserRole, Role
from src.auth.jwt import create_access_token
from src.auth.schemas import TokenResponse
from src.config import settings

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


async def authenticate_user(
    db: AsyncSession, email: str, password: str, tenant_slug: str
) -> User | None:
    tenant = await db.scalar(select(Tenant).where(Tenant.slug == tenant_slug))
    if not tenant:
        return None
    user = await db.scalar(
        select(User).where(User.email == email, User.tenant_id == tenant.id, User.is_active == True)
    )
    if not user or not verify_password(password, user.hashed_password):
        return None
    return user


async def get_user_roles(db: AsyncSession, user_id: uuid.UUID, tenant_id: uuid.UUID) -> list[str]:
    result = await db.execute(
        select(Role.name)
        .join(UserRole, UserRole.role_id == Role.id)
        .where(UserRole.user_id == user_id, UserRole.tenant_id == tenant_id)
    )
    return [row[0] for row in result.all()]


async def create_tokens(
    db: AsyncSession, user: User
) -> tuple[TokenResponse, str]:
    roles = await get_user_roles(db, user.id, user.tenant_id)
    access_token = create_access_token(
        {"sub": str(user.id), "tenant_id": str(user.tenant_id), "roles": roles},
        expires_delta=timedelta(minutes=settings.jwt_access_token_expire_minutes),
    )
    raw_refresh = str(uuid.uuid4())
    token_hash = hashlib.sha256(raw_refresh.encode()).hexdigest()
    db.add(RefreshToken(
        user_id=user.id,
        token_hash=token_hash,
        expires_at=datetime.now(timezone.utc) + timedelta(days=settings.jwt_refresh_token_expire_days),
    ))
    await db.commit()
    return TokenResponse(
        access_token=access_token,
        expires_in=settings.jwt_access_token_expire_minutes * 60,
    ), raw_refresh
```

- [ ] **Step 4: Implementar auth/router.py**

```python
# src/auth/router.py
from fastapi import APIRouter, Depends, HTTPException, Response, status, Cookie
from sqlalchemy.ext.asyncio import AsyncSession
from src.database import get_db
from src.auth.service import authenticate_user, create_tokens
from src.auth.schemas import LoginRequest, TokenResponse
from src.config import settings

router = APIRouter(prefix="/auth", tags=["auth"])

REFRESH_COOKIE = "refresh_token"


@router.post("/login", response_model=TokenResponse)
async def login(body: LoginRequest, response: Response, db: AsyncSession = Depends(get_db)):
    user = await authenticate_user(db, body.email, body.password, body.tenant_slug)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    token_response, raw_refresh = await create_tokens(db, user)
    response.set_cookie(
        key=REFRESH_COOKIE,
        value=raw_refresh,
        httponly=True,
        secure=settings.environment != "development",
        samesite="strict",
        max_age=settings.jwt_refresh_token_expire_days * 86400,
    )
    return token_response
```

- [ ] **Step 5: Registrar router en main.py**

```python
# src/main.py — agregar dentro de create_app() antes del return
from src.auth.router import router as auth_router
app.include_router(auth_router)
```

- [ ] **Step 6: Ejecutar tests de login**

```bash
pytest tests/test_auth.py::test_login_invalid_credentials -v
```

Esperado: `PASSED` (401 retornado correctamente).

- [ ] **Step 7: Commit**

```bash
git add services/platform-api/src/auth/ services/platform-api/src/main.py
git commit -m "feat: login endpoint with JWT + httponly refresh token cookie"
```

---

## Task 7: JWT dependency + inyección de tenant en request

**Files:**
- Create: `services/platform-api/src/auth/dependencies.py`

- [ ] **Step 1: Escribir test para la dependency**

```python
# Agregar a tests/test_auth.py
@pytest.mark.asyncio
async def test_protected_endpoint_without_token(client):
    response = await client.get("/users/me")
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_protected_endpoint_with_valid_token(client, seeded_user):
    login_resp = await client.post("/auth/login", json={
        "email": seeded_user["email"],
        "password": seeded_user["password"],
        "tenant_slug": seeded_user["tenant_slug"],
    })
    token = login_resp.json()["access_token"]
    resp = await client.get("/users/me", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200
    assert resp.json()["email"] == seeded_user["email"]
```

- [ ] **Step 2: Implementar auth/dependencies.py**

```python
# src/auth/dependencies.py
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from src.auth.jwt import decode_access_token
from src.auth.schemas import CurrentUser
from src.users.models import User
from src.database import get_db, get_tenant_db
import uuid

bearer_scheme = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
    db: AsyncSession = Depends(get_db),
) -> CurrentUser:
    try:
        payload = decode_access_token(credentials.credentials)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(e))

    user_id = uuid.UUID(payload["sub"])
    tenant_id = uuid.UUID(payload["tenant_id"])

    # Verificar que el usuario sigue activo (sin RLS, usamos get_db)
    user = await db.scalar(select(User).where(User.id == user_id, User.is_active == True))
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")

    return CurrentUser(
        id=user_id,
        email=user.email,
        tenant_id=tenant_id,
        roles=payload.get("roles", []),
    )


def get_scoped_db(current_user: CurrentUser = Depends(get_current_user)):
    """Retorna una sesión de DB con tenant_id inyectado. Usar en todos los endpoints autenticados."""
    return get_tenant_db(str(current_user.tenant_id))
```

- [ ] **Step 3: Commit**

```bash
git add services/platform-api/src/auth/dependencies.py
git commit -m "feat: JWT auth dependency with tenant injection for RLS"
```

---

## Task 8: RBAC — permisos y dependency

**Files:**
- Create: `services/platform-api/src/rbac/permissions.py`
- Create: `services/platform-api/src/rbac/dependencies.py`

- [ ] **Step 1: Escribir test de RBAC**

```python
# tests/test_rbac.py
import pytest
from src.rbac.permissions import Permission, ROLE_PERMISSIONS


def test_developer_can_create_workflows():
    assert Permission.WORKFLOW_CREATE in ROLE_PERMISSIONS["developer"]


def test_viewer_cannot_create_workflows():
    assert Permission.WORKFLOW_CREATE not in ROLE_PERMISSIONS["viewer"]


def test_tenant_admin_has_all_tenant_permissions():
    admin_perms = ROLE_PERMISSIONS["tenant_admin"]
    assert Permission.WORKFLOW_CREATE in admin_perms
    assert Permission.USER_MANAGE in admin_perms
    assert Permission.TENANT_CONFIG in admin_perms
```

- [ ] **Step 2: Ejecutar — deben fallar**

```bash
pytest tests/test_rbac.py -v
```

Esperado: `ImportError`

- [ ] **Step 3: Implementar rbac/permissions.py**

```python
# src/rbac/permissions.py
from enum import StrEnum


class Permission(StrEnum):
    # Workflows
    WORKFLOW_CREATE = "workflow:create"
    WORKFLOW_READ = "workflow:read"
    WORKFLOW_UPDATE = "workflow:update"
    WORKFLOW_DELETE = "workflow:delete"
    WORKFLOW_TRIGGER = "workflow:trigger"
    # Agentes
    AGENT_CREATE = "agent:create"
    AGENT_READ = "agent:read"
    AGENT_UPDATE = "agent:update"
    AGENT_DELETE = "agent:delete"
    # Usuarios
    USER_MANAGE = "user:manage"
    USER_READ = "user:read"
    # Tenant
    TENANT_CONFIG = "tenant:config"
    # Aprobaciones
    APPROVAL_MANAGE = "approval:manage"
    # Audit
    AUDIT_READ = "audit:read"
    # Reportes
    REPORT_READ = "report:read"


ROLE_PERMISSIONS: dict[str, set[Permission]] = {
    "super_admin": set(Permission),  # todos los permisos
    "tenant_admin": {
        Permission.WORKFLOW_CREATE, Permission.WORKFLOW_READ,
        Permission.WORKFLOW_UPDATE, Permission.WORKFLOW_DELETE, Permission.WORKFLOW_TRIGGER,
        Permission.AGENT_CREATE, Permission.AGENT_READ,
        Permission.AGENT_UPDATE, Permission.AGENT_DELETE,
        Permission.USER_MANAGE, Permission.USER_READ,
        Permission.TENANT_CONFIG, Permission.APPROVAL_MANAGE,
        Permission.AUDIT_READ, Permission.REPORT_READ,
    },
    "developer": {
        Permission.WORKFLOW_CREATE, Permission.WORKFLOW_READ,
        Permission.WORKFLOW_UPDATE, Permission.WORKFLOW_DELETE, Permission.WORKFLOW_TRIGGER,
        Permission.AGENT_CREATE, Permission.AGENT_READ,
        Permission.AGENT_UPDATE, Permission.AGENT_DELETE,
        Permission.USER_READ, Permission.REPORT_READ,
    },
    "project_manager": {
        Permission.WORKFLOW_READ, Permission.WORKFLOW_TRIGGER,
        Permission.AGENT_READ, Permission.APPROVAL_MANAGE, Permission.REPORT_READ,
    },
    "analyst": {
        Permission.WORKFLOW_READ, Permission.APPROVAL_MANAGE, Permission.REPORT_READ,
    },
    "viewer": {
        Permission.WORKFLOW_READ, Permission.REPORT_READ,
    },
}


def has_permission(roles: list[str], permission: Permission) -> bool:
    for role in roles:
        if permission in ROLE_PERMISSIONS.get(role, set()):
            return True
    return False
```

- [ ] **Step 4: Implementar rbac/dependencies.py**

```python
# src/rbac/dependencies.py
from fastapi import Depends, HTTPException, status
from src.auth.dependencies import get_current_user
from src.auth.schemas import CurrentUser
from src.rbac.permissions import Permission, has_permission


def require_permission(permission: Permission):
    """Factory de dependency. Uso: Depends(require_permission(Permission.WORKFLOW_CREATE))"""
    def _check(current_user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
        if not has_permission(current_user.roles, permission):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission required: {permission}",
            )
        return current_user
    return _check
```

- [ ] **Step 5: Ejecutar tests — deben pasar**

```bash
pytest tests/test_rbac.py -v
```

Esperado: `3 passed`

- [ ] **Step 6: Commit**

```bash
git add services/platform-api/src/rbac/
git commit -m "feat: RBAC permissions enum with role mapping and require_permission dependency"
```

---

## Task 9: conftest.py con fixtures de integración (TestContainers)

**Files:**
- Create: `services/platform-api/tests/conftest.py`

- [ ] **Step 1: Crear conftest.py**

```python
# tests/conftest.py
import pytest
import pytest_asyncio
from testcontainers.postgres import PostgresContainer
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from httpx import AsyncClient, ASGITransport
from alembic.config import Config
from alembic import command
from src.database import Base
from src.auth.service import hash_password
from src.tenants.models import Tenant
from src.users.models import User
from src.rbac.models import Role, UserRole
import uuid


@pytest.fixture(scope="session")
def postgres_container():
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest_asyncio.fixture(scope="session")
async def db_engine(postgres_container):
    sync_url = postgres_container.get_connection_url()
    async_url = sync_url.replace("postgresql+psycopg2", "postgresql+asyncpg")

    # Correr migraciones con URL síncrona
    alembic_cfg = Config("alembic.ini")
    alembic_cfg.set_main_option("sqlalchemy.url", sync_url)
    command.upgrade(alembic_cfg, "head")

    engine = create_async_engine(async_url)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def db_session(db_engine):
    factory = async_sessionmaker(db_engine, expire_on_commit=False)
    async with factory() as session:
        yield session
        await session.rollback()


@pytest_asyncio.fixture
async def seeded_user(db_session):
    tenant = Tenant(name="Test Corp", slug="test-corp")
    db_session.add(tenant)
    await db_session.flush()

    user = User(
        tenant_id=tenant.id,
        email="dev@test-corp.com",
        hashed_password=hash_password("secret123"),
    )
    db_session.add(user)
    await db_session.flush()

    role = await db_session.scalar(
        __import__("sqlalchemy", fromlist=["select"]).select(Role).where(Role.name == "developer")
    )
    db_session.add(UserRole(user_id=user.id, tenant_id=tenant.id, role_id=role.id))
    await db_session.commit()

    return {"email": "dev@test-corp.com", "password": "secret123", "tenant_slug": "test-corp"}


@pytest_asyncio.fixture
async def client(db_session):
    async with AsyncClient(transport=ASGITransport(app=__import__("src.main", fromlist=["app"]).app), base_url="http://test") as c:
        yield c
```

- [ ] **Step 2: Instalar testcontainers y psycopg2**

```bash
pip install "testcontainers[postgres]" psycopg2-binary
```

- [ ] **Step 3: Ejecutar todos los tests hasta ahora**

```bash
pytest tests/ -v --cov=src --cov-report=term-missing
```

Esperado: todos los tests pasan (algunos pueden requerir ajustes de imports).

- [ ] **Step 4: Commit**

```bash
git add services/platform-api/tests/conftest.py
git commit -m "test: testcontainers fixtures for integration tests with real postgres"
```

---

## Task 10: Tenant y User endpoints

**Files:**
- Create: `services/platform-api/src/tenants/schemas.py`
- Create: `services/platform-api/src/tenants/service.py`
- Create: `services/platform-api/src/tenants/router.py`
- Create: `services/platform-api/src/users/schemas.py`
- Create: `services/platform-api/src/users/service.py`
- Create: `services/platform-api/src/users/router.py`
- Create: `services/platform-api/tests/test_tenants.py`
- Create: `services/platform-api/tests/test_users.py`

- [ ] **Step 1: Escribir tests de tenants**

```python
# tests/test_tenants.py
import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_super_admin_can_create_tenant(client, super_admin_token):
    resp = await client.post(
        "/tenants",
        json={"name": "New Corp", "slug": "new-corp"},
        headers={"Authorization": f"Bearer {super_admin_token}"},
    )
    assert resp.status_code == 201
    assert resp.json()["slug"] == "new-corp"


@pytest.mark.asyncio
async def test_developer_cannot_create_tenant(client, developer_token):
    resp = await client.post(
        "/tenants",
        json={"name": "Hack Corp", "slug": "hack-corp"},
        headers={"Authorization": f"Bearer {developer_token}"},
    )
    assert resp.status_code == 403
```

- [ ] **Step 2: Crear tenants/schemas.py**

```python
# src/tenants/schemas.py
import uuid
from pydantic import BaseModel


class TenantCreate(BaseModel):
    name: str
    slug: str
    config: dict = {}


class TenantRead(BaseModel):
    id: uuid.UUID
    name: str
    slug: str
    status: str
    config: dict

    model_config = {"from_attributes": True}
```

- [ ] **Step 3: Crear tenants/service.py**

```python
# src/tenants/service.py
import uuid
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from src.tenants.models import Tenant
from src.tenants.schemas import TenantCreate


async def create_tenant(db: AsyncSession, data: TenantCreate) -> Tenant:
    tenant = Tenant(name=data.name, slug=data.slug, config=data.config)
    db.add(tenant)
    await db.commit()
    await db.refresh(tenant)
    return tenant


async def get_tenant_by_id(db: AsyncSession, tenant_id: uuid.UUID) -> Tenant | None:
    return await db.scalar(select(Tenant).where(Tenant.id == tenant_id))


async def list_tenants(db: AsyncSession) -> list[Tenant]:
    result = await db.execute(select(Tenant))
    return list(result.scalars().all())
```

- [ ] **Step 4: Crear tenants/router.py**

```python
# src/tenants/router.py
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from src.database import get_db
from src.auth.dependencies import get_current_user
from src.auth.schemas import CurrentUser
from src.rbac.dependencies import require_permission
from src.rbac.permissions import Permission
from src.tenants.service import create_tenant, list_tenants
from src.tenants.schemas import TenantCreate, TenantRead

router = APIRouter(prefix="/tenants", tags=["tenants"])


@router.post("", response_model=TenantRead, status_code=status.HTTP_201_CREATED)
async def create(
    body: TenantCreate,
    db: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(require_permission(Permission.TENANT_CONFIG)),
):
    return await create_tenant(db, body)


@router.get("", response_model=list[TenantRead])
async def list_all(
    db: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(require_permission(Permission.TENANT_CONFIG)),
):
    return await list_tenants(db)
```

- [ ] **Step 5: Crear users/schemas.py**

```python
# src/users/schemas.py
import uuid
from pydantic import BaseModel, EmailStr


class UserCreate(BaseModel):
    email: EmailStr
    password: str
    role: str = "developer"


class UserRead(BaseModel):
    id: uuid.UUID
    email: str
    tenant_id: uuid.UUID
    is_active: bool

    model_config = {"from_attributes": True}


class UserMe(UserRead):
    roles: list[str]
```

- [ ] **Step 6: Crear users/service.py**

```python
# src/users/service.py
import uuid
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from src.users.models import User, UserRole
from src.rbac.models import Role
from src.auth.service import hash_password
from src.users.schemas import UserCreate


async def create_user(
    db: AsyncSession, data: UserCreate, tenant_id: uuid.UUID
) -> User:
    role = await db.scalar(select(Role).where(Role.name == data.role))
    if not role:
        raise ValueError(f"Role '{data.role}' not found")
    user = User(
        tenant_id=tenant_id,
        email=data.email,
        hashed_password=hash_password(data.password),
    )
    db.add(user)
    await db.flush()
    db.add(UserRole(user_id=user.id, tenant_id=tenant_id, role_id=role.id))
    await db.commit()
    await db.refresh(user)
    return user


async def get_user_by_id(db: AsyncSession, user_id: uuid.UUID) -> User | None:
    return await db.scalar(select(User).where(User.id == user_id))
```

- [ ] **Step 7: Crear users/router.py con endpoint /users/me**

```python
# src/users/router.py
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from src.auth.dependencies import get_current_user
from src.auth.schemas import CurrentUser
from src.rbac.permissions import ROLE_PERMISSIONS
from src.users.schemas import UserCreate, UserRead, UserMe
from src.users.service import create_user
from src.rbac.dependencies import require_permission
from src.rbac.permissions import Permission
from src.database import get_db

router = APIRouter(prefix="/users", tags=["users"])


@router.get("/me", response_model=UserMe)
async def me(current_user: CurrentUser = Depends(get_current_user)):
    return UserMe(
        id=current_user.id,
        email=current_user.email,
        tenant_id=current_user.tenant_id,
        is_active=True,
        roles=current_user.roles,
    )


@router.post("", response_model=UserRead, status_code=201)
async def create(
    body: UserCreate,
    db: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(require_permission(Permission.USER_MANAGE)),
):
    return await create_user(db, body, current_user.tenant_id)
```

- [ ] **Step 8: Registrar routers en main.py**

```python
# src/main.py — agregar dentro de create_app()
from src.tenants.router import router as tenants_router
from src.users.router import router as users_router
app.include_router(tenants_router)
app.include_router(users_router)
```

- [ ] **Step 9: Ejecutar todos los tests**

```bash
pytest tests/ -v
```

Esperado: todos los tests pasan.

- [ ] **Step 10: Commit**

```bash
git add services/platform-api/src/tenants/ services/platform-api/src/users/ services/platform-api/tests/
git commit -m "feat: tenant and user management endpoints with RBAC"
```

---

## Task 11: Audit Log

**Files:**
- Create: `services/platform-api/src/audit/models.py`
- Create: `services/platform-api/src/audit/service.py`
- Create: `services/platform-api/migrations/versions/003_audit_log.py`

- [ ] **Step 1: Crear audit/models.py**

```python
# src/audit/models.py
import uuid
from datetime import datetime
from sqlalchemy import String, JSON, ForeignKey, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column
from src.database import Base


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    user_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(50), nullable=False)
    resource_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    metadata: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
```

- [ ] **Step 2: Crear audit/service.py**

```python
# src/audit/service.py
import uuid
from sqlalchemy.ext.asyncio import AsyncSession
from src.audit.models import AuditLog


async def log_action(
    db: AsyncSession,
    tenant_id: uuid.UUID,
    action: str,
    resource_type: str,
    user_id: uuid.UUID | None = None,
    resource_id: str | None = None,
    metadata: dict | None = None,
) -> None:
    db.add(AuditLog(
        tenant_id=tenant_id,
        user_id=user_id,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        metadata=metadata or {},
    ))
    # No hacer commit aquí — el caller decide cuándo commitear
```

- [ ] **Step 3: Crear migración para audit_logs**

```bash
alembic revision --autogenerate -m "audit_log_table"
```

Renombrar a `003_audit_log.py`. Agregar RLS a la tabla al final del `upgrade()`:

```python
op.execute("ALTER TABLE audit_logs ENABLE ROW LEVEL SECURITY")
op.execute("ALTER TABLE audit_logs FORCE ROW LEVEL SECURITY")
op.execute("""
    CREATE POLICY tenant_isolation ON audit_logs
    USING (tenant_id = current_setting('app.current_tenant', TRUE)::uuid)
""")
```

- [ ] **Step 4: Aplicar migración**

```bash
alembic upgrade head
```

- [ ] **Step 5: Agregar log_action a user creation**

En `src/users/service.py`, agregar al final de `create_user()` antes del commit:

```python
from src.audit.service import log_action
# ...dentro de create_user, antes del commit final:
await log_action(
    db, tenant_id=tenant_id,
    action="user.created", resource_type="user",
    resource_id=str(user.id), user_id=None,
)
```

- [ ] **Step 6: Commit**

```bash
git add services/platform-api/src/audit/ services/platform-api/migrations/
git commit -m "feat: immutable audit log with RLS and log_action helper"
```

---

## Task 12: Workflow Engine — modelos y CRUD

**Files:**
- Create: `services/platform-api/src/workflows/models.py`
- Create: `services/platform-api/src/workflows/schemas.py`
- Create: `services/platform-api/src/workflows/validator.py`
- Create: `services/platform-api/src/workflows/service.py`
- Create: `services/platform-api/src/workflows/router.py`
- Create: `services/platform-api/tests/test_workflows.py`
- Create: `services/platform-api/migrations/versions/004_workflows.py`

- [ ] **Step 1: Escribir tests de workflows**

```python
# tests/test_workflows.py
import pytest


@pytest.mark.asyncio
async def test_create_workflow(client, developer_token):
    resp = await client.post(
        "/workflows",
        json={
            "name": "Code Audit",
            "trigger": {"type": "webhook"},
            "steps": [
                {
                    "id": "step_1",
                    "type": "agent",
                    "config": {"model": "claude-opus-4", "prompt": "Analyze the code diff"},
                    "next": None,
                    "timeout_seconds": 300,
                }
            ],
            "on_error": "stop",
        },
        headers={"Authorization": f"Bearer {developer_token}"},
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["name"] == "Code Audit"
    assert data["version"] == 1


@pytest.mark.asyncio
async def test_invalid_step_type_rejected(client, developer_token):
    resp = await client.post(
        "/workflows",
        json={
            "name": "Bad Workflow",
            "trigger": {"type": "manual"},
            "steps": [{"id": "s1", "type": "invalid_type", "config": {}, "next": None, "timeout_seconds": 60}],
            "on_error": "stop",
        },
        headers={"Authorization": f"Bearer {developer_token}"},
    )
    assert resp.status_code == 422
```

- [ ] **Step 2: Crear workflows/models.py**

```python
# src/workflows/models.py
import uuid
from datetime import datetime
from sqlalchemy import String, JSON, ForeignKey, DateTime, Integer, func
from sqlalchemy.orm import Mapped, mapped_column
from src.database import Base


class ProcessDefinition(Base):
    __tablename__ = "process_definitions"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    trigger_config: Mapped[dict] = mapped_column(JSON, nullable=False)
    steps: Mapped[list] = mapped_column(JSON, nullable=False)
    on_error: Mapped[str] = mapped_column(String(20), default="stop")
    version: Mapped[int] = mapped_column(Integer, default=1)
    created_by: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())


class ProcessExecution(Base):
    __tablename__ = "process_executions"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    process_definition_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("process_definitions.id"), nullable=False)
    status: Mapped[str] = mapped_column(String(30), default="pending")
    context: Mapped[dict] = mapped_column(JSON, default=dict)
    triggered_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
```

- [ ] **Step 3: Crear workflows/validator.py**

```python
# src/workflows/validator.py
from typing import Literal

VALID_STEP_TYPES = {"agent", "condition", "transform", "notify", "wait", "human_approval"}
VALID_ON_ERROR = {"stop", "skip", "retry", "fallback"}


def validate_steps(steps: list[dict]) -> list[str]:
    """Retorna lista de errores. Lista vacía = válido."""
    errors = []
    if not steps:
        errors.append("Process must have at least one step")
    for i, step in enumerate(steps):
        if step.get("type") not in VALID_STEP_TYPES:
            errors.append(f"Step {i}: invalid type '{step.get('type')}'. Must be one of {VALID_STEP_TYPES}")
        if not step.get("id"):
            errors.append(f"Step {i}: 'id' is required")
        if step.get("timeout_seconds", 0) <= 0:
            errors.append(f"Step {i}: timeout_seconds must be positive")
    return errors
```

- [ ] **Step 4: Crear workflows/schemas.py**

```python
# src/workflows/schemas.py
import uuid
from datetime import datetime
from pydantic import BaseModel, field_validator
from src.workflows.validator import validate_steps, VALID_ON_ERROR


class StepSchema(BaseModel):
    id: str
    type: str
    config: dict
    next: str | dict | None = None
    timeout_seconds: int = 60


class ProcessDefinitionCreate(BaseModel):
    name: str
    trigger: dict
    steps: list[StepSchema]
    on_error: str = "stop"

    @field_validator("steps")
    @classmethod
    def validate_steps_field(cls, v):
        errors = validate_steps([s.model_dump() for s in v])
        if errors:
            raise ValueError("; ".join(errors))
        return v

    @field_validator("on_error")
    @classmethod
    def validate_on_error(cls, v):
        if v not in VALID_ON_ERROR:
            raise ValueError(f"on_error must be one of {VALID_ON_ERROR}")
        return v


class ProcessDefinitionRead(BaseModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    name: str
    trigger_config: dict
    steps: list[dict]
    on_error: str
    version: int
    created_at: datetime

    model_config = {"from_attributes": True}
```

- [ ] **Step 5: Crear workflows/service.py**

```python
# src/workflows/service.py
import uuid
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from src.workflows.models import ProcessDefinition
from src.workflows.schemas import ProcessDefinitionCreate


async def create_process(
    db: AsyncSession, data: ProcessDefinitionCreate, tenant_id: uuid.UUID, user_id: uuid.UUID
) -> ProcessDefinition:
    process = ProcessDefinition(
        tenant_id=tenant_id,
        name=data.name,
        trigger_config=data.trigger,
        steps=[s.model_dump() for s in data.steps],
        on_error=data.on_error,
        created_by=user_id,
    )
    db.add(process)
    await db.commit()
    await db.refresh(process)
    return process


async def list_processes(db: AsyncSession) -> list[ProcessDefinition]:
    result = await db.execute(select(ProcessDefinition))
    return list(result.scalars().all())


async def get_process(db: AsyncSession, process_id: uuid.UUID) -> ProcessDefinition | None:
    return await db.scalar(select(ProcessDefinition).where(ProcessDefinition.id == process_id))
```

- [ ] **Step 6: Crear workflows/router.py**

```python
# src/workflows/router.py
import uuid
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from src.auth.dependencies import get_current_user
from src.auth.schemas import CurrentUser
from src.rbac.dependencies import require_permission
from src.rbac.permissions import Permission
from src.workflows.service import create_process, list_processes, get_process
from src.workflows.schemas import ProcessDefinitionCreate, ProcessDefinitionRead
from src.database import get_db

router = APIRouter(prefix="/workflows", tags=["workflows"])


@router.post("", response_model=ProcessDefinitionRead, status_code=status.HTTP_201_CREATED)
async def create(
    body: ProcessDefinitionCreate,
    db: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(require_permission(Permission.WORKFLOW_CREATE)),
):
    return await create_process(db, body, current_user.tenant_id, current_user.id)


@router.get("", response_model=list[ProcessDefinitionRead])
async def list_all(
    db: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(require_permission(Permission.WORKFLOW_READ)),
):
    return await list_processes(db)


@router.get("/{workflow_id}", response_model=ProcessDefinitionRead)
async def get_one(
    workflow_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    current_user: CurrentUser = Depends(require_permission(Permission.WORKFLOW_READ)),
):
    process = await get_process(db, workflow_id)
    if not process:
        raise HTTPException(status_code=404, detail="Workflow not found")
    return process
```

- [ ] **Step 7: Crear migración para workflows**

```bash
alembic revision --autogenerate -m "workflow_tables"
```

Renombrar a `004_workflows.py`. Agregar RLS a ambas tablas en `upgrade()`:

```python
for table in ["process_definitions", "process_executions"]:
    op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
    op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
    op.execute(f"""
        CREATE POLICY tenant_isolation ON {table}
        USING (tenant_id = current_setting('app.current_tenant', TRUE)::uuid)
    """)
```

- [ ] **Step 8: Registrar router en main.py**

```python
from src.workflows.router import router as workflows_router
app.include_router(workflows_router)
```

- [ ] **Step 9: Ejecutar todos los tests**

```bash
pytest tests/ -v --cov=src --cov-report=term-missing
```

Esperado: todos los tests pasan, cobertura > 80%.

- [ ] **Step 10: Commit**

```bash
git add services/platform-api/src/workflows/ services/platform-api/migrations/
git commit -m "feat: workflow engine CRUD with step validation and RLS"
```

---

## Task 13: GitHub Actions CI

**Files:**
- Create: `.github/workflows/platform-api-ci.yml`

- [ ] **Step 1: Crear CI pipeline**

```yaml
# .github/workflows/platform-api-ci.yml
name: Platform API CI

on:
  push:
    paths:
      - "services/platform-api/**"
  pull_request:
    paths:
      - "services/platform-api/**"

jobs:
  test:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: services/platform-api

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
          cache: "pip"

      - name: Install dependencies
        run: pip install -e ".[dev]" psycopg2-binary

      - name: Generate RSA keys for tests
        run: |
          mkdir -p keys
          openssl genrsa -out keys/private.pem 2048
          openssl rsa -in keys/private.pem -pubout -out keys/public.pem

      - name: Run tests
        run: pytest tests/ -v --cov=src --cov-report=xml --cov-fail-under=80
        env:
          DATABASE_URL: postgresql+asyncpg://test:test@localhost:5432/test
          JWT_PRIVATE_KEY_PATH: ./keys/private.pem
          JWT_PUBLIC_KEY_PATH: ./keys/public.pem

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
        working-directory: services/platform-api

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
git add .github/
git commit -m "ci: github actions pipeline for platform-api with tests and security scan"
```

---

## Self-Review del plan

**Spec coverage check:**

| Sección del spec | Cubierto en |
|------------------|-------------|
| Arquitectura general (3 servicios) | Estructura de archivos + Task 1 |
| Workflow Engine (6 tipos de pasos) | Task 12 — validator.py cubre los 6 tipos |
| ProcessDefinition model | Task 12 — models.py |
| ProcessExecution lifecycle | Task 12 — models.py (estados en schema) |
| Multi-tenancy Shared DB + RLS | Task 4 — migración 002_rls_policies |
| Auth JWT RS256 + refresh token | Tasks 5, 6, 7 |
| RBAC 6 roles + permisos | Task 8 |
| Tenant management | Task 10 |
| User management | Task 10 |
| Audit Log append-only | Task 11 |
| CI pipeline con gates de seguridad | Task 13 |
| SSO / SAML | ❌ No incluido en Plan 1 — agregar en Plan 2 o como extensión |
| MFA | ❌ No incluido en Plan 1 — agregar en v1.1 |
| Secrets Management (AWS Secrets Manager) | ❌ Cubierto en Plan 5 (Infrastructure) |

**Gaps identificados y decisión:**
- SSO/SAML y MFA se excluyen del Plan 1 (MVP puede funcionar con email+password + JWT). Se documentan como tarea en Plan 5.
- AWS Secrets Manager se implementa en Plan 5 (Infrastructure as Code).
- El execution engine (correr un workflow paso a paso) se implementa en Plan 2 junto con el Agent Orchestration Service que lo consume.
