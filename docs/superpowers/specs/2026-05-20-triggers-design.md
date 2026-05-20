# Scheduled & Webhook Triggers — Design Spec

**Fecha:** 2026-05-20
**Estado:** Aprobado
**Feature:** Plan 4 — Triggers automáticos (schedule + webhook)

---

## 1. Resumen

Agregar dos tipos de triggers automáticos a Constructor:

- **Scheduled triggers** — los workflows se disparan por cron o intervalo fijo, orquestados por un nuevo `scheduler-service`
- **Webhook triggers** — sistemas externos disparan workflows via HTTP con validación HMAC-SHA256 y mapeo configurable del payload al contexto de ejecución

Ambos triggers crean `ProcessExecution` pasando por Platform API, reutilizando toda la lógica existente de `create_execution()`.

---

## 2. Arquitectura

```
Cron / intervalo
      │
      ▼
┌─────────────────────┐    POST /internal/executions
│  scheduler-service  │ ──────────────────────────────▶ Platform API
│  APScheduler        │    X-Internal-Secret header          │
│  Redis dist. lock   │                                      │
└─────────────────────┘                               create_execution()
                                                             │
Sistema externo                                             ▼
      │                                              ProcessExecution
      ▼                                              → executor
POST /webhooks/{webhook_id}/trigger                  → Redis Stream
      │                                              → WebSocket
  valida HMAC-SHA256
  mapea payload → context
      │
      └──────────────────────────────────────────▶ create_execution()
```

**Nuevos componentes:**
- `services/scheduler-service/` — contenedor nuevo
- `services/platform-api/src/webhooks/` — módulo nuevo en Platform API
- Migración `006_triggers.py` — tabla `webhook_configs` + campo `trigger_type` en `process_executions`
- Endpoint interno `POST /internal/executions` en Platform API

**Sin cambios a:** Agent Orchestration, WebSocket Service, executor existente, modelo de `ProcessDefinition`.

---

## 3. Modelo de datos

### Tabla `webhook_configs`

```sql
id              UUID         PK, default gen_random_uuid()
tenant_id       UUID         FK tenants — RLS aplicada
workflow_id     UUID         FK process_definitions
name            TEXT         NOT NULL — nombre descriptivo
secret_hash     TEXT         NOT NULL — bcrypt del secreto (el plano se muestra solo al crear)
payload_mapping JSONB        NOT NULL DEFAULT '{}' — mapeo JSONPath
is_active       BOOLEAN      NOT NULL DEFAULT true
created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
```

RLS: misma política de tenant isolation que las demás tablas.

### Campo nuevo en `process_executions`

```sql
trigger_type    VARCHAR(20)  NOT NULL DEFAULT 'manual'
                             -- valores: 'manual' | 'webhook' | 'schedule'
```

### Schema de `trigger_config` en `process_definitions`

El campo ya existe como JSONB. Se enriquece con las siguientes formas:

```json
// Manual (ya existente)
{ "type": "manual" }

// Schedule con cron
{ "type": "schedule", "cron": "0 9 * * MON", "timezone": "America/Buenos_Aires" }

// Schedule con intervalo
{ "type": "schedule", "interval_minutes": 30 }

// Webhook
{ "type": "webhook" }
```

---

## 4. Webhook Handler

### Endpoint público (sin JWT)

```
POST /webhooks/{webhook_id}/trigger
Content-Type: application/json
X-Constructor-Signature: sha256=<hmac-hex>

Body: cualquier JSON del sistema externo

Response 202: { "execution_id": "uuid", "status": "pending" }
Response 401: { "detail": "Invalid signature" }
Response 404: { "detail": "Webhook not found" }
```

### Flujo de validación

1. Busca `webhook_config` por `id` — 404 si no existe o `is_active = false`
2. Calcula `HMAC-SHA256(secret, raw_body_bytes)`, compara con el header usando `hmac.compare_digest` (timing-safe)
3. Aplica `payload_mapping` con JSONPath: `{"context.policy_id": "$.data.id"}` → extrae `body["data"]["id"]` → pone en `context["policy_id"]`; campos que no matchean se omiten silenciosamente
4. Llama a `create_execution(db, data, tenant_id=webhook_config.tenant_id, triggered_by=None)` con `trigger_type="webhook"`
5. Retorna `execution_id`

El body del request NO se loguea (evitar log injection y exposición de datos sensibles).

### API de gestión de webhooks (requiere JWT + permiso `WORKFLOW_CREATE`)

```
POST   /webhooks
  Body: { "workflow_id": "uuid", "name": "string", "payload_mapping": {} }
  Response 201: { "id": "uuid", "secret": "plain-text-ONLY-HERE", ... }

GET    /webhooks
  Response 200: [ { "id", "name", "workflow_id", "is_active", "created_at" } ]
  Nota: secret_hash nunca se devuelve

DELETE /webhooks/{webhook_id}
  Response 204 — soft delete (is_active = false)

POST   /webhooks/{webhook_id}/rotate
  Response 200: { "secret": "nuevo-plain-text-ONLY-HERE" }
```

El secreto plano se genera con `secrets.token_hex(32)`, se hashea con bcrypt y se almacena. Se devuelve en texto plano **únicamente** en la respuesta de creación y rotación. No existe forma de recuperarlo después.

---

## 5. Scheduler Service

### Estructura

```
services/scheduler-service/
├── src/
│   ├── config.py           — Settings (DB URL, Redis URL, Platform API URL, internal secret)
│   ├── worker.py           — Entry point: carga jobs + corre APScheduler
│   ├── job_loader.py       — Lee process_definitions con type=schedule de la DB
│   ├── dispatcher.py       — Adquiere lock, llama Platform API, libera lock
│   └── events.py           — Escucha constructor:scheduler.invalidate via Redis pub/sub
├── tests/
│   ├── test_job_loader.py
│   └── test_dispatcher.py
├── pyproject.toml
└── Dockerfile
```

### Arranque

1. Conecta a DB y Redis
2. Carga todos los `process_definitions` con `trigger_config.type = "schedule"`
3. Registra cada uno en APScheduler:
   - Si tiene `cron` → `CronTrigger` con la expresión y timezone
   - Si tiene `interval_minutes` → `IntervalTrigger(minutes=N)`
4. Suscribe a Redis pub/sub `constructor:scheduler.invalidate` para recargar jobs ante cambios
5. Corre `scheduler.start()` + loop de pub/sub concurrentes con `asyncio.gather`

### Disparo de un job

```python
async def dispatch(workflow_id: UUID, tenant_id: UUID) -> None:
    lock_key = f"scheduler:lock:{workflow_id}"
    lock_ttl = 55  # segundos — menor al intervalo mínimo de 1 minuto

    # 1. Distributed lock (SET NX EX)
    acquired = await redis.set(lock_key, "1", nx=True, ex=lock_ttl)
    if not acquired:
        logger.info("Lock not acquired for %s — skipping", workflow_id)
        return

    try:
        # 2. Llamar a Platform API
        resp = await http_client.post(
            f"{settings.platform_api_url}/internal/executions",
            json={"process_definition_id": str(workflow_id), "context": {}},
            headers={"X-Internal-Secret": settings.internal_secret},
        )
        resp.raise_for_status()
    except Exception as exc:
        logger.error("Failed to dispatch workflow %s: %s", workflow_id, exc)
        # retry con backoff — máximo 3 intentos, luego se espera al próximo ciclo
    finally:
        await redis.delete(lock_key)
```

### Endpoint interno en Platform API

```
POST /internal/executions
X-Internal-Secret: <shared_secret>
Content-Type: application/json

Body: { "process_definition_id": "uuid", "context": {} }

Response 202: { "execution_id": "uuid", "status": "pending" }
Response 401: secret inválido o ausente
```

- No requiere JWT
- Valida `X-Internal-Secret` contra variable de entorno `INTERNAL_SECRET`
- Llama a `create_execution()` con `triggered_by=None`, `trigger_type="schedule"`
- El endpoint vive en `src/internal/router.py` (prefijo `/internal`, no documentado en OpenAPI)

### Invalidación de schedules

Cuando se crea o modifica un `process_definition` con `type=schedule`, Platform API publica en Redis:

```python
await redis.publish("constructor:scheduler.invalidate", json.dumps({
    "workflow_id": str(workflow_id),
    "action": "upsert"  # o "delete"
}))
```

El scheduler recibe el mensaje y recarga solo ese job (upsert/delete del APScheduler job).

### Variables de entorno

```
DATABASE_URL          postgresql+asyncpg://...
REDIS_URL             redis://:password@redis:6379/0
PLATFORM_API_URL      http://platform-api:8000
INTERNAL_SECRET       <secreto compartido con Platform API>
POLL_INTERVAL_SECONDS 30   # recarga periódica completa como fallback
```

---

## 6. Manejo de errores

### Webhook
- Firma inválida → 401, no se loguea el body
- JSONPath que no matchea un campo → se omite ese campo del context, no falla la ejecución
- `create_execution` falla → 500, el caller puede reintentar (idempotencia en v2)

### Scheduler
- Job falla al llamar Platform API → retry con backoff exponencial (1s, 4s, 9s), luego skip hasta el próximo ciclo
- Lock TTL expirado con job colgado → el TTL garantiza que el próximo ciclo puede adquirir el lock
- DB no disponible al arrancar → `job_loader` reintenta cada `POLL_INTERVAL_SECONDS`
- Cron expression inválida en DB → log warning + skip ese workflow, no crashea el scheduler

---

## 7. Tests

### Platform API — `tests/test_webhook_trigger.py`
- `POST /webhooks/{id}/trigger` con firma válida → 202 con `execution_id`
- Firma inválida → 401
- Webhook inexistente → 404
- Webhook con `is_active=false` → 404
- Payload mapping con JSONPath → context correcto en la ejecución creada
- JSONPath que no matchea → context sin ese campo, no error

### Platform API — `tests/test_webhook_management.py`
- `POST /webhooks` → 201 con secreto en texto plano
- `GET /webhooks` → lista sin `secret_hash`
- Segundo `GET` del mismo webhook → sin secreto
- `DELETE /webhooks/{id}` → is_active=false, trigger devuelve 404
- `POST /webhooks/{id}/rotate` → nuevo secreto, el anterior ya no valida

### Platform API — `tests/test_internal_endpoint.py`
- Sin header → 401
- Header incorrecto → 401
- Header válido → 202, execution creada con `trigger_type="schedule"`

### Scheduler Service — `tests/test_job_loader.py`
- Carga workflows con type=schedule desde DB → jobs registrados correctamente
- Workflows con type=manual → ignorados
- Cron expression válida → CronTrigger registrado
- Interval_minutes → IntervalTrigger registrado
- Cron expression inválida → warning logueado, workflow skipeado

### Scheduler Service — `tests/test_dispatcher.py`
- Lock adquirido → llama a Platform API → libera lock
- Lock ya tomado → skip sin llamar a Platform API
- Platform API devuelve error → retry 3 veces con backoff → lock liberado en finally
- Invalidación via pub/sub → job recargado

---

## 8. Docker Compose

```yaml
scheduler-service:
  build: ./services/scheduler-service
  depends_on:
    postgres: { condition: service_healthy }
    redis:    { condition: service_healthy }
  environment:
    DATABASE_URL:          postgresql+asyncpg://constructor:constructor_dev@postgres:5432/constructor_dev
    REDIS_URL:             redis://:redis_dev_password@redis:6379/0
    PLATFORM_API_URL:      http://platform-api:8000
    INTERNAL_SECRET:       internal_dev_secret
    POLL_INTERVAL_SECONDS: 30
  restart: unless-stopped
```

Platform API también recibe `INTERNAL_SECRET: internal_dev_secret` como variable de entorno.

---

## 9. Scope fuera de esta feature

- Idempotencia de webhooks (deduplicación por `X-Idempotency-Key`) → v2
- UI para gestionar webhooks y schedules → cuando exista el frontend Angular
- Billing por ejecuciones automáticas → v2
- Webhooks salientes (notificaciones a sistemas externos) → feature separada
