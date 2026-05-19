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
import atexit
atexit.register(os.unlink, _pub_key_file.name)

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


@pytest.fixture(scope="session")
def redis_container():
    from testcontainers.redis import RedisContainer
    with RedisContainer("redis:7-alpine") as r:
        yield r


@pytest_asyncio.fixture(scope="session")
async def redis_client(redis_container):
    from redis.asyncio import Redis
    client = Redis(
        host=redis_container.get_container_host_ip(),
        port=int(redis_container.get_exposed_port(6379)),
        decode_responses=True,
    )
    yield client
    await client.aclose()
