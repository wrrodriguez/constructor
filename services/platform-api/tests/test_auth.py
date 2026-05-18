import pytest
from datetime import timedelta
from unittest.mock import patch

from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from httpx import AsyncClient, ASGITransport

from src.auth.jwt import create_access_token, decode_access_token
from src.main import app


def _generate_key_pair():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()
    return private_pem, public_pem


@pytest.fixture
def rsa_key_pair():
    """Generate a fresh RSA key pair per test."""
    return _generate_key_pair()


def test_create_and_decode_access_token(rsa_key_pair):
    private_key, public_key = rsa_key_pair
    with patch("src.auth.jwt.settings") as mock_settings:
        mock_settings.jwt_private_key = private_key
        mock_settings.jwt_public_key = public_key
        payload = {"sub": "user-123", "tenant_id": "tenant-abc", "roles": ["developer"]}
        token = create_access_token(payload, expires_delta=timedelta(minutes=15))
        decoded = decode_access_token(token)
        assert decoded["sub"] == "user-123"
        assert decoded["tenant_id"] == "tenant-abc"
        assert decoded["roles"] == ["developer"]


def test_expired_token_raises(rsa_key_pair):
    private_key, public_key = rsa_key_pair
    with patch("src.auth.jwt.settings") as mock_settings:
        mock_settings.jwt_private_key = private_key
        mock_settings.jwt_public_key = public_key
        payload = {"sub": "user-123", "tenant_id": "tenant-abc", "roles": []}
        token = create_access_token(payload, expires_delta=timedelta(seconds=-1))
        with pytest.raises(ValueError, match="expired"):
            decode_access_token(token)


def test_token_signed_with_wrong_key_raises(rsa_key_pair):
    _, public_key = rsa_key_pair
    other_private_key, _ = _generate_key_pair()
    with patch("src.auth.jwt.settings") as mock_settings:
        mock_settings.jwt_private_key = other_private_key
        mock_settings.jwt_public_key = public_key
        payload = {"sub": "user-123", "tenant_id": "tenant-abc", "roles": []}
        token = create_access_token(payload, expires_delta=timedelta(minutes=15))
        with pytest.raises(ValueError, match="Invalid token"):
            decode_access_token(token)


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
