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
