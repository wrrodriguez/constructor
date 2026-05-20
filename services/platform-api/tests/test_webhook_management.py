import pytest
import hmac as _hmac
import hashlib
import secrets


def test_webhook_config_model_has_expected_fields():
    from src.webhooks.models import WebhookConfig
    cols = {c.key for c in WebhookConfig.__table__.columns}
    assert {"id", "tenant_id", "workflow_id", "name", "secret",
            "payload_mapping", "is_active", "created_at"} <= cols


def test_verify_signature_valid():
    from src.webhooks.service import verify_signature
    body = b'{"data": {"id": "abc"}}'
    secret = "mysecret"
    sig = "sha256=" + _hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    assert verify_signature(secret, body, sig) is True


def test_verify_signature_invalid():
    from src.webhooks.service import verify_signature
    body = b'{"data": {"id": "abc"}}'
    assert verify_signature("right", body, "sha256=wronghex") is False


def test_apply_payload_mapping_extracts_fields():
    from src.webhooks.service import apply_payload_mapping
    body = {"data": {"id": "policy-123", "status": "active"}}
    mapping = {"policy_id": "data.id", "policy_status": "data.status"}
    result = apply_payload_mapping(body, mapping)
    assert result == {"policy_id": "policy-123", "policy_status": "active"}


def test_apply_payload_mapping_missing_field_is_omitted():
    from src.webhooks.service import apply_payload_mapping
    body = {"data": {}}
    mapping = {"policy_id": "data.id"}
    result = apply_payload_mapping(body, mapping)
    assert result == {}


def test_apply_payload_mapping_empty_mapping():
    from src.webhooks.service import apply_payload_mapping
    assert apply_payload_mapping({"any": "data"}, {}) == {}
