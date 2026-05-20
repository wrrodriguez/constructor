import pytest


def test_webhook_config_model_has_expected_fields():
    from src.webhooks.models import WebhookConfig
    cols = {c.key for c in WebhookConfig.__table__.columns}
    assert {"id", "tenant_id", "workflow_id", "name", "secret",
            "payload_mapping", "is_active", "created_at"} <= cols
