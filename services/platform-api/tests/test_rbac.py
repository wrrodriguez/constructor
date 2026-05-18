import pytest
from src.rbac.permissions import Permission, ROLE_PERMISSIONS, has_permission


def test_developer_can_create_workflows():
    assert Permission.WORKFLOW_CREATE in ROLE_PERMISSIONS["developer"]


def test_viewer_cannot_create_workflows():
    assert Permission.WORKFLOW_CREATE not in ROLE_PERMISSIONS["viewer"]


def test_tenant_admin_has_all_tenant_permissions():
    admin_perms = ROLE_PERMISSIONS["tenant_admin"]
    assert Permission.WORKFLOW_CREATE in admin_perms
    assert Permission.USER_MANAGE in admin_perms
    assert Permission.TENANT_CONFIG in admin_perms


def test_has_permission_returns_true_for_valid_role():
    assert has_permission(["developer"], Permission.WORKFLOW_CREATE) is True


def test_has_permission_returns_false_for_unknown_role():
    assert has_permission(["nonexistent_role"], Permission.WORKFLOW_CREATE) is False


def test_has_permission_grants_on_any_matching_role():
    # viewer + developer -> developer grants WORKFLOW_CREATE
    assert has_permission(["viewer", "developer"], Permission.WORKFLOW_CREATE) is True


def test_has_permission_super_admin_has_all_permissions():
    for perm in Permission:
        assert has_permission(["super_admin"], perm) is True


def test_analyst_has_audit_read():
    assert Permission.AUDIT_READ in ROLE_PERMISSIONS["analyst"]
