# src/rbac/permissions.py
from enum import StrEnum


class Permission(StrEnum):
    # Workflows
    WORKFLOW_CREATE = "workflow:create"
    WORKFLOW_READ = "workflow:read"
    WORKFLOW_UPDATE = "workflow:update"
    WORKFLOW_DELETE = "workflow:delete"
    WORKFLOW_TRIGGER = "workflow:trigger"
    # Agents
    AGENT_CREATE = "agent:create"
    AGENT_READ = "agent:read"
    AGENT_UPDATE = "agent:update"
    AGENT_DELETE = "agent:delete"
    # Users
    USER_MANAGE = "user:manage"
    USER_READ = "user:read"
    # Tenant
    TENANT_CONFIG = "tenant:config"
    # Approvals
    APPROVAL_MANAGE = "approval:manage"
    # Audit
    AUDIT_READ = "audit:read"
    # Reports
    REPORT_READ = "report:read"


ROLE_PERMISSIONS: dict[str, set[Permission]] = {
    "super_admin": set(Permission),
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
        Permission.WORKFLOW_READ, Permission.APPROVAL_MANAGE,
        Permission.AUDIT_READ, Permission.REPORT_READ,
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
