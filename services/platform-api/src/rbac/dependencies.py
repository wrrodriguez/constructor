# src/rbac/dependencies.py
from fastapi import Depends, HTTPException, status

from src.auth.dependencies import get_current_user
from src.auth.schemas import CurrentUser
from src.rbac.permissions import Permission, has_permission


def require_permission(permission: Permission):
    """Dependency factory. Usage: Depends(require_permission(Permission.WORKFLOW_CREATE))"""
    def _check(current_user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
        if not has_permission(current_user.roles, permission):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission required: {permission}",
            )
        return current_user
    return _check
