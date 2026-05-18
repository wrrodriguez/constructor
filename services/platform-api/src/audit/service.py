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
    """Append an audit log entry. Does NOT commit — caller decides when to commit."""
    db.add(AuditLog(
        tenant_id=tenant_id,
        user_id=user_id,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        log_metadata=metadata or {},
    ))
