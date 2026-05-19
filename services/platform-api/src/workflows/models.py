# src/workflows/models.py
import uuid
from datetime import datetime
from sqlalchemy import String, JSON, ForeignKey, DateTime, Integer, func
from sqlalchemy.orm import Mapped, mapped_column
from src.database import Base


class ProcessDefinition(Base):
    __tablename__ = "process_definitions"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    trigger_config: Mapped[dict] = mapped_column(JSON, nullable=False)
    steps: Mapped[list] = mapped_column(JSON, nullable=False)
    on_error: Mapped[str] = mapped_column(String(20), default="stop")
    version: Mapped[int] = mapped_column(Integer, default=1)
    created_by: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class ProcessExecution(Base):
    __tablename__ = "process_executions"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    process_definition_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("process_definitions.id"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(30), default="pending")
    context: Mapped[dict] = mapped_column(JSON, default=dict)
    current_step_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    triggered_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
