# src/executions/models.py
import uuid
from datetime import datetime
from sqlalchemy import String, Integer, ForeignKey, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column
from src.database import Base


class AgentExecutionLog(Base):
    __tablename__ = "agent_execution_logs"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("tenants.id"), nullable=False)
    execution_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("process_executions.id"), nullable=False
    )
    step_id: Mapped[str] = mapped_column(String(255), nullable=False)
    task_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    model_used: Mapped[str] = mapped_column(String(100), nullable=False)
    tokens_used: Mapped[int] = mapped_column(Integer, default=0)
    iterations: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
