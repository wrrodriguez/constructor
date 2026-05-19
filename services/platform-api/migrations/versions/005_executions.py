# migrations/versions/005_executions.py
from alembic import op
import sqlalchemy as sa
import uuid as _uuid

revision = "a1b2c3d4e5f6"
down_revision = "2960e743b868"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add current_step_id to process_executions
    op.add_column(
        "process_executions",
        sa.Column("current_step_id", sa.String(255), nullable=True),
    )

    # Create agent_execution_logs table
    op.create_table(
        "agent_execution_logs",
        sa.Column("id", sa.UUID(), nullable=False, default=_uuid.uuid4),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("execution_id", sa.UUID(), nullable=False),
        sa.Column("step_id", sa.String(255), nullable=False),
        sa.Column("task_id", sa.UUID(), nullable=False),
        sa.Column("model_used", sa.String(100), nullable=False),
        sa.Column("tokens_used", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("iterations", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"]),
        sa.ForeignKeyConstraint(["execution_id"], ["process_executions.id"]),
        sa.PrimaryKeyConstraint("id"),
    )

    # RLS para agent_execution_logs
    op.execute("ALTER TABLE agent_execution_logs ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE agent_execution_logs FORCE ROW LEVEL SECURITY")
    op.execute("""
        CREATE POLICY tenant_isolation ON agent_execution_logs
        USING (tenant_id = current_setting('app.current_tenant', TRUE)::uuid)
    """)


def downgrade() -> None:
    op.execute("DROP POLICY IF EXISTS tenant_isolation ON agent_execution_logs")
    op.execute("ALTER TABLE agent_execution_logs DISABLE ROW LEVEL SECURITY")
    op.drop_table("agent_execution_logs")
    op.drop_column("process_executions", "current_step_id")
