# migrations/versions/004_workflows.py
from alembic import op
import sqlalchemy as sa
import uuid as _uuid

revision = "2960e743b868"
down_revision = "8cf4fdbc5d3c"
branch_labels = None
depends_on = None

RLS_TABLES = ["process_definitions", "process_executions"]


def upgrade() -> None:
    op.create_table(
        "process_definitions",
        sa.Column("id", sa.UUID(), nullable=False, default=_uuid.uuid4),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("trigger_config", sa.JSON(), nullable=False),
        sa.Column("steps", sa.JSON(), nullable=False),
        sa.Column("on_error", sa.String(20), nullable=False, server_default="stop"),
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("created_by", sa.UUID(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"]),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "process_executions",
        sa.Column("id", sa.UUID(), nullable=False, default=_uuid.uuid4),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("process_definition_id", sa.UUID(), nullable=False),
        sa.Column("status", sa.String(30), nullable=False, server_default="pending"),
        sa.Column("context", sa.JSON(), nullable=True),
        sa.Column("triggered_by", sa.UUID(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"]),
        sa.ForeignKeyConstraint(["process_definition_id"], ["process_definitions.id"]),
        sa.ForeignKeyConstraint(["triggered_by"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
    )

    for table in RLS_TABLES:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
        op.execute(f"""
            CREATE POLICY tenant_isolation ON {table}
            USING (tenant_id = current_setting('app.current_tenant', TRUE)::uuid)
        """)


def downgrade() -> None:
    for table in reversed(RLS_TABLES):
        op.execute(f"DROP POLICY IF EXISTS tenant_isolation ON {table}")
    op.drop_table("process_executions")
    op.drop_table("process_definitions")
