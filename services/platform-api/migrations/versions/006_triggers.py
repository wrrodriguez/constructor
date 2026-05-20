# migrations/versions/006_triggers.py
from alembic import op
import sqlalchemy as sa
import uuid as _uuid

revision = "c3d4e5f6a7b8"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add trigger_type to process_executions (default "manual" for existing rows)
    op.add_column(
        "process_executions",
        sa.Column("trigger_type", sa.String(20), nullable=False, server_default="manual"),
    )

    # webhook_configs table
    op.create_table(
        "webhook_configs",
        sa.Column("id", sa.UUID(), nullable=False, default=_uuid.uuid4),
        sa.Column("tenant_id", sa.UUID(), nullable=False),
        sa.Column("workflow_id", sa.UUID(), nullable=False),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("secret", sa.Text(), nullable=False),
        sa.Column("payload_mapping", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"]),
        sa.ForeignKeyConstraint(["workflow_id"], ["process_definitions.id"]),
        sa.PrimaryKeyConstraint("id"),
    )

    op.execute("ALTER TABLE webhook_configs ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE webhook_configs FORCE ROW LEVEL SECURITY")
    op.execute("""
        CREATE POLICY tenant_isolation ON webhook_configs
        USING (tenant_id = current_setting('app.current_tenant', TRUE)::uuid)
    """)


def downgrade() -> None:
    op.execute("DROP POLICY IF EXISTS tenant_isolation ON webhook_configs")
    op.execute("ALTER TABLE webhook_configs DISABLE ROW LEVEL SECURITY")
    op.drop_table("webhook_configs")
    op.drop_column("process_executions", "trigger_type")
