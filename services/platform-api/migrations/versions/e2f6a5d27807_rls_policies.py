"""rls_policies

Revision ID: e2f6a5d27807
Revises: f1f69d2642ac
Create Date: 2026-05-18

"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'e2f6a5d27807'
down_revision: Union[str, Sequence[str], None] = 'f1f69d2642ac'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

TABLES_WITH_RLS = ["users", "user_roles", "refresh_tokens"]


def upgrade() -> None:
    # Enable RLS on tenant-scoped tables
    for table in TABLES_WITH_RLS:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
        op.execute(f"""
            CREATE POLICY tenant_isolation ON {table}
            USING (tenant_id = current_setting('app.current_tenant', TRUE)::uuid)
        """)

    # Seed system roles
    op.execute("""
        INSERT INTO roles (id, name, description) VALUES
        (gen_random_uuid(), 'super_admin', 'kimn internal super admin'),
        (gen_random_uuid(), 'tenant_admin', 'Full access within tenant'),
        (gen_random_uuid(), 'developer', 'Create and manage workflows and agents'),
        (gen_random_uuid(), 'project_manager', 'View and trigger workflows'),
        (gen_random_uuid(), 'analyst', 'View and approve workflows'),
        (gen_random_uuid(), 'viewer', 'Read-only access')
        ON CONFLICT (name) DO NOTHING
    """)


def downgrade() -> None:
    for table in TABLES_WITH_RLS:
        op.execute(f"DROP POLICY IF EXISTS tenant_isolation ON {table}")
        op.execute(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY")
    op.execute("DELETE FROM roles WHERE name IN ('super_admin', 'tenant_admin', 'developer', 'project_manager', 'analyst', 'viewer')")
