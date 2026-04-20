"""users + user_sessions + tenant_id columns

Revision ID: 0002_users_and_tenant
Revises: 0001_baseline
Create Date: 2026-04-19

Introduces real user accounts, signed-cookie sessions backed by a DB row,
and ``tenant_id`` columns on ``applications`` and ``users`` so multi-tenant
support is a config flip, not a future migration.

The shared ``PORTAL_PASSWORD`` auth path stays operational until an OWNER
user is created (see ``portal.bootstrap_owner_from_env``).
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "0002_users_and_tenant"
down_revision = "0001_baseline"
branch_labels = None
depends_on = None


def _has_table(name: str) -> bool:
    bind = op.get_bind()
    return name in inspect(bind).get_table_names()


def _has_column(table: str, column: str) -> bool:
    bind = op.get_bind()
    return column in {c["name"] for c in inspect(bind).get_columns(table)}


def upgrade() -> None:
    # ---- users -----------------------------------------------------------
    if not _has_table("users"):
        op.create_table(
            "users",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("email", sa.Text, nullable=False, unique=True),
            sa.Column("name", sa.Text, nullable=True),
            sa.Column("password_hash", sa.Text, nullable=False),
            sa.Column(
                "role",
                sa.Text,
                nullable=False,
                server_default=sa.text("'VIEWER'"),
            ),
            sa.Column(
                "tenant_id",
                sa.Text,
                nullable=False,
                server_default=sa.text("'global'"),
            ),
            sa.Column(
                "active",
                sa.Boolean,
                nullable=False,
                server_default=sa.text("true"),
            ),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            ),
            sa.Column("last_login_at", sa.DateTime(timezone=True), nullable=True),
        )
        op.create_index("idx_users_tenant", "users", ["tenant_id"])

    # ---- user_sessions ---------------------------------------------------
    if not _has_table("user_sessions"):
        op.create_table(
            "user_sessions",
            sa.Column("id", sa.Text, primary_key=True),
            sa.Column(
                "user_id",
                sa.Integer,
                sa.ForeignKey("users.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                nullable=False,
                server_default=sa.func.now(),
            ),
            sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("user_agent", sa.Text, nullable=True),
            sa.Column("ip", sa.Text, nullable=True),
        )
        op.create_index("idx_sessions_user", "user_sessions", ["user_id"])
        op.create_index("idx_sessions_expires", "user_sessions", ["expires_at"])

    # ---- tenant_id on applications --------------------------------------
    if not _has_column("applications", "tenant_id"):
        op.add_column(
            "applications",
            sa.Column(
                "tenant_id",
                sa.Text,
                nullable=False,
                server_default=sa.text("'global'"),
            ),
        )
        op.create_index("idx_applications_tenant", "applications", ["tenant_id"])


def downgrade() -> None:
    if _has_table("user_sessions"):
        op.drop_table("user_sessions")
    if _has_table("users"):
        op.drop_table("users")
    if _has_column("applications", "tenant_id"):
        try:
            op.drop_index("idx_applications_tenant", table_name="applications")
        except Exception:
            pass
        op.drop_column("applications", "tenant_id")
