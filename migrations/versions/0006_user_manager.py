"""users.manager_user_id column for team-view inbox

Revision ID: 0006_user_manager
Revises: 0005_verified_deadlines
Create Date: 2026-04-22

Adds ``manager_user_id`` to ``users`` so a supervising attorney can be linked
to direct reports. Powers the ``assignee=team`` branch on
``GET /portal/api/actions/inbox`` (M11).
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "0006_user_manager"
down_revision = "0005_verified_deadlines"
branch_labels = None
depends_on = None


def _has_table(name: str) -> bool:
    return name in inspect(op.get_bind()).get_table_names()


def _has_column(table: str, name: str) -> bool:
    cols = {c["name"] for c in inspect(op.get_bind()).get_columns(table)}
    return name in cols


def upgrade() -> None:
    if _has_table("users") and not _has_column("users", "manager_user_id"):
        op.add_column(
            "users",
            sa.Column(
                "manager_user_id",
                sa.Integer,
                sa.ForeignKey("users.id", ondelete="SET NULL"),
                nullable=True,
            ),
        )
        op.create_index(
            "idx_users_manager", "users", ["manager_user_id"]
        )


def downgrade() -> None:
    if _has_table("users") and _has_column("users", "manager_user_id"):
        try:
            op.drop_index("idx_users_manager", table_name="users")
        except Exception:
            pass
        op.drop_column("users", "manager_user_id")
