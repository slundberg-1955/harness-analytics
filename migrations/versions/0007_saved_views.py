"""saved_views table for per-user filter snapshots

Revision ID: 0007_saved_views
Revises: 0006_user_manager
Create Date: 2026-04-22

Adds ``saved_views`` so a user can name + persist a combination of inbox /
portfolio filters and reload them later. ``surface`` is namespaced so we can
add saved views for the portfolio explorer or the matter timeline without a
new schema (M12).
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "0007_saved_views"
down_revision = "0006_user_manager"
branch_labels = None
depends_on = None


def _has_table(name: str) -> bool:
    return name in inspect(op.get_bind()).get_table_names()


def upgrade() -> None:
    if _has_table("saved_views"):
        return
    op.create_table(
        "saved_views",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "user_id",
            sa.Integer,
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("surface", sa.Text, nullable=False),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("params_json", sa.dialects.postgresql.JSONB, nullable=False),
        sa.Column(
            "is_default",
            sa.Boolean,
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint(
            "user_id", "surface", "name", name="uq_saved_views_user_surface_name"
        ),
    )
    op.create_index(
        "idx_saved_views_user_surface", "saved_views", ["user_id", "surface"]
    )


def downgrade() -> None:
    if _has_table("saved_views"):
        op.drop_table("saved_views")
