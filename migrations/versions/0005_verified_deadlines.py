"""verified_deadlines + per-user ICS feed token

Revision ID: 0005_verified_deadlines
Revises: 0004_computed_deadlines
Create Date: 2026-04-19
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "0005_verified_deadlines"
down_revision = "0004_computed_deadlines"
branch_labels = None
depends_on = None


def _has_table(name: str) -> bool:
    return name in inspect(op.get_bind()).get_table_names()


def _has_column(table: str, name: str) -> bool:
    cols = {c["name"] for c in inspect(op.get_bind()).get_columns(table)}
    return name in cols


def upgrade() -> None:
    if not _has_table("verified_deadlines"):
        op.create_table(
            "verified_deadlines",
            sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
            sa.Column(
                "deadline_id",
                sa.BigInteger,
                sa.ForeignKey("computed_deadlines.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column(
                "verified_by_user_id",
                sa.Integer,
                sa.ForeignKey("users.id", ondelete="SET NULL"),
            ),
            sa.Column(
                "verified_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
                nullable=False,
            ),
            sa.Column("verified_date", sa.Date, nullable=False),
            sa.Column("source", sa.Text, nullable=False, server_default=sa.text("'manual'")),
            sa.Column("note", sa.Text),
            sa.UniqueConstraint("deadline_id", name="uq_verified_deadline"),
        )
        op.create_index(
            "idx_vd_user", "verified_deadlines", ["verified_by_user_id"]
        )

    # Per-user ICS feed token. Stored as opaque text so we can rotate without
    # touching the rest of the user row.
    if _has_table("users") and not _has_column("users", "ics_token"):
        op.add_column(
            "users",
            sa.Column("ics_token", sa.Text, nullable=True, unique=True),
        )


def downgrade() -> None:
    if _has_table("verified_deadlines"):
        op.drop_table("verified_deadlines")
    if _has_table("users") and _has_column("users", "ics_token"):
        op.drop_column("users", "ics_token")
