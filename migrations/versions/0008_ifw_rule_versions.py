"""ifw_rule_versions for rule edit history

Revision ID: 0008_ifw_rule_versions
Revises: 0007_saved_views
Create Date: 2026-04-22

Snapshots the pre-edit state of every ``ifw_rules`` row updated through the
admin API so changes can be reviewed and reverted (M15).
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "0008_ifw_rule_versions"
down_revision = "0007_saved_views"
branch_labels = None
depends_on = None


def _has_table(name: str) -> bool:
    return name in inspect(op.get_bind()).get_table_names()


def upgrade() -> None:
    if _has_table("ifw_rule_versions"):
        return
    op.create_table(
        "ifw_rule_versions",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "rule_id",
            sa.Integer,
            sa.ForeignKey("ifw_rules.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("version", sa.Integer, nullable=False),
        sa.Column("snapshot_json", sa.dialects.postgresql.JSONB, nullable=False),
        sa.Column(
            "edited_by_user_id",
            sa.Integer,
            sa.ForeignKey("users.id", ondelete="SET NULL"),
        ),
        sa.Column(
            "edited_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("rule_id", "version", name="uq_ifw_rule_versions_rule_version"),
    )
    op.create_index(
        "idx_ifw_rule_versions_rule",
        "ifw_rule_versions",
        ["rule_id", sa.text("version DESC")],
    )


def downgrade() -> None:
    if _has_table("ifw_rule_versions"):
        op.drop_table("ifw_rule_versions")
