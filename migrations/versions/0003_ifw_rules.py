"""ifw_rules

Revision ID: 0003_ifw_rules
Revises: 0002_users_and_tenant
Create Date: 2026-04-19
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "0003_ifw_rules"
down_revision = "0002_users_and_tenant"
branch_labels = None
depends_on = None


def _has_table(name: str) -> bool:
    return name in inspect(op.get_bind()).get_table_names()


def upgrade() -> None:
    if not _has_table("ifw_rules"):
        op.create_table(
            "ifw_rules",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column(
                "tenant_id",
                sa.Text,
                nullable=False,
                server_default=sa.text("'global'"),
            ),
            sa.Column("code", sa.Text, nullable=False),
            sa.Column("aliases", sa.dialects.postgresql.ARRAY(sa.Text), nullable=True),
            sa.Column("description", sa.Text, nullable=False),
            sa.Column("kind", sa.Text, nullable=False),
            sa.Column("ssp_months", sa.Integer, nullable=True),
            sa.Column("max_months", sa.Integer, nullable=True),
            sa.Column("due_months_from_grant", sa.Integer, nullable=True),
            sa.Column("grace_months_from_grant", sa.Integer, nullable=True),
            sa.Column("from_filing_months", sa.Integer, nullable=True),
            sa.Column("from_priority_months", sa.Integer, nullable=True),
            sa.Column("base_months_from_priority", sa.Integer, nullable=True),
            sa.Column("late_months_from_priority", sa.Integer, nullable=True),
            sa.Column(
                "extendable",
                sa.Boolean,
                nullable=False,
                server_default=sa.text("false"),
            ),
            sa.Column("trigger_label", sa.Text, nullable=False),
            sa.Column(
                "user_note", sa.Text, nullable=False, server_default=sa.text("''")
            ),
            sa.Column("authority", sa.Text, nullable=False),
            sa.Column("warnings", sa.dialects.postgresql.ARRAY(sa.Text), nullable=True),
            sa.Column("priority_tier", sa.Text, nullable=True),
            sa.Column(
                "patent_type_applicability",
                sa.dialects.postgresql.ARRAY(sa.Text),
                nullable=False,
                server_default=sa.text(
                    "ARRAY['UTILITY','DESIGN','PLANT','REISSUE','REEXAM']"
                ),
            ),
            sa.Column(
                "active", sa.Boolean, nullable=False, server_default=sa.text("true")
            ),
            sa.Column(
                "created_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
            ),
            sa.Column(
                "updated_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
            ),
            sa.UniqueConstraint(
                "tenant_id", "code", name="uq_ifw_rules_tenant_code"
            ),
        )
        op.create_index(
            "idx_ifw_rules_code_active",
            "ifw_rules",
            ["code"],
            postgresql_where=sa.text("active = TRUE"),
        )

    if not _has_table("unmapped_ifw_codes"):
        op.create_table(
            "unmapped_ifw_codes",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column(
                "tenant_id",
                sa.Text,
                nullable=False,
                server_default=sa.text("'global'"),
            ),
            sa.Column("code", sa.Text, nullable=False),
            sa.Column("count", sa.Integer, nullable=False, server_default=sa.text("0")),
            sa.Column(
                "first_seen",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
            ),
            sa.Column(
                "last_seen",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
            ),
            sa.UniqueConstraint(
                "tenant_id", "code", name="uq_unmapped_ifw_tenant_code"
            ),
        )


def downgrade() -> None:
    if _has_table("unmapped_ifw_codes"):
        op.drop_table("unmapped_ifw_codes")
    if _has_table("ifw_rules"):
        op.drop_table("ifw_rules")
