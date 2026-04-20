"""computed_deadlines + deadline_events + view extension

Revision ID: 0004_computed_deadlines
Revises: 0003_ifw_rules
Create Date: 2026-04-19
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "0004_computed_deadlines"
down_revision = "0003_ifw_rules"
branch_labels = None
depends_on = None


def _has_table(name: str) -> bool:
    return name in inspect(op.get_bind()).get_table_names()


def upgrade() -> None:
    if not _has_table("computed_deadlines"):
        op.create_table(
            "computed_deadlines",
            sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
            sa.Column(
                "application_id",
                sa.Integer,
                sa.ForeignKey("applications.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column(
                "rule_id",
                sa.Integer,
                sa.ForeignKey("ifw_rules.id"),
                nullable=False,
            ),
            sa.Column(
                "trigger_event_id",
                sa.Integer,
                sa.ForeignKey("prosecution_events.id", ondelete="SET NULL"),
            ),
            sa.Column(
                "trigger_document_id",
                sa.Integer,
                sa.ForeignKey("file_wrapper_documents.id", ondelete="SET NULL"),
            ),
            sa.Column("trigger_date", sa.Date, nullable=False),
            sa.Column("trigger_source", sa.Text, nullable=False),
            sa.Column("ssp_date", sa.Date),
            sa.Column("statutory_bar_date", sa.Date),
            sa.Column("primary_date", sa.Date, nullable=False),
            sa.Column("primary_label", sa.Text, nullable=False),
            sa.Column("rows_json", sa.dialects.postgresql.JSONB, nullable=False),
            sa.Column("window_open_date", sa.Date),
            sa.Column("grace_end_date", sa.Date),
            sa.Column("ids_phases_json", sa.dialects.postgresql.JSONB),
            sa.Column(
                "status",
                sa.Text,
                nullable=False,
                server_default=sa.text("'OPEN'"),
            ),
            sa.Column(
                "completed_event_id",
                sa.Integer,
                sa.ForeignKey("prosecution_events.id", ondelete="SET NULL"),
            ),
            sa.Column("completed_at", sa.DateTime(timezone=True)),
            sa.Column(
                "superseded_by",
                sa.BigInteger,
                sa.ForeignKey("computed_deadlines.id", ondelete="SET NULL"),
            ),
            sa.Column(
                "assigned_user_id",
                sa.Integer,
                sa.ForeignKey("users.id", ondelete="SET NULL"),
            ),
            sa.Column("snoozed_until", sa.Date),
            sa.Column("notes", sa.Text),
            sa.Column("warnings", sa.dialects.postgresql.ARRAY(sa.Text)),
            sa.Column("severity", sa.Text),
            sa.Column(
                "tenant_id",
                sa.Text,
                nullable=False,
                server_default=sa.text("'global'"),
            ),
            sa.Column(
                "computed_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
            ),
        )
        op.create_index("idx_cd_app", "computed_deadlines", ["application_id"])
        op.create_index(
            "idx_cd_primary_open",
            "computed_deadlines",
            ["primary_date"],
            postgresql_where=sa.text("status = 'OPEN'"),
        )
        op.create_index(
            "idx_cd_assigned_open",
            "computed_deadlines",
            ["assigned_user_id", "primary_date"],
            postgresql_where=sa.text("status = 'OPEN'"),
        )
        op.create_index("idx_cd_status", "computed_deadlines", ["status", "primary_date"])
        op.create_index("idx_cd_tenant_open", "computed_deadlines", ["tenant_id", "primary_date"], postgresql_where=sa.text("status = 'OPEN'"))

    if not _has_table("deadline_events"):
        op.create_table(
            "deadline_events",
            sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
            sa.Column(
                "deadline_id",
                sa.BigInteger,
                sa.ForeignKey("computed_deadlines.id", ondelete="CASCADE"),
                nullable=False,
            ),
            sa.Column(
                "user_id",
                sa.Integer,
                sa.ForeignKey("users.id", ondelete="SET NULL"),
            ),
            sa.Column("action", sa.Text, nullable=False),
            sa.Column("payload_json", sa.dialects.postgresql.JSONB),
            sa.Column(
                "occurred_at",
                sa.DateTime(timezone=True),
                server_default=sa.func.now(),
            ),
        )
        op.create_index("idx_de_deadline", "deadline_events", ["deadline_id"])

    if not _has_table("supersession_map"):
        op.create_table(
            "supersession_map",
            sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
            sa.Column("prev_kind", sa.Text, nullable=False),
            sa.Column("new_kind", sa.Text, nullable=False),
            sa.Column(
                "tenant_id",
                sa.Text,
                nullable=False,
                server_default=sa.text("'global'"),
            ),
            sa.UniqueConstraint(
                "tenant_id", "prev_kind", "new_kind", name="uq_supersession"
            ),
        )


def downgrade() -> None:
    for t in ("supersession_map", "deadline_events", "computed_deadlines"):
        if _has_table(t):
            op.drop_table(t)
