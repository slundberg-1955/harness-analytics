"""baseline (no-op stamp)

Revision ID: 0001_baseline
Revises:
Create Date: 2026-04-19

The legacy idempotent ``ensure_schema_migrations()`` (in
:mod:`harness_analytics.schema_migrations`) still owns the existing tables and
the ``patent_applications`` view. This baseline revision is intentionally a
no-op so that existing Railway databases (which already have the full schema)
can be ``alembic stamp 0001_baseline`` and start using Alembic for *new*
schema additions only. New tables introduced by the Prosecution Timeline
work get their own revisions on top of this baseline.
"""
from __future__ import annotations

revision = "0001_baseline"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
