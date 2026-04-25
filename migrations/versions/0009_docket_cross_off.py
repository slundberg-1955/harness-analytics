"""Docket cross-off / NAR feature

Revision ID: 0009_docket_cross_off
Revises: 0008_ifw_rule_versions
Create Date: 2026-04-24

Adds the schema bits needed for rule-driven docket auto-close + the new NAR
("No Action Required") lifecycle state:

* ``ifw_rules.variant_key`` — disambiguates multiple due-item variants that
  share a triggering IFW code (e.g. ``CTNF`` → "Non-Final OA Response" vs.
  "Non-Final OA with RR Response"). Default ``''`` keeps backwards-compat.
* ``ifw_rules.close_complete_codes`` / ``close_nar_codes`` — pattern arrays
  consumed by the materializer's auto-close pass.
* ``computed_deadlines.closed_by_ifw_document_id`` /
  ``closed_by_rule_pattern`` / ``closed_disposition`` — audit columns
  populated by the auto-close pass and the manual ``nar`` action.
* ``idx_deadlines_status_tenant`` — composite index that backs the inbox
  status filter (default ``status='open'``) without scanning the whole
  ``computed_deadlines`` table.

Idempotent: every alter is gated by an information-schema check using the
same ``has_column``/``has_table`` pattern as 0008.
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision = "0009_docket_cross_off"
down_revision = "0008_ifw_rule_versions"
branch_labels = None
depends_on = None


def _has_table(name: str) -> bool:
    return name in inspect(op.get_bind()).get_table_names()


def _has_column(table: str, column: str) -> bool:
    if not _has_table(table):
        return False
    cols = {c["name"] for c in inspect(op.get_bind()).get_columns(table)}
    return column in cols


def _has_constraint(table: str, name: str) -> bool:
    if not _has_table(table):
        return False
    insp = inspect(op.get_bind())
    uniques = {u["name"] for u in insp.get_unique_constraints(table)}
    return name in uniques


def _has_index(table: str, name: str) -> bool:
    if not _has_table(table):
        return False
    indexes = {i["name"] for i in inspect(op.get_bind()).get_indexes(table)}
    return name in indexes


def upgrade() -> None:
    # ------------------------------------------------------------------
    # ifw_rules: variant_key + close_*_codes
    # ------------------------------------------------------------------
    if _has_table("ifw_rules"):
        if not _has_column("ifw_rules", "variant_key"):
            op.add_column(
                "ifw_rules",
                sa.Column(
                    "variant_key",
                    sa.Text(),
                    nullable=False,
                    server_default="",
                ),
            )
        if not _has_column("ifw_rules", "close_complete_codes"):
            op.add_column(
                "ifw_rules",
                sa.Column(
                    "close_complete_codes",
                    sa.dialects.postgresql.ARRAY(sa.Text()),
                    nullable=False,
                    server_default="{}",
                ),
            )
        if not _has_column("ifw_rules", "close_nar_codes"):
            op.add_column(
                "ifw_rules",
                sa.Column(
                    "close_nar_codes",
                    sa.dialects.postgresql.ARRAY(sa.Text()),
                    nullable=False,
                    server_default="{}",
                ),
            )

        # Swap the (tenant_id, code) unique key for (tenant_id, code, variant_key).
        if _has_constraint("ifw_rules", "uq_ifw_rules_tenant_code"):
            op.drop_constraint(
                "uq_ifw_rules_tenant_code", "ifw_rules", type_="unique"
            )
        if not _has_constraint("ifw_rules", "uq_ifw_rules_tenant_code_variant"):
            op.create_unique_constraint(
                "uq_ifw_rules_tenant_code_variant",
                "ifw_rules",
                ["tenant_id", "code", "variant_key"],
            )

    # ------------------------------------------------------------------
    # computed_deadlines: close audit columns
    # ------------------------------------------------------------------
    if _has_table("computed_deadlines"):
        if not _has_column("computed_deadlines", "closed_by_ifw_document_id"):
            op.add_column(
                "computed_deadlines",
                sa.Column(
                    "closed_by_ifw_document_id",
                    sa.BigInteger(),
                    sa.ForeignKey(
                        "file_wrapper_documents.id", ondelete="SET NULL"
                    ),
                    nullable=True,
                ),
            )
        if not _has_column("computed_deadlines", "closed_by_rule_pattern"):
            op.add_column(
                "computed_deadlines",
                sa.Column("closed_by_rule_pattern", sa.Text(), nullable=True),
            )
        if not _has_column("computed_deadlines", "closed_disposition"):
            op.add_column(
                "computed_deadlines",
                sa.Column("closed_disposition", sa.Text(), nullable=True),
            )

        if not _has_index("computed_deadlines", "idx_deadlines_status_tenant"):
            op.create_index(
                "idx_deadlines_status_tenant",
                "computed_deadlines",
                ["tenant_id", "status", "primary_date"],
            )


def downgrade() -> None:
    if _has_table("computed_deadlines"):
        if _has_index("computed_deadlines", "idx_deadlines_status_tenant"):
            op.drop_index(
                "idx_deadlines_status_tenant", table_name="computed_deadlines"
            )
        for col in (
            "closed_disposition",
            "closed_by_rule_pattern",
            "closed_by_ifw_document_id",
        ):
            if _has_column("computed_deadlines", col):
                op.drop_column("computed_deadlines", col)

    if _has_table("ifw_rules"):
        if _has_constraint("ifw_rules", "uq_ifw_rules_tenant_code_variant"):
            op.drop_constraint(
                "uq_ifw_rules_tenant_code_variant",
                "ifw_rules",
                type_="unique",
            )
        if not _has_constraint("ifw_rules", "uq_ifw_rules_tenant_code"):
            op.create_unique_constraint(
                "uq_ifw_rules_tenant_code", "ifw_rules", ["tenant_id", "code"]
            )
        for col in ("close_nar_codes", "close_complete_codes", "variant_key"):
            if _has_column("ifw_rules", col):
                op.drop_column("ifw_rules", col)
