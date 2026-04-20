"""Read/write helpers for the ``ifw_rules`` table.

Two responsibilities:

1. **Lookup**: ``get_rule(db, code, tenant_id)`` returns the effective rule for
   a tenant, falling back to the ``global`` row when no tenant override exists.
2. **Seeding**: ``seed_global_rules(db)`` upserts every row from
   ``timeline/data/ifw-rules.json`` under ``tenant_id='global'``. Idempotent —
   safe to run on every deploy and from the Arq ``seed_ifw_rules`` task.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from harness_analytics.models import IfwRule as IfwRuleRow
from harness_analytics.timeline.calculator import IfwRule

_RULES_JSON = Path(__file__).resolve().parent / "data" / "ifw-rules.json"


# ---------------------------------------------------------------------------
# JSON loading
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def load_seed_rules() -> list[dict]:
    with _RULES_JSON.open("r", encoding="utf-8") as fp:
        return list(json.load(fp).get("rules", []))


# ---------------------------------------------------------------------------
# Row → dataclass
# ---------------------------------------------------------------------------


def _row_to_rule(row: IfwRuleRow) -> IfwRule:
    return IfwRule(
        code=row.code,
        kind=row.kind,
        description=row.description,
        trigger_label=row.trigger_label,
        user_note=row.user_note or "",
        authority=row.authority,
        extendable=bool(row.extendable),
        aliases=tuple(row.aliases or ()),
        ssp_months=row.ssp_months,
        max_months=row.max_months,
        due_months_from_grant=row.due_months_from_grant,
        grace_months_from_grant=row.grace_months_from_grant,
        from_filing_months=row.from_filing_months,
        from_priority_months=row.from_priority_months,
        base_months_from_priority=row.base_months_from_priority,
        late_months_from_priority=row.late_months_from_priority,
        warnings=tuple(row.warnings or ()),
        priority_tier=row.priority_tier,
    )


# ---------------------------------------------------------------------------
# Lookup
# ---------------------------------------------------------------------------


def get_rule(
    db: Session, code: str, tenant_id: str = "global"
) -> Optional[IfwRule]:
    """Resolve a rule for ``code`` honoring tenant-level overrides.

    Looks up the tenant-specific row first; falls back to the ``global`` row.
    Aliases are matched in a second pass when the direct code lookup misses.
    """
    code = code.strip()
    if not code:
        return None

    # Direct match (tenant first, then global).
    for tid in (tenant_id, "global") if tenant_id != "global" else ("global",):
        row = db.scalar(
            select(IfwRuleRow).where(
                IfwRuleRow.tenant_id == tid,
                IfwRuleRow.code == code,
                IfwRuleRow.active.is_(True),
            )
        )
        if row is not None:
            return _row_to_rule(row)

    # Alias fallback — array contains comparison.
    for tid in (tenant_id, "global") if tenant_id != "global" else ("global",):
        row = db.scalar(
            select(IfwRuleRow).where(
                IfwRuleRow.tenant_id == tid,
                IfwRuleRow.aliases.contains([code]),
                IfwRuleRow.active.is_(True),
            )
        )
        if row is not None:
            return _row_to_rule(row)
    return None


def list_rules(db: Session, tenant_id: str = "global") -> list[IfwRuleRow]:
    """Return all active rules visible to a tenant (overrides shadow globals).

    For the rules-admin UI: globals first, then tenant overrides on top.
    """
    rows: dict[str, IfwRuleRow] = {}
    globals_q = db.scalars(
        select(IfwRuleRow).where(IfwRuleRow.tenant_id == "global")
    )
    for r in globals_q:
        rows[r.code] = r
    if tenant_id != "global":
        tenant_q = db.scalars(
            select(IfwRuleRow).where(IfwRuleRow.tenant_id == tenant_id)
        )
        for r in tenant_q:
            rows[r.code] = r
    return sorted(rows.values(), key=lambda r: r.code)


# ---------------------------------------------------------------------------
# Seeding
# ---------------------------------------------------------------------------

_FIELD_NAMES = {
    "ssp_months",
    "max_months",
    "due_months_from_grant",
    "grace_months_from_grant",
    "from_filing_months",
    "from_priority_months",
    "base_months_from_priority",
    "late_months_from_priority",
    "extendable",
    "trigger_label",
    "user_note",
    "authority",
    "warnings",
    "priority_tier",
    "patent_type_applicability",
    "active",
    "aliases",
    "description",
    "kind",
}


def _normalize_seed_row(row: dict) -> dict:
    """Apply defaults so JSON authors only need to specify what's interesting."""
    out = dict(row)
    out.setdefault("user_note", "")
    out.setdefault("warnings", [])
    out.setdefault("aliases", [])
    out.setdefault("active", True)
    out.setdefault("extendable", False)
    out.setdefault("trigger_label", "Trigger date")
    out.setdefault("authority", "USPTO")
    out.setdefault(
        "patent_type_applicability",
        ["UTILITY", "DESIGN", "PLANT", "REISSUE", "REEXAM"],
    )
    return out


def seed_global_rules(db: Session, tenant_id: str = "global") -> int:
    """Upsert every row from the JSON file under the given tenant.

    Returns the number of rows inserted or updated. Idempotent.
    """
    rules = load_seed_rules()
    n = 0
    now = datetime.now(timezone.utc)
    for raw in rules:
        data = _normalize_seed_row(raw)
        existing = db.scalar(
            select(IfwRuleRow).where(
                IfwRuleRow.tenant_id == tenant_id,
                IfwRuleRow.code == data["code"],
            )
        )
        if existing is None:
            db.add(
                IfwRuleRow(
                    tenant_id=tenant_id,
                    code=data["code"],
                    **{k: data.get(k) for k in _FIELD_NAMES if k in data},
                )
            )
            n += 1
        else:
            for k in _FIELD_NAMES:
                if k in data:
                    setattr(existing, k, data[k])
            existing.updated_at = now
            n += 1
    db.commit()
    return n
