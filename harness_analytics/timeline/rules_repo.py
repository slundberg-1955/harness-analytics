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
import logging
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from harness_analytics.models import IfwRule as IfwRuleRow
from harness_analytics.models import SupersessionMap as SupersessionMapRow
from harness_analytics.timeline.calculator import IfwRule

logger = logging.getLogger(__name__)

_RULES_JSON = Path(__file__).resolve().parent / "data" / "ifw-rules.json"
_SUPERSESSION_SEED_JSON = (
    Path(__file__).resolve().parent / "data" / "supersession_seed.json"
)
_DOCKET_CLOSE_SEED_JSON = (
    Path(__file__).resolve().parent / "data" / "docket_close_conditions.json"
)


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

    Also seeds the default ``supersession_map`` pairs (M13) when the caller
    is seeding the ``global`` tenant — this keeps the materializer's
    conservative supersession logic populated out of the box.
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
    if tenant_id == "global":
        try:
            seed_supersession_pairs(db, tenant_id="global")
        except Exception:  # noqa: BLE001
            # Don't let supersession seeding break a rule-seed run; the
            # materializer falls back to "no supersession" if the table is
            # empty, which is safe (just produces extra closed deadlines).
            pass
    return n


# ---------------------------------------------------------------------------
# M13: supersession-map seeding
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def load_supersession_seed() -> list[dict]:
    if not _SUPERSESSION_SEED_JSON.exists():
        return []
    with _SUPERSESSION_SEED_JSON.open("r", encoding="utf-8") as fp:
        data = json.load(fp)
    if isinstance(data, dict):
        return list(data.get("pairs", []))
    return list(data)


def seed_supersession_pairs(db: Session, tenant_id: str = "global") -> int:
    """Idempotent upsert of default ``(prev_kind, new_kind)`` supersession pairs.

    Returns the number of newly inserted rows. Existing rows are left in
    place so admin-edited tenant overrides aren't clobbered.
    """
    inserted = 0
    for raw in load_supersession_seed():
        prev_kind = (raw.get("prev_kind") or "").strip()
        new_kind = (raw.get("new_kind") or "").strip()
        if not prev_kind or not new_kind:
            continue
        existing = db.scalar(
            select(SupersessionMapRow).where(
                SupersessionMapRow.tenant_id == tenant_id,
                SupersessionMapRow.prev_kind == prev_kind,
                SupersessionMapRow.new_kind == new_kind,
            )
        )
        if existing is None:
            db.add(
                SupersessionMapRow(
                    tenant_id=tenant_id,
                    prev_kind=prev_kind,
                    new_kind=new_kind,
                )
            )
            inserted += 1
    if inserted:
        db.commit()
    return inserted


# ---------------------------------------------------------------------------
# 0009: docket cross-off / NAR seeding
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def load_docket_close_seed() -> list[dict]:
    """Load the bundled ``docket_close_conditions.json`` shipped with the repo.

    Empty if the file is missing — keeps tests + dev environments without the
    seed file from blowing up on import.
    """
    if not _DOCKET_CLOSE_SEED_JSON.exists():
        return []
    with _DOCKET_CLOSE_SEED_JSON.open("r", encoding="utf-8") as fp:
        data = json.load(fp)
    if isinstance(data, dict):
        return list(data.get("conditions", []))
    return list(data)


# Sentinel ``kind`` for rule rows that exist *only* to drive auto-close on a
# triggering code that the calculator doesn't materialize. The materializer's
# kind dispatch (``timeline/calculator.py``) silently returns an empty result
# for unrecognized kinds, so seeding under this sentinel is safe — the
# auto-close pass only reads ``close_*_codes`` regardless of ``kind``.
DOCKET_CLOSE_ONLY_KIND = "auto_close_only"


def _merge_close_conditions(
    raw_rows: list[dict],
) -> tuple[dict[str, dict], int]:
    """Collapse the ``(code, variant_key)``-keyed seed JSON into a
    ``{code: {complete, nar, description}}`` dict by union-merging close
    arrays across all variants of the same triggering code.

    Returns ``(grouped, skipped)`` where ``skipped`` counts blank-code rows
    (which the caller logs).

    Pure function -- no DB, no I/O. Exposed for unit testing.
    """
    grouped: dict[str, dict] = {}
    skipped = 0
    for raw in raw_rows:
        code = (raw.get("code") or "").strip()
        if not code:
            skipped += 1
            continue
        bucket = grouped.setdefault(
            code,
            {"complete": [], "nar": [], "description": code},
        )
        for c in raw.get("complete_codes") or []:
            if c not in bucket["complete"]:
                bucket["complete"].append(c)
        for n in raw.get("nar_codes") or []:
            if n not in bucket["nar"]:
                bucket["nar"].append(n)
        if bucket["description"] == code:
            desc = (raw.get("description") or "").strip()
            if desc:
                bucket["description"] = desc
    return grouped, skipped


def seed_close_conditions(
    db: Session, tenant_id: str = "global"
) -> dict[str, int]:
    """Patch close-condition arrays from ``docket_close_conditions.json`` onto
    the canonical IFW rule rows.

    The seed JSON is grouped by ``(code, variant_key)`` for documentation
    purposes, but the materializer's :func:`_id_for_rule_code` keys deadlines
    off ``code`` only -- so for each ``code`` we collapse the variants by
    taking the **union** of every variant's ``complete_codes`` and
    ``nar_codes`` and writing that onto the single canonical rule row matched
    by ``(tenant_id, code)``.

    Behaviour:

    * If a calculator-backed rule row exists (the usual case -- seeded
      first by :func:`seed_global_rules` from ``ifw-rules.json``), patch
      its ``close_complete_codes`` / ``close_nar_codes`` columns and leave
      every other field (kind, months, extendable, hand-tuned overrides)
      alone.
    * If only an ``auto_close_only`` sentinel row exists for the code,
      patch its arrays in place.
    * If no row exists at all, insert a minimal ``auto_close_only`` row
      with ``variant_key=""`` carrying the merged arrays. The calculator
      treats unknown kinds as "no deadline computed" so this still feeds
      the auto-close pass without polluting the inbox.
    * Legacy orphan ``auto_close_only`` rows (created by the previous
      seed-by-variant_key implementation that mismatched the canonical
      ``variant_key=""`` row) are deactivated whenever a canonical
      sibling is found, so :func:`_id_for_rule_code` no longer
      nondeterministically picks them.

    Returns ``{inserted, updated, skipped, deactivated, codes}``.

    Idempotent -- safe to call on every container start.
    """
    inserted = 0
    updated = 0
    deactivated = 0

    raw_rows = list(load_docket_close_seed())
    grouped, skipped = _merge_close_conditions(raw_rows)
    if skipped:
        # Re-walk for the warning side-effect (helper is pure).
        for raw in raw_rows:
            if not (raw.get("code") or "").strip():
                logger.warning(
                    "seed_close_conditions: blank triggering code for %r -- skipping",
                    raw.get("description") or "(no description)",
                )

    now = datetime.now(timezone.utc)
    for code, bucket in grouped.items():
        complete = list(bucket["complete"])
        nar = list(bucket["nar"])

        # Prefer the calculator-backed row; fall back to an existing
        # auto_close_only sentinel; failing that, insert a fresh sentinel.
        canonical = db.scalar(
            select(IfwRuleRow)
            .where(
                IfwRuleRow.tenant_id == tenant_id,
                IfwRuleRow.code == code,
                IfwRuleRow.kind != DOCKET_CLOSE_ONLY_KIND,
            )
            .order_by(IfwRuleRow.id.asc())
        )
        if canonical is not None:
            target = canonical
            # Deactivate orphan auto_close_only rows for the same code so
            # _id_for_rule_code's filter on active=True excludes them.
            orphans = db.scalars(
                select(IfwRuleRow).where(
                    IfwRuleRow.tenant_id == tenant_id,
                    IfwRuleRow.code == code,
                    IfwRuleRow.kind == DOCKET_CLOSE_ONLY_KIND,
                    IfwRuleRow.active.is_(True),
                )
            ).all()
            for orphan in orphans:
                orphan.active = False
                orphan.updated_at = now
                deactivated += 1
        else:
            target = db.scalar(
                select(IfwRuleRow)
                .where(
                    IfwRuleRow.tenant_id == tenant_id,
                    IfwRuleRow.code == code,
                    IfwRuleRow.kind == DOCKET_CLOSE_ONLY_KIND,
                )
                .order_by(IfwRuleRow.id.asc())
            )

        if target is None:
            db.add(
                IfwRuleRow(
                    tenant_id=tenant_id,
                    code=code,
                    variant_key="",
                    description=str(bucket["description"]),
                    kind=DOCKET_CLOSE_ONLY_KIND,
                    trigger_label="Docket cross-off rule",
                    authority="harness-internal",
                    close_complete_codes=complete,
                    close_nar_codes=nar,
                )
            )
            inserted += 1
        else:
            target.close_complete_codes = complete
            target.close_nar_codes = nar
            target.updated_at = now
            updated += 1

    if inserted or updated or deactivated:
        db.commit()
    return {
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
        "deactivated": deactivated,
        "codes": len(grouped),
    }
