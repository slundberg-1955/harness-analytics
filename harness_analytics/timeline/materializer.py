"""Wrap the calculator with DB I/O.

For each application:

1. Resolve every IFW document to its rule (via :func:`rules_repo.get_rule`,
   honoring tenant overrides). Unmapped codes increment ``unmapped_ifw_codes``.
2. For each rule match, compute deadlines off the document's ``mail_room_date``.
3. Trigger the special "filing-date" / "issue-date" / "priority-date" rules
   off the application's stored dates.
4. Upsert into ``computed_deadlines``, keyed by
   ``(application_id, rule_id, trigger_date, trigger_document_id)``.
5. Mark stale OPEN rows ``SUPERSEDED`` per ``supersession_map`` entries.
6. Append ``deadline_events`` audit rows for any state changes.

The view's ``next_deadline_*`` columns are computed via correlated subqueries
in :mod:`harness_analytics.schema_migrations` — nothing to write here for the
denormalized summary.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Iterable, Optional

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from harness_analytics.models import (
    Application,
    ComputedDeadline,
    DeadlineEvent,
    FileWrapperDocument,
    IfwRule as IfwRuleRow,
    SupersessionMap,
    UnmappedIfwCode,
)
from harness_analytics.timeline.calculator import (
    ComputeOptions,
    DeadlineResult,
    IfwRule,
    compute_deadlines,
    primary_row,
)
from harness_analytics.timeline.holidays import federal_holidays
from harness_analytics.timeline.rules_repo import get_rule

logger = logging.getLogger(__name__)


# Rule codes that trigger off application-level dates (not an IFW doc).
_FILING_TRIGGERED = {"FILING_DATE", "FRPR", "IDS"}
_ISSUE_TRIGGERED = {"MISMTH4", "MISMTH8", "MISMTH12"}
_PRIORITY_TRIGGERED = {"PCT"}


def _match_code(pattern: str, code: str) -> bool:
    """Return True if ``pattern`` matches ``code``.

    Patterns are either an exact code (``"NOA"``) or a trailing-dot prefix
    wildcard (``"A..."`` matches ``A.NE``, ``A.AF``, ``AMSB``, …). ``...``
    is only honored at the end; embedded ``...`` is treated literally.
    """
    if not pattern or not code:
        return False
    if pattern.endswith("..."):
        return code.startswith(pattern[:-3])
    return pattern == code


def _choose_close_match(
    *,
    deadline_trigger_date: date,
    complete_patterns: Iterable[str],
    nar_patterns: Iterable[str],
    docs: Iterable["FileWrapperDocument"],
) -> Optional[tuple[str, "FileWrapperDocument", str]]:
    """Pure picker: return ``(disposition, winning_doc, matched_pattern)`` or ``None``.

    Walks ``docs`` ordered by ``(mail_room_date, id)`` ascending; only docs
    strictly after the deadline trigger participate. The first matching doc
    wins; same-day matches prefer ``complete`` over ``nar`` so a single doc
    that satisfies both lists collapses to the friendlier disposition.

    Side-effect free — the materializer wraps this with the I/O bits so the
    decision logic stays unit-testable without a DB.
    """
    complete_patterns = list(complete_patterns or ())
    nar_patterns = list(nar_patterns or ())
    if not complete_patterns and not nar_patterns:
        return None
    sorted_docs = sorted(
        (d for d in docs if d.mail_room_date is not None),
        key=lambda d: (
            d.mail_room_date.date() if isinstance(d.mail_room_date, datetime)
            else d.mail_room_date,
            d.id or 0,
        ),
    )
    for doc in sorted_docs:
        doc_date = (
            doc.mail_room_date.date()
            if isinstance(doc.mail_room_date, datetime)
            else doc.mail_room_date
        )
        if doc_date is None or doc_date <= deadline_trigger_date:
            continue
        code = (doc.document_code or "").strip()
        if not code:
            continue
        for pat in complete_patterns:
            if _match_code(pat, code):
                return ("auto_complete", doc, pat)
        for pat in nar_patterns:
            if _match_code(pat, code):
                return ("auto_nar", doc, pat)
    return None


@dataclass
class RecomputeSummary:
    application_id: int
    deadlines_written: int = 0
    deadlines_superseded: int = 0
    deadlines_auto_completed: int = 0
    deadlines_auto_nar: int = 0
    unmapped_codes: int = 0


def _row_to_json(row) -> dict:
    return {
        "label": row.label,
        "date": row.date.isoformat(),
        "fee_usd": row.fee_usd,
        "severity": row.severity,
        "eot_month": row.eot_month,
    }


def _serialize_result(result: DeadlineResult) -> dict:
    return {
        "rows": [_row_to_json(r) for r in result.rows],
        "ids_phases": [asdict(p) for p in result.ids_phases],
        "warnings": list(result.warnings),
    }


def _record_unmapped(db: Session, code: str, tenant_id: str) -> None:
    existing = db.scalar(
        select(UnmappedIfwCode).where(
            UnmappedIfwCode.tenant_id == tenant_id,
            UnmappedIfwCode.code == code,
        )
    )
    if existing is None:
        db.add(
            UnmappedIfwCode(
                tenant_id=tenant_id,
                code=code,
                count=1,
                last_seen=datetime.now(timezone.utc),
            )
        )
    else:
        existing.count = (existing.count or 0) + 1
        existing.last_seen = datetime.now(timezone.utc)


def _supersession_pairs(db: Session, tenant_id: str) -> set[tuple[str, str]]:
    rows = db.scalars(
        select(SupersessionMap).where(
            SupersessionMap.tenant_id.in_(("global", tenant_id))
        )
    ).all()
    return {(r.prev_kind, r.new_kind) for r in rows}


def _compute_one(
    rule: IfwRule, trigger_date: date, options: ComputeOptions
) -> Optional[DeadlineResult]:
    if trigger_date is None:
        return None
    return compute_deadlines(rule, trigger_date, options)


def _result_to_persisted_fields(
    result: DeadlineResult,
) -> Optional[dict]:
    """Boil a calculator result down to ``computed_deadlines`` column values."""
    pr = primary_row(result)
    rows_json = _serialize_result(result)
    common = {
        "rows_json": rows_json,
        "warnings": list(result.warnings) or None,
    }
    if result.maintenance is not None:
        m = result.maintenance
        return {
            **common,
            "primary_date": m.due,
            "primary_label": "Maintenance fee due",
            "ssp_date": m.window_open,
            "statutory_bar_date": m.grace_end,
            "window_open_date": m.window_open,
            "grace_end_date": m.grace_end,
            "severity": "warn",
        }
    if result.ids_phases:
        # Pure-reference result (37 CFR 1.97/1.98 phase windows). These are not
        # actionable deadlines, just a phase table anchored to the application's
        # filing date. Do not persist as a `computed_deadlines` row — that would
        # show up in the inbox as "thousands of days overdue" because the
        # primary_date would be the filing date. The matter detail page renders
        # IDS phases on the fly from the rule + filing date instead.
        return None
    if pr is None:
        return None
    ssp = next(
        (r for r in result.rows if r.label == "SSP"), None
    )
    bar = next(
        (r for r in result.rows if r.label == "Statutory bar"), None
    )
    return {
        **common,
        "primary_date": pr.date,
        "primary_label": pr.label,
        "ssp_date": ssp.date if ssp else None,
        "statutory_bar_date": bar.date if bar else None,
        "severity": pr.severity,
    }


def _upsert_deadline(
    db: Session,
    *,
    app: Application,
    rule_id: int,
    trigger_date: date,
    trigger_source: str,
    trigger_document_id: Optional[int],
    fields: dict,
    tenant_id: str,
) -> tuple[ComputedDeadline, bool]:
    """Upsert by (app, rule, trigger_date, doc_id). Returns (row, created)."""
    existing = db.scalar(
        select(ComputedDeadline).where(
            ComputedDeadline.application_id == app.id,
            ComputedDeadline.rule_id == rule_id,
            ComputedDeadline.trigger_date == trigger_date,
            ComputedDeadline.trigger_document_id.is_(trigger_document_id)
            if trigger_document_id is None
            else ComputedDeadline.trigger_document_id == trigger_document_id,
        )
    )
    if existing is not None:
        # Detect dates-changed for audit.
        changed = (
            existing.primary_date != fields["primary_date"]
            or existing.primary_label != fields["primary_label"]
        )
        for k, v in fields.items():
            setattr(existing, k, v)
        existing.tenant_id = tenant_id
        if changed:
            db.add(
                DeadlineEvent(
                    deadline_id=existing.id,
                    action="RECOMPUTED",
                    payload_json={"changed": True},
                )
            )
        return existing, False

    cd = ComputedDeadline(
        application_id=app.id,
        rule_id=rule_id,
        trigger_event_id=None,
        trigger_document_id=trigger_document_id,
        trigger_date=trigger_date,
        trigger_source=trigger_source,
        tenant_id=tenant_id,
        **fields,
    )
    db.add(cd)
    db.flush()
    db.add(DeadlineEvent(deadline_id=cd.id, action="CREATED"))
    return cd, True


def recompute_for_application(db: Session, application_id: int) -> int:
    """Materialize deadlines for one application. Returns # of rows written/updated."""
    app = db.get(Application, application_id)
    if app is None:
        return 0
    summary = _recompute_internal(db, app)
    db.commit()
    return summary.deadlines_written


def _options_for_app(app: Application) -> ComputeOptions:
    return ComputeOptions(
        entity_size="large",  # entity size is not in the biblio XML; assume large.
        priority_date=app.earliest_priority_date,
        roll_weekends=True,
        federal_holidays=federal_holidays(),
    )


def _recompute_internal(db: Session, app: Application) -> RecomputeSummary:
    summary = RecomputeSummary(application_id=app.id)
    tenant_id = app.tenant_id or "global"
    options = _options_for_app(app)

    # 0) Prune previously-materialized rows that we no longer want to keep.
    #    Today: rows produced by `ids_phase` rules (identified by a non-null
    #    `ids_phases_json` payload). These were retired in favor of computing
    #    the phase table on the fly on the matter detail page — keeping them
    #    around would re-pollute the inbox after a recompute.
    db.execute(
        delete(ComputedDeadline).where(
            ComputedDeadline.application_id == app.id,
            ComputedDeadline.ids_phases_json.isnot(None),
        )
    )

    # 1) Document-triggered rules.
    docs = db.scalars(
        select(FileWrapperDocument).where(
            FileWrapperDocument.application_id == app.id
        )
    ).all()
    seen_codes: set[str] = set()
    for doc in docs:
        code = (doc.document_code or "").strip()
        if not code:
            continue
        if code in _FILING_TRIGGERED or code in _ISSUE_TRIGGERED or code in _PRIORITY_TRIGGERED:
            continue  # handled below
        rule = get_rule(db, code, tenant_id)
        if rule is None:
            _record_unmapped(db, code, tenant_id)
            summary.unmapped_codes += 1
            continue
        trigger_d = (
            doc.mail_room_date.date() if isinstance(doc.mail_room_date, datetime) else doc.mail_room_date
        )
        if trigger_d is None:
            continue
        result = _compute_one(rule, trigger_d, options)
        fields = _result_to_persisted_fields(result) if result else None
        if fields is None:
            continue
        # Look up rule_id from the row (rules_repo only returns the dataclass).
        rule_row = db.execute(
            select(_id_for_rule_code(rule.code, tenant_id))
        ).scalar_one_or_none()
        if rule_row is None:
            continue
        _upsert_deadline(
            db,
            app=app,
            rule_id=rule_row,
            trigger_date=trigger_d,
            trigger_source="IFW_DOCUMENT",
            trigger_document_id=doc.id,
            fields=fields,
            tenant_id=tenant_id,
        )
        summary.deadlines_written += 1
        seen_codes.add(rule.code)

    # 2) Filing-date triggered rules (FRPR, IDS, FILING_DATE soft window).
    if app.filing_date:
        for code in _FILING_TRIGGERED:
            rule = get_rule(db, code, tenant_id)
            if rule is None:
                continue
            result = _compute_one(rule, app.filing_date, options)
            fields = _result_to_persisted_fields(result) if result else None
            if fields is None:
                continue
            rule_id = db.execute(select(_id_for_rule_code(code, tenant_id))).scalar_one_or_none()
            if rule_id is None:
                continue
            _upsert_deadline(
                db,
                app=app,
                rule_id=rule_id,
                trigger_date=app.filing_date,
                trigger_source="FILING_DATE",
                trigger_document_id=None,
                fields=fields,
                tenant_id=tenant_id,
            )
            summary.deadlines_written += 1

    # 3) Issue-date triggered (maintenance fees).
    if app.issue_date:
        for code in _ISSUE_TRIGGERED:
            rule = get_rule(db, code, tenant_id)
            if rule is None:
                continue
            result = _compute_one(rule, app.issue_date, options)
            fields = _result_to_persisted_fields(result) if result else None
            if fields is None:
                continue
            rule_id = db.execute(select(_id_for_rule_code(code, tenant_id))).scalar_one_or_none()
            if rule_id is None:
                continue
            _upsert_deadline(
                db,
                app=app,
                rule_id=rule_id,
                trigger_date=app.issue_date,
                trigger_source="ISSUE_DATE",
                trigger_document_id=None,
                fields=fields,
                tenant_id=tenant_id,
            )
            summary.deadlines_written += 1

    # 4) Priority-date triggered (PCT national stage).
    if app.earliest_priority_date:
        for code in _PRIORITY_TRIGGERED:
            rule = get_rule(db, code, tenant_id)
            if rule is None:
                continue
            result = _compute_one(rule, app.earliest_priority_date, options)
            fields = _result_to_persisted_fields(result) if result else None
            if fields is None:
                continue
            rule_id = db.execute(select(_id_for_rule_code(code, tenant_id))).scalar_one_or_none()
            if rule_id is None:
                continue
            _upsert_deadline(
                db,
                app=app,
                rule_id=rule_id,
                trigger_date=app.earliest_priority_date,
                trigger_source="EARLIEST_PRIORITY",
                trigger_document_id=None,
                fields=fields,
                tenant_id=tenant_id,
            )
            summary.deadlines_written += 1

    # 4.5) Auto-close pass: rule-driven docket cross-off (M0009).
    # Runs *before* supersession so that:
    #   • SUPERSEDED never overwrites NAR (the supersession loop only touches
    #     status='OPEN' rows, so deadlines this pass already flipped to
    #     COMPLETED or NAR are off-limits to it).
    #   • AUTO_NAR never overwrites AUTO_COMPLETE — both branches of the
    #     picker key off the same (status='OPEN') filter, and ``complete``
    #     wins on same-day ties inside ``_choose_close_match``.
    # Idempotent: re-running just no-ops because the OPEN filter excludes
    # already-closed rows. We also skip enqueuing a duplicate AUTO_* event
    # if the most recent event for this deadline is identical.
    _apply_auto_close_pass(db, app, summary)

    # 5) Conservative supersession: when a new doc-triggered rule of kind X
    # arrives and an older OPEN deadline of kind Y is in (Y, X) of
    # supersession_map, mark the older one SUPERSEDED.
    pairs = _supersession_pairs(db, tenant_id)
    if pairs:
        # Load all OPEN deadlines for this app, ordered by trigger_date.
        open_rows = db.scalars(
            select(ComputedDeadline)
            .where(
                ComputedDeadline.application_id == app.id,
                ComputedDeadline.status == "OPEN",
            )
            .order_by(ComputedDeadline.trigger_date.desc())
        ).all()
        # Map deadline.id → (rule_kind via join). Cheap approach: bring kinds in.
        from harness_analytics.models import IfwRule as IfwRuleRow

        rule_kinds: dict[int, str] = {
            row.id: row.kind
            for row in db.scalars(select(IfwRuleRow)).all()
        }
        # Pair newer over older with matching (older.kind, newer.kind).
        for newer in open_rows:
            new_kind = rule_kinds.get(newer.rule_id)
            if not new_kind:
                continue
            for older in open_rows:
                if older.id == newer.id:
                    continue
                if older.trigger_date >= newer.trigger_date:
                    continue
                old_kind = rule_kinds.get(older.rule_id)
                if not old_kind:
                    continue
                if (old_kind, new_kind) in pairs:
                    older.status = "SUPERSEDED"
                    older.superseded_by = newer.id
                    db.add(
                        DeadlineEvent(
                            deadline_id=older.id,
                            action="SUPERSEDED",
                            payload_json={"by": newer.id},
                        )
                    )
                    summary.deadlines_superseded += 1

    return summary


def _apply_auto_close_pass(
    db: Session, app: Application, summary: RecomputeSummary
) -> None:
    """Auto-close OPEN deadlines whose IFW history matches their rule's
    ``close_complete_codes`` / ``close_nar_codes``.

    Only OPEN deadlines participate — already-closed rows are intentionally
    skipped so re-running the materializer can't downgrade COMPLETED→NAR or
    NAR→OPEN. The audit trail mirrors a manual ``complete`` action: row
    columns flip + a ``DeadlineEvent`` of action ``AUTO_COMPLETE`` /
    ``AUTO_NAR`` is appended with the matched code, pattern, and IFW doc
    pointer.
    """
    open_rows = db.scalars(
        select(ComputedDeadline).where(
            ComputedDeadline.application_id == app.id,
            ComputedDeadline.status == "OPEN",
        )
    ).all()
    if not open_rows:
        return
    docs = db.scalars(
        select(FileWrapperDocument).where(
            FileWrapperDocument.application_id == app.id
        )
    ).all()
    if not docs:
        return
    rule_ids = {row.rule_id for row in open_rows}
    rules: dict[int, IfwRuleRow] = {
        r.id: r
        for r in db.scalars(
            select(IfwRuleRow).where(IfwRuleRow.id.in_(rule_ids))
        ).all()
    }
    now = datetime.now(timezone.utc)
    for cd in open_rows:
        rule = rules.get(cd.rule_id)
        if rule is None:
            continue
        complete = list(rule.close_complete_codes or ())
        nar = list(rule.close_nar_codes or ())
        if not complete and not nar:
            continue
        match = _choose_close_match(
            deadline_trigger_date=cd.trigger_date,
            complete_patterns=complete,
            nar_patterns=nar,
            docs=docs,
        )
        if match is None:
            continue
        disposition, doc, pattern = match
        is_complete = disposition == "auto_complete"
        cd.status = "COMPLETED" if is_complete else "NAR"
        cd.completed_at = now
        cd.closed_by_ifw_document_id = doc.id
        cd.closed_by_rule_pattern = pattern
        cd.closed_disposition = disposition
        action = "AUTO_COMPLETE" if is_complete else "AUTO_NAR"
        if is_complete:
            summary.deadlines_auto_completed += 1
        else:
            summary.deadlines_auto_nar += 1
        # Belt-and-suspenders dedupe: even though the OPEN filter above
        # makes re-runs no-op, double-check the most recent event so we
        # never write two consecutive identical AUTO_* rows.
        latest = db.scalar(
            select(DeadlineEvent)
            .where(DeadlineEvent.deadline_id == cd.id)
            .order_by(
                DeadlineEvent.occurred_at.desc(), DeadlineEvent.id.desc()
            )
            .limit(1)
        )
        payload = {
            "matched_code": (doc.document_code or "").strip(),
            "matched_pattern": pattern,
            "ifw_document_id": doc.id,
            "mail_room_date": _iso_for_audit(doc.mail_room_date),
            "rule_id": rule.id,
            "rule_code": rule.code,
            "variant_key": rule.variant_key,
        }
        if (
            latest is not None
            and latest.action == action
            and (latest.payload_json or {}).get("ifw_document_id") == doc.id
        ):
            continue
        db.add(
            DeadlineEvent(
                deadline_id=cd.id,
                action=action,
                payload_json=payload,
            )
        )


def _iso_for_audit(value) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return value.isoformat()


def _id_for_rule_code(code: str, tenant_id: str):
    """SQLAlchemy expression that returns the resolved rule_id for a code."""
    from harness_analytics.models import IfwRule as IfwRuleRow

    # Prefer tenant row; fall back to global. Two-step subquery via UNION ALL
    # would be more elegant, but a CASE expression is enough for our needs.
    return (
        select(IfwRuleRow.id)
        .where(
            IfwRuleRow.code == code,
            IfwRuleRow.tenant_id.in_((tenant_id, "global")),
            IfwRuleRow.active.is_(True),
        )
        .order_by(
            (IfwRuleRow.tenant_id == "global").asc()
        )
        .limit(1)
    ).scalar_subquery()


def recompute_for_tenant(db: Session, tenant_id: str = "global") -> int:
    """Recompute every application in a tenant.

    Stream-friendly: loads only application IDs upfront and fetches each app
    one at a time, expunging session state after every commit. This keeps
    memory bounded for portfolio-scale backfills (~26k apps) and emits a
    progress line to stdout every ``_PROGRESS_EVERY`` apps so a tee'd log
    file shows movement before the run completes.
    """
    app_ids = [
        i for (i,) in db.execute(
            select(Application.id).where(Application.tenant_id == tenant_id)
        ).all()
    ]
    total = len(app_ids)
    logger.info("timeline-recompute start: tenant=%s apps=%d", tenant_id, total)
    print(f"timeline-recompute start: tenant={tenant_id!r} apps={total}", flush=True)
    count = 0
    for aid in app_ids:
        app = db.get(Application, aid)
        if app is None:
            continue
        _recompute_internal(db, app)
        db.commit()
        # Free the per-app working set (Application + FileWrapperDocuments +
        # ComputedDeadlines just upserted) so the long-running session does
        # not balloon to multi-GB on a 26k-app tenant.
        db.expunge_all()
        count += 1
        if count % _PROGRESS_EVERY == 0 or count == total:
            print(
                f"  [{count}/{total}] apps recomputed",
                flush=True,
            )
    return count


_PROGRESS_EVERY = 100
