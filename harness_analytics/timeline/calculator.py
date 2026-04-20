"""Pure-Python ``compute_deadlines(rule, trigger, options)`` engine.

Mirrors the contract in ``PROSECUTION_TIMELINE_DESIGN.md § 4`` and the
authoritative ``uspto-deadline-calculator.cursorrules.md`` spec. Zero I/O —
no DB, no logging, no HTTP. Everything is deterministic and testable.

Severity levels:
  * ``info``   — milestone or non-actionable date
  * ``warn``   — approaching SSP / EOT
  * ``danger`` — statutory bar / hard date with no relief

Money in :class:`DeadlineRow.fee_usd` is stored in whole USD (engine never
deals in cents — fees come from :mod:`harness_analytics.timeline.fees`).
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass, field
from datetime import date
from typing import Literal, Optional

from harness_analytics.timeline.fees import eot_fee_usd

RuleKind = Literal[
    "standard_oa",
    "hard_noa",
    "fixed_none",
    "maintenance",
    "ids_phase",
    "priority_later_of",
    "pct_national",
    "appeal_brief",
    "soft_window",
]

Severity = Literal["info", "warn", "danger"]
EntitySize = Literal["large", "small", "micro"]


@dataclass(frozen=True)
class IfwRule:
    """A configurable IFW rule. Stored as a row in ``ifw_rules`` (M2)."""

    code: str
    kind: RuleKind
    description: str
    trigger_label: str
    user_note: str
    authority: str
    extendable: bool = False
    aliases: tuple[str, ...] = ()
    ssp_months: Optional[int] = None
    max_months: Optional[int] = None
    due_months_from_grant: Optional[int] = None
    grace_months_from_grant: Optional[int] = None
    from_filing_months: Optional[int] = None
    from_priority_months: Optional[int] = None
    base_months_from_priority: Optional[int] = None
    late_months_from_priority: Optional[int] = None
    warnings: tuple[str, ...] = ()
    priority_tier: Optional[str] = None  # CRITICAL | IMPORTANT | ROUTINE


@dataclass(frozen=True)
class ComputeOptions:
    entity_size: EntitySize = "large"
    priority_date: Optional[date] = None
    roll_weekends: bool = True
    federal_holidays: tuple[date, ...] = ()


@dataclass(frozen=True)
class DeadlineRow:
    label: str
    date: date
    fee_usd: int
    severity: Severity
    eot_month: Optional[int] = None


@dataclass(frozen=True)
class MaintenanceDates:
    window_open: date
    due: date
    grace_end: date


@dataclass(frozen=True)
class IdsPhase:
    phase: str
    label: str
    requirements: str


@dataclass(frozen=True)
class DeadlineResult:
    rule: IfwRule
    trigger: date
    rows: tuple[DeadlineRow, ...] = field(default_factory=tuple)
    maintenance: Optional[MaintenanceDates] = None
    ids_phases: tuple[IdsPhase, ...] = field(default_factory=tuple)
    warnings: tuple[str, ...] = field(default_factory=tuple)


# ---------------------------------------------------------------------------
# Date math (MPEP 710.01(a) and 37 CFR 1.7)
# ---------------------------------------------------------------------------


def add_months(d: date, months: int) -> date:
    """MPEP 710.01(a): add N months, clamping to last day of target month.

    >>> add_months(date(2024, 1, 31), 1)
    datetime.date(2024, 2, 29)
    >>> add_months(date(2024, 3, 31), 6)
    datetime.date(2024, 9, 30)
    """
    total = (d.month - 1) + months
    year = d.year + total // 12
    month = total % 12 + 1
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, min(d.day, last_day))


def roll_forward(d: date, holidays: tuple[date, ...] = ()) -> date:
    """37 CFR 1.7: if a deadline lands on Sat/Sun/holiday, it rolls forward."""
    holiday_set = set(holidays)
    out = d
    while out.weekday() >= 5 or out in holiday_set:
        out = date.fromordinal(out.toordinal() + 1)
    return out


def _maybe_roll(d: date, options: ComputeOptions) -> date:
    if options.roll_weekends:
        return roll_forward(d, options.federal_holidays)
    return d


# ---------------------------------------------------------------------------
# Per-kind dispatchers
# ---------------------------------------------------------------------------


def _standard_oa(rule: IfwRule, trigger: date, options: ComputeOptions) -> DeadlineResult:
    ssp = rule.ssp_months
    mx = rule.max_months
    if ssp is None or mx is None or mx < ssp:
        return DeadlineResult(
            rule=rule,
            trigger=trigger,
            warnings=(f"Rule {rule.code} missing ssp_months/max_months for standard_oa",),
        )

    rows: list[DeadlineRow] = []
    ssp_date = _maybe_roll(add_months(trigger, ssp), options)
    rows.append(
        DeadlineRow(
            label="SSP", date=ssp_date, fee_usd=0, severity="info", eot_month=None
        )
    )
    n_eot = mx - ssp
    for m in range(1, n_eot):
        d = _maybe_roll(add_months(trigger, ssp + m), options)
        sev: Severity = "warn"
        rows.append(
            DeadlineRow(
                label=f"{m}-mo EOT",
                date=d,
                fee_usd=eot_fee_usd(m, options.entity_size),
                severity=sev,
                eot_month=m,
            )
        )
    bar_date = _maybe_roll(add_months(trigger, mx), options)
    rows.append(
        DeadlineRow(
            label="Statutory bar",
            date=bar_date,
            fee_usd=eot_fee_usd(n_eot, options.entity_size),
            severity="danger",
            eot_month=n_eot,
        )
    )
    return DeadlineResult(
        rule=rule, trigger=trigger, rows=tuple(rows), warnings=rule.warnings
    )


def _hard_noa(rule: IfwRule, trigger: date, options: ComputeOptions) -> DeadlineResult:
    months = rule.ssp_months if rule.ssp_months is not None else 3
    due = _maybe_roll(add_months(trigger, months), options)
    rows = (
        DeadlineRow(
            label=rule.trigger_label or "Trigger",
            date=trigger,
            fee_usd=0,
            severity="info",
        ),
        DeadlineRow(
            label="Issue fee due", date=due, fee_usd=0, severity="danger"
        ),
    )
    return DeadlineResult(
        rule=rule, trigger=trigger, rows=rows, warnings=rule.warnings
    )


def _fixed_none(rule: IfwRule, trigger: date, options: ComputeOptions) -> DeadlineResult:
    return DeadlineResult(rule=rule, trigger=trigger, warnings=rule.warnings)


def _maintenance(rule: IfwRule, trigger: date, options: ComputeOptions) -> DeadlineResult:
    """Trigger date here is the *issue date*. Three windows: 3.5/7.5/11.5 years."""
    due_months = rule.due_months_from_grant
    grace_months = rule.grace_months_from_grant
    if due_months is None or grace_months is None:
        return DeadlineResult(
            rule=rule,
            trigger=trigger,
            warnings=(f"Rule {rule.code} missing due/grace months",),
        )
    # window opens 6 months before due (MPEP 2506).
    window_open = add_months(trigger, due_months - 6)
    due = add_months(trigger, due_months)
    grace_end = add_months(trigger, grace_months)
    return DeadlineResult(
        rule=rule,
        trigger=trigger,
        maintenance=MaintenanceDates(
            window_open=_maybe_roll(window_open, options),
            due=_maybe_roll(due, options),
            grace_end=_maybe_roll(grace_end, options),
        ),
        warnings=rule.warnings,
    )


def _ids_phase(rule: IfwRule, trigger: date, options: ComputeOptions) -> DeadlineResult:
    # Phases per MPEP 609.04(b). Trigger is the application filing date.
    phases = (
        IdsPhase(
            phase="Phase 1",
            label="Within 3 months of filing or before first OA on the merits",
            requirements="No fee, no statement.",
        ),
        IdsPhase(
            phase="Phase 2",
            label="After Phase 1 but before close of prosecution",
            requirements="Fee under 37 CFR 1.17(p) OR statement under 37 CFR 1.97(e).",
        ),
        IdsPhase(
            phase="Phase 3",
            label="After close of prosecution but before payment of issue fee",
            requirements="Fee AND statement under 37 CFR 1.97(e).",
        ),
        IdsPhase(
            phase="Phase 4",
            label="After issue fee payment / on issued patent",
            requirements="Supplemental Examination or Reissue only.",
        ),
    )
    return DeadlineResult(
        rule=rule, trigger=trigger, ids_phases=phases, warnings=rule.warnings
    )


def _priority_later_of(
    rule: IfwRule, trigger: date, options: ComputeOptions
) -> DeadlineResult:
    """FRPR: later of (filing + from_filing_months, priority + from_priority_months)."""
    from_filing = rule.from_filing_months or 12
    if rule.from_priority_months is None:
        return DeadlineResult(
            rule=rule,
            trigger=trigger,
            warnings=("from_priority_months not configured",),
        )
    branch_filing = add_months(trigger, from_filing)
    if options.priority_date is None:
        # No priority: filing branch alone applies.
        d = _maybe_roll(branch_filing, options)
        return DeadlineResult(
            rule=rule,
            trigger=trigger,
            rows=(
                DeadlineRow(
                    label=rule.trigger_label or "Foreign filing",
                    date=d,
                    fee_usd=0,
                    severity="warn",
                ),
            ),
            warnings=("Priority date unknown — assumed no priority claim",),
        )
    branch_priority = add_months(options.priority_date, rule.from_priority_months)
    chosen = max(branch_filing, branch_priority)
    d = _maybe_roll(chosen, options)
    return DeadlineResult(
        rule=rule,
        trigger=trigger,
        rows=(
            DeadlineRow(
                label=rule.trigger_label or "Foreign filing",
                date=d,
                fee_usd=0,
                severity="warn",
            ),
        ),
        warnings=rule.warnings,
    )


def _pct_national(
    rule: IfwRule, trigger: date, options: ComputeOptions
) -> DeadlineResult:
    """Two rows: 30 mo and 31 mo from priority date (or trigger if none)."""
    base = options.priority_date or trigger
    base_months = rule.base_months_from_priority or 30
    late_months = rule.late_months_from_priority or 31
    base_d = _maybe_roll(add_months(base, base_months), options)
    late_d = _maybe_roll(add_months(base, late_months), options)
    rows = (
        DeadlineRow(
            label=f"{base_months}-mo national stage",
            date=base_d,
            fee_usd=0,
            severity="warn",
        ),
        DeadlineRow(
            label=f"{late_months}-mo bar",
            date=late_d,
            fee_usd=0,
            severity="danger",
        ),
    )
    return DeadlineResult(rule=rule, trigger=trigger, rows=rows, warnings=rule.warnings)


def _appeal_brief(
    rule: IfwRule, trigger: date, options: ComputeOptions
) -> DeadlineResult:
    """Appeal brief: ssp=2, max=7 (per design doc)."""
    forced = IfwRule(
        **{
            **rule.__dict__,
            "ssp_months": rule.ssp_months or 2,
            "max_months": rule.max_months or 7,
        }
    )
    return _standard_oa(forced, trigger, options)


def _soft_window(
    rule: IfwRule, trigger: date, options: ComputeOptions
) -> DeadlineResult:
    months = rule.from_filing_months or 24
    d = _maybe_roll(add_months(trigger, months), options)
    rows = (
        DeadlineRow(
            label=rule.trigger_label or "Soft deadline",
            date=d,
            fee_usd=0,
            severity="info",
        ),
    )
    return DeadlineResult(rule=rule, trigger=trigger, rows=rows, warnings=rule.warnings)


_DISPATCH = {
    "standard_oa": _standard_oa,
    "hard_noa": _hard_noa,
    "fixed_none": _fixed_none,
    "maintenance": _maintenance,
    "ids_phase": _ids_phase,
    "priority_later_of": _priority_later_of,
    "pct_national": _pct_national,
    "appeal_brief": _appeal_brief,
    "soft_window": _soft_window,
}


def compute_deadlines(
    rule: IfwRule, trigger: date, options: ComputeOptions
) -> DeadlineResult:
    """Dispatch to the per-kind computation. Pure function."""
    handler = _DISPATCH.get(rule.kind)
    if handler is None:
        return DeadlineResult(
            rule=rule, trigger=trigger, warnings=(f"Unknown rule kind: {rule.kind}",)
        )
    return handler(rule, trigger, options)


def primary_row(result: DeadlineResult) -> Optional[DeadlineRow]:
    """The "next action" row — first non-info row, else the first row."""
    if not result.rows:
        return None
    for row in result.rows:
        if row.severity != "info":
            return row
    return result.rows[0]
