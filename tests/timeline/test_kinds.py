"""End-to-end tests for each rule kind in :func:`compute_deadlines`."""
from __future__ import annotations

from datetime import date

import pytest

from harness_analytics.timeline.calculator import (
    ComputeOptions,
    IfwRule,
    compute_deadlines,
    primary_row,
)


def _rule(**overrides) -> IfwRule:
    base = dict(
        code="X",
        kind="standard_oa",
        description="test rule",
        trigger_label="Mailing date of OA",
        user_note="",
        authority="35 USC X",
    )
    base.update(overrides)
    return IfwRule(**base)


# ---------------------------------------------------------------------------
# standard_oa — CTFR worked example from the design doc § 5
#   trigger 2025-03-19, ssp=3, max=6 →
#   SSP=2025-06-19, EOT-1=2025-07-21 (rolled), EOT-2=2025-08-19, bar=2025-09-19
# ---------------------------------------------------------------------------


def test_standard_oa_ctfr_worked_example() -> None:
    rule = _rule(code="CTFR", kind="standard_oa", ssp_months=3, max_months=6, extendable=True)
    res = compute_deadlines(rule, date(2025, 3, 19), ComputeOptions(roll_weekends=False))
    dates = {r.label: r.date for r in res.rows}
    assert dates["SSP"] == date(2025, 6, 19)
    assert dates["1-mo EOT"] == date(2025, 7, 19)
    assert dates["2-mo EOT"] == date(2025, 8, 19)
    assert dates["Statutory bar"] == date(2025, 9, 19)
    # SSP is info, EOTs warn, bar danger.
    assert res.rows[0].severity == "info"
    assert res.rows[-1].severity == "danger"
    # Fee on the bar row should be the 3-month EOT fee for large entity.
    assert res.rows[-1].fee_usd == 1500


def test_standard_oa_rolls_weekend() -> None:
    rule = _rule(code="CTNF", kind="standard_oa", ssp_months=3, max_months=6)
    # 2025-04-19 is Saturday → SSP rolls to Monday 2025-04-21.
    res = compute_deadlines(rule, date(2025, 1, 19), ComputeOptions())
    assert res.rows[0].date == date(2025, 4, 21)


def test_standard_oa_missing_months_warns() -> None:
    rule = _rule(code="X", ssp_months=None, max_months=None)
    res = compute_deadlines(rule, date(2024, 1, 1), ComputeOptions())
    assert not res.rows
    assert any("missing" in w for w in res.warnings)


# ---------------------------------------------------------------------------
# hard_noa — never emits EOTs
# ---------------------------------------------------------------------------


def test_hard_noa_emits_two_rows_no_eots() -> None:
    rule = _rule(code="NOA", kind="hard_noa", ssp_months=3, extendable=False)
    res = compute_deadlines(rule, date(2024, 5, 1), ComputeOptions(roll_weekends=False))
    assert len(res.rows) == 2
    labels = [r.label for r in res.rows]
    assert "Issue fee due" in labels
    assert all(r.eot_month is None for r in res.rows)
    due = next(r for r in res.rows if r.label == "Issue fee due")
    assert due.date == date(2024, 8, 1)
    assert due.severity == "danger"


# ---------------------------------------------------------------------------
# fixed_none — empty
# ---------------------------------------------------------------------------


def test_fixed_none_no_rows() -> None:
    rule = _rule(code="ACK", kind="fixed_none")
    res = compute_deadlines(rule, date(2024, 1, 1), ComputeOptions())
    assert res.rows == ()
    assert res.maintenance is None


# ---------------------------------------------------------------------------
# maintenance — three windows
# ---------------------------------------------------------------------------


def test_maintenance_three_windows() -> None:
    rule = _rule(
        code="M1",
        kind="maintenance",
        due_months_from_grant=42,
        grace_months_from_grant=48,
    )
    res = compute_deadlines(rule, date(2024, 1, 15), ComputeOptions(roll_weekends=False))
    assert res.maintenance is not None
    assert res.maintenance.window_open == date(2027, 1, 15)  # 36 months
    assert res.maintenance.due == date(2027, 7, 15)  # 42 months
    assert res.maintenance.grace_end == date(2028, 1, 15)  # 48 months


# ---------------------------------------------------------------------------
# ids_phase
# ---------------------------------------------------------------------------


def test_ids_phase_emits_phase_table() -> None:
    rule = _rule(code="IDS", kind="ids_phase")
    res = compute_deadlines(rule, date(2024, 1, 1), ComputeOptions())
    assert len(res.ids_phases) == 4
    assert res.rows == ()


def test_ids_phase_is_not_materialized_as_a_deadline() -> None:
    """IDS phases are reference data, not actionable deadlines. The materializer
    must skip them so they do not appear in `computed_deadlines` (and therefore
    do not pollute the Upcoming Actions inbox as ``2,000+ days overdue``).
    """
    from harness_analytics.timeline.materializer import _result_to_persisted_fields

    rule = _rule(code="IDS", kind="ids_phase")
    res = compute_deadlines(rule, date(2024, 1, 1), ComputeOptions())
    assert res.ids_phases  # sanity: the calculator did emit phases
    assert _result_to_persisted_fields(res) is None


# ---------------------------------------------------------------------------
# priority_later_of (FRPR / Paris Convention)
# ---------------------------------------------------------------------------


def test_priority_later_of_picks_priority_branch() -> None:
    rule = _rule(
        code="FRPR",
        kind="priority_later_of",
        from_filing_months=12,
        from_priority_months=12,
        trigger_label="Foreign filing",
    )
    # Trigger 2024-06-01, priority 2024-08-01 → priority branch wins by 2 mo.
    opts = ComputeOptions(priority_date=date(2024, 8, 1), roll_weekends=False)
    res = compute_deadlines(rule, date(2024, 6, 1), opts)
    assert res.rows[0].date == date(2025, 8, 1)


def test_priority_later_of_filing_branch_when_no_priority() -> None:
    rule = _rule(
        code="FRPR",
        kind="priority_later_of",
        from_filing_months=12,
        from_priority_months=12,
    )
    res = compute_deadlines(rule, date(2024, 6, 1), ComputeOptions(roll_weekends=False))
    assert res.rows[0].date == date(2025, 6, 1)
    assert any("Priority date unknown" in w for w in res.warnings)


# ---------------------------------------------------------------------------
# pct_national
# ---------------------------------------------------------------------------


def test_pct_national_two_dates() -> None:
    rule = _rule(
        code="PCT",
        kind="pct_national",
        base_months_from_priority=30,
        late_months_from_priority=31,
    )
    opts = ComputeOptions(priority_date=date(2023, 1, 15), roll_weekends=False)
    res = compute_deadlines(rule, date(2024, 1, 15), opts)
    labels = {r.label: r.date for r in res.rows}
    assert labels["30-mo national stage"] == date(2025, 7, 15)
    assert labels["31-mo bar"] == date(2025, 8, 15)


# ---------------------------------------------------------------------------
# appeal_brief — defaults to ssp=2, max=7
# ---------------------------------------------------------------------------


def test_appeal_brief_defaults() -> None:
    rule = _rule(code="N/AP.E", kind="appeal_brief")
    res = compute_deadlines(rule, date(2024, 6, 1), ComputeOptions(roll_weekends=False))
    # Should produce 1 SSP + 4 EOT + 1 bar = 6 rows (max-ssp=5, so EOTs 1..4).
    labels = [r.label for r in res.rows]
    assert labels[0] == "SSP"
    assert labels[-1] == "Statutory bar"
    assert any(l == "4-mo EOT" for l in labels)


# ---------------------------------------------------------------------------
# soft_window
# ---------------------------------------------------------------------------


def test_soft_window_single_info_row() -> None:
    rule = _rule(code="SOFT", kind="soft_window", from_filing_months=24)
    res = compute_deadlines(rule, date(2024, 1, 1), ComputeOptions(roll_weekends=False))
    assert len(res.rows) == 1
    assert res.rows[0].severity == "info"
    assert res.rows[0].date == date(2026, 1, 1)


# ---------------------------------------------------------------------------
# primary_row helper
# ---------------------------------------------------------------------------


def test_primary_row_picks_first_actionable() -> None:
    rule = _rule(code="CTFR", kind="standard_oa", ssp_months=3, max_months=6)
    res = compute_deadlines(rule, date(2025, 3, 19), ComputeOptions(roll_weekends=False))
    pr = primary_row(res)
    # First non-info row is the 1-mo EOT (warn).
    assert pr is not None
    assert pr.label == "1-mo EOT"


# ---------------------------------------------------------------------------
# rows_json persistence shape (regression: empty Step rows in deadline drawer)
# ---------------------------------------------------------------------------


def test_serialize_result_returns_bare_list_of_row_dicts() -> None:
    """``_serialize_result`` must return a bare list of row dicts so the
    column matches its ``Mapped[list]`` annotation and the API consumer
    (``timeline_api._deadline_to_dict``: ``"rows": list(cd.rows_json or [])``)
    iterates real rows -- not the keys of a wrapper dict.

    Pre-fix this returned ``{"rows": [...], "ids_phases": [...], "warnings":
    [...]}`` and the deadline drawer rendered three blank "rows" (one per
    dict key) for every standard_oa deadline."""
    from harness_analytics.timeline.materializer import _serialize_result

    rule = _rule(code="CTFR", kind="standard_oa", ssp_months=3, max_months=6, extendable=True)
    res = compute_deadlines(rule, date(2025, 3, 19), ComputeOptions(roll_weekends=False))
    out = _serialize_result(res)

    assert isinstance(out, list)
    assert all(isinstance(r, dict) for r in out)
    labels = [r["label"] for r in out]
    assert labels[0] == "SSP"
    assert labels[-1] == "Statutory bar"
    # Every row must carry the three fields the frontend renders.
    for r in out:
        assert {"label", "date", "fee_usd"}.issubset(r)
