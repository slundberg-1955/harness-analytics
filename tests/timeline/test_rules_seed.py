"""Tests for the IFW rules seed JSON shape + helpers (no DB)."""
from __future__ import annotations

import pytest

from harness_analytics.timeline.calculator import (
    ComputeOptions,
    IfwRule,
    compute_deadlines,
)
from harness_analytics.timeline.rules_repo import (
    _FIELD_NAMES,
    _normalize_seed_row,
    load_seed_rules,
)

ALLOWED_KINDS = {
    "standard_oa",
    "hard_noa",
    "fixed_none",
    "maintenance",
    "ids_phase",
    "priority_later_of",
    "pct_national",
    "appeal_brief",
    "soft_window",
}


def test_seed_json_loads_and_has_rules() -> None:
    rules = load_seed_rules()
    assert len(rules) >= 10  # the doc seed table is at least this big
    codes = {r["code"] for r in rules}
    # Sanity check the main USPTO codes are present.
    for must_have in {"CTNF", "CTFR", "CTRS", "NOA", "PCT", "FRPR", "IDS"}:
        assert must_have in codes, f"Missing seed rule {must_have}"


def test_every_seed_row_has_required_fields() -> None:
    for r in load_seed_rules():
        assert "code" in r and r["code"]
        assert "kind" in r and r["kind"] in ALLOWED_KINDS, (
            f"Bad kind for {r.get('code')}"
        )
        assert "description" in r and r["description"]


def test_normalize_seed_row_fills_defaults() -> None:
    raw = {"code": "X", "kind": "fixed_none", "description": "x"}
    out = _normalize_seed_row(raw)
    assert out["user_note"] == ""
    assert out["warnings"] == []
    assert out["aliases"] == []
    assert out["active"] is True
    assert out["extendable"] is False
    assert "UTILITY" in out["patent_type_applicability"]


def test_field_names_cover_calculator_inputs() -> None:
    # Anything compute_deadlines reads off IfwRule must be writable to the row.
    must_persist = {
        "ssp_months",
        "max_months",
        "due_months_from_grant",
        "grace_months_from_grant",
        "from_filing_months",
        "from_priority_months",
        "extendable",
        "kind",
        "trigger_label",
        "authority",
    }
    assert must_persist <= _FIELD_NAMES


def test_seed_rules_drive_calculator_end_to_end() -> None:
    """Round-trip every seed row through the calculator. Must not raise or warn."""
    from datetime import date

    for r in load_seed_rules():
        data = _normalize_seed_row(r)
        kwargs = {k: data.get(k) for k in (_FIELD_NAMES & {"ssp_months", "max_months", "due_months_from_grant", "grace_months_from_grant", "from_filing_months", "from_priority_months", "base_months_from_priority", "late_months_from_priority", "extendable", "trigger_label", "user_note", "authority"})}
        rule = IfwRule(
            code=data["code"],
            kind=data["kind"],
            description=data["description"],
            warnings=tuple(data.get("warnings") or ()),
            aliases=tuple(data.get("aliases") or ()),
            priority_tier=data.get("priority_tier"),
            **kwargs,
        )
        result = compute_deadlines(rule, date(2024, 6, 1), ComputeOptions())
        # We just need it to not blow up; specific shape depends on kind.
        assert result.rule.code == data["code"]
