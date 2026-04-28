"""Per-CTNF outcome extraction (ctnf_outcome.py)."""

from __future__ import annotations

from datetime import date

from harness_analytics.ctnf_outcome import (
    CtnfOutcome,
    extract_outcomes_for_application,
    extract_outcomes_from_grouped_events,
)


def _d(s: str) -> date:
    return date.fromisoformat(s)


def test_no_ctnf_returns_empty_list() -> None:
    assert (
        extract_outcomes_for_application(
            application_id=1,
            ctnf_dates=[],
            ctfr_dates=[],
            noa_dates=[_d("2024-06-01")],
            response_dates=[_d("2024-05-01")],
        )
        == []
    )


def test_single_ctnf_responded_then_noa_is_allowed() -> None:
    out = extract_outcomes_for_application(
        application_id=1,
        ctnf_dates=[_d("2024-01-01")],
        ctfr_dates=[],
        noa_dates=[_d("2024-04-15")],
        response_dates=[_d("2024-03-01")],  # 60 days to respond
    )
    assert len(out) == 1
    o = out[0]
    assert o.outcome == "allowed"
    assert o.days_to_response == 60
    assert o.next_action_date == _d("2024-04-15")
    assert o.days_response_to_next == 45


def test_single_ctnf_responded_then_ctfr_is_rejected() -> None:
    out = extract_outcomes_for_application(
        application_id=1,
        ctnf_dates=[_d("2024-01-01")],
        ctfr_dates=[_d("2024-05-01")],
        noa_dates=[],
        response_dates=[_d("2024-03-30")],
    )
    assert len(out) == 1
    assert out[0].outcome == "rejected"
    assert out[0].days_to_response == 89


def test_two_ctnfs_first_followed_by_second_ctnf_is_rejected() -> None:
    """Successive CTNFs: the next examiner action after the first response
    is the second CTNF, so the first cycle is "rejected" in the sense of
    "didn't get an NOA."
    """
    out = extract_outcomes_for_application(
        application_id=1,
        ctnf_dates=[_d("2024-01-01"), _d("2024-08-01")],
        ctfr_dates=[],
        noa_dates=[_d("2024-12-15")],
        response_dates=[_d("2024-04-01"), _d("2024-10-01")],
    )
    assert len(out) == 2
    assert out[0].outcome == "rejected"
    assert out[0].next_action_date == _d("2024-08-01")
    assert out[0].days_to_response == 91
    # Second CTNF: response 2024-10-01 -> NOA 2024-12-15 -> allowed.
    assert out[1].outcome == "allowed"
    assert out[1].next_action_date == _d("2024-12-15")
    assert out[1].days_to_response == 61


def test_pending_ctnf_with_response_but_no_next_action() -> None:
    out = extract_outcomes_for_application(
        application_id=1,
        ctnf_dates=[_d("2024-01-01")],
        ctfr_dates=[],
        noa_dates=[],
        response_dates=[_d("2024-03-15")],  # responded, awaiting examiner
    )
    assert len(out) == 1
    o = out[0]
    assert o.outcome == "pending"
    assert o.next_action_date is None
    assert o.days_response_to_next is None


def test_ctnf_with_no_response_is_dropped() -> None:
    out = extract_outcomes_for_application(
        application_id=1,
        ctnf_dates=[_d("2023-01-01")],
        ctfr_dates=[],
        noa_dates=[],
        response_dates=[],
    )
    assert out == []


def test_response_after_next_action_is_ignored() -> None:
    """A response filed AFTER the next examiner action shouldn't be credited
    to the prior CTNF cycle. (Real-world: an examiner can mail a successor
    OA before processing a late response; we don't want to give that
    response credit.)"""
    out = extract_outcomes_for_application(
        application_id=1,
        ctnf_dates=[_d("2024-01-01")],
        ctfr_dates=[_d("2024-02-15")],
        noa_dates=[],
        response_dates=[_d("2024-03-15")],  # AFTER the CTFR
    )
    assert out == []


def test_response_on_same_day_as_ctnf_is_ignored() -> None:
    """A "response" timestamped at-or-before the CTNF mail date is invalid
    -- you can't respond to an OA that hasn't been mailed yet. Drop."""
    out = extract_outcomes_for_application(
        application_id=1,
        ctnf_dates=[_d("2024-01-01")],
        ctfr_dates=[],
        noa_dates=[_d("2024-04-15")],
        response_dates=[_d("2024-01-01")],
    )
    assert out == []


def test_zero_day_response_is_kept() -> None:
    """A 1-calendar-day response is fine (filed the next day after CTNF
    mail). Verifies the strict-greater-than t0 boundary."""
    out = extract_outcomes_for_application(
        application_id=1,
        ctnf_dates=[_d("2024-01-01")],
        ctfr_dates=[],
        noa_dates=[_d("2024-04-15")],
        response_dates=[_d("2024-01-02")],
    )
    assert len(out) == 1
    assert out[0].days_to_response == 1
    assert out[0].outcome == "allowed"


def test_grouped_events_helper_handles_strings_and_datetimes() -> None:
    from datetime import datetime, timezone

    grouped = {
        7: {
            "ctnf": [datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)],
            "ctfr": [],
            "noa": ["2024-04-15"],
            "response": [datetime(2024, 3, 1, tzinfo=timezone.utc), None],
        },
        # App with no CTNFs is silently skipped.
        8: {"ctnf": [], "ctfr": [], "noa": ["2023-01-01"], "response": []},
        # Unparseable values are silently dropped.
        9: {
            "ctnf": ["not-a-date"],
            "ctfr": [],
            "noa": [],
            "response": ["2024-05-05"],
        },
    }
    out = extract_outcomes_from_grouped_events(grouped)
    assert len(out) == 1
    assert out[0].application_id == 7
    assert out[0].outcome == "allowed"
    assert out[0].days_to_response == 60


def test_outcome_dataclass_is_frozen_and_hashable() -> None:
    o = CtnfOutcome(
        application_id=1,
        ctnf_date=_d("2024-01-01"),
        response_date=_d("2024-02-01"),
        days_to_response=31,
        outcome="allowed",
        next_action_date=_d("2024-04-01"),
        days_response_to_next=59,
    )
    # Hashable -> usable in sets / dict keys for downstream dedup.
    assert {o, o} == {o}
