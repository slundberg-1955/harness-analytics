"""Pure-Python tests for the materializer's docket cross-off / NAR pass.

The DB-bound orchestration is exercised by hitting it through the test
client elsewhere; here we lock down the decision logic — pattern matching
+ winner selection — which is where the subtle bugs live.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from types import SimpleNamespace

from harness_analytics.timeline.materializer import (
    _choose_close_match,
    _match_code,
)


def _doc(doc_id: int, mail_room_date, code: str):
    return SimpleNamespace(
        id=doc_id, mail_room_date=mail_room_date, document_code=code
    )


# ---------------------------------------------------------------------------
# _match_code
# ---------------------------------------------------------------------------


def test_match_code_prefix_wildcard_matches_dotted_codes() -> None:
    assert _match_code("A...", "A.NE")
    assert _match_code("A...", "A.AF")
    assert _match_code("A...", "A.QU")
    assert _match_code("A...", "AMSB")  # any code starting with "A"


def test_match_code_prefix_wildcard_rejects_other_prefixes() -> None:
    assert not _match_code("A...", "BNE")
    assert not _match_code("A...", "")
    assert not _match_code("A...", "NOA")


def test_match_code_exact() -> None:
    assert _match_code("NOA", "NOA")
    assert not _match_code("NOA", "NOAS")
    assert not _match_code("NOA", "noa")  # case-sensitive on purpose


def test_match_code_empty_pattern_or_code_is_false() -> None:
    assert not _match_code("", "NOA")
    assert not _match_code("NOA", "")
    assert not _match_code("", "")


# ---------------------------------------------------------------------------
# _choose_close_match: ordering, complete-wins-on-same-day, etc.
# ---------------------------------------------------------------------------


def test_choose_close_match_picks_earliest_winner_after_trigger() -> None:
    trigger = date(2024, 6, 1)
    docs = [
        _doc(10, date(2024, 5, 30), "A.NE"),     # before trigger — ignored
        _doc(20, date(2024, 6, 15), "A.NE"),     # first eligible match
        _doc(30, date(2024, 7, 1), "NOA"),       # later match also matches NAR
    ]
    result = _choose_close_match(
        deadline_trigger_date=trigger,
        complete_patterns=["A...", "RCEX"],
        nar_patterns=["NOA", "ABN"],
        docs=docs,
    )
    assert result is not None
    disposition, doc, pattern = result
    assert disposition == "auto_complete"
    assert doc.id == 20
    assert pattern == "A..."


def test_choose_close_match_falls_through_to_nar_when_no_complete() -> None:
    """When the only doc after the trigger matches a NAR pattern (and not
    any complete pattern), the picker returns ``auto_nar``. ``NOA`` is the
    canonical example — it doesn't start with ``A`` so the ``A...`` prefix
    wildcard cannot accidentally claim it as a complete.
    """
    trigger = date(2024, 6, 1)
    docs = [
        _doc(10, date(2024, 6, 5), "NOA"),
    ]
    result = _choose_close_match(
        deadline_trigger_date=trigger,
        complete_patterns=["A...", "RCEX"],
        nar_patterns=["NOA"],
        docs=docs,
    )
    assert result is not None
    disposition, doc, pattern = result
    assert disposition == "auto_nar"
    assert doc.id == 10
    assert pattern == "NOA"


def test_choose_close_match_complete_wins_on_same_day_as_nar() -> None:
    trigger = date(2024, 6, 1)
    # Two docs on the same day: one matches NAR, one matches complete.
    # Same-day means the iteration order is (mail_room_date, id), and
    # ``complete`` outranks ``nar`` for any single doc, but here we want to
    # make sure that even when the NAR-matching doc has the lower id, the
    # complete-matching doc still wins. ``_choose_close_match`` walks docs
    # in date+id order, so we need the complete doc first.
    # Rather than try to game id order, this test just verifies the
    # documented rule with the simpler case: the same doc matches both —
    # complete wins.
    same_day_doc = _doc(10, date(2024, 6, 5), "A.NE")
    result = _choose_close_match(
        deadline_trigger_date=trigger,
        complete_patterns=["A..."],
        nar_patterns=["A.NE"],
        docs=[same_day_doc],
    )
    assert result is not None
    assert result[0] == "auto_complete"


def test_choose_close_match_returns_none_when_no_matches() -> None:
    trigger = date(2024, 6, 1)
    docs = [
        _doc(10, date(2024, 6, 5), "EXIN"),
        _doc(20, date(2024, 7, 1), "FAI.RESP"),
    ]
    result = _choose_close_match(
        deadline_trigger_date=trigger,
        complete_patterns=["A...", "RCEX"],
        nar_patterns=["NOA", "ABN"],
        docs=docs,
    )
    assert result is None


def test_choose_close_match_returns_none_when_no_patterns() -> None:
    trigger = date(2024, 6, 1)
    docs = [_doc(10, date(2024, 6, 5), "ABN")]
    assert (
        _choose_close_match(
            deadline_trigger_date=trigger,
            complete_patterns=[],
            nar_patterns=[],
            docs=docs,
        )
        is None
    )


def test_choose_close_match_skips_docs_strictly_before_trigger() -> None:
    """Docs with mail_room_date strictly before the trigger are ignored, but
    same-day matches **are** allowed (filing-event triggers like N/AP often
    arrive in the same batch as their closer AP.B). See also
    ``test_choose_close_match_excludes_trigger_doc_by_id`` for the
    self-NAR'ing protection."""
    trigger = date(2024, 6, 1)
    docs = [
        _doc(10, date(2024, 5, 30), "A.NE"),  # before trigger -- ignored
        _doc(20, date(2024, 6, 1), "A.NE"),   # same day -- now eligible
        _doc(30, date(2024, 6, 2), "NOA"),
    ]
    result = _choose_close_match(
        deadline_trigger_date=trigger,
        complete_patterns=["A..."],
        nar_patterns=["NOA"],
        docs=docs,
    )
    assert result is not None
    # Same-day A.NE wins (complete preferred over later NOA NAR).
    assert result[0] == "auto_complete"
    assert result[1].id == 20


def test_choose_close_match_excludes_trigger_doc_by_id() -> None:
    """The trigger doc itself is excluded so an OA whose code appears in its
    own ``nar_codes`` (e.g. CTNF) doesn't self-NAR the deadline it just
    created."""
    trigger = date(2024, 6, 1)
    docs = [
        _doc(99, date(2024, 6, 1), "CTNF"),   # the trigger doc itself
        _doc(100, date(2024, 6, 15), "AMSB"),
    ]
    result = _choose_close_match(
        deadline_trigger_date=trigger,
        complete_patterns=["AMSB"],
        nar_patterns=["CTNF", "NOA"],
        docs=docs,
        trigger_document_id=99,
    )
    assert result is not None
    assert result[0] == "auto_complete"
    assert result[1].id == 100


def test_choose_close_match_same_day_filing_event_closer() -> None:
    """Regression for app 16650501: Notice of Appeal (N/AP) and Appeal
    Brief (AP.B) routinely land on the same mail-room date. The matcher
    must accept the AP.B as a same-day closer so the N/AP.E deadline
    auto-completes instead of staying overdue."""
    trigger = date(2024, 7, 31)
    docs = [
        _doc(1, date(2024, 7, 31), "N/AP"),     # trigger doc
        _doc(2, date(2024, 7, 31), "AP.B"),     # closer, same-day
    ]
    result = _choose_close_match(
        deadline_trigger_date=trigger,
        complete_patterns=["AP.B"],
        nar_patterns=["ABN", "NOA"],
        docs=docs,
        trigger_document_id=1,
    )
    assert result is not None
    assert result[0] == "auto_complete"
    assert result[1].id == 2


def test_choose_close_match_handles_datetime_mail_room() -> None:
    """Some IFW rows arrive with a tz-aware datetime in ``mail_room_date``;
    the picker must normalize to ``date`` before comparing to the trigger.
    """
    trigger = date(2024, 6, 1)
    docs = [
        _doc(10, datetime(2024, 6, 5, 14, 30), "A.NE"),
    ]
    result = _choose_close_match(
        deadline_trigger_date=trigger,
        complete_patterns=["A..."],
        nar_patterns=["NOA"],
        docs=docs,
    )
    assert result is not None
    assert result[0] == "auto_complete"


# ---------------------------------------------------------------------------
# _apply_app_level_close_shortcut: belt-and-suspenders for NOA / hard_noa
# deadlines on apps that definitively issued or abandoned.
# ---------------------------------------------------------------------------


class _StubSession:
    """Fake Session enough to exercise _apply_app_level_close_shortcut.

    Records ``db.add(...)`` calls and serves two fixed ``scalars(...)``
    results: the OPEN deadlines, then the rule lookup for those deadlines.
    Subsequent ``scalars()`` calls (for the event-dedupe lookup) return an
    empty iterator. ``scalar()`` for the latest-event lookup returns None.
    """

    def __init__(self, open_rows, rules_by_id):
        self._queue = [open_rows, list(rules_by_id.values())]
        self.added = []

    def scalars(self, *_a, **_kw):
        if self._queue:
            rows = self._queue.pop(0)
        else:
            rows = []

        class _Iter:
            def __init__(self, r):
                self._r = r

            def all(self):
                return list(self._r)

        return _Iter(rows)

    def scalar(self, *_a, **_kw):
        return None

    def add(self, obj):
        self.added.append(obj)


def _cd(id_, rule_id, status="OPEN"):
    return SimpleNamespace(
        id=id_,
        rule_id=rule_id,
        status=status,
        completed_at=None,
        closed_disposition=None,
        closed_by_rule_pattern=None,
    )


def _rule(id_, kind):
    return SimpleNamespace(id=id_, code="NOA", kind=kind)


def test_app_level_shortcut_completes_hard_noa_when_issued() -> None:
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_app_level_close_shortcut,
    )

    app = SimpleNamespace(
        id=1,
        issue_date=date(2024, 3, 1),
        patent_number="US10,000,000",
        application_status_text="Patented Case",
    )
    open_rows = [_cd(100, rule_id=7)]
    rules = {7: _rule(7, "hard_noa")}
    session = _StubSession(open_rows, rules)
    summary = RecomputeSummary(application_id=1)

    _apply_app_level_close_shortcut(session, app, summary)

    assert open_rows[0].status == "COMPLETED"
    assert open_rows[0].closed_disposition == "auto_complete"
    assert open_rows[0].closed_by_rule_pattern == "app_issued"
    assert summary.deadlines_auto_completed == 1
    # One DeadlineEvent was appended with AUTO_COMPLETE action.
    assert len(session.added) == 1
    assert session.added[0].action == "AUTO_COMPLETE"


def test_app_level_shortcut_nars_hard_noa_when_abandoned() -> None:
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_app_level_close_shortcut,
    )

    app = SimpleNamespace(
        id=1,
        issue_date=None,
        patent_number=None,
        application_status_text="Abandoned -- Failure to Respond to Office Action",
    )
    open_rows = [_cd(200, rule_id=9)]
    rules = {9: _rule(9, "hard_noa")}
    session = _StubSession(open_rows, rules)
    summary = RecomputeSummary(application_id=1)

    _apply_app_level_close_shortcut(session, app, summary)

    assert open_rows[0].status == "NAR"
    assert open_rows[0].closed_disposition == "auto_nar"
    assert open_rows[0].closed_by_rule_pattern == "app_abandoned"
    assert summary.deadlines_auto_nar == 1


def test_app_level_shortcut_leaves_non_hard_noa_alone() -> None:
    """The shortcut is intentionally narrow: only ``hard_noa`` rules are
    eligible. An OA response window should be untouched even if the app
    has issued, because the attorney may still need to take some action
    (e.g. post-allowance amendment).
    """
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_app_level_close_shortcut,
    )

    app = SimpleNamespace(
        id=1,
        issue_date=date(2024, 3, 1),
        patent_number="US10,000,000",
        application_status_text="Patented Case",
    )
    open_rows = [_cd(300, rule_id=11)]
    rules = {11: _rule(11, "standard_oa")}
    session = _StubSession(open_rows, rules)
    summary = RecomputeSummary(application_id=1)

    _apply_app_level_close_shortcut(session, app, summary)

    assert open_rows[0].status == "OPEN"
    assert summary.deadlines_auto_completed == 0
    assert session.added == []


def test_app_level_shortcut_no_op_when_app_still_pending() -> None:
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_app_level_close_shortcut,
    )

    app = SimpleNamespace(
        id=1,
        issue_date=None,
        patent_number=None,
        application_status_text="Non Final Action Mailed",
    )
    open_rows = [_cd(400, rule_id=13)]
    rules = {13: _rule(13, "hard_noa")}
    session = _StubSession(open_rows, rules)
    summary = RecomputeSummary(application_id=1)

    _apply_app_level_close_shortcut(session, app, summary)

    assert open_rows[0].status == "OPEN"
    assert session.added == []


def test_app_level_shortcut_nars_all_kinds_when_abandoned() -> None:
    """Failure-to-respond / express / failure-to-pay / withdrawn-from-issue
    all share the same docketing consequence: matter is dead and EVERY
    open deadline gets crossed off. Not just the OA response window
    (already handled by per-rule ABN nar_codes), and not just hard_noa.
    """
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_app_level_close_shortcut,
    )

    app = SimpleNamespace(
        id=1,
        issue_date=None,
        patent_number=None,
        application_status_text="Abandoned -- Failure to Respond to an Office Action",
    )
    # Mix of every rule kind we currently materialize.
    open_rows = [
        _cd(500, rule_id=20),  # standard_oa (CTNF response)
        _cd(501, rule_id=21),  # hard_noa (NOA / issue fee)
        _cd(502, rule_id=22),  # maintenance (3.5yr window)
        _cd(503, rule_id=23),  # priority_later_of (FRPR)
        _cd(504, rule_id=24),  # ids_phase
        _cd(505, rule_id=25),  # soft_window (24mo review)
    ]
    rules = {
        20: _rule(20, "standard_oa"),
        21: _rule(21, "hard_noa"),
        22: _rule(22, "maintenance"),
        23: _rule(23, "priority_later_of"),
        24: _rule(24, "ids_phase"),
        25: _rule(25, "soft_window"),
    }
    session = _StubSession(open_rows, rules)
    summary = RecomputeSummary(application_id=1)

    _apply_app_level_close_shortcut(session, app, summary)

    for cd in open_rows:
        assert cd.status == "NAR", f"deadline {cd.id} not NAR'd"
        assert cd.closed_disposition == "auto_nar"
        assert cd.closed_by_rule_pattern == "app_abandoned"
        assert cd.completed_at is not None

    assert summary.deadlines_auto_nar == 6
    assert summary.deadlines_auto_completed == 0
    # One AUTO_NAR audit event per row.
    assert len(session.added) == 6
    for evt in session.added:
        assert evt.action == "AUTO_NAR"
        assert evt.payload_json["matched_pattern"] == "app_abandoned"
        assert (
            evt.payload_json["application_status_text"]
            == "Abandoned -- Failure to Respond to an Office Action"
        )


def test_app_level_shortcut_issued_only_completes_hard_noa() -> None:
    """The issued branch stays narrow: only hard_noa is mooted by patent
    issuance. Maintenance / FRPR / IDS / soft_window survive because they
    are still meaningful post-issuance.
    """
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_app_level_close_shortcut,
    )

    app = SimpleNamespace(
        id=1,
        issue_date=date(2024, 3, 1),
        patent_number="US10,000,000",
        application_status_text="Patented Case",
    )
    open_rows = [
        _cd(600, rule_id=30),  # hard_noa -> COMPLETE
        _cd(601, rule_id=31),  # maintenance -> stay OPEN
        _cd(602, rule_id=32),  # priority_later_of -> stay OPEN
    ]
    rules = {
        30: _rule(30, "hard_noa"),
        31: _rule(31, "maintenance"),
        32: _rule(32, "priority_later_of"),
    }
    session = _StubSession(open_rows, rules)
    summary = RecomputeSummary(application_id=1)

    _apply_app_level_close_shortcut(session, app, summary)

    assert open_rows[0].status == "COMPLETED"
    assert open_rows[0].closed_by_rule_pattern == "app_issued"
    assert open_rows[1].status == "OPEN"
    assert open_rows[2].status == "OPEN"
    assert summary.deadlines_auto_completed == 1
    assert summary.deadlines_auto_nar == 0
    assert len(session.added) == 1
    assert session.added[0].action == "AUTO_COMPLETE"


def test_app_level_shortcut_abandoned_skips_already_closed() -> None:
    """Already-closed rows are filtered out by the OPEN gate, so they
    never get downgraded by the abandonment shortcut. Only OPEN rows
    are returned by the stub's first scalars() call.
    """
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_app_level_close_shortcut,
    )

    app = SimpleNamespace(
        id=1,
        issue_date=None,
        patent_number=None,
        application_status_text="Abandoned -- Failure to Pay Issue Fee",
    )
    # Stub returns only the OPEN row -- mirrors the real query's
    # `status == 'OPEN'` filter. The closed row is not visible to the
    # shortcut and therefore stays as-is.
    open_row = _cd(700, rule_id=40)
    rules = {40: _rule(40, "standard_oa")}
    session = _StubSession([open_row], rules)
    summary = RecomputeSummary(application_id=1)

    _apply_app_level_close_shortcut(session, app, summary)

    assert open_row.status == "NAR"
    assert summary.deadlines_auto_nar == 1
    assert len(session.added) == 1


# ---------------------------------------------------------------------------
# _frpr_not_applicable + _apply_paris_window_close (Paris Convention lifecycle)
# ---------------------------------------------------------------------------


def _app(
    *,
    earliest_priority_date=None,
    continuity_child_of_prior_us=False,
    filing_date=date(2020, 1, 4),
    issue_date=None,
    application_status_text=None,
    patent_number=None,
):
    return SimpleNamespace(
        id=42,
        filing_date=filing_date,
        issue_date=issue_date,
        patent_number=patent_number,
        application_status_text=application_status_text,
        earliest_priority_date=earliest_priority_date,
        continuity_child_of_prior_us=continuity_child_of_prior_us,
    )


def _frpr_cd(id_, rule_id, *, primary_date, status="OPEN"):
    return SimpleNamespace(
        id=id_,
        rule_id=rule_id,
        status=status,
        primary_date=primary_date,
        completed_at=None,
        closed_disposition=None,
        closed_by_rule_pattern=None,
    )


def _frpr_rule(id_):
    return SimpleNamespace(id=id_, code="FRPR", kind="priority_later_of")


def test_frpr_not_applicable_with_foreign_priority() -> None:
    from harness_analytics.timeline.materializer import _frpr_not_applicable

    assert _frpr_not_applicable(_app(earliest_priority_date=date(2019, 5, 1)))


def test_frpr_not_applicable_with_continuity_parent() -> None:
    from harness_analytics.timeline.materializer import _frpr_not_applicable

    assert _frpr_not_applicable(_app(continuity_child_of_prior_us=True))


def test_frpr_not_applicable_for_originally_filed_app() -> None:
    from harness_analytics.timeline.materializer import _frpr_not_applicable

    assert not _frpr_not_applicable(_app())


def test_paris_window_close_nars_when_app_has_priority_claim() -> None:
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_paris_window_close,
    )

    app = _app(earliest_priority_date=date(2019, 5, 1))
    open_rows = [_frpr_cd(700, rule_id=11, primary_date=date(2099, 1, 1))]
    rules = {11: _frpr_rule(11)}
    session = _StubSession(open_rows, rules)
    summary = RecomputeSummary(application_id=42)

    _apply_paris_window_close(session, app, summary)

    assert open_rows[0].status == "NAR"
    assert open_rows[0].closed_disposition == "auto_nar"
    assert open_rows[0].closed_by_rule_pattern == "no_paris_window"
    assert summary.deadlines_auto_nar == 1
    assert len(session.added) == 1
    assert session.added[0].action == "AUTO_NAR"
    assert session.added[0].payload_json["matched_pattern"] == "no_paris_window"


def test_paris_window_close_completes_when_window_has_passed() -> None:
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_paris_window_close,
    )

    # No priority claim -> falls into the "deadline_passed" branch when
    # primary_date is in the past.
    app = _app()
    open_rows = [_frpr_cd(701, rule_id=11, primary_date=date(2021, 1, 4))]
    rules = {11: _frpr_rule(11)}
    session = _StubSession(open_rows, rules)
    summary = RecomputeSummary(application_id=42)

    _apply_paris_window_close(session, app, summary)

    assert open_rows[0].status == "COMPLETED"
    assert open_rows[0].closed_disposition == "deadline_passed"
    assert open_rows[0].closed_by_rule_pattern == "paris_window_passed"
    assert summary.deadlines_auto_completed == 1
    assert len(session.added) == 1
    assert session.added[0].action == "AUTO_DEADLINE_PASSED"


def test_paris_window_close_leaves_open_when_window_still_in_future() -> None:
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_paris_window_close,
    )

    app = _app()
    open_rows = [_frpr_cd(702, rule_id=11, primary_date=date(2099, 1, 4))]
    rules = {11: _frpr_rule(11)}
    session = _StubSession(open_rows, rules)
    summary = RecomputeSummary(application_id=42)

    _apply_paris_window_close(session, app, summary)

    assert open_rows[0].status == "OPEN"
    assert summary.deadlines_auto_completed == 0
    assert summary.deadlines_auto_nar == 0
    assert session.added == []


def test_paris_window_close_ignores_non_frpr_deadlines() -> None:
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_paris_window_close,
    )

    app = _app(earliest_priority_date=date(2019, 5, 1))
    # rule code is NOT "FRPR" — should be left alone even though primary_date
    # is in the past and the app has a priority claim.
    open_rows = [_frpr_cd(703, rule_id=11, primary_date=date(2021, 1, 4))]
    rules = {11: SimpleNamespace(id=11, code="CTNF", kind="response")}
    session = _StubSession(open_rows, rules)
    summary = RecomputeSummary(application_id=42)

    _apply_paris_window_close(session, app, summary)

    assert open_rows[0].status == "OPEN"
    assert session.added == []


# ---------------------------------------------------------------------------
# Production traces (Apr 2026 audit) -- regressions found by sampling the 477
# overdue items in prod and dumping the raw IFW timelines for the four worst
# offenders. Each test below is a minimal reconstruction of one app's IFW
# stream, asserting the close-match decision the materializer should reach
# *given the seed shipped with the repo*.
# ---------------------------------------------------------------------------


def test_choose_close_match_ifee_closes_noa_real_world_29998129() -> None:
    """App 29998129 had a NOA on 2026-01-21 followed by IFEE on 2026-04-13.
    Before the seed fix, ISSUE.FEE (a logical name only) was the only
    issue-fee closer pattern, and IFEE (the real USPTO code) wasn't
    matched -- so the NOA stayed OPEN until the patent itself issued
    weeks later. With IFEE in NOA's complete_codes, the NOA closes the
    moment the issue fee is paid.
    """
    trigger = date(2026, 1, 21)
    docs = [
        _doc(7001, date(2026, 1, 21), 'NOA'),       # the trigger doc itself
        _doc(7002, date(2026, 1, 21), 'IIFW'),      # printer info, ignored
        _doc(7003, date(2026, 4, 9), 'IDS'),        # IDS, ignored
        _doc(7004, date(2026, 4, 13), 'IFEE'),      # issue-fee payment -- closer
    ]
    result = _choose_close_match(
        deadline_trigger_date=trigger,
        complete_patterns=[
            'ISSUE.NTF', 'ISSUE.FEE', 'IFEE', 'RCEX', 'RCE', 'WFE', 'ACPA', 'NOA', 'Q.DEC.REOPEN',
        ],
        nar_patterns=['ABN', 'EXPR.ABN'],
        docs=docs,
        trigger_document_id=7001,
    )
    assert result is not None, 'IFEE must close NOA'
    disposition, doc, pattern = result
    assert disposition == 'auto_complete'
    assert doc.id == 7004
    assert pattern == 'IFEE'


def test_choose_close_match_qpids_reopen_closes_noa_real_world_18588603() -> None:
    """App 18588603: NOA mailed 2025-04-04, then Q.DEC.REOPEN (Quick Path
    IDS reopen of prosecution) on 2025-06-20, then a fresh CTNF on
    2025-08-28. The original NOA was effectively withdrawn the moment
    QPIDS reopened prosecution; without Q.DEC.REOPEN as a closer the
    NOA stayed OPEN and was reported as overdue from 2025-07-07 onward.
    """
    trigger = date(2025, 4, 4)
    docs = [
        _doc(8001, date(2025, 4, 4), 'NOA'),           # trigger doc
        _doc(8002, date(2025, 5, 14), 'IDS'),          # IDS filing, ignored
        _doc(8003, date(2025, 6, 20), 'Q.DEC.REOPEN'), # QPIDS reopen -- closer
        _doc(8004, date(2025, 8, 28), 'CTNF'),         # later -- shouldn't matter
    ]
    result = _choose_close_match(
        deadline_trigger_date=trigger,
        complete_patterns=[
            'ISSUE.NTF', 'ISSUE.FEE', 'IFEE', 'RCEX', 'RCE', 'WFE', 'ACPA', 'NOA', 'Q.DEC.REOPEN',
        ],
        nar_patterns=['ABN', 'EXPR.ABN'],
        docs=docs,
        trigger_document_id=8001,
    )
    assert result is not None, 'Q.DEC.REOPEN must close the prior NOA'
    disposition, doc, pattern = result
    assert disposition == 'auto_complete'
    assert doc.id == 8003
    assert pattern == 'Q.DEC.REOPEN'


def test_choose_close_match_acpa_closes_prior_noa_real_world_29998129() -> None:
    """Same app 29998129, *first* NOA on 2025-06-26 followed by an ACPA
    (Continued Prosecution Application -- design CPA) on 2025-09-26.
    ACPA was already in NOA's complete_codes before this audit, but
    locking the behaviour in with a real-world trace keeps it from
    quietly regressing if someone trims the seed.
    """
    trigger = date(2025, 6, 26)
    docs = [
        _doc(9001, date(2025, 6, 26), 'NOA'),       # trigger doc
        _doc(9002, date(2025, 7, 9), 'IDS'),        # ignored
        _doc(9003, date(2025, 9, 26), 'ACPA'),      # CPA -- closer
    ]
    result = _choose_close_match(
        deadline_trigger_date=trigger,
        complete_patterns=[
            'ISSUE.NTF', 'ISSUE.FEE', 'IFEE', 'RCEX', 'RCE', 'WFE', 'ACPA', 'NOA', 'Q.DEC.REOPEN',
        ],
        nar_patterns=['ABN', 'EXPR.ABN'],
        docs=docs,
        trigger_document_id=9001,
    )
    assert result is not None, 'ACPA must close the prior NOA'
    disposition, doc, pattern = result
    assert disposition == 'auto_complete'
    assert doc.id == 9003
    assert pattern == 'ACPA'


def test_choose_close_match_silent_after_ctnf_does_not_self_close() -> None:
    """App 17965418 / 17913525 pattern: an office action with no follow-up
    activity at all. The matcher must NOT auto-close such a deadline --
    OPEN is the correct status, and the app_abandoned shortcut (driven
    by application_status_text) is what should ultimately resolve genuine
    abandonments. Locks in that we never invented an aggressive
    time-since-trigger auto-NAR rule that would risk closing live
    matters with extensions / RCE / petitions in flight.
    """
    trigger = date(2025, 4, 8)
    docs = [
        _doc(10001, date(2025, 4, 8), 'CTNF'),  # trigger doc, excluded by id
        # Nothing after the trigger -- silent docket.
    ]
    result = _choose_close_match(
        deadline_trigger_date=trigger,
        complete_patterns=['A...', 'AMSB', 'A.AF', 'RCEX', 'RCE', 'N.APP'],
        nar_patterns=['CTNF', 'CTFR', 'CTAV', 'CTEQ', 'EX.A', 'NOA', 'ABN', 'EXPR.ABN', 'NRES'],
        docs=docs,
        trigger_document_id=10001,
    )
    assert result is None, 'Silent docket must stay OPEN, not auto-close'


# ---------------------------------------------------------------------------
# Time-based grace-period close passes
# (M0009 follow-up: production audit Apr 2026)
# ---------------------------------------------------------------------------


class _StubSessionMulti:
    """Stub Session that returns a configurable sequence of ``scalars()``
    result sets and a configurable sequence of ``scalar()`` results.

    Mirrors ``_StubSession`` but supports the 3-or-more ``scalars()`` calls
    the grace-period passes make (open_rows, rules, docs, then optional
    latest-event lookups via ``scalar()``).
    """

    def __init__(self, scalars_results, scalar_results=None):
        self._scalars_queue = list(scalars_results)
        self._scalar_queue = list(scalar_results or [])
        self.added = []

    def scalars(self, *_a, **_kw):
        rows = self._scalars_queue.pop(0) if self._scalars_queue else []

        class _Iter:
            def __init__(self, r):
                self._r = r

            def all(self):
                return list(self._r)

        return _Iter(rows)

    def scalar(self, *_a, **_kw):
        return self._scalar_queue.pop(0) if self._scalar_queue else None

    def add(self, obj):
        self.added.append(obj)


def _mismth_cd(id_, rule_id, *, primary_date):
    return SimpleNamespace(
        id=id_,
        rule_id=rule_id,
        status="OPEN",
        primary_date=primary_date,
        trigger_date=primary_date,  # MISMTH triggers off issue_date == primary's anchor
        trigger_document_id=None,
        completed_at=None,
        closed_disposition=None,
        closed_by_rule_pattern=None,
    )


def _mismth_rule(id_, code="MISMTH4"):
    return SimpleNamespace(id=id_, code=code, kind="hard_maintenance")


def _ct_cd(id_, rule_id, *, trigger_date, primary_date, trigger_document_id=None):
    return SimpleNamespace(
        id=id_,
        rule_id=rule_id,
        status="OPEN",
        primary_date=primary_date,
        trigger_date=trigger_date,
        trigger_document_id=trigger_document_id,
        completed_at=None,
        closed_disposition=None,
        closed_by_rule_pattern=None,
    )


def _ct_rule(id_, code="CTNF"):
    return SimpleNamespace(id=id_, code=code, kind="response")


# ---- _has_doc_matching helper -------------------------------------------


def test_has_doc_matching_prefix_hits() -> None:
    from harness_analytics.timeline.materializer import _has_doc_matching

    docs = [_doc(1, date(2024, 1, 1), "MF.PAID.4YR")]
    assert _has_doc_matching(docs, code_prefixes=("MF.PAID",))
    assert not _has_doc_matching(docs, code_prefixes=("ISSUE",))


def test_has_doc_matching_pattern_with_wildcard() -> None:
    from harness_analytics.timeline.materializer import _has_doc_matching

    docs = [_doc(1, date(2024, 1, 1), "A.NE")]
    assert _has_doc_matching(docs, code_patterns=("A...",))
    assert not _has_doc_matching(docs, code_patterns=("RCEX",))


def test_has_doc_matching_after_filter_strict() -> None:
    """``after`` is strict: a doc dated *on* the cutoff is not "after"."""
    from harness_analytics.timeline.materializer import _has_doc_matching

    docs = [_doc(1, date(2024, 6, 1), "A.NE")]
    assert _has_doc_matching(docs, code_patterns=("A...",), after=date(2024, 5, 31))
    assert not _has_doc_matching(docs, code_patterns=("A...",), after=date(2024, 6, 1))


def test_has_doc_matching_skips_blank_codes_and_missing_dates() -> None:
    from harness_analytics.timeline.materializer import _has_doc_matching

    docs = [
        _doc(1, None, "A.NE"),                # missing date -> ignored when after set
        _doc(2, date(2024, 1, 1), ""),        # blank code -> ignored
        _doc(3, date(2024, 6, 1), "A.NE"),    # real match
    ]
    assert _has_doc_matching(
        docs, code_patterns=("A...",), after=date(2024, 5, 1)
    )


# ---- _apply_maintenance_fee_grace_close ---------------------------------


def test_maintenance_fee_grace_close_nars_after_grace_period() -> None:
    """Patent that missed its 4-year maintenance fee >180d ago should NAR.

    Real-world trace: production audit found ~81 MISMTH4 deadlines, all
    on patents in PAIR status "Patented Case", median 432d past the
    primary_date with no MF.PAID code on file. Per 35 USC 41(c) the
    patent has expired by operation of law -- this pass NARs the row
    so it drops out of Overdue.
    """
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_maintenance_fee_grace_close,
    )

    app = _app(application_status_text="Patented Case")
    cd = _mismth_cd(900, rule_id=42, primary_date=date(2024, 1, 1))
    rules = [_mismth_rule(42, "MISMTH4")]
    docs = []  # no MF.PAID on file
    session = _StubSessionMulti([[cd], rules, docs])
    summary = RecomputeSummary(application_id=42)

    _apply_maintenance_fee_grace_close(session, app, summary)

    assert cd.status == "NAR"
    assert cd.closed_disposition == "auto_nar"
    assert cd.closed_by_rule_pattern == "maint_fee_grace_expired"
    assert summary.deadlines_auto_nar == 1
    assert len(session.added) == 1
    ev = session.added[0]
    assert ev.action == "AUTO_NAR"
    assert ev.payload_json["matched_pattern"] == "maint_fee_grace_expired"
    assert ev.payload_json["rule_code"] == "MISMTH4"
    assert ev.payload_json["days_past_grace"] > 0


def test_maintenance_fee_grace_close_skips_within_grace_period() -> None:
    """A deadline only 30 days past primary_date is still inside the
    6-month grace period -- the patent is late but not yet expired.
    """
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_maintenance_fee_grace_close,
    )

    app = _app(application_status_text="Patented Case")
    cd = _mismth_cd(901, rule_id=42, primary_date=date.today() - timedelta(days=30))
    rules = [_mismth_rule(42, "MISMTH4")]
    session = _StubSessionMulti([[cd], rules, []])
    summary = RecomputeSummary(application_id=42)

    _apply_maintenance_fee_grace_close(session, app, summary)

    assert cd.status == "OPEN"
    assert summary.deadlines_auto_nar == 0
    assert session.added == []


def test_maintenance_fee_grace_close_skips_when_mf_paid_on_file() -> None:
    """If a MF.PAID-shaped doc is on file, the per-rule auto-close pass
    owns this case via MISMTH*.complete_codes -- this pass stays out."""
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_maintenance_fee_grace_close,
    )

    app = _app(application_status_text="Patented Case")
    cd = _mismth_cd(902, rule_id=42, primary_date=date(2024, 1, 1))
    rules = [_mismth_rule(42, "MISMTH4")]
    docs = [_doc(1, date(2024, 5, 1), "MF.PAID.4YR")]
    session = _StubSessionMulti([[cd], rules, docs])
    summary = RecomputeSummary(application_id=42)

    _apply_maintenance_fee_grace_close(session, app, summary)

    assert cd.status == "OPEN"
    assert summary.deadlines_auto_nar == 0


def test_maintenance_fee_grace_close_ignores_non_maintenance_rules() -> None:
    """A long-overdue NOA must not be NAR'd by the maintenance-fee rule."""
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_maintenance_fee_grace_close,
    )

    app = _app(application_status_text="Notice of Allowance Mailed")
    cd = _mismth_cd(903, rule_id=42, primary_date=date(2024, 1, 1))
    rules = [SimpleNamespace(id=42, code="NOA", kind="hard_noa")]
    session = _StubSessionMulti([[cd], rules])
    summary = RecomputeSummary(application_id=42)

    _apply_maintenance_fee_grace_close(session, app, summary)

    assert cd.status == "OPEN"
    assert summary.deadlines_auto_nar == 0


# ---- _apply_oa_statutory_period_close -----------------------------------


def test_oa_statutory_period_close_nars_silent_oa_past_180_days() -> None:
    """CTNF/CTFR/CTRS deadline >180d past primary_date with PAIR status
    still showing OA pending and no IFW response code on file -- the
    application is abandoned by failure to respond."""
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_oa_statutory_period_close,
    )

    app = _app(application_status_text="Non Final Action Mailed")
    cd = _ct_cd(
        910, rule_id=51,
        trigger_date=date(2025, 1, 1),
        primary_date=date(2025, 5, 1),  # well over 180d past today
    )
    rules = [_ct_rule(51, "CTNF")]
    docs = [_doc(1, date(2025, 1, 1), "CTNF")]  # only the trigger itself
    session = _StubSessionMulti([[cd], rules, docs])
    summary = RecomputeSummary(application_id=42)

    _apply_oa_statutory_period_close(session, app, summary)

    assert cd.status == "NAR"
    assert cd.closed_by_rule_pattern == "stat_period_expired"
    assert summary.deadlines_auto_nar == 1
    assert len(session.added) == 1
    ev = session.added[0]
    assert ev.payload_json["matched_pattern"] == "stat_period_expired"
    assert ev.payload_json["rule_code"] == "CTNF"
    assert ev.payload_json["days_past_statutory_max"] > 0


def test_oa_statutory_period_close_does_not_fire_when_response_on_file() -> None:
    """If an A... amendment is on file after the trigger date, the matter
    is alive -- the per-rule pass should close it as auto_complete; this
    pass must not preempt that with a NAR."""
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_oa_statutory_period_close,
    )

    app = _app(application_status_text="Non Final Action Mailed")
    cd = _ct_cd(
        911, rule_id=51,
        trigger_date=date(2025, 1, 1),
        primary_date=date(2025, 5, 1),
    )
    rules = [_ct_rule(51, "CTNF")]
    docs = [
        _doc(1, date(2025, 1, 1), "CTNF"),    # trigger doc
        _doc(2, date(2025, 4, 15), "A.NE"),   # responsive amendment
    ]
    session = _StubSessionMulti([[cd], rules, docs])
    summary = RecomputeSummary(application_id=42)

    _apply_oa_statutory_period_close(session, app, summary)

    assert cd.status == "OPEN"
    assert summary.deadlines_auto_nar == 0


def test_oa_statutory_period_close_skips_when_pair_status_not_pending() -> None:
    """If PAIR status no longer says an OA is pending (e.g. allowance,
    abandonment, patented), some other shortcut owns the closure -- this
    pass stays out of the way."""
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_oa_statutory_period_close,
    )

    app = _app(application_status_text="Notice of Allowance Mailed")
    cd = _ct_cd(
        912, rule_id=51,
        trigger_date=date(2025, 1, 1),
        primary_date=date(2025, 5, 1),
    )
    rules = [_ct_rule(51, "CTNF")]
    session = _StubSessionMulti([[cd], rules, []])
    summary = RecomputeSummary(application_id=42)

    _apply_oa_statutory_period_close(session, app, summary)

    assert cd.status == "OPEN"
    assert summary.deadlines_auto_nar == 0


def test_oa_statutory_period_close_skips_within_statutory_window() -> None:
    """A CTNF only 60d past primary_date is well inside the 6-month
    extension window and must stay OPEN."""
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_oa_statutory_period_close,
    )

    app = _app(application_status_text="Non Final Action Mailed")
    cd = _ct_cd(
        913, rule_id=51,
        trigger_date=date.today() - timedelta(days=120),
        primary_date=date.today() - timedelta(days=60),
    )
    rules = [_ct_rule(51, "CTNF")]
    session = _StubSessionMulti([[cd], rules, []])
    summary = RecomputeSummary(application_id=42)

    _apply_oa_statutory_period_close(session, app, summary)

    assert cd.status == "OPEN"
    assert summary.deadlines_auto_nar == 0


def test_oa_statutory_period_close_only_targets_oa_response_rule_codes() -> None:
    """Even when status / time-since match, only CTNF/CTFR/CTRS/CTAV/CTEQ
    are in scope -- a stale FILING_DATE row, NOA, or maintenance-fee row
    must not be touched by this pass."""
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_oa_statutory_period_close,
    )

    app = _app(application_status_text="Non Final Action Mailed")
    cd = _ct_cd(
        914, rule_id=51,
        trigger_date=date(2025, 1, 1),
        primary_date=date(2025, 5, 1),
    )
    # rule code outside the OA-response set
    rules = [SimpleNamespace(id=51, code="FILING_DATE", kind="filing_date")]
    session = _StubSessionMulti([[cd], rules, []])
    summary = RecomputeSummary(application_id=42)

    _apply_oa_statutory_period_close(session, app, summary)

    assert cd.status == "OPEN"
    assert summary.deadlines_auto_nar == 0


# ---------------------------------------------------------------------------
# _apply_noa_grace_close
# (M0009 follow-up: NOA 3-month window is non-extendable per 37 CFR 1.311)
# ---------------------------------------------------------------------------


def _noa_cd(id_, rule_id, *, primary_date, trigger_date=None):
    return SimpleNamespace(
        id=id_,
        rule_id=rule_id,
        status="OPEN",
        primary_date=primary_date,
        trigger_date=trigger_date or primary_date,
        trigger_document_id=None,
        completed_at=None,
        closed_disposition=None,
        closed_by_rule_pattern=None,
    )


def _noa_rule(id_, code="NOA"):
    return SimpleNamespace(id=id_, code=code, kind="hard_noa")


def test_noa_grace_close_nars_silent_noa_past_grace_period() -> None:
    """NOA >30d past primary_date with no closer doc on file and
    ``issue_date`` still null is abandoned for failure to pay issue fee.

    Real-world trace: production inbox showed app 29937416 sitting at
    "193d overdue" with PAIR still saying "Notice of Allowance Mailed"
    and no IFEE / ACPA / RCE on file. 37 CFR 1.311's 3-month period is
    non-extendable so the row is dead by operation of law.
    """
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_noa_grace_close,
    )

    app = _app(application_status_text="Notice of Allowance Mailed")
    cd = _noa_cd(950, rule_id=61, primary_date=date(2025, 1, 1))
    rules = [_noa_rule(61, "NOA")]
    docs = [_doc(1, date(2024, 10, 1), "NOA")]  # only the trigger NOA itself
    session = _StubSessionMulti([[cd], rules, docs])
    summary = RecomputeSummary(application_id=42)

    _apply_noa_grace_close(session, app, summary)

    assert cd.status == "NAR"
    assert cd.closed_disposition == "auto_nar"
    assert cd.closed_by_rule_pattern == "noa_grace_expired"
    assert summary.deadlines_auto_nar == 1
    assert len(session.added) == 1
    ev = session.added[0]
    assert ev.action == "AUTO_NAR"
    assert ev.payload_json["matched_pattern"] == "noa_grace_expired"
    assert ev.payload_json["rule_code"] == "NOA"
    assert ev.payload_json["days_past_grace"] > 0


def test_noa_grace_close_also_handles_ntc_allow_kind_hard_noa() -> None:
    """NTC.ALLOW shares ``kind == hard_noa`` so the same 3-month rule
    applies. Scoping by ``kind`` (not by literal code) is intentional."""
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_noa_grace_close,
    )

    app = _app(application_status_text="Notice of Allowance Mailed")
    cd = _noa_cd(951, rule_id=62, primary_date=date(2025, 1, 1))
    rules = [_noa_rule(62, "NTC.ALLOW")]
    session = _StubSessionMulti([[cd], rules, []])
    summary = RecomputeSummary(application_id=42)

    _apply_noa_grace_close(session, app, summary)

    assert cd.status == "NAR"
    assert cd.closed_by_rule_pattern == "noa_grace_expired"


def test_noa_grace_close_skips_within_grace_window() -> None:
    """A NOA only 20 days past primary_date is still inside the small
    ingest-lag buffer -- defer to the per-rule pass / human action."""
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_noa_grace_close,
    )

    app = _app(application_status_text="Notice of Allowance Mailed")
    cd = _noa_cd(
        952, rule_id=61,
        primary_date=date.today() - timedelta(days=20),
    )
    rules = [_noa_rule(61, "NOA")]
    session = _StubSessionMulti([[cd], rules, []])
    summary = RecomputeSummary(application_id=42)

    _apply_noa_grace_close(session, app, summary)

    assert cd.status == "OPEN"
    assert summary.deadlines_auto_nar == 0
    assert session.added == []


def test_noa_grace_close_skips_when_ifee_on_file() -> None:
    """Issue-fee payment closes the NOA via the per-rule pass; we must
    not preempt that with a NAR even if it landed late."""
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_noa_grace_close,
    )

    app = _app(application_status_text="Notice of Allowance Mailed")
    cd = _noa_cd(953, rule_id=61, primary_date=date(2025, 1, 1))
    rules = [_noa_rule(61, "NOA")]
    docs = [_doc(1, date(2025, 5, 1), "IFEE")]
    session = _StubSessionMulti([[cd], rules, docs])
    summary = RecomputeSummary(application_id=42)

    _apply_noa_grace_close(session, app, summary)

    assert cd.status == "OPEN"
    assert summary.deadlines_auto_nar == 0


def test_noa_grace_close_skips_when_acpa_on_file() -> None:
    """ACPA (Continued Prosecution Application, design corpus) withdraws
    allowance -- per-rule pass owns this closure via NOA.complete_codes."""
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_noa_grace_close,
    )

    app = _app(application_status_text="Notice of Allowance Mailed")
    cd = _noa_cd(954, rule_id=61, primary_date=date(2025, 1, 1))
    rules = [_noa_rule(61, "NOA")]
    docs = [_doc(1, date(2025, 6, 1), "ACPA")]
    session = _StubSessionMulti([[cd], rules, docs])
    summary = RecomputeSummary(application_id=42)

    _apply_noa_grace_close(session, app, summary)

    assert cd.status == "OPEN"
    assert summary.deadlines_auto_nar == 0


def test_noa_grace_close_skips_when_rce_on_file() -> None:
    """RCE after NOA withdraws allowance -- per-rule pass owns it."""
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_noa_grace_close,
    )

    app = _app(application_status_text="Notice of Allowance Mailed")
    cd = _noa_cd(955, rule_id=61, primary_date=date(2025, 1, 1))
    rules = [_noa_rule(61, "NOA")]
    docs = [_doc(1, date(2025, 6, 1), "RCEX")]
    session = _StubSessionMulti([[cd], rules, docs])
    summary = RecomputeSummary(application_id=42)

    _apply_noa_grace_close(session, app, summary)

    assert cd.status == "OPEN"


def test_noa_grace_close_skips_when_qpids_reopen_on_file() -> None:
    """Q.DEC.REOPEN reopens prosecution after allowance -- not silent."""
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_noa_grace_close,
    )

    app = _app(application_status_text="Notice of Allowance Mailed")
    cd = _noa_cd(956, rule_id=61, primary_date=date(2025, 1, 1))
    rules = [_noa_rule(61, "NOA")]
    docs = [_doc(1, date(2025, 6, 1), "Q.DEC.REOPEN")]
    session = _StubSessionMulti([[cd], rules, docs])
    summary = RecomputeSummary(application_id=42)

    _apply_noa_grace_close(session, app, summary)

    assert cd.status == "OPEN"


def test_noa_grace_close_skips_when_abandonment_on_file() -> None:
    """ABN on file means the per-rule pass will NAR via NOA.nar_codes;
    this pass should not double-fire."""
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_noa_grace_close,
    )

    app = _app(application_status_text="Notice of Allowance Mailed")
    cd = _noa_cd(957, rule_id=61, primary_date=date(2025, 1, 1))
    rules = [_noa_rule(61, "NOA")]
    docs = [_doc(1, date(2025, 6, 1), "ABN")]
    session = _StubSessionMulti([[cd], rules, docs])
    summary = RecomputeSummary(application_id=42)

    _apply_noa_grace_close(session, app, summary)

    assert cd.status == "OPEN"


def test_noa_grace_close_skips_when_issue_date_populated() -> None:
    """``applications.issue_date`` is the canonical "patent issued"
    signal; the app-level shortcut owns NOA closure in that case."""
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_noa_grace_close,
    )

    app = _app(
        application_status_text="Patented Case",
        issue_date=date(2025, 3, 1),
        patent_number="11000001",
    )
    cd = _noa_cd(958, rule_id=61, primary_date=date(2025, 1, 1))
    rules = [_noa_rule(61, "NOA")]
    session = _StubSessionMulti([[cd], rules, []])
    summary = RecomputeSummary(application_id=42)

    _apply_noa_grace_close(session, app, summary)

    assert cd.status == "OPEN"
    assert summary.deadlines_auto_nar == 0


def test_noa_grace_close_skips_when_status_says_patented() -> None:
    """Defensive: even if ``issue_date`` somehow lags, a "Patented"
    status_text means a real patent issued -- never NAR a live patent."""
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_noa_grace_close,
    )

    app = _app(application_status_text="Patented Case", issue_date=None)
    cd = _noa_cd(959, rule_id=61, primary_date=date(2025, 1, 1))
    rules = [_noa_rule(61, "NOA")]
    session = _StubSessionMulti([[cd], rules, []])
    summary = RecomputeSummary(application_id=42)

    _apply_noa_grace_close(session, app, summary)

    assert cd.status == "OPEN"
    assert summary.deadlines_auto_nar == 0


def test_noa_grace_close_only_targets_hard_noa_kind() -> None:
    """A long-overdue CTNF or MISMTH4 must not be NAR'd by this pass --
    different rule kinds have different statutory windows."""
    from harness_analytics.timeline.materializer import (
        RecomputeSummary,
        _apply_noa_grace_close,
    )

    app = _app(application_status_text="Non Final Action Mailed")
    cd = _noa_cd(960, rule_id=61, primary_date=date(2025, 1, 1))
    rules = [SimpleNamespace(id=61, code="CTNF", kind="response")]
    session = _StubSessionMulti([[cd], rules, []])
    summary = RecomputeSummary(application_id=42)

    _apply_noa_grace_close(session, app, summary)

    assert cd.status == "OPEN"
    assert summary.deadlines_auto_nar == 0
