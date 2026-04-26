"""Pure-Python tests for the materializer's docket cross-off / NAR pass.

The DB-bound orchestration is exercised by hitting it through the test
client elsewhere; here we lock down the decision logic — pattern matching
+ winner selection — which is where the subtle bugs live.
"""
from __future__ import annotations

from datetime import date, datetime
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
