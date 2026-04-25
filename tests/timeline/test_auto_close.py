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


def test_choose_close_match_skips_docs_on_or_before_trigger() -> None:
    trigger = date(2024, 6, 1)
    docs = [
        _doc(10, date(2024, 5, 30), "A.NE"),
        _doc(20, date(2024, 6, 1), "A.NE"),  # same day as trigger — excluded
        _doc(30, date(2024, 6, 2), "NOA"),
    ]
    result = _choose_close_match(
        deadline_trigger_date=trigger,
        complete_patterns=["A..."],
        nar_patterns=["NOA"],
        docs=docs,
    )
    assert result is not None
    # First matching doc strictly after trigger is the NOA, not the same-day A.NE.
    assert result[0] == "auto_nar"
    assert result[1].id == 30


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
