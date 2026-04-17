"""Tests for heuristic OA/CTRS extension-of-time bucket counts."""

from datetime import date

from harness_analytics.extension_metrics import compute_extension_time_counts


class _Evt:
    __slots__ = ("transaction_date", "event_type", "seq_order")

    def __init__(self, transaction_date: date, event_type: str, seq_order: int = 0) -> None:
        self.transaction_date = transaction_date
        self.event_type = event_type
        self.seq_order = seq_order


class _Doc:
    __slots__ = ("document_code", "mail_room_date", "id")

    def __init__(self, document_code: str, mail_room_date: date, id_: int = 1) -> None:
        self.document_code = document_code
        self.mail_room_date = mail_room_date
        self.id = id_


def _sum_ext(out: dict[str, int]) -> int:
    return sum(out.values())


def test_oa_on_time_no_extension_buckets() -> None:
    oa = _Doc("CTNF", date(2024, 1, 1), 1)
    ev = _Evt(date(2024, 3, 15), "RESPONSE_NONFINAL")
    out = compute_extension_time_counts([oa], [], [oa], [ev], date(2025, 1, 1))
    assert _sum_ext(out) == 0


def test_ctnf_late_12_days_first_bucket() -> None:
    oa = _Doc("CTNF", date(2024, 1, 1), 1)
    ev = _Evt(date(2024, 4, 13), "RESPONSE_NONFINAL")
    out = compute_extension_time_counts([oa], [], [oa], [ev], date(2025, 1, 1))
    assert out["ctnf_1mo"] == 1
    assert out["ctnf_2mo"] == out["ctnf_3mo"] == out["ctfr_1mo"] == 0


def test_ctfr_late_45_days_second_bucket() -> None:
    oa = _Doc("CTFR", date(2024, 1, 1), 1)
    ev = _Evt(date(2024, 5, 16), "RESPONSE_FINAL")
    out = compute_extension_time_counts([], [oa], [oa], [ev], date(2025, 1, 1))
    assert out["ctfr_2mo"] == 1
    assert out["ctfr_1mo"] == out["ctfr_3mo"] == 0


def test_oa_horizon_ignores_response_after_next_oa() -> None:
    oa1 = _Doc("CTNF", date(2024, 1, 1), 1)
    oa2 = _Doc("CTNF", date(2024, 6, 1), 2)
    ev = _Evt(date(2024, 6, 10), "RESPONSE_NONFINAL")
    out = compute_extension_time_counts([oa1, oa2], [], [oa1, oa2], [ev], date(2025, 1, 1))
    assert _sum_ext(out) == 0


def test_oa_horizon_counts_response_before_next_oa_when_late() -> None:
    oa1 = _Doc("CTNF", date(2024, 1, 1), 1)
    oa2 = _Doc("CTNF", date(2024, 8, 1), 2)
    ev = _Evt(date(2024, 5, 1), "RESPONSE_NONFINAL")
    out = compute_extension_time_counts([oa1, oa2], [], [oa1, oa2], [ev], date(2025, 1, 1))
    assert out["ctnf_1mo"] == 1


def test_ctrs_two_month_deadline_bucket() -> None:
    ctrs = _Doc("CTRS", date(2024, 1, 1), 1)
    ev = _Evt(date(2024, 3, 20), "RESPONSE_NONFINAL")
    out = compute_extension_time_counts([], [], [ctrs], [ev], date(2025, 1, 1))
    assert out["ctrs_1mo"] == 1
    assert out["ctrs_2mo"] == out["ctrs_3mo"] == 0


def test_ctrs_over_90_days_late_not_counted() -> None:
    ctrs = _Doc("CTRS", date(2024, 1, 1), 1)
    ev = _Evt(date(2024, 6, 9), "RCE")
    out = compute_extension_time_counts([], [], [ctrs], [ev], date(2025, 1, 1))
    assert _sum_ext(out) == 0
