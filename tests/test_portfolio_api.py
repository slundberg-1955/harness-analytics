"""Portfolio API: response shape + filter/sort pushdown via a fake DB session."""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

pytest.importorskip("fastapi")
from starlette.testclient import TestClient


# Fixture portfolio: 3 rows, varied status / dates / OA counts.
_FIXTURE_ROWS: list[dict[str, Any]] = [
    {
        "application_id": 1,
        "invention_title": "Sample Invention",
        "application_number": "17552591",
        "application_status_code": 150,
        "application_status_text": "Patented Case",
        "filing_date": date(2022, 1, 15),
        "issue_date": date(2025, 3, 1),
        "patent_number": "US12000000",
        "customer_number": "15639",
        "hdp_customer_number": "15639",
        "group_art_unit": "2100",
        "patent_class": "606",
        "examiner_name": "Jane Examiner",
        "assignee_name": "Acme Corp.",
        "is_continuation": False,
        "has_restriction_ctrs_count": 0,
        "ifw_a_ne_count": 0,
        "nonfinal_oa_count": 1,
        "final_oa_count": 1,
        "total_substantive_oas": 2,
        "first_noa_date": date(2024, 4, 1),
        "had_examiner_interview": True,
        "interview_count": 1,
        "noa_within_90_days_of_interview": True,
        "days_last_interview_to_noa": 60,
        "rce_count": 0,
        "days_filing_to_first_oa": 500,
        "days_filing_to_noa": 807,
        "days_filing_to_issue": 1141,
        "is_jac": False,
        "office_name": "DC",
        "updated_at": datetime(2026, 4, 17, 10, 0, tzinfo=timezone.utc),
    },
    {
        "application_id": 2,
        "application_number": "18649980",
        "invention_title": "Thin-Film Force Sensor",
        "application_status_code": 93,
        "application_status_text": "Notice of Allowance Mailed",
        "filing_date": date(2024, 4, 29),
        "issue_date": None,
        "patent_number": None,
        "customer_number": "31561",
        "hdp_customer_number": "31561",
        "group_art_unit": "2855",
        "patent_class": "073",
        "examiner_name": "Jamel E. Williams",
        "assignee_name": "Southeast University",
        "is_continuation": True,
        "has_restriction_ctrs_count": 0,
        "ifw_a_ne_count": 0,
        "nonfinal_oa_count": 0,
        "final_oa_count": 0,
        "total_substantive_oas": 0,
        "first_noa_date": date(2026, 4, 10),
        "had_examiner_interview": False,
        "interview_count": 0,
        "noa_within_90_days_of_interview": False,
        "days_last_interview_to_noa": None,
        "rce_count": 0,
        "days_filing_to_first_oa": None,
        "days_filing_to_noa": 712,
        "days_filing_to_issue": None,
        "is_jac": False,
        "office_name": None,
        "updated_at": datetime(2026, 4, 10, 10, 0, tzinfo=timezone.utc),
    },
    {
        "application_id": 3,
        "application_number": "17000001",
        "invention_title": "Abandoned Thing",
        "application_status_code": 161,
        "application_status_text": "Abandoned",
        "filing_date": date(2020, 5, 1),
        "issue_date": None,
        "patent_number": None,
        "customer_number": "15639",
        "hdp_customer_number": "15639",
        "group_art_unit": "2100",
        "patent_class": "606",
        "examiner_name": "Jane Examiner",
        "assignee_name": "Acme Corp.",
        "is_continuation": False,
        "has_restriction_ctrs_count": 0,
        "ifw_a_ne_count": 0,
        "nonfinal_oa_count": 2,
        "final_oa_count": 1,
        "total_substantive_oas": 3,
        "first_noa_date": None,
        "had_examiner_interview": False,
        "interview_count": 0,
        "noa_within_90_days_of_interview": False,
        "days_last_interview_to_noa": None,
        "rce_count": 3,
        "days_filing_to_first_oa": None,
        "days_filing_to_noa": None,
        "days_filing_to_issue": None,
        "is_jac": False,
        "office_name": "DC",
        "updated_at": datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
    },
]


class _FakeResult:
    """Mimics the subset of SQLAlchemy Result used by portfolio_api._fetch_rows."""

    def __init__(self, rows: list[dict[str, Any]]):
        self._rows = rows
        self._cols = list(rows[0].keys()) if rows else []

    def keys(self):
        return self._cols

    def fetchall(self):
        return [tuple(r[c] for c in self._cols) for r in self._rows]


class _FakeSession:
    """DB dependency override: filter the in-memory fixture by the SQL body."""

    def __init__(
        self,
        rows: list[dict[str, Any]],
        *,
        ifw_docs: list[tuple[int, str, Any]] | None = None,
        responses: list[tuple[int, Any]] | None = None,
    ):
        self.all_rows = rows
        # Per-test injectable fixtures for the CTNF outcome chart.
        # ifw_docs: (application_id, document_code, mail_room_date)
        # responses: (application_id, transaction_date)
        self._ifw_docs = ifw_docs or []
        self._responses = responses or []

    def execute(self, statement, binds: dict[str, Any] | None = None):
        sql = str(statement)

        # CTNF chart sub-queries (file_wrapper_documents + prosecution_events).
        # These are scoped by application_id = ANY(:ids); the binds carry the
        # ID list. We pre-canned columns so _FakeResult can iterate cleanly.
        if "FROM file_wrapper_documents" in sql:
            ids = set(binds.get("ids") or []) if binds else set()
            filtered = [(a, c, d) for (a, c, d) in self._ifw_docs if a in ids]
            return _FakeResult(
                [
                    {"application_id": a, "document_code": c, "mail_room_date": d}
                    for (a, c, d) in filtered
                ]
            )
        if "FROM prosecution_events" in sql:
            ids = set(binds.get("ids") or []) if binds else set()
            filtered = [(a, d) for (a, d) in self._responses if a in ids]
            return _FakeResult(
                [
                    {"application_id": a, "transaction_date": d}
                    for (a, d) in filtered
                ]
            )

        rows = list(self.all_rows)

        def _eq_list_filter(col: str, placeholder_prefix: str, value_cast=str):
            values = [
                value_cast(v) for k, v in (binds or {}).items() if k.startswith(placeholder_prefix)
            ]
            if not values:
                return rows
            return [r for r in rows if r.get(col) in values]

        if " WHERE " in sql or " AND " in sql:
            if "LOWER(COALESCE(invention_title" in sql and binds and "q" in binds:
                needle = binds["q"].replace("%", "").lower()
                rows = [
                    r for r in rows
                    if needle in (r.get("invention_title") or "").lower()
                    or needle in (r.get("examiner_name") or "").lower()
                    or needle in (r.get("assignee_name") or "").lower()
                    or needle in (r.get("application_number") or "").lower()
                ]
            if "application_status_code IN" in sql:
                rows = _eq_list_filter("application_status_code", "status_", int)
            if "issue_year IN" in sql:
                # Fixture rows don't carry issue_year explicitly; compute.
                wanted = {int(v) for k, v in (binds or {}).items() if k.startswith("year_")}
                rows = [
                    r for r in rows
                    if r.get("issue_date") and r["issue_date"].year in wanted
                ]
            if "group_art_unit IN" in sql:
                rows = _eq_list_filter("group_art_unit", "au_")
            if "examiner_name IN" in sql:
                rows = _eq_list_filter("examiner_name", "ex_")
            if "had_examiner_interview = TRUE" in sql:
                rows = [r for r in rows if r.get("had_examiner_interview")]
            if "had_examiner_interview = FALSE" in sql:
                rows = [r for r in rows if not r.get("had_examiner_interview")]
            if "rce_count = :rce_eq" in sql and binds and "rce_eq" in binds:
                rows = [r for r in rows if r.get("rce_count") == binds["rce_eq"]]
            if "rce_count >= 3" in sql:
                rows = [r for r in rows if (r.get("rce_count") or 0) >= 3]

        # Order (NULLS LAST mirrors the real SQL).
        rev = " DESC " in sql.upper()
        if "ORDER BY days_filing_to_noa" in sql:
            rows.sort(
                key=lambda r: (
                    r.get("days_filing_to_noa") is None,
                    -(r.get("days_filing_to_noa") or 0) if rev else (r.get("days_filing_to_noa") or 0),
                ),
            )
        else:
            rows.sort(key=lambda r: r.get("application_number") or "", reverse=rev)

        # Honor `LIMIT N` so cap behavior is exercised end-to-end.
        m = re.search(r"LIMIT\s+(\d+)\s*$", sql)
        if m:
            rows = rows[: int(m.group(1))]

        return _FakeResult(rows)


def _make_client(
    monkeypatch: pytest.MonkeyPatch,
    rows: list[dict[str, Any]],
    *,
    ifw_docs: list[tuple[int, str, Any]] | None = None,
    responses: list[tuple[int, Any]] | None = None,
) -> TestClient:
    monkeypatch.setenv("PORTAL_PASSWORD", "test-pw")
    monkeypatch.setenv("SECRET_KEY", "unit-test-secret-key-32-chars-minimum!!")
    from harness_analytics.db import get_db
    from harness_analytics.server import create_app

    app = create_app()

    def override():
        yield _FakeSession(rows, ifw_docs=ifw_docs, responses=responses)

    app.dependency_overrides[get_db] = override
    return TestClient(app)


def test_portfolio_response_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(monkeypatch, _FIXTURE_ROWS)
    r = client.get("/portal/api/portfolio", auth=("viewer", "test-pw"))
    assert r.status_code == 200
    body = r.json()
    assert set(body.keys()) >= {"rows", "total", "kpis", "charts", "statusPill"}
    assert body["total"] == 3
    assert len(body["rows"]) == 3
    row = body["rows"][0]
    # Spec-required row fields:
    for key in ("applicationNumber", "inventionTitle", "applicationStatusCode", "filingDate",
                "nonfinalOaCount", "finalOaCount", "isContinuation", "rceCount",
                "daysFilingToNoa", "applicationStatusLabel", "applicationStatusTone"):
        assert key in row, f"missing key {key}"
    # Server-side application_id must NOT leak into the row payload.
    assert "applicationId" not in row
    assert "application_id" not in row
    # CTNF response speed chart is always present, even with no events.
    assert "ctnfResponseSpeed" in body["charts"]
    cs = body["charts"]["ctnfResponseSpeed"]
    assert cs["totalEvents"] == 0
    assert len(cs["buckets"]) == 6


def test_portfolio_kpis_computed_against_filtered_set(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(monkeypatch, _FIXTURE_ROWS)
    r = client.get("/portal/api/portfolio?status=150", auth=("viewer", "test-pw"))
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 1
    k = body["kpis"]
    assert k["totalApps"] == 1
    assert k["patentedCount"] == 1
    # 1 patented / (1 + 0 abandoned) = 100%.
    assert k["allowanceRatePct"] == 100.0


def test_portfolio_sort_days_to_noa_desc(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(monkeypatch, _FIXTURE_ROWS)
    r = client.get(
        "/portal/api/portfolio?sort=daysFilingToNoa&dir=desc",
        auth=("viewer", "test-pw"),
    )
    assert r.status_code == 200
    rows = r.json()["rows"]
    # Non-null days come first in desc order; null tail.
    days = [row["daysFilingToNoa"] for row in rows]
    assert days[0] == 807  # largest
    assert days[-1] is None


def test_portfolio_rejects_bad_page_size(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(monkeypatch, _FIXTURE_ROWS)
    r = client.get("/portal/api/portfolio?pageSize=10000", auth=("viewer", "test-pw"))
    assert r.status_code == 422


def test_portfolio_aggregate_row_cap_default_in_response(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PORTFOLIO_AGG_ROW_CAP", raising=False)
    client = _make_client(monkeypatch, _FIXTURE_ROWS)
    r = client.get("/portal/api/portfolio", auth=("viewer", "test-pw"))
    body = r.json()
    assert body["aggregateRowCap"] == 5000
    assert body["capped"] is False


def test_portfolio_aggregate_row_cap_env_override_marks_capped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Cap below the fixture size so the response should report `capped: true`.
    monkeypatch.setenv("PORTFOLIO_AGG_ROW_CAP", "2")
    client = _make_client(monkeypatch, _FIXTURE_ROWS)
    r = client.get("/portal/api/portfolio", auth=("viewer", "test-pw"))
    body = r.json()
    assert body["aggregateRowCap"] == 2
    assert body["total"] == 2
    assert body["capped"] is True
    # KPI math is computed against the capped set, not the original fixture.
    assert body["kpis"]["totalApps"] == 2


def test_portfolio_aggregate_row_cap_disabled_with_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PORTFOLIO_AGG_ROW_CAP", "0")
    client = _make_client(monkeypatch, _FIXTURE_ROWS)
    r = client.get("/portal/api/portfolio", auth=("viewer", "test-pw"))
    body = r.json()
    assert body["aggregateRowCap"] is None
    assert body["capped"] is False
    assert body["total"] == 3


def test_portfolio_aggregate_row_cap_invalid_env_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PORTFOLIO_AGG_ROW_CAP", "not-a-number")
    from harness_analytics.portfolio_api import _aggregate_row_cap

    assert _aggregate_row_cap() == 5000


def test_portfolio_csv_streams(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(monkeypatch, _FIXTURE_ROWS)
    r = client.get("/portal/api/portfolio.csv", auth=("viewer", "test-pw"))
    assert r.status_code == 200
    text = r.text
    lines = [ln for ln in text.splitlines() if ln.strip()]
    # 1 header + 3 rows
    assert len(lines) == 4
    assert lines[0].startswith("application_number,")
    # Server-side application_id must NOT appear in the CSV header.
    assert ",application_id" not in lines[0]
    assert "application_id" not in lines[0].split(",")


def test_portfolio_ctnf_response_speed_chart_aggregates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end: seed CTNF + response + NOA mail dates and confirm the
    chart payload reflects per-CTNF outcomes for the filtered row set."""
    ifw = [
        # App 1: one CTNF, fast response, then NOA -> "allowed" in 0-30d.
        (1, "CTNF", date(2024, 1, 1)),
        (1, "NOA", date(2024, 4, 1)),
        # App 2: one CTNF, slow response, followed by another CTNF -> "rejected"
        # in 121-180d.
        (2, "CTNF", date(2023, 1, 1)),
        (2, "CTNF", date(2023, 9, 1)),
    ]
    responses = [
        (1, date(2024, 1, 20)),  # 19 days post-CTNF -> bucket 0-30
        (2, date(2023, 5, 15)),  # 134 days post-CTNF -> bucket 121-180
    ]
    client = _make_client(
        monkeypatch, _FIXTURE_ROWS, ifw_docs=ifw, responses=responses
    )
    r = client.get("/portal/api/portfolio", auth=("viewer", "test-pw"))
    assert r.status_code == 200
    cs = r.json()["charts"]["ctnfResponseSpeed"]
    assert cs["totalEvents"] == 2
    assert cs["totalAllowed"] == 1
    assert cs["totalRejected"] == 1
    assert cs["overallAllowedPct"] == 50.0
    by_label = {b["label"]: b for b in cs["buckets"]}
    assert by_label["0\u201330d"]["allowed"] == 1
    assert by_label["0\u201330d"]["responses"] == 1
    assert by_label["121\u2013180d"]["rejected"] == 1
    assert by_label["121\u2013180d"]["responses"] == 1


def test_portfolio_ctnf_response_speed_chart_respects_status_filter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the user filters the table to status=150, the chart must only
    include CTNF events for applications still in the filtered row set
    (the chart is per-row-set, not global)."""
    ifw = [
        (1, "CTNF", date(2024, 1, 1)),
        (1, "NOA", date(2024, 4, 1)),
        (3, "CTNF", date(2024, 1, 1)),  # row 3 is status=161 (Abandoned)
        (3, "NOA", date(2024, 4, 1)),
    ]
    responses = [
        (1, date(2024, 1, 20)),
        (3, date(2024, 1, 20)),
    ]
    client = _make_client(
        monkeypatch, _FIXTURE_ROWS, ifw_docs=ifw, responses=responses
    )
    r = client.get(
        "/portal/api/portfolio?status=150", auth=("viewer", "test-pw")
    )
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 1  # only app 1 (status 150) survives the filter
    cs = body["charts"]["ctnfResponseSpeed"]
    # App 3's CTNF must be excluded from the chart.
    assert cs["totalEvents"] == 1
    assert cs["totalAllowed"] == 1
