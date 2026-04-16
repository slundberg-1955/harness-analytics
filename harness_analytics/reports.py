"""SQL → pandas report helpers."""

from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

# Human-readable Excel / portal headers (database column names unchanged).
ANALYTICS_REPORT_HEADER_LABELS: dict[str, str] = {
    "interview_led_to_noa": "NOA WITHIN 90 DAYS OF INTERVIEW",
    "days_interview_to_noa": "DAYS LAST INTERVIEW TO NOA",
    "ifw_a_ne_count": "IFW A.NE COUNT",
}


def analytics_column_header(db_column_name: str) -> str:
    return ANALYTICS_REPORT_HEADER_LABELS.get(
        db_column_name, str(db_column_name).upper().replace("_", " ")
    )


BASE_QUERY = """
SELECT
    a.application_number,
    a.invention_title,
    a.filing_date,
    a.issue_date,
    a.issue_year,
    a.patent_number,
    a.customer_number,
    a.hdp_customer_number,
    a.group_art_unit,
    a.patent_class,
    a.examiner_first_name || ' ' || a.examiner_last_name AS examiner_name,
    a.assignee_name,
    aa.nonfinal_oa_count,
    aa.final_oa_count,
    aa.total_substantive_oas,
    aa.first_noa_date,
    aa.had_examiner_interview,
    aa.interview_count,
    aa.interview_led_to_noa,
    aa.days_interview_to_noa,
    aa.rce_count,
    aa.days_filing_to_first_oa,
    aa.days_filing_to_noa,
    aa.days_filing_to_issue,
    aa.billing_attorney_reg,
    aa.billing_attorney_name,
    aa.is_jac,
    aa.office_name
FROM applications a
JOIN application_analytics aa ON aa.application_id = a.id
WHERE a.issue_year IN (2024, 2025)
  AND a.application_status_code = '150'
"""

# Same column list as BASE_QUERY, without issue-year / status filters (portal testing).
SPREADSHEET_ROW_QUERY = """
SELECT
    a.application_number,
    a.invention_title,
    a.filing_date,
    a.issue_date,
    a.issue_year,
    a.patent_number,
    a.customer_number,
    a.hdp_customer_number,
    a.group_art_unit,
    a.patent_class,
    a.examiner_first_name || ' ' || a.examiner_last_name AS examiner_name,
    a.assignee_name,
    aa.nonfinal_oa_count,
    aa.final_oa_count,
    aa.total_substantive_oas,
    aa.first_noa_date,
    aa.had_examiner_interview,
    aa.interview_count,
    aa.interview_led_to_noa,
    aa.days_interview_to_noa,
    aa.rce_count,
    aa.days_filing_to_first_oa,
    aa.days_filing_to_noa,
    aa.days_filing_to_issue,
    aa.billing_attorney_reg,
    aa.billing_attorney_name,
    aa.is_jac,
    aa.office_name,
    aa.ifw_a_ne_count
FROM applications a
JOIN application_analytics aa ON aa.application_id = a.id
WHERE a.application_number = :application_number
"""


def _read_df(db: Session, sql: str) -> pd.DataFrame:
    """Run SELECT and build a DataFrame (avoids older pandas/SQLAlchemy2 read_sql quirks)."""
    result = db.execute(text(sql))
    columns = list(result.keys())
    rows = result.fetchall()
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame([tuple(r) for r in rows], columns=columns)


def report_all_harness(db: Session) -> pd.DataFrame:
    """All issued applications 2024–2025 with analytics."""
    return _read_df(db, BASE_QUERY)


def report_spreadsheet_row_for_application(db: Session, application_number: str) -> pd.DataFrame:
    """One-row DataFrame matching Excel 'All Harness IP' columns, or empty if no analytics row."""
    result = db.execute(text(SPREADSHEET_ROW_QUERY), {"application_number": application_number})
    columns = list(result.keys())
    rows = result.fetchall()
    if not rows:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame([tuple(r) for r in rows], columns=columns)


def report_by_office(db: Session) -> dict[str, pd.DataFrame]:
    """Same base query, split by office_name."""
    df = report_all_harness(db)
    if df.empty:
        return {}
    return {str(office): grp for office, grp in df.groupby("office_name", dropna=False)}


def report_dc_office(db: Session) -> pd.DataFrame:
    q = BASE_QUERY + " AND aa.office_name = 'DC'"
    return _read_df(db, q)


def report_specific_clients(db: Session, customer_numbers: list[str]) -> pd.DataFrame:
    """Filter by Harness HDP customer identifiers (matches hdp_customer_number or customer_number)."""
    nums = ", ".join(f"'{n}'" for n in customer_numbers)
    q = (
        BASE_QUERY
        + f" AND (a.hdp_customer_number IN ({nums}) OR a.customer_number IN ({nums}))"
    )
    return _read_df(db, q)


def report_interview_to_noa(db: Session) -> pd.DataFrame:
    q = BASE_QUERY + " AND aa.had_examiner_interview = TRUE"
    df = _read_df(db, q)
    if df.empty:
        return df
    return df.sort_values("days_interview_to_noa", na_position="last")


def report_art_unit_summary(db: Session) -> pd.DataFrame:
    q = """
    SELECT
        a.group_art_unit,
        a.issue_year,
        COUNT(*) AS application_count,
        ROUND(AVG(aa.total_substantive_oas), 2) AS avg_oas_before_noa,
        ROUND(AVG(aa.nonfinal_oa_count), 2)     AS avg_nonfinal_oas,
        ROUND(AVG(aa.final_oa_count), 2)       AS avg_final_oas,
        SUM(CASE WHEN aa.is_jac THEN 1 ELSE 0 END) AS jac_applications,
        ROUND(AVG(aa.days_filing_to_noa), 0)   AS avg_days_to_noa
    FROM applications a
    JOIN application_analytics aa ON aa.application_id = a.id
    WHERE a.issue_year IN (2024, 2025)
      AND a.application_status_code = '150'
    GROUP BY a.group_art_unit, a.issue_year
    ORDER BY a.group_art_unit, a.issue_year
    """
    return _read_df(db, q)
