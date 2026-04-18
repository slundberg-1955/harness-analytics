"""Portfolio Explorer JSON endpoints (rows, KPIs, CSV, biblio).

Mounted on the existing `/portal` router, so the portal auth middleware
automatically guards every route defined here.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import re
from datetime import date, datetime
from functools import lru_cache
from typing import Any, Iterable, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from lxml import etree
from sqlalchemy import text
from sqlalchemy.orm import Session

from harness_analytics import app_settings
from harness_analytics.db import get_db
from harness_analytics.portfolio_aggregates import (
    STATUS_PILL,
    compute_charts,
    compute_kpis,
    status_label,
    status_tone,
)
from harness_analytics.xml_parser import parse_date, parse_datetime_utc

router = APIRouter(prefix="/portal/api", tags=["portal-api"])


# Whitelist of sort keys exposed to the API → (SQL expression, default direction)
_SORT_COLUMNS: dict[str, str] = {
    "applicationNumber": "application_number",
    "inventionTitle": "invention_title",
    "applicationStatusCode": "application_status_code",
    "filingDate": "filing_date",
    "issueDate": "issue_date",
    "patentNumber": "patent_number",
    "groupArtUnit": "group_art_unit",
    "examinerName": "examiner_name",
    "assigneeName": "assignee_name",
    "applicantName": "applicant_name",
    "isContinuation": "is_continuation",
    "nonfinalOaCount": "nonfinal_oa_count",
    "finalOaCount": "final_oa_count",
    "interviewCount": "interview_count",
    "rceCount": "rce_count",
    "daysFilingToNoa": "days_filing_to_noa",
    "daysFilingToIssue": "days_filing_to_issue",
    "updatedAt": "updated_at",
}

# Columns selected from the `patent_applications` view and returned in `rows`.
# Ordered to mirror the default-visible table column order so CSV export
# stays readable without extra shaping.
_ROW_COLUMNS: list[str] = [
    "application_number",
    "invention_title",
    "application_status_code",
    "application_status_text",
    "filing_date",
    "issue_date",
    "patent_number",
    "customer_number",
    "hdp_customer_number",
    "group_art_unit",
    "patent_class",
    "examiner_name",
    "assignee_name",
    "applicant_name",
    "is_continuation",
    "has_restriction_ctrs_count",
    "ifw_a_ne_count",
    "nonfinal_oa_count",
    "final_oa_count",
    "total_substantive_oas",
    "first_noa_date",
    "had_examiner_interview",
    "interview_count",
    "noa_within_90_days_of_interview",
    "days_last_interview_to_noa",
    "rce_count",
    "days_filing_to_first_oa",
    "days_filing_to_noa",
    "days_filing_to_issue",
    "is_jac",
    "office_name",
    "updated_at",
]

_DEFAULT_PAGE_SIZE = 50
_MAX_PAGE_SIZE = 200

# Cap on the rows materialized in Python for KPI/chart math per request.
# Precedence: DB setting (`portfolio.aggregateRowCap`) > env
# (`PORTFOLIO_AGG_ROW_CAP`) > _DEFAULT_AGG_ROW_CAP. `0` disables the cap.
_DEFAULT_AGG_ROW_CAP = 5000
SETTING_KEY_AGG_ROW_CAP = "portfolio.aggregateRowCap"

_logger = logging.getLogger(__name__)


def _coerce_cap(raw: str | None) -> int | None:
    if raw is None or raw.strip() == "":
        return None
    try:
        v = int(raw)
    except ValueError:
        _logger.warning(
            "Invalid portfolio aggregate row cap %r; ignoring", raw
        )
        return None
    if v < 0:
        return None
    return v


def _aggregate_row_cap() -> int:
    """DB setting overrides env var overrides default."""
    db_value = _coerce_cap(app_settings.get_setting(SETTING_KEY_AGG_ROW_CAP))
    if db_value is not None:
        return db_value
    env_value = _coerce_cap(os.environ.get("PORTFOLIO_AGG_ROW_CAP"))
    if env_value is not None:
        return env_value
    return _DEFAULT_AGG_ROW_CAP


# ---------------------------------------------------------------------------
# Filter parsing / SQL assembly
# ---------------------------------------------------------------------------


def _split_csv(raw: Optional[str]) -> list[str]:
    if not raw:
        return []
    return [p.strip() for p in raw.split(",") if p.strip()]


def _parse_bool(raw: Optional[str]) -> Optional[bool]:
    if raw is None:
        return None
    v = raw.strip().lower()
    if v in ("true", "1", "yes", "y"):
        return True
    if v in ("false", "0", "no", "n"):
        return False
    return None


def _parse_iso_date(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    try:
        d = datetime.fromisoformat(raw).date()
    except ValueError:
        return None
    return d.isoformat()


def _build_where(params: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Return (WHERE sql, bindparams) for the current filter state."""
    conditions: list[str] = []
    binds: dict[str, Any] = {}

    q = (params.get("q") or "").strip()
    if q:
        binds["q"] = f"%{q.lower()}%"
        conditions.append(
            "("
            "LOWER(COALESCE(invention_title, '')) LIKE :q "
            "OR LOWER(COALESCE(examiner_name, '')) LIKE :q "
            "OR LOWER(COALESCE(assignee_name, '')) LIKE :q "
            "OR LOWER(COALESCE(applicant_name, '')) LIKE :q "
            "OR LOWER(COALESCE(application_number, '')) LIKE :q"
            ")"
        )

    status_codes = [s for s in _split_csv(params.get("status")) if s.lstrip("-").isdigit()]
    if status_codes:
        placeholders = []
        for i, s in enumerate(status_codes):
            name = f"status_{i}"
            placeholders.append(f":{name}")
            binds[name] = int(s)
        conditions.append(f"application_status_code IN ({', '.join(placeholders)})")

    issue_years = [y for y in _split_csv(params.get("issueYear")) if y.isdigit()]
    if issue_years:
        placeholders = []
        for i, y in enumerate(issue_years):
            name = f"year_{i}"
            placeholders.append(f":{name}")
            binds[name] = int(y)
        conditions.append(f"issue_year IN ({', '.join(placeholders)})")

    art_units = _split_csv(params.get("artUnit"))
    if art_units:
        placeholders = []
        for i, v in enumerate(art_units):
            name = f"au_{i}"
            placeholders.append(f":{name}")
            binds[name] = v
        conditions.append(f"group_art_unit IN ({', '.join(placeholders)})")

    examiners = _split_csv(params.get("examiner"))
    if examiners:
        placeholders = []
        for i, v in enumerate(examiners):
            name = f"ex_{i}"
            placeholders.append(f":{name}")
            binds[name] = v
        conditions.append(f"examiner_name IN ({', '.join(placeholders)})")

    assignees = _split_csv(params.get("assignee"))
    if assignees:
        placeholders = []
        for i, v in enumerate(assignees):
            name = f"as_{i}"
            placeholders.append(f":{name}")
            binds[name] = v
        conditions.append(f"assignee_name IN ({', '.join(placeholders)})")

    applicants = _split_csv(params.get("applicant"))
    if applicants:
        # Applicant names from XML are free-form text and frequently differ in
        # punctuation/case across filings; match case-insensitively against any
        # comma-separated value.
        clauses = []
        for i, v in enumerate(applicants):
            name = f"ap_{i}"
            clauses.append(f"LOWER(COALESCE(applicant_name, '')) LIKE :{name}")
            binds[name] = f"%{v.lower()}%"
        conditions.append("(" + " OR ".join(clauses) + ")")

    had_interview = _parse_bool(params.get("hadInterview"))
    if had_interview is not None:
        conditions.append(f"had_examiner_interview = {str(had_interview).upper()}")

    rce_count = (params.get("rceCount") or "").strip().lower()
    if rce_count in ("0", "1", "2"):
        binds["rce_eq"] = int(rce_count)
        conditions.append("rce_count = :rce_eq")
    elif rce_count in ("gte3", "3+", ">=3"):
        conditions.append("rce_count >= 3")

    filing_from = _parse_iso_date(params.get("filingFrom"))
    if filing_from:
        binds["filing_from"] = filing_from
        conditions.append("filing_date >= :filing_from")

    filing_to = _parse_iso_date(params.get("filingTo"))
    if filing_to:
        binds["filing_to"] = filing_to
        conditions.append("filing_date <= :filing_to")

    where_sql = (" WHERE " + " AND ".join(conditions)) if conditions else ""
    return where_sql, binds


def _sort_clause(params: dict[str, Any]) -> str:
    key = (params.get("sort") or "applicationNumber").strip()
    dir_raw = (params.get("dir") or "asc").strip().lower()
    direction = "DESC" if dir_raw == "desc" else "ASC"
    column = _SORT_COLUMNS.get(key, "application_number")
    # Stable secondary sort so paginated results don't shuffle.
    return f" ORDER BY {column} {direction} NULLS LAST, application_number ASC"


def _row_to_json(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "applicationNumber": row.get("application_number"),
        "inventionTitle": row.get("invention_title"),
        "applicationStatusCode": row.get("application_status_code"),
        "applicationStatusText": row.get("application_status_text"),
        "applicationStatusLabel": status_label(
            row.get("application_status_code"), row.get("application_status_text")
        ),
        "applicationStatusTone": status_tone(row.get("application_status_code")),
        "filingDate": _iso(row.get("filing_date")),
        "issueDate": _iso(row.get("issue_date")),
        "patentNumber": row.get("patent_number"),
        "customerNumber": row.get("customer_number"),
        "hdpCustomerNumber": row.get("hdp_customer_number"),
        "groupArtUnit": row.get("group_art_unit"),
        "patentClass": row.get("patent_class"),
        "examinerName": row.get("examiner_name"),
        "assigneeName": row.get("assignee_name"),
        "applicantName": row.get("applicant_name"),
        "isContinuation": bool(row.get("is_continuation")),
        "hasRestrictionCtrsCount": row.get("has_restriction_ctrs_count") or 0,
        "ifwANeCount": row.get("ifw_a_ne_count") or 0,
        "nonfinalOaCount": row.get("nonfinal_oa_count") or 0,
        "finalOaCount": row.get("final_oa_count") or 0,
        "totalSubstantiveOas": row.get("total_substantive_oas") or 0,
        "firstNoaDate": _iso(row.get("first_noa_date")),
        "hadExaminerInterview": bool(row.get("had_examiner_interview")),
        "interviewCount": row.get("interview_count") or 0,
        "noaWithin90DaysOfInterview": bool(row.get("noa_within_90_days_of_interview")),
        "daysLastInterviewToNoa": row.get("days_last_interview_to_noa"),
        "rceCount": row.get("rce_count") or 0,
        "daysFilingToFirstOa": row.get("days_filing_to_first_oa"),
        "daysFilingToNoa": row.get("days_filing_to_noa"),
        "daysFilingToIssue": row.get("days_filing_to_issue"),
        "isJac": bool(row.get("is_jac")),
        "officeName": row.get("office_name"),
        "updatedAt": _iso(row.get("updated_at")),
    }


def _iso(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return str(v)


def _fetch_rows(
    db: Session, params: dict[str, Any], cap: int | None = None
) -> list[dict[str, Any]]:
    """Fetch matching rows for KPI/chart math.

    `cap` is the maximum number of rows to materialize. `0` (or any falsy
    value other than `None`) disables the cap and pulls every matching row;
    `None` reads the cap from `PORTFOLIO_AGG_ROW_CAP`.
    """
    if cap is None:
        cap = _aggregate_row_cap()
    where_sql, binds = _build_where(params)
    sql = (
        f"SELECT {', '.join(_ROW_COLUMNS)} FROM patent_applications"
        + where_sql
        + _sort_clause(params)
    )
    if cap and cap > 0:
        sql += f" LIMIT {int(cap)}"
    result = db.execute(text(sql), binds)
    cols = list(result.keys())
    return [dict(zip(cols, r)) for r in result.fetchall()]


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


def _params_from_request(
    q: Optional[str],
    status: Optional[str],
    issueYear: Optional[str],
    artUnit: Optional[str],
    examiner: Optional[str],
    assignee: Optional[str],
    applicant: Optional[str],
    hadInterview: Optional[str],
    rceCount: Optional[str],
    filingFrom: Optional[str],
    filingTo: Optional[str],
    sort: Optional[str],
    dir: Optional[str],
) -> dict[str, Any]:
    return {
        "q": q,
        "status": status,
        "issueYear": issueYear,
        "artUnit": artUnit,
        "examiner": examiner,
        "assignee": assignee,
        "applicant": applicant,
        "hadInterview": hadInterview,
        "rceCount": rceCount,
        "filingFrom": filingFrom,
        "filingTo": filingTo,
        "sort": sort,
        "dir": dir,
    }


@router.get("/portfolio")
def portfolio(
    q: Optional[str] = None,
    status: Optional[str] = None,
    issueYear: Optional[str] = None,
    artUnit: Optional[str] = None,
    examiner: Optional[str] = None,
    assignee: Optional[str] = None,
    applicant: Optional[str] = None,
    hadInterview: Optional[str] = None,
    rceCount: Optional[str] = None,
    filingFrom: Optional[str] = None,
    filingTo: Optional[str] = None,
    sort: Optional[str] = None,
    dir: Optional[str] = None,
    page: int = Query(1, ge=1),
    pageSize: int = Query(_DEFAULT_PAGE_SIZE, ge=1, le=_MAX_PAGE_SIZE),
    db: Session = Depends(get_db),
) -> JSONResponse:
    params = _params_from_request(
        q, status, issueYear, artUnit, examiner, assignee, applicant,
        hadInterview, rceCount, filingFrom, filingTo, sort, dir,
    )

    cap = _aggregate_row_cap()
    all_rows = _fetch_rows(db, params, cap=cap)
    total = len(all_rows)
    capped = bool(cap and total >= cap)
    start = (page - 1) * pageSize
    end = start + pageSize
    page_rows = all_rows[start:end]

    return JSONResponse(
        {
            "rows": [_row_to_json(r) for r in page_rows],
            "total": total,
            "page": page,
            "pageSize": pageSize,
            "aggregateRowCap": cap if cap and cap > 0 else None,
            "capped": capped,
            "kpis": compute_kpis(all_rows),
            "charts": compute_charts(all_rows),
            "statusPill": {str(k): v for k, v in STATUS_PILL.items()},
        }
    )


@router.get("/portfolio.csv")
def portfolio_csv(
    q: Optional[str] = None,
    status: Optional[str] = None,
    issueYear: Optional[str] = None,
    artUnit: Optional[str] = None,
    examiner: Optional[str] = None,
    assignee: Optional[str] = None,
    applicant: Optional[str] = None,
    hadInterview: Optional[str] = None,
    rceCount: Optional[str] = None,
    filingFrom: Optional[str] = None,
    filingTo: Optional[str] = None,
    sort: Optional[str] = None,
    dir: Optional[str] = None,
    db: Session = Depends(get_db),
) -> StreamingResponse:
    params = _params_from_request(
        q, status, issueYear, artUnit, examiner, assignee, applicant,
        hadInterview, rceCount, filingFrom, filingTo, sort, dir,
    )
    rows = _fetch_rows(db, params)

    def iter_csv() -> Iterable[bytes]:
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(_ROW_COLUMNS)
        yield buf.getvalue().encode("utf-8")
        buf.seek(0)
        buf.truncate(0)
        for row in rows:
            writer.writerow([_iso(row.get(col)) if col in {"filing_date", "issue_date", "first_noa_date", "updated_at"} else row.get(col) for col in _ROW_COLUMNS])
            yield buf.getvalue().encode("utf-8")
            buf.seek(0)
            buf.truncate(0)

    return StreamingResponse(
        iter_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="portfolio.csv"'},
    )


# ---------------------------------------------------------------------------
# Filter facets endpoint (powers the chip dropdowns)
# ---------------------------------------------------------------------------

# Map filter key -> SQL column expression on `patent_applications`. Anything
# not in this map is handled below with a hard-coded option list (boolean,
# bucketed, date) or rejected with 400.
_FACET_COLUMNS: dict[str, str] = {
    "issueYear": "issue_year",
    "artUnit": "group_art_unit",
    "examiner": "examiner_name",
    "applicant": "applicant_name",
}

# Hard cap on how many distinct values we ship to the client per facet.
# Most dimensions stay well under this in practice (status/year/art unit are
# tiny; examiner/applicant can grow but the dropdown UI has a search box on
# top and we sort by frequency so the long tail is reachable via search).
_FACET_LIMIT = 2000


def _facet_options_for_column(
    db: Session, column: str, limit: int = _FACET_LIMIT
) -> list[dict[str, Any]]:
    sql = (
        f"SELECT {column} AS value, COUNT(*) AS count "
        f"FROM patent_applications "
        f"WHERE {column} IS NOT NULL "
        f"AND CAST({column} AS TEXT) <> '' "
        f"GROUP BY {column} "
        f"ORDER BY count DESC, value ASC "
        f"LIMIT :lim"
    )
    rows = db.execute(text(sql), {"lim": int(limit)}).all()
    return [
        {"value": _facet_value(r[0]), "label": _facet_value(r[0]), "count": int(r[1])}
        for r in rows
    ]


def _facet_value(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        # Drop trailing .0 from years/etc.
        if isinstance(v, float) and v.is_integer():
            return str(int(v))
        return str(v)
    return str(v)


def _facet_status(db: Session) -> list[dict[str, Any]]:
    rows = db.execute(
        text(
            "SELECT application_status_code AS code, MAX(application_status_text) AS txt, "
            "COUNT(*) AS count "
            "FROM patent_applications "
            "WHERE application_status_code IS NOT NULL "
            "GROUP BY application_status_code "
            "ORDER BY count DESC, code ASC "
            "LIMIT :lim"
        ),
        {"lim": _FACET_LIMIT},
    ).all()
    out: list[dict[str, Any]] = []
    for code, txt, count in rows:
        if code is None:
            continue
        label = status_label(int(code), txt)
        out.append(
            {
                "value": str(int(code)),
                "label": f"{label} (Code {int(code)})",
                "count": int(count),
            }
        )
    return out


@router.get("/portfolio/facets")
def portfolio_facets(
    key: str,
    db: Session = Depends(get_db),
) -> JSONResponse:
    """Distinct filter values + per-value counts, used to populate filter dropdowns."""
    if key == "status":
        return JSONResponse({"key": key, "options": _facet_status(db)})
    if key == "hadInterview":
        rows = db.execute(
            text(
                "SELECT had_examiner_interview AS v, COUNT(*) AS c "
                "FROM patent_applications GROUP BY had_examiner_interview"
            )
        ).all()
        counts = {bool(r[0]): int(r[1]) for r in rows}
        return JSONResponse(
            {
                "key": key,
                "options": [
                    {"value": "true", "label": "Yes", "count": counts.get(True, 0)},
                    {"value": "false", "label": "No", "count": counts.get(False, 0)},
                ],
            }
        )
    if key == "rceCount":
        rows = db.execute(
            text(
                "SELECT CASE WHEN rce_count >= 3 THEN 'gte3' "
                "ELSE rce_count::text END AS bucket, COUNT(*) AS c "
                "FROM patent_applications GROUP BY bucket ORDER BY bucket"
            )
        ).all()
        labels = {"0": "0", "1": "1", "2": "2", "gte3": "3 or more"}
        counts = {str(r[0]): int(r[1]) for r in rows}
        return JSONResponse(
            {
                "key": key,
                "options": [
                    {"value": v, "label": labels[v], "count": counts.get(v, 0)}
                    for v in ("0", "1", "2", "gte3")
                ],
            }
        )
    if key in ("filingFrom", "filingTo"):
        # Special case: not enumerable. Surface the min/max of filing_date so
        # the UI can render a date input with sensible bounds.
        row = db.execute(
            text(
                "SELECT MIN(filing_date) AS min_d, MAX(filing_date) AS max_d "
                "FROM patent_applications"
            )
        ).first()
        return JSONResponse(
            {
                "key": key,
                "kind": "date",
                "min": _iso(row[0]) if row else None,
                "max": _iso(row[1]) if row else None,
            }
        )
    column = _FACET_COLUMNS.get(key)
    if not column:
        raise HTTPException(status_code=400, detail=f"Unknown facet key: {key}")
    return JSONResponse({"key": key, "options": _facet_options_for_column(db, column)})


# ---------------------------------------------------------------------------
# Biblio endpoint
# ---------------------------------------------------------------------------


def _extract_text(el: Any, xpath: str) -> Optional[str]:
    if el is None:
        return None
    res = el.xpath(xpath)
    if res:
        return str(res[0]).strip() or None
    return None


def _person(first: Optional[str], middle: Optional[str], last: Optional[str]) -> dict[str, Any]:
    return {
        "firstName": first,
        "middleName": middle,
        "lastName": last,
    }


def _build_biblio_from_xml(xml_text: str) -> dict[str, Any]:
    """Parse biblio XML into the spec's `UsptoBiblio` shape."""
    root = etree.fromstring(xml_text.encode("utf-8"))
    bib = root.find(".//ApplicationBibliographicData")

    publications: list[dict[str, Any]] = []
    for pub_el in root.xpath(".//PatentPublicationIdentification"):
        publications.append(
            {
                "sequenceNumber": _extract_text(pub_el, "PatentPublicationNumber/text()")
                or _extract_text(pub_el, "SequenceNumber/text()"),
                "kindCode": _extract_text(pub_el, "KindCode/text()"),
                "publicationDate": _extract_text(pub_el, "PublicationDate/text()"),
            }
        )

    inventors: list[dict[str, Any]] = []
    for inv_el in root.xpath(".//Inventors/Inventor"):
        inventors.append(
            {
                "name": _person(
                    _extract_text(inv_el, "InventorName/FirstName/text()"),
                    _extract_text(inv_el, "InventorName/MiddleName/text()"),
                    _extract_text(inv_el, "InventorName/LastName/text()"),
                ),
                "city": _extract_text(inv_el, ".//City/text()"),
                "region": _extract_text(inv_el, ".//GeographicRegionName/text()")
                or _extract_text(inv_el, ".//StateOrProvinceName/text()"),
                "postalCode": _extract_text(inv_el, ".//PostalCode/text()"),
                "countryCode": _extract_text(inv_el, ".//CountryCode/text()"),
                "countryName": _extract_text(inv_el, ".//CountryName/text()"),
            }
        )

    applicants: list[dict[str, Any]] = []
    for app_el in root.xpath(".//Applicants/Applicant"):
        addr_lines = [
            ln
            for ln in (
                _extract_text(app_el, ".//AddressLineOneText/text()"),
                _extract_text(app_el, ".//AddressLineTwoText/text()"),
            )
            if ln
        ]
        applicants.append(
            {
                "legalEntityName": _extract_text(app_el, "LegalEntityName/text()"),
                "addressLines": addr_lines,
                "city": _extract_text(app_el, ".//City/text()"),
                "countryCode": _extract_text(app_el, ".//CountryCode/text()"),
            }
        )

    parents: list[dict[str, Any]] = []
    for el in root.xpath(".//Continuity/ParentContinuityList/ParentContinuity"):
        parents.append(
            {
                "parentApplicationNumber": _extract_text(el, "ParentApplicationNumber/text()"),
                "childApplicationNumber": _extract_text(el, "ChildApplicationNumber/text()"),
                "description": _extract_text(el, "ContinuityDescription/text()"),
                "filingDate": _extract_text(el, "ParentApplicationFilingDate/text()"),
                "statusNumber": _extract_text(el, "ParentApplicationStatusNumber/text()"),
            }
        )
    children: list[dict[str, Any]] = []
    for el in root.xpath(".//Continuity/ChildContinuityList/ChildContinuity"):
        children.append(
            {
                "parentApplicationNumber": _extract_text(el, "ParentApplicationNumber/text()"),
                "childApplicationNumber": _extract_text(el, "ChildApplicationNumber/text()"),
                "description": _extract_text(el, "ContinuityDescription/text()"),
                "filingDate": _extract_text(el, "ChildApplicationFilingDate/text()"),
                "statusNumber": _extract_text(el, "ChildApplicationStatusNumber/text()"),
            }
        )

    foreign: list[dict[str, Any]] = []
    for el in root.xpath(".//ForeignPriorities/ForeignPriority"):
        foreign.append(
            {
                "countryCode": _extract_text(el, "IPOfficeCode/text()")
                or _extract_text(el, "CountryCode/text()"),
                "countryName": _extract_text(el, "CountryName/text()"),
                "priorityNumber": _extract_text(el, "ApplicationNumber/text()")
                or _extract_text(el, "PriorityApplicationNumber/text()"),
                "priorityDate": _extract_text(el, "FilingDate/text()")
                or _extract_text(el, "PriorityDate/text()"),
            }
        )

    events: list[dict[str, Any]] = []
    for el in root.xpath(".//FileContentHistories/FileContentHistory"):
        events.append(
            {
                "transactionDate": _extract_text(el, "TransactionDate/text()"),
                "transactionDescription": _extract_text(el, "TransactionDescription/text()"),
                "statusNumber": _extract_text(el, "StatusNumber/text()"),
                "statusDescription": _extract_text(el, "StatusDescription/text()"),
            }
        )
    # Newest first per spec §10 item 8.
    events.sort(key=lambda e: e.get("transactionDate") or "", reverse=True)

    ifw: list[dict[str, Any]] = []
    for el in root.xpath(".//ImageFileWrapperList/ImageFileWrapperDocument"):
        ifw.append(
            {
                "mailRoomDate": _extract_text(el, "MailRoomDate/text()"),
                "documentDescription": _extract_text(el, "DocumentDescription/text()"),
                "fileWrapperDocumentCode": _extract_text(el, "FileWrapperDocumentCode/text()"),
                "pageQuantity": _extract_text(el, "PageQuantity/text()"),
                "category": _extract_text(el, "DocumentCategory/text()"),
            }
        )
    ifw.sort(key=lambda d: d.get("mailRoomDate") or "", reverse=True)

    corr_el = root.find(".//CorrespondenceAddress")
    correspondence: Optional[dict[str, Any]] = None
    if corr_el is not None:
        correspondence = {
            "nameLine1": _extract_text(corr_el, ".//NameLineOneText/text()")
            or _extract_text(corr_el, ".//OrganizationStandardName/text()"),
            "addressLine1": _extract_text(corr_el, ".//AddressLineOneText/text()"),
            "addressLine2": _extract_text(corr_el, ".//AddressLineTwoText/text()"),
            "city": _extract_text(corr_el, ".//City/text()"),
            "postalCode": _extract_text(corr_el, ".//PostalCode/text()"),
            "countryCode": _extract_text(corr_el, ".//CountryCode/text()"),
            "countryName": _extract_text(corr_el, ".//CountryName/text()"),
        }

    attorneys: list[dict[str, Any]] = []
    seen_regs: set[str] = set()
    for atty_el in root.xpath(".//Attorneys/Attorney"):
        reg = _extract_text(atty_el, "RegistrationNumber/text()")
        key = reg or "|".join(
            (
                _extract_text(atty_el, "AttorneyName/FirstName/text()") or "",
                _extract_text(atty_el, "AttorneyName/LastName/text()") or "",
            )
        )
        if key in seen_regs:
            continue
        seen_regs.add(key)
        phones: list[str] = []
        for p in atty_el.xpath(
            "AttorneyContacts/AttorneyContact/TelecommunicationNumber/text()"
        ):
            s = str(p).strip()
            if s:
                phones.append(s)
        attorneys.append(
            {
                "registrationNumber": reg,
                "name": _person(
                    _extract_text(atty_el, "AttorneyName/FirstName/text()"),
                    _extract_text(atty_el, "AttorneyName/MiddleName/text()"),
                    _extract_text(atty_el, "AttorneyName/LastName/text()"),
                ),
                "phones": phones,
                "status": _extract_text(atty_el, "AgentStatus/text()") or "ACTIVE",
            }
        )

    app_num = _extract_text(bib, "ApplicationNumber/text()") if bib is not None else None
    status_code_raw = _extract_text(bib, "ApplicationStatusCode/text()") if bib is not None else None
    try:
        status_code = int(status_code_raw) if status_code_raw else None
    except ValueError:
        status_code = None

    is_public_raw = _extract_text(bib, "IsPublic/text()") if bib is not None else None
    is_public = (is_public_raw or "").strip().lower() in ("true", "yes", "1", "y")

    return {
        "applicationNumber": app_num,
        "applicationBibliographicData": {
            "confirmationNumber": _extract_text(bib, "ConfirmationNumber/text()") if bib is not None else None,
            "attorneyDocketNumber": _extract_text(bib, "AttorneyDocketNumber/text()") if bib is not None else None,
            "customerNumber": _extract_text(bib, "CustomerNumber/text()") if bib is not None else None,
            "filingDate": _extract_text(bib, "FilingDate/text()") if bib is not None else None,
            "applicationStatusCode": status_code,
            "applicationStatusText": _extract_text(bib, "ApplicationStatusText/text()") if bib is not None else None,
            "applicationStatusDate": _extract_text(bib, "ApplicationStatusDate/text()") if bib is not None else None,
            "groupArtUnit": _extract_text(bib, "GroupArtUnit/text()") if bib is not None else None,
            "patentClass": _extract_text(bib, "PatentClass/text()") if bib is not None else None,
            "patentSubclass": _extract_text(bib, "PatentSubclass/text()") if bib is not None else None,
            "inventionSubjectMatterType": _extract_text(bib, "InventionSubjectMatterCategory/text()") if bib is not None else None,
            "inventionTitle": _extract_text(bib, "InventionTitle/text()") if bib is not None else None,
            "isPublic": is_public,
            "examinerName": _person(
                _extract_text(bib, "ExaminerName/FirstName/text()") if bib is not None else None,
                _extract_text(bib, "ExaminerName/MiddleName/text()") if bib is not None else None,
                _extract_text(bib, "ExaminerName/LastName/text()") if bib is not None else None,
            ),
            "publications": publications,
        },
        "inventors": inventors,
        "applicants": applicants,
        "continuity": {"parents": parents, "children": children},
        "foreignPriorities": foreign,
        "fileContentHistories": events,
        "imageFileWrapper": ifw,
        "correspondence": correspondence,
        "attorneys": attorneys,
        "supplementalContents": [],
    }


@lru_cache(maxsize=256)
def _parse_biblio_xml_cached(xml_text: str) -> dict[str, Any]:
    return _build_biblio_from_xml(xml_text)


def _merge_normalized_fallback(
    biblio: dict[str, Any],
    row: dict[str, Any],
    db_inventors: list[dict[str, Any]],
    db_attorneys: list[dict[str, Any]],
    db_events: list[dict[str, Any]],
    db_documents: list[dict[str, Any]],
) -> dict[str, Any]:
    """When the XML parse came up empty for a section, fall back to DB rows."""
    abd = biblio.setdefault("applicationBibliographicData", {})
    abd.setdefault("applicationNumber", row.get("application_number"))
    if not abd.get("filingDate"):
        abd["filingDate"] = _iso(row.get("filing_date"))
    if abd.get("applicationStatusCode") is None:
        abd["applicationStatusCode"] = row.get("application_status_code")
    if not abd.get("applicationStatusText"):
        abd["applicationStatusText"] = row.get("application_status_text")
    if not abd.get("groupArtUnit"):
        abd["groupArtUnit"] = row.get("group_art_unit")
    if not abd.get("patentClass"):
        abd["patentClass"] = row.get("patent_class")
    if not abd.get("inventionTitle"):
        abd["inventionTitle"] = row.get("invention_title")
    if not abd.get("customerNumber"):
        abd["customerNumber"] = row.get("customer_number")
    ex = abd.get("examinerName") or {}
    if not (ex.get("firstName") or ex.get("lastName")):
        abd["examinerName"] = _person(
            row.get("examiner_first_name"),
            None,
            row.get("examiner_last_name"),
        )

    if not biblio.get("inventors") and db_inventors:
        biblio["inventors"] = [
            {
                "name": _person(i.get("first_name"), None, i.get("last_name")),
                "city": i.get("city"),
                "countryCode": i.get("country_code"),
                "countryName": None,
                "postalCode": None,
                "region": None,
            }
            for i in db_inventors
        ]

    if not biblio.get("attorneys") and db_attorneys:
        biblio["attorneys"] = [
            {
                "registrationNumber": a.get("registration_number"),
                "name": _person(a.get("first_name"), None, a.get("last_name")),
                "phones": [a["phone"]] if a.get("phone") else [],
                "status": a.get("agent_status") or "ACTIVE",
            }
            for a in db_attorneys
        ]

    if not biblio.get("fileContentHistories") and db_events:
        biblio["fileContentHistories"] = [
            {
                "transactionDate": _iso(e.get("transaction_date")),
                "transactionDescription": e.get("transaction_description"),
                "statusNumber": e.get("status_number"),
                "statusDescription": e.get("status_description"),
            }
            for e in db_events
        ]
        biblio["fileContentHistories"].sort(
            key=lambda e: e.get("transactionDate") or "", reverse=True
        )

    if not biblio.get("imageFileWrapper") and db_documents:
        biblio["imageFileWrapper"] = [
            {
                "mailRoomDate": _iso(d.get("mail_room_date")),
                "documentDescription": d.get("document_description"),
                "fileWrapperDocumentCode": d.get("document_code"),
                "pageQuantity": d.get("page_quantity"),
                "category": d.get("document_category"),
            }
            for d in db_documents
        ]

    return biblio


def _normalize_lookup(raw: str) -> str:
    return "".join(raw.strip().split())


@router.get("/applications/{application_number:path}/biblio")
def application_biblio(
    application_number: str,
    db: Session = Depends(get_db),
) -> JSONResponse:
    key = _normalize_lookup(application_number)
    if not key:
        raise HTTPException(status_code=400, detail="Missing application number")

    app_row = db.execute(
        text(
            "SELECT id, application_number, invention_title, filing_date, "
            "application_status_code, application_status_text, group_art_unit, "
            "patent_class, customer_number, examiner_first_name, examiner_last_name, "
            "xml_raw FROM applications WHERE application_number = :key LIMIT 1"
        ),
        {"key": key},
    ).mappings().first()
    if app_row is None:
        digits = re.sub(r"\D", "", key)
        if digits and digits != key:
            app_row = db.execute(
                text(
                    "SELECT id, application_number, invention_title, filing_date, "
                    "application_status_code, application_status_text, group_art_unit, "
                    "patent_class, customer_number, examiner_first_name, examiner_last_name, "
                    "xml_raw FROM applications WHERE application_number = :key LIMIT 1"
                ),
                {"key": digits},
            ).mappings().first()
    if app_row is None:
        raise HTTPException(status_code=404, detail="Application not found")

    row = dict(app_row)
    try:
        row["application_status_code"] = (
            int(row["application_status_code"])
            if row.get("application_status_code") and str(row["application_status_code"]).isdigit()
            else None
        )
    except (TypeError, ValueError):
        row["application_status_code"] = None

    xml_text = row.get("xml_raw")
    biblio: dict[str, Any]
    if xml_text and xml_text.strip():
        try:
            biblio = dict(_parse_biblio_xml_cached(xml_text))
            # lru_cache returns shared refs; deep-copy nested containers we mutate.
            biblio["applicationBibliographicData"] = dict(biblio.get("applicationBibliographicData") or {})
        except etree.XMLSyntaxError:
            biblio = {"applicationBibliographicData": {}}
    else:
        biblio = {"applicationBibliographicData": {}}

    app_id = row["id"]
    inv_rows = db.execute(
        text(
            "SELECT first_name, last_name, city, country_code FROM inventors "
            "WHERE application_id = :id"
        ),
        {"id": app_id},
    ).mappings().all()
    att_rows = db.execute(
        text(
            "SELECT registration_number, first_name, last_name, phone, agent_status "
            "FROM application_attorneys WHERE application_id = :id"
        ),
        {"id": app_id},
    ).mappings().all()
    ev_rows = db.execute(
        text(
            "SELECT transaction_date, transaction_description, status_number, "
            "status_description FROM prosecution_events WHERE application_id = :id "
            "ORDER BY transaction_date DESC, seq_order DESC NULLS LAST"
        ),
        {"id": app_id},
    ).mappings().all()
    doc_rows = db.execute(
        text(
            "SELECT mail_room_date, document_description, document_code, page_quantity, "
            "document_category FROM file_wrapper_documents WHERE application_id = :id "
            "ORDER BY mail_room_date DESC NULLS LAST"
        ),
        {"id": app_id},
    ).mappings().all()

    biblio = _merge_normalized_fallback(
        biblio,
        row,
        [dict(r) for r in inv_rows],
        [dict(r) for r in att_rows],
        [dict(r) for r in ev_rows],
        [dict(r) for r in doc_rows],
    )
    biblio["applicationNumber"] = row["application_number"]
    return JSONResponse(biblio)
