"""Multi-sheet Excel workbook from report queries."""

from __future__ import annotations

import pandas as pd
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from openpyxl.workbook import Workbook
from sqlalchemy.orm import Session

from harness_analytics.reports import (
    report_all_harness,
    report_art_unit_summary,
    report_by_office,
    report_interview_to_noa,
    report_specific_clients,
)

JAC_FILL = PatternFill("solid", fgColor="FFFF00")
HEADER_FILL = PatternFill("solid", fgColor="1F3864")
HEADER_FONT = Font(color="FFFFFF", bold=True)
YEAR_FILLS = {
    2024: PatternFill("solid", fgColor="EBF3FB"),
    2025: PatternFill("solid", fgColor="E2EFDA"),
}


def _write_df_to_sheet(ws, df: pd.DataFrame, *, highlight_jac: bool = False) -> None:
    if df.empty:
        ws.cell(row=1, column=1, value="No rows")
        return

    cols = list(df.columns)
    for col_idx, col_name in enumerate(cols, 1):
        cell = ws.cell(row=1, column=col_idx, value=str(col_name).upper().replace("_", " "))
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = Alignment(horizontal="center")

    for row_idx, row in enumerate(df.itertuples(index=False), start=2):
        issue_year = getattr(row, "issue_year", None) if hasattr(row, "issue_year") else None
        is_jac = bool(getattr(row, "is_jac", False)) if hasattr(row, "is_jac") else False

        for col_idx, val in enumerate(row, start=1):
            cell = ws.cell(row=row_idx, column=col_idx, value=val)
            if highlight_jac and is_jac:
                cell.fill = JAC_FILL
            elif issue_year in YEAR_FILLS:
                cell.fill = YEAR_FILLS[int(issue_year)]

    for col_idx, col_name in enumerate(cols, 1):
        max_len = max(len(str(col_name)), 10, *(len(str(c)) for c in df[col_name].head(200)))
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max_len + 2, 40)

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions


def _write_summary_tab(ws, df: pd.DataFrame) -> None:
    if df.empty:
        ws.cell(row=1, column=1, value="No data for summary")
        return

    rows = [
        ["Metric", "2024", "2025"],
        [
            "Total Applications Issued",
            int((df.issue_year == 2024).sum()),
            int((df.issue_year == 2025).sum()),
        ],
        [
            "Avg Substantive OAs Before NOA",
            round(df.loc[df.issue_year == 2024, "total_substantive_oas"].mean(), 2),
            round(df.loc[df.issue_year == 2025, "total_substantive_oas"].mean(), 2),
        ],
        [
            "Avg Non-Final OAs",
            round(df.loc[df.issue_year == 2024, "nonfinal_oa_count"].mean(), 2),
            round(df.loc[df.issue_year == 2025, "nonfinal_oa_count"].mean(), 2),
        ],
        [
            "Avg Final OAs",
            round(df.loc[df.issue_year == 2024, "final_oa_count"].mean(), 2),
            round(df.loc[df.issue_year == 2025, "final_oa_count"].mean(), 2),
        ],
        [
            "% With 0 OAs (straight to NOA)",
            f"{100 * (df.loc[df.issue_year == 2024, 'total_substantive_oas'] == 0).mean():.1f}%",
            f"{100 * (df.loc[df.issue_year == 2025, 'total_substantive_oas'] == 0).mean():.1f}%",
        ],
        [
            "% With ≥ 1 Final OA",
            f"{100 * (df.loc[df.issue_year == 2024, 'final_oa_count'] >= 1).mean():.1f}%",
            f"{100 * (df.loc[df.issue_year == 2025, 'final_oa_count'] >= 1).mean():.1f}%",
        ],
        [
            "Avg Days Filing to NOA",
            round(df.loc[df.issue_year == 2024, "days_filing_to_noa"].mean(), 0),
            round(df.loc[df.issue_year == 2025, "days_filing_to_noa"].mean(), 0),
        ],
        [
            "Applications with Interviews",
            int((df.loc[df.issue_year == 2024, "had_examiner_interview"]).sum()),
            int((df.loc[df.issue_year == 2025, "had_examiner_interview"]).sum()),
        ],
        [
            "Interviews Led to Immediate NOA (≤90d)",
            int((df.loc[df.issue_year == 2024, "interview_led_to_noa"]).sum()),
            int((df.loc[df.issue_year == 2025, "interview_led_to_noa"]).sum()),
        ],
    ]
    for r_idx, row in enumerate(rows, start=1):
        for c_idx, val in enumerate(row, start=1):
            cell = ws.cell(row=r_idx, column=c_idx, value=val)
            if r_idx == 1:
                cell.font = HEADER_FONT
                cell.fill = HEADER_FILL


def build_excel_report(db: Session, output_path: str) -> None:
    """Build the full multi-tab Excel workbook."""
    wb = Workbook()
    wb.remove(wb.active)

    df_all = report_all_harness(db)
    ws1 = wb.create_sheet("All Harness IP")
    _write_df_to_sheet(ws1, df_all, highlight_jac=True)

    offices = report_by_office(db)
    for office_name, df_office in offices.items():
        safe = (office_name or "UNKNOWN")[:31]
        ws = wb.create_sheet(f"Office - {safe}")
        _write_df_to_sheet(ws, df_office, highlight_jac=(office_name == "DC"))

    df_clients = report_specific_clients(db, ["15639", "2557SI", "9587SI"])
    ws3 = wb.create_sheet("Samsung Clients")
    _write_df_to_sheet(ws3, df_clients, highlight_jac=True)

    df_int = report_interview_to_noa(db)
    ws4 = wb.create_sheet("Interview to NOA")
    _write_df_to_sheet(ws4, df_int)

    df_au = report_art_unit_summary(db)
    ws5 = wb.create_sheet("Art Unit Summary")
    _write_df_to_sheet(ws5, df_au)

    ws6 = wb.create_sheet("Summary Statistics")
    _write_summary_tab(ws6, df_all)

    wb.save(output_path)
