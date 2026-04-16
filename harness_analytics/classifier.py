"""Classify prosecution transaction descriptions and IFW document metadata."""

from __future__ import annotations

EVENT_TYPE_MAP: dict[str, str] = {
    "Non-Final Rejection": "NONFINAL_OA",
    "Mail Non-Final Rejection": "NONFINAL_OA",
    "Final Rejection": "FINAL_OA",
    "Mail Final Rejection": "FINAL_OA",
    "Notice of Allowance and Fees Due": "NOA",
    "Mail Notice of Allowance": "NOA",
    "Notice of Allowance Data Verification Completed": "NOA_INTERNAL",
    "Response after Non-Final Action": "RESPONSE_NONFINAL",
    "Response After Final Action": "RESPONSE_FINAL",
    "Amendment/Request for Reconsideration-After Non-Final Rejection": "RESPONSE_NONFINAL",
    "Request for Continued Examination": "RCE",
    "Filing of RCE": "RCE",
    "Examiner Interview Summary": "INTERVIEW",
    "Interview Summary": "INTERVIEW",
    "Telephone Interview Summary": "INTERVIEW",
    "Filing Receipt": "FILING_RECEIPT",
    "Case Docketed to Examiner in GAU": "DOCKETED",
    "Issue Fee Payment Received": "ISSUE_FEE",
    "Issue Notification Mailed": "ISSUE_NOTIFICATION",
}

# IFW document codes (Part 9) — used to supplement history-based classification.
NONFINAL_OA_DOC_CODES = frozenset({"CTNF"})
FINAL_OA_DOC_CODES = frozenset({"CTFR"})
NOA_DOC_CODES = frozenset({"NOA"})
# Interview signals in analytics are IFW-only; these codes only (no description fallback).
INTERVIEW_IFW_DOC_CODES = frozenset({"EXIN", "INTV.SUM.EX", "INTV.SUM.APP"})
# IFW document code counted in analytics (file wrapper).
IFW_A_NE_DOC_CODE = "A.NE"
INTERVIEW_DOC_CODES = INTERVIEW_IFW_DOC_CODES


def classify_event(description: str) -> str:
    """Return event_type for a FileContentHistory transaction description string."""
    desc = (description or "").strip()
    for pattern, etype in EVENT_TYPE_MAP.items():
        if pattern.lower() in desc.lower():
            return etype
    return "OTHER"


def classify_event_with_ifw_fallback(
    description: str,
    *,
    document_code: str | None = None,
    document_description: str | None = None,
) -> str:
    """
    Classify a history-line description; if still OTHER, infer from IFW codes/descriptions
    when the same logical event is ambiguous in history text.
    """
    _ = document_description
    primary = classify_event(description)
    if primary != "OTHER":
        return primary
    code = (document_code or "").strip().upper()

    if code in NONFINAL_OA_DOC_CODES:
        return "NONFINAL_OA"
    if code in FINAL_OA_DOC_CODES:
        return "FINAL_OA"
    if code in NOA_DOC_CODES:
        return "NOA"
    if code in INTERVIEW_DOC_CODES:
        return "INTERVIEW"
    return "OTHER"


def ifw_document_suggests_interview(document_code: str | None, document_description: str | None) -> bool:
    """True if IFW document_code is one of the interview codes used for analytics."""
    _ = document_description
    code = (document_code or "").strip().upper()
    return code in INTERVIEW_IFW_DOC_CODES


def ifw_document_suggests_noa(document_code: str | None, document_description: str | None) -> bool:
    """True only for IFW Notice of Allowance (code NOA)."""
    _ = document_description
    code = (document_code or "").strip().upper()
    return code == "NOA"
