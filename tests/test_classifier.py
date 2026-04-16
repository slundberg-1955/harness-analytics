"""Tests for event / IFW classification."""

import pytest

from harness_analytics.classifier import (
    IFW_A_NE_DOC_CODE,
    classify_event,
    classify_event_with_ifw_fallback,
    ifw_document_suggests_interview,
    ifw_document_suggests_noa,
)


@pytest.mark.parametrize(
    "desc,expected",
    [
        ("Mail Non-Final Rejection", "NONFINAL_OA"),
        ("Final Rejection", "FINAL_OA"),
        ("Mail Notice of Allowance", "NOA"),
        ("Notice of Allowance Data Verification Completed", "NOA_INTERNAL"),
        ("Request for Continued Examination", "RCE"),
        ("Examiner Interview Summary", "INTERVIEW"),
        ("Random admin text", "OTHER"),
    ],
)
def test_classify_event(desc: str, expected: str) -> None:
    assert classify_event(desc) == expected


def test_classify_event_case_insensitive() -> None:
    assert classify_event("mail non-final rejection") == "NONFINAL_OA"


def test_ifw_fallback_nonfinal_code() -> None:
    assert (
        classify_event_with_ifw_fallback(
            "Some unclear status",
            document_code="CTNF",
            document_description="Office action",
        )
        == "NONFINAL_OA"
    )


def test_ifw_a_ne_doc_code_constant() -> None:
    assert IFW_A_NE_DOC_CODE == "A.NE"


def test_ifw_fallback_mctnf_is_other() -> None:
    assert (
        classify_event_with_ifw_fallback(
            "Some unclear status",
            document_code="MCTNF",
            document_description="Office action",
        )
        == "OTHER"
    )


def test_ifw_document_suggests_interview() -> None:
    assert ifw_document_suggests_interview("EXIN", None) is True
    assert ifw_document_suggests_interview("INTV.SUM.EX", "") is True
    assert ifw_document_suggests_interview("intv.sum.app", None) is True
    assert ifw_document_suggests_interview("INTSUM", "Interview Summary") is False
    assert ifw_document_suggests_interview("CTNF", "Non-Final") is False


def test_ifw_document_suggests_noa() -> None:
    assert ifw_document_suggests_noa("NOA", "Notice of Allowance and Fees Due") is True
    assert ifw_document_suggests_noa("CTFR", "Final") is False
    assert ifw_document_suggests_noa("MN/=.", "Mail Notice of Allowance") is False


def test_ifw_fallback_interview_code_only() -> None:
    assert (
        classify_event_with_ifw_fallback(
            "Unclear",
            document_code="INTV.SUM.EX",
            document_description="x",
        )
        == "INTERVIEW"
    )
