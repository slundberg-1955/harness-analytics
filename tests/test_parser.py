"""Tests for Biblio XML parsing."""

from pathlib import Path

import pytest

from harness_analytics.xml_parser import parse_biblio_xml

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "sample_17552591.xml"


def test_parse_sample_application_number() -> None:
    data = parse_biblio_xml(FIXTURE.read_text(encoding="utf-8"))
    assert data["application_number"] == "17552591"
    assert data["customer_number"] == "15639"
    assert data["hdp_customer_number"] == "15639"
    assert data["issue_date"].year == 2025


def test_parse_attorneys_first_poa() -> None:
    data = parse_biblio_xml(FIXTURE.read_text(encoding="utf-8"))
    poa = [a for a in data["attorneys"] if a["role"] == "POA"]
    assert len(poa) == 1
    assert poa[0]["is_first"] is True
    assert poa[0]["registration_number"] == "35094"


def test_parse_events_sorted_input_order() -> None:
    data = parse_biblio_xml(FIXTURE.read_text(encoding="utf-8"))
    assert len(data["events"]) == 4
    assert data["events"][0]["transaction_description"].startswith("Mail Non-Final")


def test_parse_ifw_documents() -> None:
    data = parse_biblio_xml(FIXTURE.read_text(encoding="utf-8"))
    codes = {d["document_code"] for d in data["documents"]}
    assert "INTSUM" in codes
    assert "CTNF" in codes


def test_parse_minimal_xml_no_biblio() -> None:
    xml = """<?xml version="1.0"?><Root><FileContentHistories/></Root>"""
    data = parse_biblio_xml(xml)
    assert data["application_number"] is None
    assert data["events"] == []
