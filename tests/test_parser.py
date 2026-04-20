"""Tests for Biblio XML parsing."""

from pathlib import Path

import pytest

from harness_analytics.xml_parser import (
    child_of_prior_us_parent_from_xml,
    continuity_child_of_prior_us_parent,
    earliest_priority_date_from_xml,
    has_child_continuation_from_xml,
    parse_biblio_xml,
)

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "sample_17552591.xml"


def test_parse_sample_application_number() -> None:
    data = parse_biblio_xml(FIXTURE.read_text(encoding="utf-8"))
    assert data["application_number"] == "17552591"
    assert data["customer_number"] == "15639"
    assert data["hdp_customer_number"] == "15639"
    assert data["issue_date"].year == 2025
    assert data.get("continuity_child_of_prior_us") is False


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


def test_continuity_child_of_prior_us_parent() -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<PatentCenterApplication>
  <ApplicationBibliographicData>
    <ApplicationNumber>17135687</ApplicationNumber>
    <FilingDate>2015-01-01T00:00:00</FilingDate>
  </ApplicationBibliographicData>
  <Continuity>
    <ParentContinuityList>
      <ParentContinuity>
        <ParentApplicationNumber>14125698</ParentApplicationNumber>
        <ChildApplicationNumber>17135687</ChildApplicationNumber>
        <ContinuityDescription>is a Continuation of</ContinuityDescription>
      </ParentContinuity>
      <ParentContinuity>
        <ParentApplicationNumber>PCT/US2012/042281</ParentApplicationNumber>
        <ChildApplicationNumber>14125698</ChildApplicationNumber>
        <ContinuityDescription>is a National Stage Entry of</ContinuityDescription>
      </ParentContinuity>
    </ParentContinuityList>
    <ChildContinuityList/>
  </Continuity>
  <FileContentHistories/>
  <ImageFileWrapperList/>
</PatentCenterApplication>"""
    data = parse_biblio_xml(xml)
    assert data["application_number"] == "17135687"
    assert data["continuity_child_of_prior_us"] is True
    assert child_of_prior_us_parent_from_xml("17135687", xml) is True
    assert child_of_prior_us_parent_from_xml("1713 5687", xml) is True


def test_continuity_false_when_only_pct_parent_for_child() -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<PatentCenterApplication>
  <ApplicationBibliographicData>
    <ApplicationNumber>14125698</ApplicationNumber>
  </ApplicationBibliographicData>
  <Continuity>
    <ParentContinuityList>
      <ParentContinuity>
        <ParentApplicationNumber>PCT/US2012/042281</ParentApplicationNumber>
        <ChildApplicationNumber>14125698</ChildApplicationNumber>
        <ContinuityDescription>is a National Stage Entry of</ContinuityDescription>
      </ParentContinuity>
    </ParentContinuityList>
  </Continuity>
  <FileContentHistories/>
  <ImageFileWrapperList/>
</PatentCenterApplication>"""
    data = parse_biblio_xml(xml)
    assert data["continuity_child_of_prior_us"] is False


def test_continuity_helper_on_element() -> None:
    from lxml import etree

    root = etree.fromstring(
        b"""<PatentCenterApplication><ApplicationBibliographicData>
        <ApplicationNumber>1</ApplicationNumber></ApplicationBibliographicData>
        <Continuity><ParentContinuityList/></Continuity></PatentCenterApplication>"""
    )
    assert continuity_child_of_prior_us_parent("1", root) is False


def test_has_child_continuation_strict_chm_match() -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<PatentCenterApplication>
  <ApplicationBibliographicData><ApplicationNumber>17000001</ApplicationNumber></ApplicationBibliographicData>
  <Continuity>
    <ParentContinuityList/>
    <ChildContinuityList>
      <ChildContinuity>
        <ParentApplicationNumber>17000001</ParentApplicationNumber>
        <ChildApplicationNumber>18000002</ChildApplicationNumber>
        <ContinuityDescription>Continuation</ContinuityDescription>
      </ChildContinuity>
    </ChildContinuityList>
  </Continuity>
  <FileContentHistories/>
  <ImageFileWrapperList/>
</PatentCenterApplication>"""
    data = parse_biblio_xml(xml)
    assert data["has_child_continuation"] is True
    assert has_child_continuation_from_xml(xml) is True


def test_has_child_continuation_accepts_division_and_cip_variants() -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<PatentCenterApplication>
  <ApplicationBibliographicData><ApplicationNumber>17000010</ApplicationNumber></ApplicationBibliographicData>
  <Continuity>
    <ParentContinuityList/>
    <ChildContinuityList>
      <ChildContinuity>
        <ContinuityDescription>Continuation-in-Part</ContinuityDescription>
      </ChildContinuity>
      <ChildContinuity>
        <ContinuityDescription>Division</ContinuityDescription>
      </ChildContinuity>
    </ChildContinuityList>
  </Continuity>
  <FileContentHistories/>
  <ImageFileWrapperList/>
</PatentCenterApplication>"""
    data = parse_biblio_xml(xml)
    assert data["has_child_continuation"] is True


def test_has_child_continuation_false_for_non_chm_descriptions() -> None:
    # Children that are National Stage entries or Provisional priorities don't qualify.
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<PatentCenterApplication>
  <ApplicationBibliographicData><ApplicationNumber>17000020</ApplicationNumber></ApplicationBibliographicData>
  <Continuity>
    <ParentContinuityList/>
    <ChildContinuityList>
      <ChildContinuity>
        <ContinuityDescription>is the National Stage of International Application</ContinuityDescription>
      </ChildContinuity>
      <ChildContinuity>
        <ContinuityDescription>Claims Priority from Provisional Application</ContinuityDescription>
      </ChildContinuity>
    </ChildContinuityList>
  </Continuity>
  <FileContentHistories/>
  <ImageFileWrapperList/>
</PatentCenterApplication>"""
    data = parse_biblio_xml(xml)
    assert data["has_child_continuation"] is False


def test_has_child_continuation_false_when_no_child_list() -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<PatentCenterApplication>
  <ApplicationBibliographicData><ApplicationNumber>17000030</ApplicationNumber></ApplicationBibliographicData>
  <Continuity><ParentContinuityList/><ChildContinuityList/></Continuity>
  <FileContentHistories/>
  <ImageFileWrapperList/>
</PatentCenterApplication>"""
    data = parse_biblio_xml(xml)
    assert data["has_child_continuation"] is False
    assert has_child_continuation_from_xml("") is False
    assert has_child_continuation_from_xml(None) is False


def test_earliest_priority_date_picks_min_across_lists() -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<PatentCenterApplication>
  <ApplicationBibliographicData><ApplicationNumber>17000040</ApplicationNumber><FilingDate>2024-06-01</FilingDate></ApplicationBibliographicData>
  <DomesticPriorityList>
    <DomesticPriority><FilingDate>2023-08-15</FilingDate></DomesticPriority>
    <DomesticPriority><FilingDate>2024-02-01</FilingDate></DomesticPriority>
  </DomesticPriorityList>
  <ForeignPriorityList>
    <ForeignPriority><FilingDate>2023-04-10</FilingDate></ForeignPriority>
  </ForeignPriorityList>
  <FileContentHistories/>
  <ImageFileWrapperList/>
</PatentCenterApplication>"""
    from datetime import date

    data = parse_biblio_xml(xml)
    assert data["earliest_priority_date"] == date(2023, 4, 10)
    assert earliest_priority_date_from_xml(xml) == date(2023, 4, 10)


def test_earliest_priority_date_none_when_no_claims() -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<PatentCenterApplication>
  <ApplicationBibliographicData><ApplicationNumber>17000050</ApplicationNumber></ApplicationBibliographicData>
  <FileContentHistories/>
  <ImageFileWrapperList/>
</PatentCenterApplication>"""
    data = parse_biblio_xml(xml)
    assert data["earliest_priority_date"] is None
    assert earliest_priority_date_from_xml("") is None
    assert earliest_priority_date_from_xml(None) is None
