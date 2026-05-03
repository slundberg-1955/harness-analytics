"""Tests for Biblio XML parsing."""

from pathlib import Path

import pytest

from harness_analytics.xml_parser import (
    abandonment_date_from_xml,
    child_of_prior_us_parent_from_xml,
    classify_application_type_from_xml,
    continuity_child_of_prior_us_parent,
    earliest_priority_date_from_xml,
    family_root_app_no_from_xml,
    has_child_continuation_from_xml,
    has_foreign_priority_from_xml,
    noa_mailed_date_from_xml,
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


# ---------------------------------------------------------------------------
# Allowance Analytics v2 derived fields (spec §6).
# ---------------------------------------------------------------------------


def test_noa_mailed_date_prefers_explicit_element() -> None:
    """When <NoticeOfAllowanceMailedDate> exists, use it verbatim."""
    from datetime import date

    xml = """<?xml version="1.0" encoding="UTF-8"?>
<PatentCenterApplication>
  <ApplicationBibliographicData><ApplicationNumber>17000100</ApplicationNumber></ApplicationBibliographicData>
  <NoticeOfAllowanceMailedDate>2024-03-15T00:00:00</NoticeOfAllowanceMailedDate>
  <FileContentHistories>
    <FileContentHistory>
      <TransactionDate>2024-09-01T00:00:00</TransactionDate>
      <TransactionDescription>Mail Notice of Allowance</TransactionDescription>
    </FileContentHistory>
  </FileContentHistories>
  <ImageFileWrapperList/>
</PatentCenterApplication>"""
    data = parse_biblio_xml(xml)
    assert data["noa_mailed_date"] == date(2024, 3, 15)
    assert noa_mailed_date_from_xml(xml) == date(2024, 3, 15)


def test_noa_mailed_date_falls_back_to_file_content_history() -> None:
    from datetime import date

    xml = """<?xml version="1.0" encoding="UTF-8"?>
<PatentCenterApplication>
  <ApplicationBibliographicData><ApplicationNumber>17000101</ApplicationNumber></ApplicationBibliographicData>
  <FileContentHistories>
    <FileContentHistory>
      <TransactionDate>2023-06-01T00:00:00</TransactionDate>
      <TransactionDescription>Mail Non-Final Rejection</TransactionDescription>
    </FileContentHistory>
    <FileContentHistory>
      <TransactionDate>2024-04-12T00:00:00</TransactionDate>
      <TransactionDescription>Mail Notice of Allowance</TransactionDescription>
    </FileContentHistory>
    <FileContentHistory>
      <TransactionDate>2024-08-01T00:00:00</TransactionDate>
      <TransactionDescription>Notice of Allowance and Fees Due</TransactionDescription>
    </FileContentHistory>
  </FileContentHistories>
  <ImageFileWrapperList/>
</PatentCenterApplication>"""
    data = parse_biblio_xml(xml)
    assert data["noa_mailed_date"] == date(2024, 4, 12)


def test_noa_mailed_date_none_when_no_signal() -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<PatentCenterApplication>
  <ApplicationBibliographicData><ApplicationNumber>17000102</ApplicationNumber></ApplicationBibliographicData>
  <FileContentHistories/>
  <ImageFileWrapperList/>
</PatentCenterApplication>"""
    data = parse_biblio_xml(xml)
    assert data["noa_mailed_date"] is None
    assert noa_mailed_date_from_xml("") is None
    assert noa_mailed_date_from_xml(None) is None


def test_abandonment_date_prefers_explicit_element() -> None:
    from datetime import date

    xml = """<?xml version="1.0" encoding="UTF-8"?>
<PatentCenterApplication>
  <ApplicationBibliographicData><ApplicationNumber>17000110</ApplicationNumber></ApplicationBibliographicData>
  <AbandonmentDate>2023-11-20T00:00:00</AbandonmentDate>
  <FileContentHistories/>
  <ImageFileWrapperList/>
</PatentCenterApplication>"""
    data = parse_biblio_xml(xml)
    assert data["abandonment_date"] == date(2023, 11, 20)
    assert abandonment_date_from_xml(xml) == date(2023, 11, 20)


def test_abandonment_date_falls_back_to_file_content_history() -> None:
    from datetime import date

    xml = """<?xml version="1.0" encoding="UTF-8"?>
<PatentCenterApplication>
  <ApplicationBibliographicData><ApplicationNumber>17000111</ApplicationNumber></ApplicationBibliographicData>
  <FileContentHistories>
    <FileContentHistory>
      <TransactionDate>2023-04-01T00:00:00</TransactionDate>
      <TransactionDescription>Notice of Abandonment</TransactionDescription>
    </FileContentHistory>
  </FileContentHistories>
  <ImageFileWrapperList/>
</PatentCenterApplication>"""
    data = parse_biblio_xml(xml)
    assert data["abandonment_date"] == date(2023, 4, 1)


def test_family_root_picks_first_non_pct_parent() -> None:
    """When this app has a non-PCT parent, that's the family root."""
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<PatentCenterApplication>
  <ApplicationBibliographicData><ApplicationNumber>17000200</ApplicationNumber></ApplicationBibliographicData>
  <Continuity>
    <ParentContinuityList>
      <ParentContinuity>
        <ParentApplicationNumber>15123456</ParentApplicationNumber>
        <ParentApplicationFilingDate>2018-07-15</ParentApplicationFilingDate>
        <ChildApplicationNumber>17000200</ChildApplicationNumber>
        <ContinuityDescription>is a Continuation of</ContinuityDescription>
      </ParentContinuity>
      <ParentContinuity>
        <ParentApplicationNumber>14654321</ParentApplicationNumber>
        <ParentApplicationFilingDate>2015-01-10</ParentApplicationFilingDate>
        <ChildApplicationNumber>15123456</ChildApplicationNumber>
        <ContinuityDescription>is a Continuation of</ContinuityDescription>
      </ParentContinuity>
    </ParentContinuityList>
    <ChildContinuityList/>
  </Continuity>
  <FileContentHistories/>
  <ImageFileWrapperList/>
</PatentCenterApplication>"""
    data = parse_biblio_xml(xml)
    # Picks the only entry where this app is the direct child; the
    # grandparent listing is correctly ignored (it describes the *parent's*
    # parent and would otherwise leak into other rows in the family tree).
    assert data["family_root_app_no"] == "15123456"
    assert family_root_app_no_from_xml("17000200", xml) == "15123456"


def test_family_root_falls_back_to_self_when_no_parent_listed() -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<PatentCenterApplication>
  <ApplicationBibliographicData><ApplicationNumber>17000201</ApplicationNumber></ApplicationBibliographicData>
  <Continuity><ParentContinuityList/><ChildContinuityList/></Continuity>
  <FileContentHistories/>
  <ImageFileWrapperList/>
</PatentCenterApplication>"""
    data = parse_biblio_xml(xml)
    assert data["family_root_app_no"] == "17000201"


def test_family_root_skips_pct_parents() -> None:
    """PCT parents shouldn't anchor the family root — they're a separate axis."""
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<PatentCenterApplication>
  <ApplicationBibliographicData><ApplicationNumber>17000202</ApplicationNumber></ApplicationBibliographicData>
  <Continuity>
    <ParentContinuityList>
      <ParentContinuity>
        <ParentApplicationNumber>PCT/US2018/050123</ParentApplicationNumber>
        <ChildApplicationNumber>17000202</ChildApplicationNumber>
        <ContinuityDescription>is a National Stage Entry of</ContinuityDescription>
      </ParentContinuity>
    </ParentContinuityList>
    <ChildContinuityList/>
  </Continuity>
  <FileContentHistories/>
  <ImageFileWrapperList/>
</PatentCenterApplication>"""
    data = parse_biblio_xml(xml)
    assert data["family_root_app_no"] == "17000202"


def test_has_foreign_priority_true_for_foreign_priority_list() -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<PatentCenterApplication>
  <ApplicationBibliographicData><ApplicationNumber>17000300</ApplicationNumber></ApplicationBibliographicData>
  <ForeignPriorityList>
    <ForeignPriority>
      <ApplicationNumber>EP1234567</ApplicationNumber>
      <FilingDate>2021-05-01</FilingDate>
      <CountryCode>EP</CountryCode>
    </ForeignPriority>
  </ForeignPriorityList>
  <FileContentHistories/>
  <ImageFileWrapperList/>
</PatentCenterApplication>"""
    data = parse_biblio_xml(xml)
    assert data["has_foreign_priority"] is True
    assert has_foreign_priority_from_xml(xml) is True


def test_has_foreign_priority_true_for_pct_parent() -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<PatentCenterApplication>
  <ApplicationBibliographicData><ApplicationNumber>17000301</ApplicationNumber></ApplicationBibliographicData>
  <Continuity>
    <ParentContinuityList>
      <ParentContinuity>
        <ParentApplicationNumber>PCT/US2019/012345</ParentApplicationNumber>
        <ChildApplicationNumber>17000301</ChildApplicationNumber>
        <ContinuityDescription>is a National Stage Entry of</ContinuityDescription>
      </ParentContinuity>
    </ParentContinuityList>
    <ChildContinuityList/>
  </Continuity>
  <FileContentHistories/>
  <ImageFileWrapperList/>
</PatentCenterApplication>"""
    data = parse_biblio_xml(xml)
    assert data["has_foreign_priority"] is True


def test_has_foreign_priority_false_for_us_only_application() -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<PatentCenterApplication>
  <ApplicationBibliographicData><ApplicationNumber>17000302</ApplicationNumber></ApplicationBibliographicData>
  <FileContentHistories/>
  <ImageFileWrapperList/>
</PatentCenterApplication>"""
    data = parse_biblio_xml(xml)
    assert data["has_foreign_priority"] is False
    assert has_foreign_priority_from_xml("") is False
    assert has_foreign_priority_from_xml(None) is False


# Live USPTO bib XML wraps entries in <ForeignPriorities> (plural) and
# uses <ForeignPriorityDate> instead of <FilingDate>. The parser should
# handle this shape too, otherwise has_foreign_priority is silently false
# for ~every real application (which is exactly the bug we just fixed).
def test_has_foreign_priority_true_for_live_uspto_wrapper() -> None:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<PatentCenterApplication>
  <ApplicationBibliographicData><ApplicationNumber>18489076</ApplicationNumber></ApplicationBibliographicData>
  <ForeignPriorities>
    <ForeignPriority>
      <ApplicationNumber>10-2022-0123456</ApplicationNumber>
      <ForeignPriorityDate>2022-09-15</ForeignPriorityDate>
      <CountryCode>KR</CountryCode>
    </ForeignPriority>
  </ForeignPriorities>
  <FileContentHistories/>
  <ImageFileWrapperList/>
</PatentCenterApplication>"""
    data = parse_biblio_xml(xml)
    assert data["has_foreign_priority"] is True
    assert has_foreign_priority_from_xml(xml) is True


def test_earliest_priority_date_reads_foreign_priority_date_element() -> None:
    from datetime import date

    xml = """<?xml version="1.0" encoding="UTF-8"?>
<PatentCenterApplication>
  <ApplicationBibliographicData><ApplicationNumber>18489077</ApplicationNumber></ApplicationBibliographicData>
  <DomesticPriorityList>
    <DomesticPriority><FilingDate>2024-02-01</FilingDate></DomesticPriority>
  </DomesticPriorityList>
  <ForeignPriorities>
    <ForeignPriority>
      <ApplicationNumber>EP9999999</ApplicationNumber>
      <ForeignPriorityDate>2022-09-15</ForeignPriorityDate>
      <CountryCode>EP</CountryCode>
    </ForeignPriority>
  </ForeignPriorities>
  <FileContentHistories/>
  <ImageFileWrapperList/>
</PatentCenterApplication>"""
    data = parse_biblio_xml(xml)
    assert data["earliest_priority_date"] == date(2022, 9, 15)
    assert earliest_priority_date_from_xml(xml) == date(2022, 9, 15)


# ---------------------------------------------------------------------------
# Application-type classifier (Filings by Type chart).
# ---------------------------------------------------------------------------


def _continuity_xml(child_app_no: str, parent_app_no: str, description: str) -> str:
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<PatentCenterApplication>
  <ApplicationBibliographicData><ApplicationNumber>{child_app_no}</ApplicationNumber></ApplicationBibliographicData>
  <Continuity>
    <ParentContinuityList>
      <ParentContinuity>
        <ChildApplicationNumber>{child_app_no}</ChildApplicationNumber>
        <ParentApplicationNumber>{parent_app_no}</ParentApplicationNumber>
        <ContinuityDescription>{description}</ContinuityDescription>
      </ParentContinuity>
    </ParentContinuityList>
  </Continuity>
  <FileContentHistories/>
  <ImageFileWrapperList/>
</PatentCenterApplication>"""


@pytest.mark.parametrize(
    "app_no,expected",
    [
        ("60/123,456", "provisional"),
        ("61/987654", "provisional"),
        ("62/555555", "provisional"),
        ("63/111111", "provisional"),
        ("29/123456", "design"),
        ("35/123456", "other"),
        ("90/000001", "other"),
        ("95/000001", "other"),
        ("96/000001", "other"),
        # Bare-form (no slash) variants the field sometimes carries.
        ("60123456",  "provisional"),
        ("29111111",  "design"),
        # Utility series with no continuity entry -> regular.
        ("17000302",  "regular"),
    ],
)
def test_classify_application_type_by_app_number_prefix(app_no: str, expected: str) -> None:
    # Use empty XML so only the app-number-prefix branch runs.
    assert classify_application_type_from_xml(app_no, "") == expected


@pytest.mark.parametrize(
    "description,expected",
    [
        ("Continuation", "con"),
        ("Continuation in Part", "cip"),
        ("Continuation-in-Part", "cip"),
        ("Division", "div"),
        ("Divisional", "div"),
    ],
)
def test_classify_application_type_uses_continuity_description(description: str, expected: str) -> None:
    """Utility-series app with a ParentContinuity entry that names this app
    as the child should be classified by the entry's
    ContinuityDescription, mirroring the Python aggregator's behavior.
    """
    xml = _continuity_xml("17552591", "16111222", description)
    assert classify_application_type_from_xml("17552591", xml) == expected


def test_classify_application_type_falls_back_to_regular_for_pct_only_parent() -> None:
    """A PCT parent doesn't carry a CHM-qualifying ContinuityDescription
    (we treat PCT national-stage as a separate priority axis), so the row
    falls through to ``regular``.
    """
    xml = _continuity_xml("17552591", "PCT/US2020/012345", "Continuation")
    assert classify_application_type_from_xml("17552591", xml) == "regular"


def test_classify_application_type_handles_missing_or_invalid_xml() -> None:
    # No XML, utility number -> regular.
    assert classify_application_type_from_xml("17552591", None) == "regular"
    assert classify_application_type_from_xml("17552591", "") == "regular"
    # Invalid XML, design number -> design (prefix wins).
    assert classify_application_type_from_xml("29123456", "<not xml") == "design"


def test_parse_biblio_xml_surfaces_application_type() -> None:
    xml = _continuity_xml("17999111", "16000222", "Divisional")
    data = parse_biblio_xml(xml)
    assert data["application_type"] == "div"
