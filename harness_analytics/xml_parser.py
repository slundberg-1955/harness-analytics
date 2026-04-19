"""Patent Center Biblio XML → structured dict for ingestion."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Optional

from lxml import etree


def parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.split("T")[0]).date()
    except (ValueError, TypeError):
        return None


def parse_datetime_utc(s: Optional[str]) -> Optional[datetime]:
    """Parse MailRoomDate-style timestamps to timezone-aware datetime (UTC if naive)."""
    if not s:
        return None
    text = s.strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def extract_text(el: Any, xpath: str) -> Optional[str]:
    if el is None:
        return None
    results = el.xpath(xpath)
    if results:
        return str(results[0]).strip() or None
    return None


def normalize_application_number_key(raw: str | None) -> str:
    """Match Patent Center / portal lookups: strip and remove whitespace."""
    return "".join((raw or "").strip().split())


def is_non_pct_parent_application_number(parent: str | None) -> bool:
    """True if parent number is treated as a prior US application (excludes PCT parents)."""
    p = (parent or "").strip()
    if not p:
        return False
    pu = p.upper()
    if pu.startswith("PCT/") or pu.startswith("PCT "):
        return False
    return True


def continuity_child_of_prior_us_parent(application_number: str, root: Any) -> bool:
    """
    True when ``ParentContinuityList`` lists this application as ``ChildApplicationNumber``
    with a ``ParentApplicationNumber`` that is not a PCT filing (per Harness definition of
    prior US parent).
    """
    child_key = normalize_application_number_key(application_number)
    if not child_key:
        return False
    for el in root.xpath(".//Continuity/ParentContinuityList/ParentContinuity"):
        child = extract_text(el, "ChildApplicationNumber/text()")
        parent = extract_text(el, "ParentApplicationNumber/text()")
        if not child or not parent:
            continue
        if normalize_application_number_key(child) != child_key:
            continue
        if is_non_pct_parent_application_number(parent):
            return True
    return False


def child_of_prior_us_parent_from_xml(application_number: str | None, xml_text: str | None) -> bool:
    """Parse stored Biblio XML; false when XML missing or invalid."""
    if not application_number or not xml_text or not xml_text.strip():
        return False
    try:
        root = etree.fromstring(xml_text.encode("utf-8"))
    except etree.XMLSyntaxError:
        return False
    return continuity_child_of_prior_us_parent(application_number, root)


# Strict CHM definition: a child counts only when it's a Continuation,
# Continuation-in-Part, or Divisional. The corpus contains a few minor spelling
# variants (with/without hyphen, "Division" vs "Divisional") so we accept all.
_CHM_CHILD_DESCRIPTIONS = {
    "continuation",
    "continuation in part",
    "continuation-in-part",
    "division",
    "divisional",
}


def has_child_continuation_from_root(root: Any) -> bool:
    """True when ``ChildContinuityList`` includes a CHM-qualifying child."""
    if root is None:
        return False
    for el in root.xpath(".//Continuity/ChildContinuityList/ChildContinuity"):
        desc = (extract_text(el, "ContinuityDescription/text()") or "").strip().lower()
        if desc in _CHM_CHILD_DESCRIPTIONS:
            return True
    return False


def has_child_continuation_from_xml(xml_text: str | None) -> bool:
    """Parse stored Biblio XML; false when XML missing or invalid."""
    if not xml_text or not xml_text.strip():
        return False
    try:
        root = etree.fromstring(xml_text.encode("utf-8"))
    except etree.XMLSyntaxError:
        return False
    return has_child_continuation_from_root(root)


def parse_biblio_xml(xml_text: str) -> dict[str, Any]:
    """
    Parse a Patent Center Biblio XML string.
    Returns a dict suitable for bulk-inserting into the schema.
    """
    root = etree.fromstring(xml_text.encode("utf-8"))

    bib = root.find(".//ApplicationBibliographicData")
    app_num = extract_text(bib, "ApplicationNumber/text()") if bib is not None else None

    examiner_first = extract_text(bib, "ExaminerName/FirstName/text()") if bib is not None else None
    examiner_last = extract_text(bib, "ExaminerName/LastName/text()") if bib is not None else None

    if not examiner_first:
        examiner_first = extract_text(root, ".//examinerDetails/givenName/text()")
        examiner_last = extract_text(root, ".//examinerDetails/familyName/text()")
    examiner_phone = extract_text(root, ".//examinerDetails/phoneNumber/text()")

    attorneys: list[dict[str, Any]] = []
    for role, section_xpath in [
        ("CORRESPONDENCE", ".//CorrespondenceInfo/CorrespondenceInfo/Attorneys/Attorney"),
        ("POA", ".//POAInfo/POAInfo/Attorneys/Attorney"),
    ]:
        for atty_el in root.xpath(section_xpath):
            attorneys.append(
                {
                    "role": role,
                    "registration_number": extract_text(atty_el, "RegistrationNumber/text()"),
                    "first_name": extract_text(atty_el, "AttorneyName/FirstName/text()"),
                    "last_name": extract_text(atty_el, "AttorneyName/LastName/text()"),
                    "phone": extract_text(
                        atty_el, "AttorneyContacts/AttorneyContact/TelecommunicationNumber/text()"
                    ),
                    "agent_status": extract_text(atty_el, "AgentStatus/text()"),
                    "is_first": False,
                }
            )

    poa_attorneys = [a for a in attorneys if a["role"] == "POA"]
    if poa_attorneys:
        poa_attorneys[0]["is_first"] = True

    events: list[dict[str, Any]] = []
    for seq, event_el in enumerate(
        root.xpath(".//FileContentHistories/FileContentHistory"), start=1
    ):
        events.append(
            {
                "transaction_date": parse_date(extract_text(event_el, "TransactionDate/text()")),
                "transaction_description": extract_text(event_el, "TransactionDescription/text()") or "",
                "status_number": extract_text(event_el, "StatusNumber/text()"),
                "status_description": extract_text(event_el, "StatusDescription/text()"),
                "seq_order": seq,
            }
        )

    documents: list[dict[str, Any]] = []
    for doc_el in root.xpath(".//ImageFileWrapperList/ImageFileWrapperDocument"):
        documents.append(
            {
                "document_code": extract_text(doc_el, "FileWrapperDocumentCode/text()"),
                "document_description": extract_text(doc_el, "DocumentDescription/text()"),
                "mail_room_date": extract_text(doc_el, "MailRoomDate/text()"),
                "page_quantity": extract_text(doc_el, "PageQuantity/text()"),
                "document_category": extract_text(doc_el, "DocumentCategory/text()"),
            }
        )

    # Applicant comes from <Applicants/Applicant/LegalEntityName>; pick first non-empty.
    applicant_name: str | None = None
    for app_el in root.xpath(".//Applicants/Applicant"):
        v = extract_text(app_el, "LegalEntityName/text()")
        if v:
            applicant_name = v
            break

    assignee_name = (
        extract_text(root, ".//assigneeBag/organizationStandardName/text()")
        or applicant_name
    )

    inventors: list[dict[str, Any]] = []
    for inv_el in root.xpath(".//Inventors/Inventor"):
        inventors.append(
            {
                "first_name": extract_text(inv_el, "InventorName/FirstName/text()"),
                "last_name": extract_text(inv_el, "InventorName/LastName/text()"),
                "city": extract_text(inv_el, ".//City/text()"),
                "country_code": extract_text(inv_el, ".//CountryCode/text()"),
            }
        )

    uspto_customer = extract_text(bib, "CustomerNumber/text()") if bib is not None else None

    continuity_child = continuity_child_of_prior_us_parent(app_num, root) if app_num else False
    has_child_continuation = has_child_continuation_from_root(root)

    return {
        "application_number": app_num,
        "filing_date": parse_date(extract_text(bib, "FilingDate/text()")) if bib is not None else None,
        "issue_date": parse_date(extract_text(bib, "IssueDate/text()")) if bib is not None else None,
        "patent_number": extract_text(bib, "PatentNumber/text()") if bib is not None else None,
        "application_status_code": extract_text(bib, "ApplicationStatusCode/text()")
        if bib is not None
        else None,
        "application_status_text": extract_text(bib, "ApplicationStatusText/text()")
        if bib is not None
        else None,
        "application_status_date": parse_date(extract_text(bib, "ApplicationStatusDate/text()"))
        if bib is not None
        else None,
        "invention_title": extract_text(bib, "InventionTitle/text()") if bib is not None else None,
        "customer_number": uspto_customer,
        "hdp_customer_number": uspto_customer,
        "attorney_docket_number": extract_text(bib, "AttorneyDocketNumber/text()")
        if bib is not None
        else None,
        "confirmation_number": extract_text(bib, "ConfirmationNumber/text()") if bib is not None else None,
        "group_art_unit": extract_text(bib, "GroupArtUnit/text()") if bib is not None else None,
        "patent_class": extract_text(bib, "PatentClass/text()") if bib is not None else None,
        "patent_subclass": extract_text(bib, "PatentSubclass/text()") if bib is not None else None,
        "examiner_first_name": examiner_first,
        "examiner_last_name": examiner_last,
        "examiner_phone": examiner_phone,
        "assignee_name": assignee_name,
        "applicant_name": applicant_name,
        "attorneys": attorneys,
        "events": events,
        "documents": documents,
        "inventors": inventors,
        "continuity_child_of_prior_us": continuity_child,
        "has_child_continuation": has_child_continuation,
    }
