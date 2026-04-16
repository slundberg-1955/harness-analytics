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

    assignee_name = extract_text(root, ".//assigneeBag/organizationStandardName/text()") or extract_text(
        root, ".//Applicants/Applicant/LegalEntityName/text()"
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
        "attorneys": attorneys,
        "events": events,
        "documents": documents,
        "inventors": inventors,
    }
