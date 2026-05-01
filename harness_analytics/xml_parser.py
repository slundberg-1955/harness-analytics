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


# Allowance Analytics v2 derived fields. All come from the already-stored
# Biblio XML so we never need to refetch from Patent Center.
#
# `noa_mailed_date`: prefer the explicit `<NoticeOfAllowanceMailedDate>`
# element (newer Patent Center schemas surface it directly); otherwise fall
# back to the earliest FileContentHistory entry whose TransactionDescription
# matches "Notice of Allowance" (mailed direction). The
# ``application_analytics`` row also stores ``first_noa_date`` derived from
# event classification — but that table isn't always populated for a row
# (e.g. fresh ingests, partial backfills), so the XML helper is the more
# reliable source for the recency cohort axis.
_NOA_DESC_PATTERNS = (
    "notice of allowance",
    "mail notice of allowance",
)


def noa_mailed_date_from_root(root: Any) -> Optional[date]:
    if root is None:
        return None
    for raw in root.xpath(".//NoticeOfAllowanceMailedDate/text()"):
        d = parse_date(str(raw))
        if d is not None:
            return d
    candidates: list[date] = []
    for el in root.xpath(".//FileContentHistories/FileContentHistory"):
        desc = (extract_text(el, "TransactionDescription/text()") or "").lower()
        if any(p in desc for p in _NOA_DESC_PATTERNS):
            d = parse_date(extract_text(el, "TransactionDate/text()"))
            if d is not None:
                candidates.append(d)
    return min(candidates) if candidates else None


def noa_mailed_date_from_xml(xml_text: str | None) -> Optional[date]:
    if not xml_text or not xml_text.strip():
        return None
    try:
        root = etree.fromstring(xml_text.encode("utf-8"))
    except etree.XMLSyntaxError:
        return None
    return noa_mailed_date_from_root(root)


# `abandonment_date`: prefer explicit `<AbandonmentDate>`; otherwise pick the
# earliest FileContentHistory entry whose description is a Notice/Letter of
# Abandonment. Status code 161 alone is insufficient because it doesn't tell
# us *when* the application went abandoned.
_ABANDON_DESC_PATTERNS = (
    "abandonment",
    "letter of abandonment",
    "notice of abandonment",
)


def abandonment_date_from_root(root: Any) -> Optional[date]:
    if root is None:
        return None
    for raw in root.xpath(".//AbandonmentDate/text()"):
        d = parse_date(str(raw))
        if d is not None:
            return d
    candidates: list[date] = []
    for el in root.xpath(".//FileContentHistories/FileContentHistory"):
        desc = (extract_text(el, "TransactionDescription/text()") or "").lower()
        if any(p in desc for p in _ABANDON_DESC_PATTERNS):
            d = parse_date(extract_text(el, "TransactionDate/text()"))
            if d is not None:
                candidates.append(d)
    return min(candidates) if candidates else None


def abandonment_date_from_xml(xml_text: str | None) -> Optional[date]:
    if not xml_text or not xml_text.strip():
        return None
    try:
        root = etree.fromstring(xml_text.encode("utf-8"))
    except etree.XMLSyntaxError:
        return None
    return abandonment_date_from_root(root)


# `family_root_app_no`: walk ParentContinuityList where the current app is
# the child, take the parent application number, and recurse upward. Patent
# Center XML typically lists the immediate parent only, so the most useful
# heuristic is "earliest parent in the chain we can see in this row's XML".
# We pick the parent with the earliest filing date if available, otherwise
# the first non-PCT parent listed. When no parent is listed, the application
# is its own family root.
def family_root_app_no_from_root(root: Any, current_app_no: str | None) -> Optional[str]:
    if root is None:
        return None
    me = normalize_application_number_key(current_app_no) if current_app_no else ""
    parents: list[tuple[Optional[date], str]] = []
    for el in root.xpath(".//Continuity/ParentContinuityList/ParentContinuity"):
        child = extract_text(el, "ChildApplicationNumber/text()")
        parent = extract_text(el, "ParentApplicationNumber/text()")
        if not parent:
            continue
        # Skip PCT parents: harness convention treats PCT national-stage
        # parents as a separate priority axis, not as a family root.
        if not is_non_pct_parent_application_number(parent):
            continue
        if me and child and normalize_application_number_key(child) != me:
            # Only entries where this app is the direct child describe the
            # current row's prosecution chain. Other entries describe other
            # generations of the same family and can confuse the picker.
            continue
        d = parse_date(extract_text(el, "ParentApplicationFilingDate/text()") or extract_text(el, "FilingDate/text()"))
        parents.append((d, parent.strip()))
    if not parents:
        return current_app_no.strip() if current_app_no else None
    parents.sort(key=lambda p: (p[0] is None, p[0] or date.max))
    return parents[0][1]


def family_root_app_no_from_xml(application_number: str | None, xml_text: str | None) -> Optional[str]:
    if not xml_text or not xml_text.strip():
        return application_number.strip() if application_number else None
    try:
        root = etree.fromstring(xml_text.encode("utf-8"))
    except etree.XMLSyntaxError:
        return application_number.strip() if application_number else None
    return family_root_app_no_from_root(root, application_number)


# `has_foreign_priority`: spec §5.7 — true when ForeignPriorityList has any
# entries, OR when ParentContinuityList contains a PCT national-stage entry.
def has_foreign_priority_from_root(root: Any) -> bool:
    if root is None:
        return False
    if root.xpath(".//ForeignPriorityList/ForeignPriority"):
        return True
    for el in root.xpath(".//Continuity/ParentContinuityList/ParentContinuity"):
        parent = (extract_text(el, "ParentApplicationNumber/text()") or "").upper()
        if parent.startswith("PCT/") or parent.startswith("PCT "):
            return True
        status = (extract_text(el, "ParentApplicationStatusCode/text()") or "").upper()
        if status == "PCT":
            return True
    return False


def has_foreign_priority_from_xml(xml_text: str | None) -> bool:
    if not xml_text or not xml_text.strip():
        return False
    try:
        root = etree.fromstring(xml_text.encode("utf-8"))
    except etree.XMLSyntaxError:
        return False
    return has_foreign_priority_from_root(root)


def earliest_priority_date_from_root(root: Any) -> Any:
    """Earliest claim from <DomesticPriorityList> + <ForeignPriorityList>.

    Used by the timeline ``priority_later_of`` and ``pct_national`` rules.
    Returns a ``date`` or ``None`` if no claim is present or parseable.
    """
    if root is None:
        return None
    candidates = []
    for path in (
        ".//DomesticPriorityList/DomesticPriority/FilingDate/text()",
        ".//DomesticPriority/FilingDate/text()",
        ".//ForeignPriorityList/ForeignPriority/FilingDate/text()",
        ".//ForeignPriority/FilingDate/text()",
        ".//DomesticBenefit/FilingDate/text()",
    ):
        for raw in root.xpath(path):
            d = parse_date(str(raw))
            if d is not None:
                candidates.append(d)
    if not candidates:
        return None
    return min(candidates)


def earliest_priority_date_from_xml(xml_text: str | None):
    if not xml_text or not xml_text.strip():
        return None
    try:
        root = etree.fromstring(xml_text.encode("utf-8"))
    except etree.XMLSyntaxError:
        return None
    return earliest_priority_date_from_root(root)


# Application-type classifier. Buckets each application into one of seven
# kinds so the Applicant Trends "Filings by Type" chart can stack bars by
# discrete bucket. Provisional and design are derivable from the
# application-number series alone (USPTO assigns 60/61/62/63 to provisionals
# and 29 to designs); CON / CIP / DIV require inspecting the row's own
# ParentContinuity entry's ContinuityDescription. Reissue / reexam series
# (35/90/95/96) are surfaced as "other" so they don't masquerade as
# regular non-provisional utility filings.
_PROVISIONAL_PREFIXES: tuple[str, ...] = ("60/", "61/", "62/", "63/")
_DESIGN_PREFIXES: tuple[str, ...] = ("29/",)
_OTHER_PREFIXES: tuple[str, ...] = ("35/", "90/", "95/", "96/")


def _strip_app_no(app_no: str | None) -> str:
    return (app_no or "").strip()


def _has_prefix(app_no: str, prefixes: tuple[str, ...]) -> bool:
    if not app_no:
        return False
    for p in prefixes:
        if app_no.startswith(p):
            return True
        # Tolerate a missing slash (some sources strip it): "60" + 8 digits.
        bare = p.rstrip("/")
        if app_no.startswith(bare) and len(app_no) > len(bare) and app_no[len(bare)].isdigit():
            return True
    return False


def _continuation_kind_from_root(application_number: str | None, root: Any) -> Optional[str]:
    """Return ``"con"`` / ``"cip"`` / ``"div"`` based on this row's own
    ParentContinuity entry. Returns ``None`` when no qualifying parent entry
    exists for this application (so caller falls back to ``regular``).
    """
    if root is None:
        return None
    child_key = normalize_application_number_key(application_number) if application_number else ""
    if not child_key:
        return None
    for el in root.xpath(".//Continuity/ParentContinuityList/ParentContinuity"):
        child = extract_text(el, "ChildApplicationNumber/text()")
        parent = extract_text(el, "ParentApplicationNumber/text()")
        if not child or not parent:
            continue
        if normalize_application_number_key(child) != child_key:
            continue
        if not is_non_pct_parent_application_number(parent):
            continue
        desc = (extract_text(el, "ContinuityDescription/text()") or "").strip().lower()
        if desc in {"continuation in part", "continuation-in-part", "continuation in-part"}:
            return "cip"
        if desc in {"division", "divisional"}:
            return "div"
        if desc == "continuation":
            return "con"
    return None


def classify_application_type(application_number: str | None, root: Any) -> str:
    """Bucket an application into one of:
    ``provisional`` | ``regular`` | ``con`` | ``div`` | ``cip`` | ``design`` | ``other``.

    Order matters: app-number-series checks come first because a design
    continuation still wants to read as ``design`` (the chart is about
    application-type taxonomy, not prosecution lineage).
    """
    app_no = _strip_app_no(application_number)
    if _has_prefix(app_no, _PROVISIONAL_PREFIXES):
        return "provisional"
    if _has_prefix(app_no, _DESIGN_PREFIXES):
        return "design"
    if _has_prefix(app_no, _OTHER_PREFIXES):
        return "other"
    cont = _continuation_kind_from_root(application_number, root)
    if cont is not None:
        return cont
    return "regular"


def classify_application_type_from_xml(application_number: str | None, xml_text: str | None) -> str:
    """Best-effort classifier when only stored XML is available.

    Falls back to app-number-only inference when XML is missing or
    malformed; that path still gets provisional/design/other right and
    classifies anything else as ``regular`` (potentially under-counting
    CON/CIP/DIV when the XML can't be parsed — flagged as a known
    limitation in the methodology footer).
    """
    app_no = _strip_app_no(application_number)
    if _has_prefix(app_no, _PROVISIONAL_PREFIXES):
        return "provisional"
    if _has_prefix(app_no, _DESIGN_PREFIXES):
        return "design"
    if _has_prefix(app_no, _OTHER_PREFIXES):
        return "other"
    if not xml_text or not xml_text.strip():
        return "regular"
    try:
        root = etree.fromstring(xml_text.encode("utf-8"))
    except etree.XMLSyntaxError:
        return "regular"
    cont = _continuation_kind_from_root(application_number, root)
    return cont if cont is not None else "regular"


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
    earliest_priority_date = earliest_priority_date_from_root(root)
    abandonment_date = abandonment_date_from_root(root)
    noa_mailed_date = noa_mailed_date_from_root(root)
    family_root_app_no = family_root_app_no_from_root(root, app_num)
    has_foreign_priority = has_foreign_priority_from_root(root)
    application_type = classify_application_type(app_num, root)

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
        "earliest_priority_date": earliest_priority_date,
        "abandonment_date": abandonment_date,
        "noa_mailed_date": noa_mailed_date,
        "family_root_app_no": family_root_app_no,
        "has_foreign_priority": has_foreign_priority,
        "application_type": application_type,
    }
