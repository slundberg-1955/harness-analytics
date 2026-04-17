"""Biblio endpoint: feeds fixture XML through a fake session and asserts shape."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("fastapi")
from starlette.testclient import TestClient

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "sample_17552591.xml"


class _FakeMappingsResult:
    def __init__(self, rows: list[dict[str, Any]]):
        self._rows = rows

    def all(self):
        return list(self._rows)

    def first(self):
        return self._rows[0] if self._rows else None


class _FakeResultWithMappings:
    def __init__(self, rows: list[dict[str, Any]]):
        self._rows = rows

    def mappings(self):
        return _FakeMappingsResult(self._rows)


class _FakeBiblioSession:
    """Returns the fixture application row + normalized child rows."""

    def __init__(self, xml_text: str):
        self.xml_text = xml_text

    def execute(self, statement, binds: dict[str, Any] | None = None):
        sql = str(statement)
        if "FROM applications WHERE application_number" in sql:
            return _FakeResultWithMappings([
                {
                    "id": 1,
                    "application_number": "17552591",
                    "invention_title": "Sample Invention",
                    "filing_date": date(2022, 1, 15),
                    "application_status_code": "150",
                    "application_status_text": "Patented Case",
                    "group_art_unit": "2100",
                    "patent_class": "606",
                    "customer_number": "15639",
                    "examiner_first_name": "Jane",
                    "examiner_last_name": "Examiner",
                    "xml_raw": self.xml_text,
                }
            ])
        if "FROM inventors" in sql:
            return _FakeResultWithMappings([
                {
                    "first_name": "Alice",
                    "last_name": "Inventor",
                    "city": "Austin",
                    "country_code": "US",
                }
            ])
        if "FROM application_attorneys" in sql:
            return _FakeResultWithMappings([
                {
                    "registration_number": "35094",
                    "first_name": "John",
                    "last_name": "Castellano",
                    "phone": "703-555-0100",
                    "agent_status": "ACTIVE",
                }
            ])
        if "FROM prosecution_events" in sql:
            return _FakeResultWithMappings([])
        if "FROM file_wrapper_documents" in sql:
            return _FakeResultWithMappings([])
        return _FakeResultWithMappings([])


def _make_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("PORTAL_PASSWORD", "test-pw")
    monkeypatch.setenv("SECRET_KEY", "unit-test-secret-key-32-chars-minimum!!")
    from harness_analytics.db import get_db
    from harness_analytics.portfolio_api import _parse_biblio_xml_cached
    from harness_analytics.server import create_app

    # Clear the process-wide cache so XML changes take effect between tests.
    _parse_biblio_xml_cached.cache_clear()

    xml_text = FIXTURE.read_text(encoding="utf-8")
    app = create_app()

    def override():
        yield _FakeBiblioSession(xml_text)

    app.dependency_overrides[get_db] = override
    return TestClient(app)


def test_biblio_returns_all_required_sections(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _make_client(monkeypatch)
    r = client.get("/portal/api/applications/17552591/biblio", auth=("viewer", "test-pw"))
    assert r.status_code == 200
    body = r.json()
    # UsptoBiblio top-level keys (spec §10 sections).
    required = {
        "applicationNumber",
        "applicationBibliographicData",
        "inventors",
        "applicants",
        "continuity",
        "foreignPriorities",
        "fileContentHistories",
        "imageFileWrapper",
        "correspondence",
        "attorneys",
        "supplementalContents",
    }
    assert required.issubset(body.keys())

    abd = body["applicationBibliographicData"]
    assert abd["applicationStatusCode"] == 150
    assert abd["inventionTitle"] == "Sample Invention"
    assert abd["examinerName"]["firstName"] == "Jane"
    assert abd["examinerName"]["lastName"] == "Examiner"

    assert len(body["inventors"]) == 1
    assert body["inventors"][0]["name"]["firstName"] == "Alice"

    assert len(body["attorneys"]) == 1
    assert body["attorneys"][0]["registrationNumber"] == "35094"

    # File content histories are sorted newest first per spec.
    fch = body["fileContentHistories"]
    assert len(fch) >= 1
    dates = [e["transactionDate"] for e in fch if e["transactionDate"]]
    assert dates == sorted(dates, reverse=True)

    # Image File Wrapper comes from the XML fixture.
    assert any(d["fileWrapperDocumentCode"] == "CTNF" for d in body["imageFileWrapper"])


def test_biblio_404_for_unknown_application(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORTAL_PASSWORD", "test-pw")
    monkeypatch.setenv("SECRET_KEY", "unit-test-secret-key-32-chars-minimum!!")
    from harness_analytics.db import get_db
    from harness_analytics.server import create_app

    class _NoneSession:
        def execute(self, statement, binds=None):
            return _FakeResultWithMappings([])

    app = create_app()

    def override():
        yield _NoneSession()

    app.dependency_overrides[get_db] = override
    client = TestClient(app)
    r = client.get("/portal/api/applications/99999999/biblio", auth=("viewer", "test-pw"))
    assert r.status_code == 404
