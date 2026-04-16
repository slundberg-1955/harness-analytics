"""Offline tests for office config and attribution helpers."""

import json
from pathlib import Path

import pytest

from harness_analytics.analytics import _count_ifw_doc_code, _resolve_office_name, load_office_config
from harness_analytics.classifier import IFW_A_NE_DOC_CODE


def test_load_office_config_defaults(tmp_path: Path) -> None:
    cfg = load_office_config(tmp_path / "nonexistent.json")
    assert cfg["area_code_to_office"]["703"] == "DC"


def test_load_office_config_merges_file(tmp_path: Path) -> None:
    p = tmp_path / "office_map.json"
    p.write_text(
        json.dumps(
            {
                "uspto_customer_number_to_office": {"99999": "Dallas"},
                "area_code_to_office": {"214": "Dallas"},
            }
        ),
        encoding="utf-8",
    )
    cfg = load_office_config(p)
    assert cfg["uspto_customer_number_to_office"]["99999"] == "Dallas"
    assert cfg["area_code_to_office"]["703"] == "DC"
    assert cfg["area_code_to_office"]["214"] == "Dallas"


@pytest.mark.parametrize(
    "customer,phone,expected",
    [
        ("99999", None, "Dallas"),
        (None, "703-555-0100", "DC"),
        (None, "(571) 555-0100", "DC"),
        ("unknown", "415-555-0100", "UNKNOWN"),
    ],
)
def test_resolve_office_name(customer: str | None, phone: str | None, expected: str, tmp_path: Path) -> None:
    p = tmp_path / "office_map.json"
    p.write_text(
        json.dumps({"uspto_customer_number_to_office": {"99999": "Dallas"}, "area_code_to_office": {}}),
        encoding="utf-8",
    )
    cfg = load_office_config(p)
    assert _resolve_office_name(customer, phone, cfg) == expected


def test_count_ifw_a_ne_strict_and_unicode_dot() -> None:
    """A.NE count tolerates middle-dot / spacing variants seen in some exports."""

    class _Doc:
        __slots__ = ("document_code",)

        def __init__(self, document_code: str | None) -> None:
            self.document_code = document_code

    docs = [
        _Doc("A.NE"),
        _Doc("a.ne"),
        _Doc("A\u00b7NE"),  # middle dot
        _Doc("A. NE"),
        _Doc("CTNF"),
        _Doc(None),
    ]
    assert _count_ifw_doc_code(docs, IFW_A_NE_DOC_CODE) == 4
