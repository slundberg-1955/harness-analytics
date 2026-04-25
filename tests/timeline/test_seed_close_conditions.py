"""Tests for the docket cross-off seed JSON shape + helpers (no DB).

Mirrors the pattern in :file:`tests/timeline/test_supersession.py`: shape
checks against the bundled JSON plus a couple of unit tests for the
importer's pure functions. The DB-bound ``seed_close_conditions`` is
exercised end-to-end by the seed/run on container start; here we just
verify the seed file is well-formed and the importer's slug + skip rules
behave.
"""
from __future__ import annotations

from harness_analytics.timeline.rules_repo import load_docket_close_seed
from scripts.import_docket_conditions import (
    rows_to_conditions,
    slugify,
    split_codes,
)


def test_docket_close_seed_loads() -> None:
    conditions = load_docket_close_seed()
    assert len(conditions) >= 50, "Expected the bundled spreadsheet seed"
    # Spot-check a few canonical rows.
    by_key = {(c["code"], c["variant_key"]): c for c in conditions}
    assert ("CTNF", "non-final-office-action-response") in by_key
    ctnf_response = by_key[("CTNF", "non-final-office-action-response")]
    assert "A..." in ctnf_response["complete_codes"]
    assert "NOA" in ctnf_response["nar_codes"]


def test_docket_close_seed_covers_noa_and_maintenance() -> None:
    """M0009 follow-up: NOA wasn't in the spreadsheet, so its close
    conditions were empty and the auto-close pass skipped it. The seed
    now carries entries for NOA + FRPR + PCT + all three maintenance
    windows, all with ``variant_key=""`` so they update the existing
    rule rows rather than inserting disconnected auto_close_only rows.
    """
    conditions = load_docket_close_seed()
    by_key = {(c["code"], c["variant_key"]): c for c in conditions}
    for code in ("NOA", "FRPR", "PCT", "MISMTH4", "MISMTH8", "MISMTH12"):
        assert (code, "") in by_key, f"missing ({code}, '') in seed"
    noa = by_key[("NOA", "")]
    # NOA close patterns should include issuance / fee payment / RCE.
    assert "ISSUE.NTF" in noa["complete_codes"]
    assert "RCE" in noa["complete_codes"] or "RCEX" in noa["complete_codes"]
    assert "ABN" in noa["nar_codes"]
    # Maintenance rules NAR on expiration.
    mth4 = by_key[("MISMTH4", "")]
    assert "MF.PAID" in mth4["complete_codes"]
    assert any("EXPIR" in c for c in mth4["nar_codes"])


def test_seed_creates_variant_rows_for_shared_triggering_codes() -> None:
    """CTNF appears twice with distinct ``variant_key``; ditto CTRS, NRES,
    APEA, and a handful of others. Verifies the plan's decision to make
    ``variant_key`` part of the unique key actually shows up in the seed.
    """
    conditions = load_docket_close_seed()
    multi_variant = {"CTNF", "CTRS", "APEA", "NRES", "PPH.DECISION"}
    counts: dict[str, int] = {}
    for c in conditions:
        counts[c["code"]] = counts.get(c["code"], 0) + 1
    for code in multi_variant:
        assert counts.get(code, 0) >= 2, (
            f"{code} should have at least 2 variants in the seed"
        )


def test_seed_rows_have_required_shape() -> None:
    for c in load_docket_close_seed():
        assert c.get("code"), c
        # variant_key is allowed to be empty string for legacy single-variant rules,
        # but the bundled seed should always populate it.
        assert "variant_key" in c, c
        assert isinstance(c.get("complete_codes", []), list)
        assert isinstance(c.get("nar_codes", []), list)
        # Every row should contribute at least one closer pattern.
        assert c["complete_codes"] or c["nar_codes"], (
            f"{c['code']!r} {c['variant_key']!r} has no close patterns"
        )


def test_slugify_strips_due_suffix_and_punctuation() -> None:
    assert slugify("Non Final Office Action Response Due") == (
        "non-final-office-action-response"
    )
    assert slugify("Reply Brief Due") == "reply-brief"
    # Trailing "Due?" (rare but appears in the spreadsheet) also stripped.
    assert slugify("Ex Parte Reexamination Certificate Received?") == (
        "ex-parte-reexamination-certificate-received"
    )
    # Punctuation collapses to single hyphens; no leading/trailing hyphens.
    assert slugify("Review Order re Oral Hearing and Docket as Req'd") == (
        "review-order-re-oral-hearing-and-docket-as-req-d"
    )
    assert slugify("") == ""


def test_split_codes_trims_dedupes_and_preserves_order() -> None:
    assert split_codes("A..., AMSB ,A.AF, AMSB") == ["A...", "AMSB", "A.AF"]
    assert split_codes("") == []
    assert split_codes(None) == []
    assert split_codes("NOA") == ["NOA"]


def test_blank_triggering_code_skipped_with_warning() -> None:
    rows = [
        {
            "Triggering IFW Code": "",
            "Due Item": "Stray informational reminder",
            "Complete codes": "FOO",
            "NAR codes": "BAR",
            "Notes": "",
        },
        {
            "Triggering IFW Code": "CTNF",
            "Due Item": "Non Final Office Action Response Due",
            "Complete codes": "A..., RCEX",
            "NAR codes": "NOA",
            "Notes": "",
        },
    ]
    conditions, warnings = rows_to_conditions(rows)
    assert len(conditions) == 1
    assert conditions[0]["code"] == "CTNF"
    assert any("blank triggering code" in w for w in warnings)


def test_rows_to_conditions_coalesces_duplicate_keys() -> None:
    rows = [
        {
            "Triggering IFW Code": "CTNF",
            "Due Item": "Non Final Office Action Response Due",
            "Complete codes": "A...",
            "NAR codes": "NOA",
            "Notes": "first",
        },
        {
            "Triggering IFW Code": "CTNF",
            "Due Item": "Non Final Office Action Response Due",
            "Complete codes": "RCEX",
            "NAR codes": "ABN",
            "Notes": "second",
        },
    ]
    conditions, warnings = rows_to_conditions(rows)
    assert len(conditions) == 1
    merged = conditions[0]
    assert "A..." in merged["complete_codes"] and "RCEX" in merged["complete_codes"]
    assert "NOA" in merged["nar_codes"] and "ABN" in merged["nar_codes"]
    assert any("duplicate" in w for w in warnings)
