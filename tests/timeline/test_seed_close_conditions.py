"""Tests for the docket cross-off seed JSON shape + helpers (no DB).

Mirrors the pattern in :file:`tests/timeline/test_supersession.py`: shape
checks against the bundled JSON plus a couple of unit tests for the
importer's pure functions. The DB-bound ``seed_close_conditions`` is
exercised end-to-end by the seed/run on container start; here we just
verify the seed file is well-formed and the importer's slug + skip rules
behave.
"""
from __future__ import annotations

from harness_analytics.timeline.rules_repo import (
    _merge_close_conditions,
    load_docket_close_seed,
)
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


def test_noa_close_codes_cover_real_world_issue_fee_and_qpids_reopen() -> None:
    """Production audit (Apr 2026) found 477 overdue items, several of which
    were NOAs that *had* satisfying follow-up actions on file but no closer
    fired. Two real IFW codes were missing from the seed:

    * ``IFEE`` -- Issue Fee Payment (PTO-85B). The seed only had the logical
      name ``ISSUE.FEE`` which never appears in actual USPTO IFW data, so
      every NOA closed by a paid issue fee stayed OPEN until the patent
      ultimately issued (the ``app_issued`` shortcut took over).
    * ``Q.DEC.REOPEN`` -- Quick Path IDS (QPIDS) decision that reopens
      prosecution after allowance. Once prosecution is reopened the NOA
      is dead, but the seed had no closer code for it.

    Pin both into the canonical NOA row so a regression here would surface
    the next time a NOA gets stuck open behind one of these codes.
    """
    conditions = load_docket_close_seed()
    noa = next(c for c in conditions if c["code"] == "NOA" and c["variant_key"] == "")
    assert "IFEE" in noa["complete_codes"], (
        "IFEE (real USPTO issue-fee-payment code) must close NOA"
    )
    assert "Q.DEC.REOPEN" in noa["complete_codes"], (
        "QPIDS reopen decision must close NOA -- prosecution was reopened"
    )
    # ACPA was already there but is part of the same audit; lock it in too.
    assert "ACPA" in noa["complete_codes"], (
        "ACPA (design CPA) after NOA withdraws allowance and must close it"
    )


def test_seed_carries_multiple_variants_per_triggering_code() -> None:
    """CTNF appears twice with distinct ``variant_key``; ditto CTRS, NRES,
    APEA, and a handful of others. The seed JSON keeps these rows
    separate for documentation / spreadsheet-fidelity reasons even though
    :func:`seed_close_conditions` collapses them onto the canonical rule
    (see :func:`_merge_close_conditions`).
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


def test_merge_close_conditions_unions_variants_for_same_code() -> None:
    """``_merge_close_conditions`` should fold every variant of a code
    into a single ``{complete, nar, description}`` bucket, taking the
    union of close arrays. This is the fix for the bug where
    ``seed_close_conditions`` was inserting orphan ``auto_close_only``
    rows keyed by ``variant_key`` that were never linked to any
    materialized deadline -- so e.g. CTFR deadlines never auto-completed
    on AMSB / RCE.
    """
    raw = [
        {
            "code": "CTNF",
            "variant_key": "non-final-office-action-response",
            "description": "Non Final Office Action Response Due",
            "complete_codes": ["A...", "AMSB", "RCEX"],
            "nar_codes": ["NOA", "ABN"],
        },
        {
            "code": "CTNF",
            "variant_key": "non-final-office-action-with-rr-response",
            "description": "Non Final OA with RR Response Due",
            "complete_codes": ["A.AF", "AMSB"],
            "nar_codes": ["EXPR.ABN", "ABN"],
        },
        {
            "code": "NOA",
            "variant_key": "",
            "description": "Notice of Allowance",
            "complete_codes": ["ISSUE.NTF", "RCE"],
            "nar_codes": ["ABN"],
        },
    ]
    grouped, skipped = _merge_close_conditions(raw)
    assert skipped == 0
    # CTNF: union of both variants, dedup, insertion order preserved.
    assert grouped["CTNF"]["complete"] == ["A...", "AMSB", "RCEX", "A.AF"]
    assert grouped["CTNF"]["nar"] == ["NOA", "ABN", "EXPR.ABN"]
    # First non-empty description sticks (later variants don't overwrite).
    assert grouped["CTNF"]["description"] == "Non Final Office Action Response Due"
    # Single-variant code passes through unchanged.
    assert grouped["NOA"]["complete"] == ["ISSUE.NTF", "RCE"]
    assert grouped["NOA"]["nar"] == ["ABN"]


def test_merge_close_conditions_skips_blank_codes() -> None:
    raw = [
        {"code": "", "complete_codes": ["FOO"], "nar_codes": ["BAR"]},
        {"code": "  ", "complete_codes": ["FOO"], "nar_codes": ["BAR"]},
        {"code": "NOA", "complete_codes": ["ISSUE.NTF"], "nar_codes": ["ABN"]},
    ]
    grouped, skipped = _merge_close_conditions(raw)
    assert skipped == 2
    assert set(grouped) == {"NOA"}


def test_merge_close_conditions_covers_ctfr_amsb_path() -> None:
    """Regression: the bundled seed must surface ``AMSB`` as a CTFR
    completer once merged. This is the exact scenario the original bug
    report was about (CTFR deadline staying overdue after an after-final
    AMSB was filed)."""
    grouped, _skipped = _merge_close_conditions(load_docket_close_seed())
    assert "CTFR" in grouped
    assert "AMSB" in grouped["CTFR"]["complete"], grouped["CTFR"]
    assert "RCE" in grouped["CTFR"]["complete"] or "RCEX" in grouped["CTFR"]["complete"]
    # And NAR side stays sane.
    assert "ABN" in grouped["CTFR"]["nar"]


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
