"""Tests for the M13 supersession seed JSON shape."""
from __future__ import annotations

from harness_analytics.timeline.rules_repo import load_supersession_seed


def test_supersession_seed_loads() -> None:
    pairs = load_supersession_seed()
    assert len(pairs) >= 3
    for p in pairs:
        assert p.get("prev_kind"), p
        assert p.get("new_kind"), p


def test_supersession_seed_pairs_unique() -> None:
    pairs = load_supersession_seed()
    keys = [(p["prev_kind"], p["new_kind"]) for p in pairs]
    assert len(set(keys)) == len(keys), "Duplicate supersession pairs in seed JSON"


def test_supersession_seed_includes_oa_to_noa() -> None:
    """The materializer relies on this pair being present so that an
    incoming Notice of Allowance properly closes any open OA window."""
    pairs = load_supersession_seed()
    keys = {(p["prev_kind"], p["new_kind"]) for p in pairs}
    assert ("standard_oa", "hard_noa") in keys
