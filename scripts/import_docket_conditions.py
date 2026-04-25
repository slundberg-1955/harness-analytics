"""Convert a TSV export of the docket-cross-off spreadsheet into the
``docket_close_conditions.json`` seed file.

The spreadsheet — pasted by the user during planning of M0009 — enumerates
for each ``(triggering IFW code, due-item description)`` pair the IFW codes
that should auto-COMPLETE or auto-NAR the matching docketed deadline. The
seed loader (``rules_repo.seed_close_conditions``) reads the JSON shape this
script produces.

Usage::

    python scripts/import_docket_conditions.py \
        --tsv path/to/spreadsheet.tsv \
        --out harness_analytics/timeline/data/docket_close_conditions.json

TSV columns (tab-separated; first row is the header)::

    Triggering IFW Code | Due Item | Complete codes | NAR codes | Notes

Codes inside a cell may be separated by commas with optional whitespace.
``A...`` is intentionally preserved (prefix wildcard for the matcher).
Rows whose triggering code is blank are skipped with a warning — those are
informational rows that the v1 auto-close pass intentionally does not own.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
from pathlib import Path
from typing import Iterable, Optional

logger = logging.getLogger(__name__)


_SLUG_RE = re.compile(r"[^a-z0-9]+")
_SUFFIX_NOISE = (
    " due",
    " due?",
)


def slugify(text: str) -> str:
    """Lower-case, hyphen-separated, alnum-only key for ``variant_key``.

    Trailing "Due" / "Due?" is stripped because nearly every spreadsheet row
    ends in it; keeping it would just bloat the slug. Empty input returns
    ``""`` so the legacy single-variant rules still round-trip.
    """
    s = (text or "").strip().lower()
    for noise in _SUFFIX_NOISE:
        if s.endswith(noise):
            s = s[: -len(noise)].rstrip()
    s = _SLUG_RE.sub("-", s).strip("-")
    return s


def split_codes(cell: Optional[str]) -> list[str]:
    """Comma-split a cell into trimmed code patterns. Empty cells → ``[]``."""
    if not cell:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for raw in cell.split(","):
        token = raw.strip()
        if not token:
            continue
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def rows_to_conditions(
    rows: Iterable[dict[str, str]],
) -> tuple[list[dict], list[str]]:
    """Translate raw spreadsheet rows into seed JSON entries.

    Returns ``(conditions, warnings)``. ``conditions`` carries the entries in
    the order they appeared so a diff against the previous JSON is readable;
    ``warnings`` flags rows skipped or coalesced so the operator can audit.
    """
    conditions: list[dict] = []
    warnings: list[str] = []
    seen_keys: set[tuple[str, str]] = set()
    for idx, row in enumerate(rows, start=2):  # 2 because header is row 1
        code = (row.get("Triggering IFW Code") or "").strip()
        due_item = (row.get("Due Item") or "").strip()
        complete = split_codes(row.get("Complete codes"))
        nar = split_codes(row.get("NAR codes"))
        notes = (row.get("Notes") or "").strip()
        if not code:
            warnings.append(
                f"row {idx}: blank triggering code — skipping ({due_item!r})"
            )
            continue
        if not complete and not nar:
            warnings.append(
                f"row {idx}: {code} {due_item!r} has no close codes — skipping"
            )
            continue
        variant_key = slugify(due_item)
        key = (code, variant_key)
        if key in seen_keys:
            warnings.append(
                f"row {idx}: duplicate ({code}, {variant_key!r}) — coalescing"
            )
            for existing in conditions:
                if (existing["code"], existing["variant_key"]) == key:
                    existing["complete_codes"] = sorted(
                        set(existing["complete_codes"]) | set(complete),
                        key=lambda x: (complete + existing["complete_codes"]).index(x)
                        if x in (complete + existing["complete_codes"])
                        else 0,
                    )
                    existing["nar_codes"] = sorted(
                        set(existing["nar_codes"]) | set(nar),
                        key=lambda x: (nar + existing["nar_codes"]).index(x)
                        if x in (nar + existing["nar_codes"])
                        else 0,
                    )
                    if notes and notes not in (existing.get("notes") or ""):
                        existing["notes"] = (
                            (existing.get("notes") + "\n" if existing.get("notes") else "")
                            + notes
                        )
                    break
            continue
        seen_keys.add(key)
        conditions.append(
            {
                "code": code,
                "variant_key": variant_key,
                "description": due_item or code,
                "complete_codes": complete,
                "nar_codes": nar,
                "notes": notes or None,
            }
        )
    return conditions, warnings


def load_tsv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        reader = csv.DictReader(fp, delimiter="\t")
        return [dict(row) for row in reader]


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="TSV → docket_close_conditions.json importer"
    )
    p.add_argument("--tsv", required=True, type=Path, help="Input TSV path")
    p.add_argument(
        "--out",
        default=Path(__file__).resolve().parent.parent
        / "harness_analytics"
        / "timeline"
        / "data"
        / "docket_close_conditions.json",
        type=Path,
        help="Output JSON path (default: bundled seed)",
    )
    p.add_argument(
        "--print-warnings",
        action="store_true",
        help="Echo skip / coalesce warnings to stderr",
    )
    args = p.parse_args(argv)

    rows = load_tsv(args.tsv)
    conditions, warnings = rows_to_conditions(rows)
    if args.print_warnings:
        for w in warnings:
            print(f"warning: {w}", file=sys.stderr)
    payload = {
        "conditions": conditions,
        "_metadata": {
            "source": str(args.tsv),
            "row_count": len(conditions),
            "skipped_or_coalesced": len(warnings),
        },
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, indent=2, ensure_ascii=False)
        fp.write("\n")
    print(
        f"Wrote {len(conditions)} conditions to {args.out} "
        f"({len(warnings)} skipped/coalesced)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
