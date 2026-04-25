"""Print the most common IFW document codes on issued vs. abandoned
applications so we can pick verified close-condition codes for the
``docket_close_conditions.json`` seed.

Read-only. Run against ``DATABASE_URL`` (same connection the portal uses).

Usage::

    python scripts/inspect_ifw_codes.py
    python scripts/inspect_ifw_codes.py --top 40 --after-days 60

The ``--after-days`` knob only looks at IFW rows whose ``mail_room_date``
is within that many days *after* the biblio-level anchor event (NOA mail
for issued apps, first ABN-ish event for abandoned apps). That filters
out the noise from pre-allowance IFW docs so the "which codes signal
this lifecycle transition" signal-to-noise ratio is readable.
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import timedelta
from typing import Iterable

from sqlalchemy import select

from harness_analytics.db import get_session_factory
from harness_analytics.models import Application, FileWrapperDocument


def _top_codes(
    docs: Iterable[FileWrapperDocument], limit: int
) -> list[tuple[str, int]]:
    counter: Counter[str] = Counter()
    for d in docs:
        code = (d.document_code or "").strip()
        if not code:
            continue
        counter[code] += 1
    return counter.most_common(limit)


def _print_block(title: str, rows: list[tuple[str, int]], total: int) -> None:
    print(f"\n=== {title}  (sample apps: {total})")
    if not rows:
        print("  (no IFW docs matched)")
        return
    width = max(len(code) for code, _ in rows)
    for code, n in rows:
        print(f"  {code:<{width}}  {n:>6}")


def inspect(top: int, after_days: int, sample_limit: int) -> int:
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        # --- Issued patents -------------------------------------------------
        # Anchor: the NOA document's mail_room_date. Print codes that appear
        # after it (ISSUE.NTF, ISSUE.FEE, etc.).
        issued_ids = db.scalars(
            select(Application.id)
            .where(Application.issue_date.isnot(None))
            .limit(sample_limit)
        ).all()
        issued_codes: list[FileWrapperDocument] = []
        for aid in issued_ids:
            noa = db.scalar(
                select(FileWrapperDocument)
                .where(
                    FileWrapperDocument.application_id == aid,
                    FileWrapperDocument.document_code == "NOA",
                )
                .order_by(FileWrapperDocument.mail_room_date.asc())
                .limit(1)
            )
            if noa is None or noa.mail_room_date is None:
                continue
            cutoff = noa.mail_room_date + timedelta(days=after_days)
            rows = db.scalars(
                select(FileWrapperDocument)
                .where(
                    FileWrapperDocument.application_id == aid,
                    FileWrapperDocument.mail_room_date > noa.mail_room_date,
                    FileWrapperDocument.mail_room_date <= cutoff,
                )
            ).all()
            issued_codes.extend(rows)
        _print_block(
            f"ISSUED apps — IFW codes within {after_days} days after NOA",
            _top_codes(issued_codes, top),
            total=len(issued_ids),
        )

        # --- Abandoned apps -------------------------------------------------
        # Anchor: we'll just look for "ABN" in document_code since
        # application_status_text varies. For apps that have it, we want
        # the codes that came in during the closing window.
        abn_app_ids = db.scalars(
            select(Application.id)
            .join(
                FileWrapperDocument,
                FileWrapperDocument.application_id == Application.id,
            )
            .where(FileWrapperDocument.document_code == "ABN")
            .limit(sample_limit)
        ).all()
        abn_app_ids = list(dict.fromkeys(abn_app_ids))  # dedupe
        abn_codes: list[FileWrapperDocument] = []
        for aid in abn_app_ids:
            first_abn = db.scalar(
                select(FileWrapperDocument)
                .where(
                    FileWrapperDocument.application_id == aid,
                    FileWrapperDocument.document_code == "ABN",
                )
                .order_by(FileWrapperDocument.mail_room_date.asc())
                .limit(1)
            )
            if first_abn is None or first_abn.mail_room_date is None:
                continue
            window_start = first_abn.mail_room_date - timedelta(days=after_days)
            rows = db.scalars(
                select(FileWrapperDocument)
                .where(
                    FileWrapperDocument.application_id == aid,
                    FileWrapperDocument.mail_room_date >= window_start,
                    FileWrapperDocument.mail_room_date <= first_abn.mail_room_date,
                )
            ).all()
            abn_codes.extend(rows)
        _print_block(
            f"ABANDONED apps — IFW codes within {after_days} days before first ABN",
            _top_codes(abn_codes, top),
            total=len(abn_app_ids),
        )

        # --- 371 / PCT national stage --------------------------------------
        # For FRPR / PCT deadline closers.
        pct_app_ids = db.scalars(
            select(Application.id)
            .where(Application.earliest_priority_date.isnot(None))
            .limit(sample_limit)
        ).all()
        filing_receipt_codes: list[FileWrapperDocument] = []
        for aid in pct_app_ids:
            rows = db.scalars(
                select(FileWrapperDocument)
                .where(FileWrapperDocument.application_id == aid)
                .limit(20)
            ).all()
            filing_receipt_codes.extend(rows)
        _print_block(
            "PCT / priority apps — all IFW codes (first 20 per app)",
            _top_codes(filing_receipt_codes, top),
            total=len(pct_app_ids),
        )

    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--top", type=int, default=30, help="How many codes to print per section")
    p.add_argument(
        "--after-days",
        type=int,
        default=180,
        help="Window (days) around the anchor event",
    )
    p.add_argument(
        "--sample-limit",
        type=int,
        default=500,
        help="Max applications to pull per section (read-only, but the "
        "queries can get heavy on a full corpus)",
    )
    args = p.parse_args(argv)
    return inspect(args.top, args.after_days, args.sample_limit)


if __name__ == "__main__":
    sys.exit(main())
