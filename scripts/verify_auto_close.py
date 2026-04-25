"""Read-only sanity checker for the docket cross-off / NAR feature.

Prints, for each application number passed on the command line, the
deadlines we have on file and their recent ``deadline_events`` history.
Useful after a recompute to confirm the AUTO_COMPLETE / AUTO_NAR pass
actually fired on known closed sequences (e.g. CTNF → A.NE → NOA).

Usage::

    python scripts/verify_auto_close.py 18158386 17552591

Connects to ``DATABASE_URL``. Read-only — never writes; safe in production.
"""
from __future__ import annotations

import argparse
import sys
from typing import Iterable

from sqlalchemy import select

from harness_analytics.db import get_session_factory
from harness_analytics.models import (
    Application,
    ComputedDeadline,
    DeadlineEvent,
    IfwRule,
)


def _fmt_deadline(cd: ComputedDeadline, rule: IfwRule | None) -> str:
    bits = [
        f"#{cd.id:>6}",
        f"{cd.status:<10}",
        f"trig={cd.trigger_date}",
        f"due={cd.primary_date}",
        rule.code if rule else "?",
    ]
    if cd.closed_disposition:
        bits.append(f"close={cd.closed_disposition}({cd.closed_by_rule_pattern})")
    return "  ".join(bits)


def _fmt_event(ev: DeadlineEvent) -> str:
    payload = ev.payload_json or {}
    code = payload.get("matched_code") or ""
    pat = payload.get("matched_pattern") or ""
    extra = f"  [{code} ~ {pat}]" if code or pat else ""
    return f"    {ev.occurred_at}  {ev.action:<14}{extra}"


def verify(app_numbers: Iterable[str]) -> int:
    SessionLocal = get_session_factory()
    with SessionLocal() as db:
        for raw in app_numbers:
            num = "".join(raw.split())
            app = db.scalar(
                select(Application).where(Application.application_number == num)
            )
            if app is None:
                print(f"\n=== {raw}: NOT FOUND")
                continue
            print(f"\n=== {raw} (app_id={app.id}) {app.invention_title or ''}")
            deadlines = db.scalars(
                select(ComputedDeadline)
                .where(ComputedDeadline.application_id == app.id)
                .order_by(ComputedDeadline.trigger_date.asc())
            ).all()
            if not deadlines:
                print("  (no deadlines)")
                continue
            rule_ids = {cd.rule_id for cd in deadlines}
            rules: dict[int, IfwRule] = {
                r.id: r
                for r in db.scalars(
                    select(IfwRule).where(IfwRule.id.in_(rule_ids))
                ).all()
            }
            for cd in deadlines:
                print("  " + _fmt_deadline(cd, rules.get(cd.rule_id)))
                events = db.scalars(
                    select(DeadlineEvent)
                    .where(DeadlineEvent.deadline_id == cd.id)
                    .order_by(DeadlineEvent.occurred_at.desc())
                    .limit(8)
                ).all()
                for ev in events:
                    print(_fmt_event(ev))
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Print docket cross-off / NAR audit for given applications"
    )
    p.add_argument(
        "app_numbers",
        nargs="+",
        help="Application numbers to inspect (slashes/commas/spaces tolerated)",
    )
    args = p.parse_args(argv)
    return verify(args.app_numbers)


if __name__ == "__main__":
    sys.exit(main())
