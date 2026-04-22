"""CLI: init | ingest | report | all | analytics."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from harness_analytics.analytics import compute_analytics
from harness_analytics.excel_builder import build_excel_report
from harness_analytics.ingest import ingest_folder
from harness_analytics.models import Base


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s %(name)s: %(message)s")


def _resolve_db_url(cli_value: str | None) -> str:
    """Prefer CLI --db-url, then Railway/Heroku DATABASE_URL, then local default."""
    raw = cli_value or os.environ.get("DATABASE_URL") or "postgresql://localhost/harness_analytics"
    if raw.startswith("postgres://"):
        return raw.replace("postgres://", "postgresql://", 1)
    return raw


def _users_subcommand(argv: list[str]) -> int:
    """`python -m harness_analytics users add ...` — bootstrap users without the UI."""
    parser = argparse.ArgumentParser(prog="harness_analytics users")
    sub = parser.add_subparsers(dest="action", required=True)

    add_p = sub.add_parser("add", help="Create a user")
    add_p.add_argument("--email", required=True)
    add_p.add_argument("--password", required=True)
    add_p.add_argument("--name", default=None)
    add_p.add_argument(
        "--role",
        default="OWNER",
        choices=["OWNER", "ADMIN", "ATTORNEY", "PARALEGAL", "VIEWER"],
    )
    add_p.add_argument("--tenant-id", default="global")
    add_p.add_argument("--db-url", default=None)

    list_p = sub.add_parser("list", help="List users")
    list_p.add_argument("--db-url", default=None)

    args = parser.parse_args(argv)

    db_url = _resolve_db_url(args.db_url)
    os.environ["DATABASE_URL"] = db_url
    # Import lazily so passlib loads only when needed.
    from harness_analytics.auth import create_user
    from harness_analytics.db import get_session_factory
    from harness_analytics.models import User
    from harness_analytics.schema_migrations import ensure_schema_migrations

    ensure_schema_migrations()
    SessionLocal = get_session_factory()

    if args.action == "add":
        with SessionLocal() as db:
            user = create_user(
                db,
                email=args.email,
                password=args.password,
                name=args.name,
                role=args.role,
                tenant_id=args.tenant_id,
            )
        print(f"Created user id={user.id} email={user.email} role={user.role}")
        return 0

    if args.action == "list":
        with SessionLocal() as db:
            users = db.query(User).order_by(User.id).all()
        for u in users:
            print(
                f"#{u.id:<4} {u.role:<9} {u.tenant_id:<10} {u.email}"
                f"{'  (inactive)' if not u.active else ''}"
            )
        print(f"Total: {len(users)}")
        return 0

    return 1


def main() -> None:
    import sys

    # Top-level `users` command bypasses the legacy positional parser so we can
    # have proper subcommands without breaking existing CLIs.
    if len(sys.argv) >= 2 and sys.argv[1] == "users":
        rc = _users_subcommand(sys.argv[2:])
        raise SystemExit(rc)

    parser = argparse.ArgumentParser(description="Harness IP prosecution analytics")
    parser.add_argument(
        "command",
        choices=["init", "ingest", "report", "all", "analytics", "timeline-recompute", "unmapped-codes"],
        help=(
            "init=DDL; ingest=XML folder; report=Excel; all=init+ingest+report; "
            "analytics=recompute only; timeline-recompute=rebuild deadlines for the tenant; "
            "unmapped-codes=print top unmapped IFW codes"
        ),
    )
    parser.add_argument(
        "--db-url",
        default=None,
        help="PostgreSQL URL (default: DATABASE_URL env or postgresql://localhost/harness_analytics)",
    )
    parser.add_argument("--folder", help="Folder containing XML files")
    parser.add_argument("--output", default="harness_analytics_report.xlsx")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument(
        "--interview-window",
        type=int,
        default=90,
        help="Max days from last IFW interview (EXIN / INTV.SUM.EX / INTV.SUM.APP) before first IFW NOA to set interview_led_to_noa",
    )
    parser.add_argument(
        "--office-map",
        type=Path,
        default=None,
        help="Path to office_map.json (default: config/office_map.json next to package)",
    )
    parser.add_argument(
        "--commit-every",
        type=int,
        default=50,
        help="Commit after this many successful imports (0 = only at end)",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Discover *.xml recursively under --folder",
    )
    parser.add_argument("--limit", type=int, default=None, help="Max XML files to process")
    parser.add_argument(
        "--error-log",
        type=Path,
        default=Path("ingest_errors.jsonl"),
        help="Append JSONL error records here",
    )
    parser.add_argument(
        "--no-xml-raw",
        action="store_true",
        help="Do not store full XML in applications.xml_raw",
    )
    parser.add_argument(
        "--skip-analytics",
        action="store_true",
        help="After ingest, skip analytics pass (use analytics command later)",
    )
    parser.add_argument(
        "--tenant-id",
        default="global",
        help="Tenant scope for timeline-recompute / unmapped-codes (default: global)",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=20,
        help="How many unmapped codes to print (default: 20)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()
    _configure_logging(args.verbose)

    db_url = _resolve_db_url(args.db_url)
    engine = create_engine(db_url)
    Session = sessionmaker(bind=engine)

    if args.command in ("init", "all"):
        Base.metadata.create_all(engine)
        print("Database schema created.")

    if args.command in ("ingest", "all"):
        if not args.folder:
            parser.error("--folder required for ingest/all")
        with Session() as db:
            stats = ingest_folder(
                args.folder,
                db,
                overwrite=args.overwrite,
                commit_every=args.commit_every,
                recursive=args.recursive,
                limit=args.limit,
                error_log=args.error_log,
                store_xml_raw=not args.no_xml_raw,
                skip_analytics=args.skip_analytics,
                interview_window_days=args.interview_window,
                office_map_path=args.office_map,
            )
        print(
            f"Imported: {stats['imported']}  Skipped: {stats['skipped']}  "
            f"Errors: {stats['errors']}  Total files: {stats['total']}"
        )

    if args.command == "analytics":
        with Session() as db:
            compute_analytics(
                db,
                interview_window_days=args.interview_window,
                office_map_path=args.office_map,
            )
            db.commit()
        print("Analytics recomputed.")

    if args.command in ("report", "all"):
        with Session() as db:
            build_excel_report(db, args.output)
        print(f"Report saved to {args.output}")

    if args.command == "timeline-recompute":
        from harness_analytics.timeline.materializer import recompute_for_tenant
        with Session() as db:
            n = recompute_for_tenant(db, args.tenant_id)
            db.commit()
        print(f"Recomputed deadlines for tenant {args.tenant_id!r}: {n} applications.")

    if args.command == "unmapped-codes":
        from harness_analytics.models import UnmappedIfwCode
        from sqlalchemy import select as _select
        with Session() as db:
            rows = db.scalars(
                _select(UnmappedIfwCode)
                .where(UnmappedIfwCode.tenant_id.in_([args.tenant_id, "global"]))
                .order_by(UnmappedIfwCode.count.desc())
                .limit(args.top)
            ).all()
        if not rows:
            print("No unmapped codes recorded.")
        else:
            print(f"Top {len(rows)} unmapped IFW codes (tenant={args.tenant_id}):")
            print(f"  {'CODE':<10} {'COUNT':>10}  LAST SEEN")
            for r in rows:
                last = r.last_seen.isoformat() if r.last_seen else "-"
                print(f"  {r.code:<10} {r.count:>10,}  {last}")


if __name__ == "__main__":
    main()
