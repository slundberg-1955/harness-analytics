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


def main() -> None:
    parser = argparse.ArgumentParser(description="Harness IP prosecution analytics")
    parser.add_argument(
        "command",
        choices=["init", "ingest", "report", "all", "analytics"],
        help="init=DDL; ingest=XML folder; report=Excel; all=init+ingest+report; analytics=recompute only",
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
        help="Days from first interview to first NOA to count as interview_led_to_noa",
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


if __name__ == "__main__":
    main()
