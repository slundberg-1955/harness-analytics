"""Folder ingestion: parse XML, classify events, persist to PostgreSQL."""

from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, Optional

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from harness_analytics.analytics import compute_analytics
from harness_analytics.classifier import classify_event
from harness_analytics.models import (
    Application,
    ApplicationAttorney,
    ApplicationAnalytics,
    FileWrapperDocument,
    Inventor,
    ProsecutionEvent,
)
from harness_analytics.xml_parser import parse_biblio_xml, parse_datetime_utc

logger = logging.getLogger(__name__)


def _iter_xml_files(folder: Path, *, recursive: bool) -> list[Path]:
    if recursive:
        return sorted(folder.rglob("*.xml"))
    return sorted(folder.glob("*.xml"))


def _append_error_log(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, default=str) + "\n")


def _ingest_one_file(
    session: Session,
    xml_path: Path,
    *,
    overwrite: bool,
    store_xml_raw: bool,
) -> tuple[str, str | None]:
    """
    Ingest a single file inside an active SAVEPOINT (caller uses begin_nested).
    Returns ('imported'|'skipped', application_number or None).
    """
    xml_text = xml_path.read_text(encoding="utf-8")
    data = parse_biblio_xml(xml_text)
    app_num = data.get("application_number")
    if not app_num:
        raise ValueError("No application number found")

    existing = session.scalar(select(Application).where(Application.application_number == app_num))
    if existing and not overwrite:
        return "skipped", app_num

    if existing:
        app = existing
    else:
        app = Application(application_number=app_num)
        session.add(app)

    for field in [
        "filing_date",
        "issue_date",
        "patent_number",
        "application_status_code",
        "application_status_text",
        "application_status_date",
        "invention_title",
        "customer_number",
        "hdp_customer_number",
        "attorney_docket_number",
        "confirmation_number",
        "group_art_unit",
        "patent_class",
        "patent_subclass",
        "examiner_first_name",
        "examiner_last_name",
        "examiner_phone",
        "assignee_name",
    ]:
        setattr(app, field, data.get(field))

    app.continuity_child_of_prior_us = bool(data.get("continuity_child_of_prior_us"))
    app.xml_raw = xml_text if store_xml_raw else None
    session.flush()

    if existing and overwrite:
        session.execute(delete(ApplicationAttorney).where(ApplicationAttorney.application_id == app.id))
        session.execute(delete(ProsecutionEvent).where(ProsecutionEvent.application_id == app.id))
        session.execute(delete(FileWrapperDocument).where(FileWrapperDocument.application_id == app.id))
        session.execute(delete(Inventor).where(Inventor.application_id == app.id))
        session.execute(delete(ApplicationAnalytics).where(ApplicationAnalytics.application_id == app.id))
        session.flush()

    for atty in data["attorneys"]:
        session.add(
            ApplicationAttorney(
                application_id=app.id,
                registration_number=atty["registration_number"],
                first_name=atty["first_name"],
                last_name=atty["last_name"],
                phone=atty["phone"],
                agent_status=atty["agent_status"],
                attorney_role=atty["role"],
                is_first_attorney=bool(atty.get("is_first")),
            )
        )

    sorted_events = sorted(
        data["events"],
        key=lambda e: (e["transaction_date"] or date.min, e.get("seq_order") or 0),
    )
    seq = 0
    for evt in sorted_events:
        td = evt["transaction_date"]
        if td is None:
            logger.warning("Skipping prosecution event with no date in %s", xml_path)
            continue
        seq += 1
        etype = classify_event(evt["transaction_description"])
        session.add(
            ProsecutionEvent(
                application_id=app.id,
                transaction_date=td,
                transaction_description=evt["transaction_description"],
                status_number=evt["status_number"],
                status_description=evt["status_description"],
                event_type=etype,
                seq_order=seq,
            )
        )

    for doc in data["documents"]:
        mq = parse_datetime_utc(doc.get("mail_room_date"))
        pq = doc.get("page_quantity")
        pq_int = int(pq) if pq and str(pq).strip().isdigit() else None
        session.add(
            FileWrapperDocument(
                application_id=app.id,
                document_code=doc.get("document_code"),
                document_description=doc.get("document_description"),
                mail_room_date=mq,
                page_quantity=pq_int,
                document_category=doc.get("document_category"),
            )
        )

    for inv in data["inventors"]:
        session.add(
            Inventor(
                application_id=app.id,
                first_name=inv["first_name"],
                last_name=inv["last_name"],
                city=inv["city"],
                country_code=inv["country_code"],
            )
        )

    return "imported", app_num


def ingest_folder(
    folder_path: str,
    db_session: Session,
    *,
    overwrite: bool = False,
    commit_every: int = 50,
    recursive: bool = False,
    limit: Optional[int] = None,
    error_log: Optional[Path] = None,
    store_xml_raw: bool = True,
    skip_analytics: bool = False,
    interview_window_days: int = 90,
    office_map_path: Optional[Path] = None,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> dict[str, int]:
    """
    Read XML files from folder_path, parse, and upsert into the database.

    Uses SAVEPOINT per file so one failure does not roll back the whole batch.
    Commits every ``commit_every`` successful imports (0 = commit only at end).
    """
    folder = Path(folder_path).expanduser().resolve()
    if not folder.is_dir():
        raise NotADirectoryError(str(folder))

    xml_files = _iter_xml_files(folder, recursive=recursive)
    if limit is not None:
        xml_files = xml_files[:limit]

    total = len(xml_files)
    logger.info("Found %s XML files under %s", total, folder)

    imported = 0
    skipped = 0
    errors = 0
    t0 = time.perf_counter()

    for idx, xml_file in enumerate(xml_files, start=1):
        if progress_callback:
            progress_callback(idx, total, str(xml_file))

        try:
            with db_session.begin_nested():
                status, app_num = _ingest_one_file(
                    db_session,
                    xml_file,
                    overwrite=overwrite,
                    store_xml_raw=store_xml_raw,
                )
            if status == "imported":
                imported += 1
            else:
                skipped += 1
        except Exception as exc:  # noqa: BLE001 — surface per-file failures
            errors += 1
            logger.exception("Ingest failed for %s", xml_file)
            if error_log:
                _append_error_log(
                    error_log,
                    {
                        "path": str(xml_file),
                        "error": str(exc),
                        "time": datetime.now(timezone.utc).isoformat(),
                    },
                )

        if commit_every > 0 and imported > 0 and imported % commit_every == 0:
            db_session.commit()
            logger.debug("Committed batch (%s imported so far)", imported)

    db_session.commit()

    if not skip_analytics:
        logger.info("Computing analytics...")
        compute_analytics(
            db_session,
            interview_window_days=interview_window_days,
            office_map_path=office_map_path,
        )
        db_session.commit()

    elapsed = time.perf_counter() - t0
    logger.info(
        "Ingest complete: imported=%s skipped=%s errors=%s elapsed_s=%.2f",
        imported,
        skipped,
        errors,
        elapsed,
    )
    return {"imported": imported, "skipped": skipped, "errors": errors, "total": total}
