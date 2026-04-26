"""Read-only dump of file_wrapper_documents for given app numbers.

Prints raw IFW codes in chronological order so we can spot what should
have closed an open deadline but didn't. Connects via DATABASE_URL.
"""
from __future__ import annotations

import sys
from sqlalchemy import select

from harness_analytics.db import get_session_factory
from harness_analytics.models import Application, FileWrapperDocument


def main(app_numbers: list[str]) -> int:
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
            docs = db.scalars(
                select(FileWrapperDocument)
                .where(FileWrapperDocument.application_id == app.id)
                .order_by(FileWrapperDocument.mail_room_date.asc().nullslast())
            ).all()
            if not docs:
                print("  (no IFW documents)")
                continue
            for d in docs:
                date = d.mail_room_date.date().isoformat() if d.mail_room_date else "?"
                code = (d.document_code or "")[:12]
                desc = (d.document_description or "")[:80]
                print(f"  {date}  {code:<12}  {desc}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
