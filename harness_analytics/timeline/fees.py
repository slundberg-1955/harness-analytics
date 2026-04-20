"""USPTO Extension-of-Time fee lookups (37 CFR 1.17(a)).

Numbers come from ``timeline/data/fees.json`` (loaded once at import). The
JSON file is editable so paralegals can update fees on the next deploy
without touching code; longer-term these will move into a DB table managed
through the rules admin UI.

Defaults reflect the USPTO fee schedule as of FY2024 — verify against the
current schedule when ingesting fresh data.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Literal

EntitySize = Literal["large", "small", "micro"]

_FEES_PATH = Path(__file__).resolve().parent / "data" / "fees.json"


@lru_cache(maxsize=1)
def _fee_table() -> dict[str, list[int]]:
    with _FEES_PATH.open("r", encoding="utf-8") as fp:
        data = json.load(fp)
    return data["eot"]


def eot_fee_usd(eot_month: int, entity_size: EntitySize) -> int:
    """Return the EOT fee for ``eot_month`` (1-based) in whole USD."""
    if eot_month <= 0:
        return 0
    table = _fee_table()
    sched = table.get(entity_size) or table.get("large") or []
    idx = min(eot_month - 1, len(sched) - 1)
    if idx < 0:
        return 0
    return int(sched[idx])
