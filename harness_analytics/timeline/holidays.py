"""Federal holiday loader for the calculator engine."""

from __future__ import annotations

import json
from datetime import date
from functools import lru_cache
from pathlib import Path

_PATH = Path(__file__).resolve().parent / "data" / "federal-holidays.json"


@lru_cache(maxsize=1)
def federal_holidays() -> tuple[date, ...]:
    with _PATH.open("r", encoding="utf-8") as fp:
        data = json.load(fp)
    return tuple(date.fromisoformat(s) for s in data.get("holidays", []))
