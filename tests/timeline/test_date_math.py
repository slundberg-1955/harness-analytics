"""MPEP 710.01(a) month arithmetic + 37 CFR 1.7 weekend/holiday rolls."""
from __future__ import annotations

from datetime import date

import pytest

from harness_analytics.timeline.calculator import add_months, roll_forward


@pytest.mark.parametrize(
    "start, months, expected",
    [
        (date(2024, 1, 31), 1, date(2024, 2, 29)),  # leap year clamp
        (date(2023, 1, 31), 1, date(2023, 2, 28)),  # non-leap clamp
        (date(2024, 3, 31), 6, date(2024, 9, 30)),
        (date(2024, 1, 15), 12, date(2025, 1, 15)),
        (date(2024, 1, 15), 0, date(2024, 1, 15)),
        (date(2024, 5, 31), 1, date(2024, 6, 30)),
        (date(2024, 12, 31), 2, date(2025, 2, 28)),
        (date(2025, 3, 19), 3, date(2025, 6, 19)),  # design doc CTFR example
        (date(2025, 3, 19), 6, date(2025, 9, 19)),  # CTFR statutory bar
    ],
)
def test_add_months(start: date, months: int, expected: date) -> None:
    assert add_months(start, months) == expected


def test_add_months_property_no_invalid_dates() -> None:
    """Brute force: every (year, month, day, +months) combo yields a real date."""
    for y in (2023, 2024):
        for m in range(1, 13):
            import calendar

            last = calendar.monthrange(y, m)[1]
            for d in (1, 15, last):
                for plus in (0, 1, 3, 6, 12, 18, 36):
                    out = add_months(date(y, m, d), plus)
                    assert isinstance(out, date)
                    assert 1 <= out.month <= 12
                    assert 1 <= out.day <= 31


def test_roll_forward_weekend() -> None:
    # 2025-06-21 is a Saturday → roll to Monday 2025-06-23
    assert roll_forward(date(2025, 6, 21)) == date(2025, 6, 23)
    # Sunday 2025-06-22 → Monday 2025-06-23
    assert roll_forward(date(2025, 6, 22)) == date(2025, 6, 23)
    # Already Monday → stays.
    assert roll_forward(date(2025, 6, 23)) == date(2025, 6, 23)


def test_roll_forward_holiday_chain() -> None:
    # Fri 2025-07-04 is a holiday; Sat/Sun follow → Mon 2025-07-07.
    holidays = (date(2025, 7, 4),)
    assert roll_forward(date(2025, 7, 4), holidays) == date(2025, 7, 7)
    # Holiday on Monday → roll to Tuesday.
    holidays = (date(2025, 6, 23),)
    assert roll_forward(date(2025, 6, 23), holidays) == date(2025, 6, 24)
