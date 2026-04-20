"""Prosecution Timeline engine and materializer.

Layer 2 in the design doc — pure-Python rules engine with no DB or HTTP I/O.
The materializer (built on top of the calculator) lives in
:mod:`harness_analytics.timeline.materializer`.
"""

from harness_analytics.timeline.calculator import (  # noqa: F401
    ComputeOptions,
    DeadlineResult,
    DeadlineRow,
    IdsPhase,
    IfwRule,
    MaintenanceDates,
    add_months,
    compute_deadlines,
    roll_forward,
)
