"""Pure-Python KPI and chart aggregates for the Portfolio Explorer.

All functions operate on the already-filtered row list (list of dicts with the
`patent_applications` view column names) so they can be unit-tested without a
database. Keep this module free of FastAPI / SQLAlchemy imports.
"""

from __future__ import annotations

import math
from datetime import date, datetime
from statistics import mean, median
from typing import Any, Iterable, Optional


# Status code -> pill label/tone (spec §8). `_` is the fallback entry the UI
# falls back to when a status code is not in the map.
STATUS_PILL: dict[int | str, dict[str, str]] = {
    150: {"label": "Patented", "tone": "emerald"},
    93:  {"label": "NOA Mailed", "tone": "blue"},
    41:  {"label": "Non-Final", "tone": "amber"},
    42:  {"label": "Final", "tone": "rose"},
    161: {"label": "Abandoned", "tone": "slate"},
    30:  {"label": "Published", "tone": "violet"},
    "_": {"label": "Other", "tone": "slate"},
}


def status_label(code: int | None, text: str | None) -> str:
    if code is not None and code in STATUS_PILL:
        return STATUS_PILL[code]["label"]
    return (text or "").strip() or (f"Status {code}" if code is not None else "Unknown")


def status_tone(code: int | None) -> str:
    if code is not None and code in STATUS_PILL:
        return STATUS_PILL[code]["tone"]
    return STATUS_PILL["_"]["tone"]


def _get_int(row: dict[str, Any], key: str, default: int = 0) -> int:
    v = row.get(key)
    if v is None:
        return default
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _days_to_noa_values(rows: Iterable[dict[str, Any]]) -> list[int]:
    return [
        int(r["days_filing_to_noa"])
        for r in rows
        if r.get("days_filing_to_noa") is not None
    ]


# Status codes treated as "Allowed" for the Carley-Hegde-Marco rate.
# 150 = Patented Case, 93 = NOA Mailed, 159 = Issue Fee Payment Verified.
# (The Traditional rate intentionally still uses Patented (150) only.)
_CHM_ALLOWED_STATUS_CODES: frozenset[int] = frozenset({150, 93, 159})

# Allowance Analytics v2 (spec §4-5).
#
# Cohort-axis options the user picks in the recency filter. Each maps to a
# date column on the patent_applications view. Rows whose cohort date is null
# are excluded from the analytic entirely (spec §4.3 — null cohort dates
# would otherwise be silently bucketed and counsel will misread).
COHORT_AXIS_TO_FIELD: dict[str, str] = {
    "filing": "filing_date",
    "disposal": "disposal_date",
    "noa": "noa_mailed_date",
}

_RECENCY_PRESETS_YEARS: dict[str, int] = {"3y": 3, "5y": 5, "10y": 10}


def _coerce_date(v: Any) -> Optional[date]:
    """Best-effort cast to ``date`` for fields that may arrive as ISO strings."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.split("T")[0]).date()
    except ValueError:
        return None


def _years_ago(today: date, n: int) -> date:
    """``today`` shifted ``n`` years earlier; safe for Feb 29 by clamping to 28."""
    try:
        return today.replace(year=today.year - n)
    except ValueError:
        return today.replace(year=today.year - n, day=28)


def resolve_recency_window(
    preset: Optional[str],
    custom_start: Optional[date],
    custom_end: Optional[date],
    *,
    today: Optional[date] = None,
) -> tuple[Optional[date], Optional[date]]:
    """Spec §4.2 — resolve a window selection into a ``(start, end)`` date pair.

    ``None`` for either bound means "unbounded on that side". When ``preset``
    is ``None`` or ``"all"``, returns ``(None, None)`` so callers can detect
    "no recency filter active" and skip the slice.
    """
    if today is None:
        today = date.today()
    if not preset or preset == "all":
        return (None, None)
    if preset == "custom":
        return (custom_start, custom_end or today)
    years = _RECENCY_PRESETS_YEARS.get(preset)
    if years is None:
        return (None, None)
    return (_years_ago(today, years), today)


def apply_recency_window(
    rows: Iterable[dict[str, Any]],
    cohort_axis: str = "filing",
    window: Optional[tuple[Optional[date], Optional[date]]] = None,
) -> list[dict[str, Any]]:
    """Slice ``rows`` to those whose cohort-axis date falls in ``window``.

    Drops rows whose cohort-axis date is null (spec §4.3 — null dates are
    "invisible" to the analytic, not silently bucketed). When ``window`` is
    ``None`` or ``(None, None)``, returns the row list unchanged so the
    caller can detect "no filter".
    """
    if window is None or (window[0] is None and window[1] is None):
        return list(rows)
    field = COHORT_AXIS_TO_FIELD.get(cohort_axis, "filing_date")
    start, end = window
    out: list[dict[str, Any]] = []
    for r in rows:
        d = _coerce_date(r.get(field))
        if d is None:
            continue
        if start is not None and d < start:
            continue
        if end is not None and d > end:
            continue
        out.append(r)
    return out


def _percentile(sorted_vals: list[float], p: float) -> float:
    """Linear interpolation between adjacent sorted values; safe for any n>=1."""
    n = len(sorted_vals)
    if n == 1:
        return sorted_vals[0]
    pos = p * (n - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return sorted_vals[lo]
    frac = pos - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


# ---------------------------------------------------------------------------
# Allowance Analytics v2 KPIs (spec §5).
#
# Empty-window rule (spec §9): every NEW KPI returns ``None`` (not 0) when
# its denominator is empty so the frontend can render ``—``. Existing legacy
# fields (``allowanceRatePct``, ``chmAllowanceRatePct``) keep their original
# zero-when-empty behavior to preserve byte-identical responses on rows that
# pre-date this feature.
# ---------------------------------------------------------------------------


def compute_first_action_allowance(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """First-Action Allowance Rate.

    Of all allowed applications, the share where the examiner's FIRST
    action was the Notice of Allowance — no non-final OA, no final
    rejection, no RCE in the file history.

    * Denominator = ``status ∈ CHM_ALLOWED`` (Patented + NOA Mailed +
      Allowed for Issue) — ALL allowances, in-flight or already issued.
    * Numerator   = same status set AND ``nonfinal_oa_count == 0``
      AND ``final_oa_count == 0`` AND ``rce_count == 0``.

    User-validated against industry expectation: this rate is normally
    25-35% (USPTO baseline ~10-25% by art unit, higher for portfolios
    weighted toward mechanical / chemical art units). Earlier iterations
    of this function (a) used ``status ∈ closed`` as the denominator and
    ``status ∈ CHM_ALLOWED`` as the numerator, producing impossible
    cohort rates >100%, and (b) ignored ``nonfinal_oa_count`` so apps
    allowed after one or more non-final rejections were counted as
    first-action. Both behaviors are corrected here.

    Data-quality guard: when an allowed-class app has no row in
    ``application_analytics`` (``has_analytics_row IS FALSE``), we cannot
    verify the OA / RCE / Final-Rejection counts (the underlying COALESCE
    treats them as zero) — exclude such apps from the numerator AND from
    the denominator (they're not allowed-with-known-data). Production
    data currently has zero such rows; guard remains as future-proofing.
    """
    denom = 0
    excluded = 0
    num = 0
    for r in rows:
        code = r.get("application_status_code")
        if code not in _CHM_ALLOWED_STATUS_CODES:
            continue
        if r.get("has_analytics_row") is False:
            excluded += 1
            continue
        denom += 1
        if (
            _get_int(r, "nonfinal_oa_count") == 0
            and _get_int(r, "final_oa_count") == 0
            and _get_int(r, "rce_count") == 0
        ):
            num += 1
    if not denom:
        return {"pct": None, "count": 0, "denom": 0, "excluded": excluded}
    return {
        "pct": round(100.0 * num / denom, 1),
        "count": num,
        "denom": denom,
        "excluded": excluded,
    }


def compute_single_ctnf_allowance(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Single-CTNF Allowance Rate (companion to FAA).

    Of all allowed applications, the share that were allowed after EXACTLY
    one non-final office action — the classic "respond once, get allowed"
    prosecution path. Numerator requires:

    * ``nonfinal_oa_count == 1`` (one CTNF, no more)
    * ``final_oa_count == 0`` (no Final Rejection)
    * ``rce_count == 0`` (no RCE)

    Mutually exclusive with first-action allowance (which requires zero
    non-finals); both share the same denominator. Industry baseline is
    typically 50-65% for mixed portfolios.
    """
    denom = 0
    excluded = 0
    num = 0
    for r in rows:
        code = r.get("application_status_code")
        if code not in _CHM_ALLOWED_STATUS_CODES:
            continue
        if r.get("has_analytics_row") is False:
            excluded += 1
            continue
        denom += 1
        if (
            _get_int(r, "nonfinal_oa_count") == 1
            and _get_int(r, "final_oa_count") == 0
            and _get_int(r, "rce_count") == 0
        ):
            num += 1
    if not denom:
        return {"pct": None, "count": 0, "denom": 0, "excluded": excluded}
    return {
        "pct": round(100.0 * num / denom, 1),
        "count": num,
        "denom": denom,
        "excluded": excluded,
    }


def compute_allowances_by_rejection_count(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Distribution of allowances by total rejection count (CTNF + CTFR).

    Of all allowed apps (CHM_ALLOWED) with a verifiable analytics row,
    bucket by ``nonfinal_oa_count + final_oa_count``:

    * "zero"     — 0 rejections (mirrors FAA "no rejections")
    * "one"      — 1 rejection (mirrors Single-CTNF, plus solo-CTFR edge case)
    * "two"      — 2 rejections
    * "three"    — 3 rejections
    * "fourPlus" — 4 or more rejections

    Counts ignore RCE: an app's rejection count is just the number of
    examiner OAs that rejected it, regardless of whether the applicant
    filed an RCE between them. The five buckets partition the allowance
    population so shares sum to 100% (modulo ``excluded`` for missing
    analytics rows).
    """
    bucket_keys = ("zero", "one", "two", "three", "fourPlus")
    bucket_groups: dict[str, list[dict[str, Any]]] = {k: [] for k in bucket_keys}
    excluded = 0
    for r in rows:
        if r.get("application_status_code") not in _CHM_ALLOWED_STATUS_CODES:
            continue
        if r.get("has_analytics_row") is False:
            excluded += 1
            continue
        rejs = _get_int(r, "nonfinal_oa_count") + _get_int(r, "final_oa_count")
        if rejs == 0:
            bucket_groups["zero"].append(r)
        elif rejs == 1:
            bucket_groups["one"].append(r)
        elif rejs == 2:
            bucket_groups["two"].append(r)
        elif rejs == 3:
            bucket_groups["three"].append(r)
        else:
            bucket_groups["fourPlus"].append(r)
    total = sum(len(b) for b in bucket_groups.values())
    labels = (
        ("zero",     "0 rejections"),
        ("one",      "1 rejection"),
        ("two",      "2 rejections"),
        ("three",    "3 rejections"),
        ("fourPlus", "4+ rejections"),
    )
    buckets: list[dict[str, Any]] = []
    for key, label in labels:
        bucket = bucket_groups[key]
        count = len(bucket)
        share = round(100.0 * count / total, 1) if total else 0.0
        months = [
            float(r["months_to_allowance"])
            for r in bucket
            if r.get("months_to_allowance") is not None
        ]
        med = round(median(months), 1) if months else None
        buckets.append(
            {
                "key": key,
                "label": label,
                "count": count,
                "sharePct": share,
                "medianMonths": med,
            }
        )
    return {
        "buckets": buckets,
        "totalAllowed": total,
        "excluded": excluded,
    }


def compute_time_to_allowance(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Median / P25 / P75 months from filing to NOA mailed (spec §5.2)."""
    months: list[float] = []
    for r in rows:
        v = r.get("months_to_allowance")
        if v is None:
            continue
        try:
            months.append(float(v))
        except (TypeError, ValueError):
            continue
    if not months:
        return {"medianMonths": None, "p25Months": None, "p75Months": None, "n": 0}
    months.sort()
    return {
        "medianMonths": round(_percentile(months, 0.50), 1),
        "p25Months": round(_percentile(months, 0.25), 1),
        "p75Months": round(_percentile(months, 0.75), 1),
        "n": len(months),
    }


def compute_rce_intensity(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """RCE Intensity among allowed apps (spec §5.3)."""
    allowed = [
        r for r in rows
        if r.get("application_status_code") in _CHM_ALLOWED_STATUS_CODES
    ]
    if not allowed:
        return {"avgRceAmongAllowed": None, "pctAllowancesWithRce": None, "n": 0}
    rce_counts = [_get_int(r, "rce_count") for r in allowed]
    avg = round(sum(rce_counts) / len(allowed), 2)
    with_rce = sum(1 for n in rce_counts if n >= 1)
    pct = round(100.0 * with_rce / len(allowed), 1)
    return {
        "avgRceAmongAllowed": avg,
        "pctAllowancesWithRce": pct,
        "n": len(allowed),
    }


def compute_strategic_abandonment(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Of abandoned, the share with a CON/CIP/DIV child (spec §5.4)."""
    abandoned = [r for r in rows if r.get("application_status_code") == 161]
    if not abandoned:
        return {"pct": None, "withChild": 0, "totalAbandoned": 0}
    with_child = sum(1 for r in abandoned if r.get("has_child_continuation"))
    return {
        "pct": round(100.0 * with_child / len(abandoned), 1),
        "withChild": with_child,
        "totalAbandoned": len(abandoned),
    }


def compute_family_yield(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """For each patented app, count OTHER patented apps in the same family
    chain (spec §5.5) and report the average. Anchors on
    ``family_root_app_no`` populated by PR 1's XML backfill; falls back to
    the application's own number when the root is missing (means the app is
    its own family root, contributes 0 to the average).
    """
    patented = [r for r in rows if r.get("application_status_code") == 150]
    if not patented:
        return {"avg": None, "n": 0}
    by_family: dict[str, list[Any]] = {}
    for r in patented:
        root = r.get("family_root_app_no") or r.get("application_number")
        if not root:
            continue
        by_family.setdefault(str(root), []).append(r.get("application_number"))
    counts: list[int] = []
    for r in patented:
        root = r.get("family_root_app_no") or r.get("application_number")
        if root and str(root) in by_family:
            counts.append(max(0, len(by_family[str(root)]) - 1))
        else:
            counts.append(0)
    return {
        "avg": round(sum(counts) / len(counts), 2),
        "n": len(patented),
    }


def compute_pendency(rows: list[dict[str, Any]], *, today: Optional[date] = None) -> dict[str, Any]:
    """Median months in prosecution among PENDING apps (spec §5.6).

    Uses the open cohort, not the closed cohort — this is the live-portfolio
    sanity check that pairs with Time-to-Allowance.
    """
    if today is None:
        today = date.today()
    pending_days: list[int] = []
    for r in rows:
        code = r.get("application_status_code")
        if code in (150, 161):
            continue
        fd = _coerce_date(r.get("filing_date"))
        if fd is None:
            continue
        pending_days.append((today - fd).days)
    if not pending_days:
        return {"medianMonths": None, "n": 0}
    return {
        "medianMonths": round(median(pending_days) / 30.44, 1),
        "n": len(pending_days),
    }


def compute_foreign_priority_share(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Share of apps claiming non-US priority benefit (spec §5.7).

    Sourced from ``has_foreign_priority`` populated by PR 1's XML backfill.
    Rows where the column is still NULL (mid-backfill) are counted as False
    so the metric stays well-defined; flag this in the methodology footer.
    """
    if not rows:
        return {"pct": None, "n": 0, "total": 0}
    n = sum(1 for r in rows if r.get("has_foreign_priority"))
    return {
        "pct": round(100.0 * n / len(rows), 1),
        "n": n,
        "total": len(rows),
    }


def compute_scope(rows: list[dict[str, Any]]) -> dict[str, int]:
    """Three counts the scope-line under the filter bar reads from."""
    total = len(rows)
    closed = sum(
        1 for r in rows if r.get("application_status_code") in (150, 161)
    )
    return {
        "totalInWindow": total,
        "closedInWindow": closed,
        "pendingInWindow": total - closed,
    }


def _trad_chm_faa_for_group(group: list[dict[str, Any]]) -> dict[str, Any]:
    """Helper used by the cohort trend and the byArtUnit breakdown.

    Returns rates plus the closed-app count and the count of allowed-class
    apps excluded from the FAA numerator due to a missing
    ``application_analytics`` row (mirrors the data-quality guard in
    ``compute_first_action_allowance``). Consumers (cohort chart,
    art-unit table) surface ``closed`` so users can see the denominator
    behind a 100% bar, and ``faaExcluded`` to flag groups whose FAA may
    be artificially deflated by missing data.

    CHM stays at the legacy semantics (rce/final-rejection treated as 0
    when missing) to keep the byte-stable regression tests green; the
    pre-fix CHM-vs-FAA gap is small in practice and we prefer to fix one
    metric at a time.
    """
    patented = sum(1 for r in group if r.get("application_status_code") == 150)
    abandoned = sum(1 for r in group if r.get("application_status_code") == 161)
    closed = patented + abandoned
    trad = round(100.0 * patented / closed, 1) if closed else None

    chm_a = sum(
        1 for r in group
        if r.get("application_status_code") in _CHM_ALLOWED_STATUS_CODES
        and _get_int(r, "rce_count") == 0
    )
    chm_ca = sum(
        1 for r in group
        if r.get("application_status_code") in _CHM_ALLOWED_STATUS_CODES
        and _get_int(r, "rce_count") >= 1
    )
    chm_ab = sum(
        1 for r in group
        if r.get("application_status_code") == 161
        and not r.get("has_child_continuation")
    )
    chm_num = chm_a + chm_ca
    chm_den = chm_num + chm_ab
    chm = round(100.0 * chm_num / chm_den, 1) if chm_den else None

    # FAA + Single-CTNF: both share the same allowed-class denominator.
    # FAA           = "examiner's first action was the NOA" (0/0/0).
    # Single-CTNF   = "allowed after exactly one non-final OA" (1/0/0).
    # Numerators are by definition strict subsets of the denominator so
    # both rates are bounded [0, 100]. Mutually exclusive cohorts.
    faa_denom = 0
    faa_num = 0
    single_ctnf_num = 0
    faa_excluded = 0
    for r in group:
        if r.get("application_status_code") not in _CHM_ALLOWED_STATUS_CODES:
            continue
        if r.get("has_analytics_row") is False:
            faa_excluded += 1
            continue
        faa_denom += 1
        nonfinal = _get_int(r, "nonfinal_oa_count")
        final = _get_int(r, "final_oa_count")
        rce = _get_int(r, "rce_count")
        if final == 0 and rce == 0:
            if nonfinal == 0:
                faa_num += 1
            elif nonfinal == 1:
                single_ctnf_num += 1
    faa = round(100.0 * faa_num / faa_denom, 1) if faa_denom else None
    single_ctnf = round(100.0 * single_ctnf_num / faa_denom, 1) if faa_denom else None
    return {
        "traditionalPct": trad,
        "chmPct": chm,
        "faaPct": faa,
        "singleCtnfPct": single_ctnf,
        "closed": closed,
        "faaCount": faa_num,
        "singleCtnfCount": single_ctnf_num,
        "faaExcluded": faa_excluded,
    }


def compute_rce_per_allowance_by_year(
    rows: list[dict[str, Any]], cohort_axis: str = "noa"
) -> list[dict[str, Any]]:
    """Average RCEs filed per allowance, bucketed by cohort year.

    For each year on the chosen cohort axis (NOA-mailed by default), takes
    all allowed apps (status in CHM_ALLOWED) and reports:

    * ``allowances`` — count of allowed apps in the year
    * ``totalRces`` — sum of ``rce_count`` across those allowances
    * ``avgRcePerAllowance`` — ``totalRces / allowances``
    * ``pctWithRce`` — share of allowances with ≥ 1 RCE

    Apps without an analytics row are still included (rce_count COALESCEs
    to 0). The ratio is denominated against all allowances in the year so
    a year that's entirely first-action allowances correctly reads 0.0
    rather than ``None``.
    """
    field = COHORT_AXIS_TO_FIELD.get(cohort_axis, "noa_mailed_date")
    by_year: dict[int, list[dict[str, Any]]] = {}
    for r in rows:
        if r.get("application_status_code") not in _CHM_ALLOWED_STATUS_CODES:
            continue
        d = _coerce_date(r.get(field))
        if d is None:
            continue
        by_year.setdefault(d.year, []).append(r)
    out: list[dict[str, Any]] = []
    for year in sorted(by_year):
        allowed = by_year[year]
        n = len(allowed)
        if n == 0:
            continue
        total_rces = sum(_get_int(r, "rce_count") for r in allowed)
        with_rce = sum(1 for r in allowed if _get_int(r, "rce_count") >= 1)
        out.append(
            {
                "year": year,
                "allowances": n,
                "totalRces": total_rces,
                "avgRcePerAllowance": round(total_rces / n, 2),
                "pctWithRce": round(100.0 * with_rce / n, 1),
            }
        )
    return out


def compute_interviews_per_allowance_by_year(
    rows: list[dict[str, Any]], cohort_axis: str = "noa"
) -> list[dict[str, Any]]:
    """Average examiner interviews per allowance, bucketed by cohort year.

    Mirror of ``compute_rce_per_allowance_by_year`` but for examiner
    interviews. For each year on the chosen cohort axis (NOA-mailed by
    default), takes all allowed apps (status in CHM_ALLOWED) and reports:

    * ``allowances`` — count of allowed apps in the year
    * ``totalInterviews`` — sum of ``interview_count`` across those allowances
    * ``avgInterviewsPerAllowance`` — ``totalInterviews / allowances``
    * ``pctWithInterview`` — share of allowances with ≥ 1 examiner interview
    """
    field = COHORT_AXIS_TO_FIELD.get(cohort_axis, "noa_mailed_date")
    by_year: dict[int, list[dict[str, Any]]] = {}
    for r in rows:
        if r.get("application_status_code") not in _CHM_ALLOWED_STATUS_CODES:
            continue
        d = _coerce_date(r.get(field))
        if d is None:
            continue
        by_year.setdefault(d.year, []).append(r)
    out: list[dict[str, Any]] = []
    for year in sorted(by_year):
        allowed = by_year[year]
        n = len(allowed)
        if n == 0:
            continue
        total_interviews = sum(_get_int(r, "interview_count") for r in allowed)
        with_interview = sum(
            1 for r in allowed if _get_int(r, "interview_count") >= 1
        )
        out.append(
            {
                "year": year,
                "allowances": n,
                "totalInterviews": total_interviews,
                "avgInterviewsPerAllowance": round(total_interviews / n, 2),
                "pctWithInterview": round(100.0 * with_interview / n, 1),
            }
        )
    return out


def compute_cohort_trend(
    rows: list[dict[str, Any]], cohort_axis: str = "filing"
) -> list[dict[str, Any]]:
    """One row per cohort year (spec §7.1).

    Marks ``maturing=True`` when any pending app falls in the year — the
    frontend renders those years as hollow circles + dashed connectors so
    counsel doesn't read still-evolving data as a final number.
    """
    field = COHORT_AXIS_TO_FIELD.get(cohort_axis, "filing_date")
    by_year: dict[int, list[dict[str, Any]]] = {}
    for r in rows:
        d = _coerce_date(r.get(field))
        if d is None:
            continue
        by_year.setdefault(d.year, []).append(r)
    out: list[dict[str, Any]] = []
    for year in sorted(by_year):
        group = by_year[year]
        rates = _trad_chm_faa_for_group(group)
        maturing = any(
            r.get("application_status_code") not in (150, 161) for r in group
        )
        # #region agent log — DEBUG-MODE per-cohort diagnostics. Counts the
        # has_analytics_row distribution + the numerator-eligible candidates
        # so we can see whether 100% FAA on recent cohorts is (a) survivorship
        # of clean closes (hyp A), (b) the NULL guard never firing because the
        # column is missing/None (hyp B/E), or (c) something in between.
        _allowed = [
            r for r in group
            if r.get("application_status_code") in _CHM_ALLOWED_STATUS_CODES
        ]
        _diag = {
            "harTrue": sum(1 for r in group if r.get("has_analytics_row") is True),
            "harFalse": sum(1 for r in group if r.get("has_analytics_row") is False),
            "harNone": sum(1 for r in group if r.get("has_analytics_row") is None),
            "allowedClass": len(_allowed),
            "allowedHarTrue": sum(1 for r in _allowed if r.get("has_analytics_row") is True),
            "allowedHarFalse": sum(1 for r in _allowed if r.get("has_analytics_row") is False),
            "allowedHarNone": sum(1 for r in _allowed if r.get("has_analytics_row") is None),
            "preGuardFaaNum": sum(
                1 for r in _allowed
                if _get_int(r, "rce_count") == 0
                and _get_int(r, "final_rejection_count") == 0
            ),
            "postGuardFaaNum": rates["faaCount"],
            "faaExcluded": rates["faaExcluded"],
        }
        # #endregion
        out.append(
            {
                "year": year,
                "n": len(group),
                # Closed-app count (patented + abandoned) is the FAA / Trad
                # denominator. Surfaced so the chart can show "n=closed"
                # under each year — the visual cure for the survivorship
                # bias that makes recent cohorts read as 100%.
                "closed": rates["closed"],
                "traditionalPct": rates["traditionalPct"],
                "chmPct": rates["chmPct"],
                "faaPct": rates["faaPct"],
                # Companion to FAA: allowance after exactly one non-final OA.
                "singleCtnfPct": rates["singleCtnfPct"],
                # Allowed-class apps in this cohort with no application_analytics
                # row, dropped from the FAA numerator (mirrors the headline KPI).
                "faaExcluded": rates["faaExcluded"],
                "maturing": maturing,
                # Per-cohort rejection-count distribution. Five buckets
                # (0/1/2/3/4+ examiner OAs that rejected) over allowed
                # apps in this year. Drives the headline cohort chart now
                # that absolute allowance rates are off the page. Shares
                # sum to 100% per year (modulo `excluded`).
                "byRejectionCount": compute_allowances_by_rejection_count(group)["buckets"],
                "_diag": _diag,  # DEBUG-MODE only: render in ?debug=1 panel
            }
        )
    return out


# Path-to-allowance bucket display labels in the order they should render.
_PATH_BUCKETS: tuple[tuple[str, str], ...] = (
    ("firstAction",     "First-action allowance"),
    ("afterOaNoRce",    "After ≥1 OA, no RCE"),
    ("after1Rce",       "After 1 RCE"),
    ("after2PlusRce",   "After ≥2 RCEs"),
)


def _classify_allowance_path(rce: int, final_rej: int) -> str:
    if rce == 0 and final_rej == 0:
        return "firstAction"
    if rce == 0:
        return "afterOaNoRce"
    if rce == 1:
        return "after1Rce"
    return "after2PlusRce"


def compute_breakdowns(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Top-10 art unit breakdown + 4-bucket path-to-allowance (spec §7.1).

    Art-unit rows missing a ``group_art_unit`` are dropped (you can't show a
    blank row in the table). Path-to-allowance buckets every allowed app
    that we can classify (i.e. has an analytics row) into one of the four
    buckets; allowed apps with no analytics row are reported separately as
    ``pathExcluded`` so the UI can show a data-coverage footnote instead of
    silently misclassifying them as ``firstAction`` (which is the bug that
    inflated the headline FAA to ~75% pre-fix).
    """
    by_au_groups: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        au = r.get("group_art_unit")
        if not au:
            continue
        by_au_groups.setdefault(str(au), []).append(r)

    art_unit_rows: list[dict[str, Any]] = []
    for au, group in by_au_groups.items():
        rc = compute_allowances_by_rejection_count(group)
        # Drop art units with no allowed apps; nothing to show in the
        # action-count distribution table for them.
        if rc["totalAllowed"] == 0:
            continue
        months = [
            float(r["months_to_allowance"])
            for r in group
            if r.get("months_to_allowance") is not None
        ]
        med = round(median(months), 1) if months else None
        bucket_shares = {b["key"]: b["sharePct"] for b in rc["buckets"]}
        art_unit_rows.append(
            {
                "artUnit": au,
                "totalAllowed": rc["totalAllowed"],
                "excluded": rc["excluded"],
                "zeroPct": bucket_shares.get("zero", 0.0),
                "onePct": bucket_shares.get("one", 0.0),
                "twoPct": bucket_shares.get("two", 0.0),
                "threePct": bucket_shares.get("three", 0.0),
                "fourPlusPct": bucket_shares.get("fourPlus", 0.0),
                "medianMonths": med,
            }
        )
    art_unit_rows.sort(key=lambda x: (-x["totalAllowed"], x["artUnit"]))
    art_unit_rows = art_unit_rows[:10]

    allowed = [
        r for r in rows
        if r.get("application_status_code") in _CHM_ALLOWED_STATUS_CODES
    ]
    bucket_groups: dict[str, list[dict[str, Any]]] = {key: [] for key, _ in _PATH_BUCKETS}
    path_excluded = 0
    for r in allowed:
        if r.get("has_analytics_row") is False:
            path_excluded += 1
            continue
        key = _classify_allowance_path(
            _get_int(r, "rce_count"),
            _get_int(r, "final_rejection_count"),
        )
        bucket_groups[key].append(r)

    total_classified = sum(len(b) for b in bucket_groups.values())
    by_path: list[dict[str, Any]] = []
    for key, label in _PATH_BUCKETS:
        bucket = bucket_groups[key]
        count = len(bucket)
        # Shares are denominated against classifiable apps so the four
        # buckets sum to 100% (excluding the unknown count from the
        # share base). The unknown count is reported separately.
        share = round(100.0 * count / total_classified, 1) if total_classified else 0.0
        months = [
            float(r["months_to_allowance"])
            for r in bucket
            if r.get("months_to_allowance") is not None
        ]
        med = round(median(months), 1) if months else None
        by_path.append(
            {
                "key": key,
                "path": label,
                "count": count,
                "sharePct": share,
                "medianMonths": med,
            }
        )

    by_rejection_count = compute_allowances_by_rejection_count(rows)

    return {
        "byArtUnit": art_unit_rows,
        "byPathToAllowance": by_path,
        # Allowed-class apps that couldn't be bucketed because there's no
        # application_analytics row. UI surfaces this as a footnote.
        "pathExcluded": path_excluded,
        "pathTotalAllowed": len(allowed),
        "byRejectionCount": by_rejection_count["buckets"],
        "rejectionCountExcluded": by_rejection_count["excluded"],
        "rejectionCountTotalAllowed": by_rejection_count["totalAllowed"],
    }


def _deadlines_within(rows: Iterable[dict[str, Any]], days: int) -> int:
    """Count rows whose next_deadline_date falls within ``days`` of today.

    Today's date is computed at call time (UTC date is fine for KPI counts —
    USPTO deadlines are date-only). Rows without a next_deadline_date are
    excluded. Past-due dates count for ``days >= 0`` so the "Due in 30d" KPI
    naturally absorbs overdue items into the same number — that's the design
    doc behavior.
    """
    from datetime import date as _date

    today = _date.today()
    cutoff_days = days
    out = 0
    for r in rows:
        nd = r.get("next_deadline_date")
        if nd is None:
            continue
        if hasattr(nd, "isoformat"):
            d = nd
        else:
            try:
                d = _date.fromisoformat(str(nd)[:10])
            except ValueError:
                continue
        delta = (d - today).days
        if delta <= cutoff_days:
            out += 1
    return out


def compute_kpis(
    rows: list[dict[str, Any]],
    *,
    cohort_axis: str = "filing",
    recency_window: Optional[tuple[Optional[date], Optional[date]]] = None,
) -> dict[str, Any]:
    """Spec §7.1.

    When ``recency_window`` is ``None`` (or ``(None, None)``), behavior is
    byte-identical to the pre-v2 implementation (regression-tested in
    ``test_traditional_and_chm_unchanged_when_no_recency_filter``). When a
    window is supplied, ``rows`` is sliced via ``apply_recency_window`` at
    the very top so every downstream KPI runs against the windowed set.
    """
    rows = apply_recency_window(rows, cohort_axis=cohort_axis, window=recency_window)

    total = len(rows)
    patented = sum(1 for r in rows if r.get("application_status_code") == 150)
    abandoned = sum(1 for r in rows if r.get("application_status_code") == 161)
    # Pending = anything that is neither patented nor abandoned. Matches the
    # mockup's "13 patented · 2 pending" subtitle for a 15-row portfolio.
    pending = total - patented - abandoned

    # Traditional (#1) rate: Patented / (Patented + Abandoned). Pending matters
    # are excluded from both numerator and denominator.
    allowance_denom = patented + abandoned
    allowance_pct = round(100.0 * patented / allowance_denom, 1) if allowance_denom else 0.0

    # Carley-Hegde-Marco (#2) "true" allowance rate:
    #   (A + CA) / (A + CA + AB)
    #     A  = Allowed without ever filing an RCE
    #     CA = Allowed after one or more RCEs
    #     AB = Abandoned without a subsequent continuation/CIP/divisional
    chm_allowed_no_rce = 0
    chm_allowed_with_rce = 0
    chm_abandoned_no_child = 0
    for r in rows:
        code = r.get("application_status_code")
        if code in _CHM_ALLOWED_STATUS_CODES:
            if _get_int(r, "rce_count") >= 1:
                chm_allowed_with_rce += 1
            else:
                chm_allowed_no_rce += 1
        elif code == 161 and not r.get("has_child_continuation"):
            chm_abandoned_no_child += 1
    chm_num = chm_allowed_no_rce + chm_allowed_with_rce
    chm_den = chm_num + chm_abandoned_no_child
    chm_pct = round(100.0 * chm_num / chm_den, 1) if chm_den else 0.0

    days = _days_to_noa_values(rows)
    avg_days = round(mean(days), 0) if days else None
    med_days = round(median(days), 0) if days else None

    oa_counts = [
        _get_int(r, "nonfinal_oa_count") + _get_int(r, "final_oa_count") for r in rows
    ]
    avg_oa = round(mean(oa_counts), 2) if oa_counts else 0.0
    apps_with_any_oa = sum(1 for n in oa_counts if n > 0)

    interview_count = sum(1 for r in rows if r.get("had_examiner_interview"))
    interview_rate = round(100.0 * interview_count / total, 1) if total else 0.0

    rce_count = sum(1 for r in rows if _get_int(r, "rce_count") > 0)
    rce_rate = round(100.0 * rce_count / total, 1) if total else 0.0

    # Allowance Analytics v2 secondary KPIs (spec §5). All of these honor the
    # empty-window rule: NULL pct (rendered as "—" by the frontend) when the
    # denominator is empty, never 0.0.
    faa = compute_first_action_allowance(rows)
    single_ctnf = compute_single_ctnf_allowance(rows)
    tta = compute_time_to_allowance(rows)
    rce_intensity = compute_rce_intensity(rows)
    strategic_ab = compute_strategic_abandonment(rows)
    family_yield = compute_family_yield(rows)
    pendency = compute_pendency(rows)
    foreign_priority = compute_foreign_priority_share(rows)

    return {
        "totalApps": total,
        "patentedCount": patented,
        "pendingCount": pending,
        "abandonedCount": abandoned,
        "allowanceRatePct": allowance_pct,
        # Prior-period delta is not tracked yet; surfaced as 0.0 so the UI can
        # show a neutral indicator. PR 5 wires it via a second compute_kpis
        # call against the prior-shifted window.
        "allowanceRateDeltaPctPts": 0.0,
        "chmAllowanceRatePct": chm_pct,
        "chmAllowanceRateDeltaPctPts": 0.0,
        "chmAllowedNoRce": chm_allowed_no_rce,
        "chmAllowedWithRce": chm_allowed_with_rce,
        "chmAbandonedNoChild": chm_abandoned_no_child,
        "avgDaysToNoa": int(avg_days) if avg_days is not None else None,
        "medianDaysToNoa": int(med_days) if med_days is not None else None,
        "avgOaCount": avg_oa,
        "appsWithAtLeastOneOa": apps_with_any_oa,
        "interviewRatePct": interview_rate,
        "interviewCount": interview_count,
        "rceRatePct": rce_rate,
        "rceCount": rce_count,
        # M7: Deadlines Due (30d) KPI. Pulls from the
        # patent_applications view's next_deadline_date column, which is
        # populated by a correlated subquery on computed_deadlines.
        "deadlinesDue30d": _deadlines_within(rows, 30),
        "overdueDeadlines": sum(
            (_get_int(r, "overdue_deadline_count")) for r in rows
        ),
        "openDeadlines": sum(
            (_get_int(r, "open_deadline_count")) for r in rows
        ),
        # Allowance Analytics v2 secondary KPIs.
        "faaPct": faa["pct"],
        "faaCount": faa["count"],
        "faaDenom": faa["denom"],
        # Allowed-class apps with no application_analytics row, dropped from
        # the FAA numerator (data-quality guard, see compute_first_action_allowance).
        "faaExcluded": faa.get("excluded", 0),
        "faaDeltaPctPts": 0.0,
        # Companion to FAA: allowance after exactly one non-final OA, no FR,
        # no RCE. Same denominator as FAA.
        "singleCtnfPct": single_ctnf["pct"],
        "singleCtnfCount": single_ctnf["count"],
        "singleCtnfDenom": single_ctnf["denom"],
        "singleCtnfExcluded": single_ctnf.get("excluded", 0),
        "singleCtnfDeltaPctPts": 0.0,
        "timeToAllowance": tta,
        "rceIntensity": rce_intensity,
        "strategicAbandonment": strategic_ab,
        "familyYield": family_yield,
        "pendency": pendency,
        "foreignPriority": foreign_priority,
    }


def compute_days_to_noa_by_app(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sorted ascending by days (nulls last so muted bars cluster on the right).

    Kept for backwards compatibility; UI now consumes ``daysToNoaHistogram``.
    """
    ordered = sorted(
        rows,
        key=lambda r: (
            r.get("days_filing_to_noa") is None,
            r.get("days_filing_to_noa") or 0,
        ),
    )
    return [
        {
            "applicationNumber": r.get("application_number"),
            "title": r.get("invention_title"),
            "days": r["days_filing_to_noa"] if r.get("days_filing_to_noa") is not None else None,
        }
        for r in ordered
    ]


# Bucket widths (in days) the histogram is allowed to use, in increasing order.
# All values are multiples of 30 days (or aligned to 365) so the auto-generated
# labels read as whole months / years.
_HISTOGRAM_BIN_CANDIDATES_DAYS: tuple[int, ...] = (15, 30, 60, 90, 180, 365, 730)


def _pick_histogram_bin_days(max_days: int) -> int:
    """Pick a bin width yielding ~5–12 bars across [0, max_days]."""
    for c in _HISTOGRAM_BIN_CANDIDATES_DAYS:
        if math.ceil((max_days + 1) / c) <= 12:
            chosen = c
            break
    else:
        chosen = _HISTOGRAM_BIN_CANDIDATES_DAYS[-1]
    # Step down to a smaller candidate when the chart would otherwise have
    # fewer than 5 bars (looks sparse and uninformative).
    while (
        math.ceil((max_days + 1) / chosen) < 5
        and chosen > _HISTOGRAM_BIN_CANDIDATES_DAYS[0]
    ):
        idx = _HISTOGRAM_BIN_CANDIDATES_DAYS.index(chosen)
        chosen = _HISTOGRAM_BIN_CANDIDATES_DAYS[idx - 1]
    return chosen


def _format_histogram_bin_label(lo: int, hi_exclusive: int, bin_days: int) -> str:
    if bin_days < 30:
        return f"{lo}\u2013{hi_exclusive - 1}d"
    if bin_days % 365 == 0 and lo % 365 == 0 and hi_exclusive % 365 == 0:
        return f"{lo // 365}\u2013{hi_exclusive // 365}y"
    lo_m = round(lo / 30)
    hi_m = round(hi_exclusive / 30)
    return f"{lo_m}\u2013{hi_m}mo"


def compute_days_to_noa_histogram(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Histogram of days-from-filing-to-NOA for the filtered set.

    Returns auto-sized bins, the median / mean for marker rendering, and counts
    of apps with vs. without an NOA so the UI can disclose what's missing.
    """
    days_values: list[int] = [
        int(r["days_filing_to_noa"])
        for r in rows
        if r.get("days_filing_to_noa") is not None
    ]
    no_noa_count = sum(1 for r in rows if r.get("days_filing_to_noa") is None)

    if not days_values:
        return {
            "bins": [],
            "binDays": 0,
            "median": None,
            "mean": None,
            "totalWithNoa": 0,
            "totalWithoutNoa": no_noa_count,
        }

    max_v = max(days_values)
    bin_days = _pick_histogram_bin_days(max_v)
    n_bins = max(1, math.ceil((max_v + 1) / bin_days))

    counts = [0] * n_bins
    for v in days_values:
        idx = min(n_bins - 1, v // bin_days)
        counts[idx] += 1

    total = len(days_values)
    bins: list[dict[str, Any]] = []
    for i, count in enumerate(counts):
        lo = i * bin_days
        hi_exclusive = (i + 1) * bin_days
        bins.append(
            {
                "label": _format_histogram_bin_label(lo, hi_exclusive, bin_days),
                "minDays": lo,
                "maxDays": hi_exclusive - 1,
                "count": count,
                "pct": round(100.0 * count / total, 1),
            }
        )

    return {
        "bins": bins,
        "binDays": bin_days,
        "median": int(median(days_values)),
        "mean": round(sum(days_values) / total, 1),
        "totalWithNoa": total,
        "totalWithoutNoa": no_noa_count,
    }


def compute_status_mix(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    tallies: dict[int | None, dict[str, Any]] = {}
    for r in rows:
        code = r.get("application_status_code")
        label = status_label(code, r.get("application_status_text"))
        key = code if code is not None else -1
        entry = tallies.setdefault(
            key,
            {"label": label, "code": code, "count": 0, "tone": status_tone(code)},
        )
        entry["count"] += 1
    # Largest slices first so the donut looks stable.
    return sorted(tallies.values(), key=lambda e: (-e["count"], e["label"]))


def compute_prosecution_signals(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    if not total:
        return {
            "avgNonfinalOa": 0.0,
            "avgFinalOa": 0.0,
            "avgInterviews": 0.0,
            "noaWithin90DaysOfInterviewPct": 0.0,
            "continuationCount": 0,
            "continuationTotal": 0,
            "jacCount": 0,
        }
    avg_nonfinal = round(
        mean(_get_int(r, "nonfinal_oa_count") for r in rows), 2
    )
    avg_final = round(mean(_get_int(r, "final_oa_count") for r in rows), 2)
    avg_interviews = round(mean(_get_int(r, "interview_count") for r in rows), 2)

    interviewed = [r for r in rows if r.get("had_examiner_interview")]
    if interviewed:
        within_90 = sum(1 for r in interviewed if r.get("noa_within_90_days_of_interview"))
        noa_pct = round(100.0 * within_90 / len(interviewed), 1)
    else:
        noa_pct = 0.0

    continuations = sum(1 for r in rows if r.get("is_continuation"))
    jacs = sum(1 for r in rows if r.get("is_jac"))

    return {
        "avgNonfinalOa": avg_nonfinal,
        "avgFinalOa": avg_final,
        "avgInterviews": avg_interviews,
        "noaWithin90DaysOfInterviewPct": noa_pct,
        "continuationCount": continuations,
        "continuationTotal": total,
        "jacCount": jacs,
    }


# Bucket edges for the CTNF response-speed -> outcome chart, in days from
# CTNF mail to the applicant's response.
#
# 0-30 / 31-60 / 61-90 / 91-120 / 121-180 land on USPTO-meaningful month
# boundaries (1, 2, 3, 4, 6 months) so the bars line up with how a
# prosecutor actually thinks about the response window. Beyond 180d we
# collapse into a single "181+" bucket -- past the statutory cliff,
# response-speed effects are dominated by petition-revival mechanics, not
# examiner behavior, so finer granularity isn't informative.
CTNF_RESPONSE_BUCKETS: tuple[tuple[str, int, Optional[int]], ...] = (
    ("0\u201330d", 0, 30),
    ("31\u201360d", 31, 60),
    ("61\u201390d", 61, 90),
    ("91\u2013120d", 91, 120),
    ("121\u2013180d", 121, 180),
    ("181d+", 181, None),
)


def _bucket_index_for_days(days: int) -> int:
    for i, (_label, lo, hi) in enumerate(CTNF_RESPONSE_BUCKETS):
        if days < lo:
            continue
        if hi is None or days <= hi:
            return i
    return len(CTNF_RESPONSE_BUCKETS) - 1


def compute_ctnf_response_speed_to_noa(
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    """Bucket per-CTNF outcome events by days-to-respond, compute allowance %.

    ``events`` is the list-of-dicts produced from
    ``ctnf_outcome.extract_outcomes_from_grouped_events`` (each dict has
    ``daysToResponse``, ``outcome``, optional ``daysResponseToNext``).

    Per bucket we emit:
      * ``responses``  total CTNFs whose response landed in this bucket
      * ``allowed``    next examiner action was an NOA
      * ``rejected``   next examiner action was another CTNF / CTFR
      * ``pending``    no successor examiner action yet
      * ``allowedPct`` allowed / (allowed + rejected) -- pending excluded
                       so a still-prosecuting cohort doesn't drag the rate
      * ``medianDaysResponseToNoa`` median days from response to NOA among
                       the ``allowed`` events in this bucket (handy for
                       "fast response also means fast allowance" stories)

    Top-level fields:
      ``buckets`` is the per-bucket list (always all 6 buckets, even when
      empty, so the chart axis stays stable across filter changes).
      ``totalEvents`` / ``totalAllowed`` / ``totalRejected`` /
      ``totalPending`` are corpus-wide.
      ``overallAllowedPct`` is allowed / (allowed + rejected) across the
      whole filtered set.
      ``medianDaysToResponse`` is the median days-to-respond across all
      events (sanity gut-check vs the chart).
    """
    buckets: list[dict[str, Any]] = [
        {
            "label": label,
            "minDays": lo,
            "maxDays": hi,
            "responses": 0,
            "allowed": 0,
            "rejected": 0,
            "pending": 0,
            "allowedPct": 0.0,
            "medianDaysResponseToNoa": None,
        }
        for label, lo, hi in CTNF_RESPONSE_BUCKETS
    ]
    response_to_noa_days_per_bucket: list[list[int]] = [
        [] for _ in CTNF_RESPONSE_BUCKETS
    ]
    days_to_response_all: list[int] = []

    total_allowed = 0
    total_rejected = 0
    total_pending = 0

    for ev in events:
        try:
            d = int(ev["daysToResponse"])
        except (KeyError, TypeError, ValueError):
            continue
        if d < 0:
            # Defensive: a response can't predate the CTNF in the real
            # world, but bad ingest data shouldn't crash the chart.
            continue
        idx = _bucket_index_for_days(d)
        b = buckets[idx]
        b["responses"] += 1
        days_to_response_all.append(d)
        outcome = ev.get("outcome")
        if outcome == "allowed":
            b["allowed"] += 1
            total_allowed += 1
            r2n = ev.get("daysResponseToNext")
            if isinstance(r2n, int) and r2n >= 0:
                response_to_noa_days_per_bucket[idx].append(r2n)
        elif outcome == "rejected":
            b["rejected"] += 1
            total_rejected += 1
        elif outcome == "pending":
            b["pending"] += 1
            total_pending += 1

    for i, b in enumerate(buckets):
        decided = b["allowed"] + b["rejected"]
        if decided:
            b["allowedPct"] = round(100.0 * b["allowed"] / decided, 1)
        if response_to_noa_days_per_bucket[i]:
            b["medianDaysResponseToNoa"] = int(
                median(response_to_noa_days_per_bucket[i])
            )

    decided_total = total_allowed + total_rejected
    overall_allowed_pct = (
        round(100.0 * total_allowed / decided_total, 1) if decided_total else 0.0
    )
    median_dtr = (
        int(median(days_to_response_all)) if days_to_response_all else None
    )

    return {
        "buckets": buckets,
        "totalEvents": total_allowed + total_rejected + total_pending,
        "totalAllowed": total_allowed,
        "totalRejected": total_rejected,
        "totalPending": total_pending,
        "overallAllowedPct": overall_allowed_pct,
        "medianDaysToResponse": median_dtr,
    }


def compute_charts(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "daysToNoaHistogram": compute_days_to_noa_histogram(rows),
        # `daysToNoaByApp` retained for any external consumers; the in-app UI
        # now uses the histogram above.
        "daysToNoaByApp": compute_days_to_noa_by_app(rows),
        "statusMix": compute_status_mix(rows),
        "prosecutionSignals": compute_prosecution_signals(rows),
    }


# ---------------------------------------------------------------------------
# Applicant Trends tab — per-year filing volume + YoY growth.
#
# Two views ship in the response:
#
# * ``byYear``       — total filings per year across the chip-filtered
#                      selection, with a year-over-year delta column. The
#                      current calendar year is partial; we mark it
#                      ``isPartial=True`` and compare its YTD count to the
#                      same date-of-year span in the prior year so the
#                      growth column doesn't read as a misleading drop
#                      on January 5th.
# * ``byApplicant``  — top N applicants by recent-window filing volume.
#                      Each row exposes a per-year filing count for the
#                      last few years plus the same prior-year (or
#                      same-period prior-year for a partial current year)
#                      delta. Lets counsel see at a glance which applicants
#                      are accelerating or pulling back.
#
# Years bucket on ``filing_date``. Rows missing a filing_date are silently
# dropped (a filing without a filing date can't be placed on the timeline).
# ---------------------------------------------------------------------------


def _day_of_year(d: date) -> int:
    return d.timetuple().tm_yday


def _yoy_delta(current: int, prior: int) -> tuple[Optional[int], Optional[float]]:
    """Absolute and percent change from ``prior`` -> ``current``.

    Returns ``(None, None)`` when there is no prior baseline (so the UI
    renders ``—`` instead of fabricating a growth number from zero).
    """
    if prior <= 0:
        # First-ever year of data, or a prior with literally zero filings.
        # Either way there is no meaningful base — surface as "n/a" rather
        # than ``+inf%`` or ``+100%`` which counsel would misread.
        return (current - prior if prior else None, None)
    delta_abs = current - prior
    delta_pct = round(100.0 * delta_abs / prior, 1)
    return (delta_abs, delta_pct)


def compute_applicant_trends(
    rows: list[dict[str, Any]],
    *,
    today: Optional[date] = None,
    top_applicants: int = 20,
    matrix_years: int = 5,
) -> dict[str, Any]:
    """Per-year filing counts + YoY growth, both portfolio-wide and per applicant.

    ``today`` is injectable for testability; defaults to ``date.today()``.
    The current year row is flagged ``isPartial`` and its growth column
    compares its YTD count to the same date-of-year window in the prior
    year — so on Jan 5 we don't show "filings down 99% YoY".

    ``top_applicants`` caps the per-applicant breakdown so the response
    payload stays small for portfolios with thousands of distinct
    applicants. Ranking is by filings in the most recent year (with the
    full-history total as a tiebreak), so the table answers "who's filing
    the most right now?".
    """
    if today is None:
        today = date.today()
    cur_year = today.year
    cur_doy = _day_of_year(today)

    counts_by_year: dict[int, int] = {}
    counts_by_year_ytd: dict[int, int] = {}

    by_app_year: dict[str, dict[int, int]] = {}
    by_app_year_ytd: dict[str, dict[int, int]] = {}

    for r in rows:
        fd = _coerce_date(r.get("filing_date"))
        if fd is None:
            continue
        year = fd.year
        counts_by_year[year] = counts_by_year.get(year, 0) + 1
        if _day_of_year(fd) <= cur_doy:
            counts_by_year_ytd[year] = counts_by_year_ytd.get(year, 0) + 1

        applicant = (r.get("applicant_name") or "").strip()
        if applicant:
            ay = by_app_year.setdefault(applicant, {})
            ay[year] = ay.get(year, 0) + 1
            if _day_of_year(fd) <= cur_doy:
                ayy = by_app_year_ytd.setdefault(applicant, {})
                ayy[year] = ayy.get(year, 0) + 1

    if not counts_by_year:
        return {
            "byYear": [],
            "byApplicant": [],
            "yearsShown": [],
            "currentYear": cur_year,
            "asOf": today.isoformat(),
            "totalApplicantsWithFilings": 0,
        }

    y_min = min(counts_by_year)
    # Always extend the row range up through the current calendar year so
    # the in-progress YTD row is visible even if the most recent filing
    # date in the dataset is in a prior year.
    y_max = max(max(counts_by_year), cur_year)

    by_year: list[dict[str, Any]] = []
    for y in range(y_min, y_max + 1):
        count = counts_by_year.get(y, 0)
        is_partial = y == cur_year
        prior_full = counts_by_year.get(y - 1, 0)
        if is_partial:
            # YTD compare: this year's count (which is by definition only
            # the YTD slice — no future filings exist yet) vs the prior
            # year's filings up to the same day-of-year.
            prior_basis = counts_by_year_ytd.get(y - 1, 0)
            delta_abs, delta_pct = _yoy_delta(count, prior_basis)
            compare_label = "vs same period last year"
        else:
            prior_basis = prior_full if y > y_min else 0
            if y == y_min:
                # First year in the window has no prior — surface "—".
                delta_abs, delta_pct = (None, None)
            else:
                delta_abs, delta_pct = _yoy_delta(count, prior_basis)
            compare_label = "vs prior year"
        by_year.append(
            {
                "year": y,
                "filings": count,
                "priorFilings": prior_basis if (y > y_min or is_partial) else None,
                "deltaAbs": delta_abs,
                "deltaPct": delta_pct,
                "isPartial": is_partial,
                "compareLabel": compare_label if (y > y_min or is_partial) else None,
            }
        )

    # Per-applicant: choose the matrix year window. For a 1-2 year dataset
    # we just show what's there; for longer histories we cap at
    # ``matrix_years`` ending at the current year (so the partial-year
    # column is always present when we have any current-year activity).
    span = y_max - y_min + 1
    if span <= matrix_years:
        years_in_matrix = list(range(y_min, y_max + 1))
    else:
        years_in_matrix = list(range(y_max - matrix_years + 1, y_max + 1))

    applicants: list[dict[str, Any]] = []
    for app, year_counts in by_app_year.items():
        total = sum(year_counts.values())
        cur_count = year_counts.get(cur_year, 0)
        prior_year_full = year_counts.get(cur_year - 1, 0)
        if cur_count or cur_year in years_in_matrix:
            # Apply the same YTD compare logic for the current year so a
            # high-volume applicant doesn't appear to have crashed in
            # January.
            prior_basis = (
                by_app_year_ytd.get(app, {}).get(cur_year - 1, 0)
                if _day_of_year(today) < 366
                else prior_year_full
            )
            delta_abs, delta_pct = _yoy_delta(cur_count, prior_basis)
            is_partial = True
            compare_label = "vs same period last year"
        else:
            prior_basis = prior_year_full
            delta_abs, delta_pct = _yoy_delta(
                year_counts.get(y_max, 0), year_counts.get(y_max - 1, 0)
            )
            is_partial = False
            compare_label = "vs prior year"

        applicants.append(
            {
                "applicant": app,
                "total": total,
                "perYear": [
                    {"year": y, "filings": year_counts.get(y, 0)}
                    for y in years_in_matrix
                ],
                "latestYear": cur_year if is_partial else y_max,
                "latestFilings": cur_count if is_partial else year_counts.get(y_max, 0),
                "priorFilings": prior_basis,
                "deltaAbs": delta_abs,
                "deltaPct": delta_pct,
                "isPartial": is_partial,
                "compareLabel": compare_label,
            }
        )

    # Rank by latest-period activity — that's the column the eye lands on
    # first. Tie-break by lifetime total (so a 50-app applicant doesn't get
    # buried behind 12 single-filing applicants who all happened to file
    # one app this year), then alphabetically for stable ordering across
    # refreshes.
    applicants.sort(
        key=lambda r: (
            -(r["latestFilings"] or 0),
            -(r["total"] or 0),
            r["applicant"].lower(),
        )
    )
    top = applicants[: max(1, top_applicants)]

    return {
        "byYear": by_year,
        "byApplicant": top,
        "yearsShown": years_in_matrix,
        "currentYear": cur_year,
        "asOf": today.isoformat(),
        "totalApplicantsWithFilings": len(applicants),
    }
