"""Deterministic decision-support rules used by silver/gold transforms, ML features, and tests.

All logic is mock-data safe. No PII, CUI, or classified fields are referenced.
"""

from __future__ import annotations

from typing import Optional

# Unity Catalog tags applied to every securable in this POC.
# Do NOT put CUI, program names, or real identifiers in tags (IL5 name-field rule).
DATA_CLASSIFICATION_TAGS = {
    "classification": "MOCK_UNCLASSIFIED",
    "cui": "false",
    "pii": "false",
    "domain": "onr_snt_mock",
    "demo_element": "onr_itss_poc",
}


def burn_rate(expended: Optional[float], months_elapsed: Optional[float]) -> Optional[float]:
    """Average monthly expenditure. Returns None when inputs are not usable."""
    if expended is None or months_elapsed is None:
        return None
    if months_elapsed <= 0:
        return 0.0
    return float(expended) / float(months_elapsed)


def predicted_velocity(
    expended: Optional[float],
    months_elapsed: Optional[float],
    remaining_months: Optional[float],
) -> Optional[float]:
    """Linear projection of remaining spend at the current burn rate."""
    rate = burn_rate(expended, months_elapsed)
    if rate is None or remaining_months is None:
        return None
    return rate * max(float(remaining_months), 0.0)


def months_to_exhaustion(
    award_amount: Optional[float],
    expended: Optional[float],
    monthly_burn: Optional[float],
) -> Optional[float]:
    """Months until remaining award is exhausted at current burn."""
    if award_amount is None or expended is None or monthly_burn is None:
        return None
    remaining = float(award_amount) - float(expended)
    if monthly_burn <= 0:
        return None if remaining > 0 else 0.0
    return remaining / float(monthly_burn)


def execution_risk(
    projected_total: Optional[float],
    award_amount: Optional[float],
    months_remaining: Optional[float] = None,
    obligated: Optional[float] = None,
) -> str:
    """Leadership risk class for financial execution.

    OVERRUN  — projected total exceeds award by >5%
    AT_RISK  — projected within 5% of award, or <3 months remaining with >20% unexpended
    UNDER_EXEC — projected <80% of award with <3 months remaining
    ON_TRACK — otherwise
    UNKNOWN  — insufficient inputs
    """
    if projected_total is None or award_amount is None or award_amount <= 0:
        return "UNKNOWN"
    ratio = float(projected_total) / float(award_amount)
    if ratio > 1.05:
        return "OVERRUN"
    remaining = months_remaining if months_remaining is not None else 12.0
    unexpended_pct = None
    if obligated is not None and obligated > 0 and projected_total is not None:
        # used as a secondary signal when near period-end
        unexpended_pct = max(0.0, 1.0 - (float(projected_total) / float(max(obligated, award_amount))))
    if remaining is not None and remaining < 3:
        if ratio < 0.80:
            return "UNDER_EXEC"
        if unexpended_pct is not None and unexpended_pct > 0.20 and ratio < 0.95:
            return "UNDER_EXEC"
    if ratio >= 0.95:
        return "AT_RISK"
    return "ON_TRACK"


def trend_id(
    current_burn: Optional[float],
    prior_burn: Optional[float],
    risk_class: str,
) -> str:
    """Stable, human-readable trend identifier for Element 5 structured output."""
    if current_burn is None:
        return "TRD-UNKNOWN"
    direction = "FLAT"
    if prior_burn is not None and prior_burn > 0:
        delta = (float(current_burn) - float(prior_burn)) / float(prior_burn)
        if delta > 0.10:
            direction = "ACCEL"
        elif delta < -0.10:
            direction = "DECEL"
    return f"TRD-{risk_class}-{direction}"


def quality_score(
    null_rate: float,
    reject_rate: float,
    freshness_hours: Optional[float],
    schema_drift_count: int = 0,
) -> float:
    """0-100 health score used by the catalog / Element 4 demo.

    Weights: completeness 40, validity 35, freshness 20, stability 5.
    """
    completeness = max(0.0, 1.0 - min(null_rate, 1.0)) * 40.0
    validity = max(0.0, 1.0 - min(reject_rate, 1.0)) * 35.0
    if freshness_hours is None:
        freshness = 10.0
    elif freshness_hours <= 1:
        freshness = 20.0
    elif freshness_hours <= 24:
        freshness = 16.0
    elif freshness_hours <= 72:
        freshness = 10.0
    else:
        freshness = 4.0
    stability = max(0.0, 5.0 - min(schema_drift_count, 5))
    return round(completeness + validity + freshness + stability, 1)


def vendor_gap_status(
    status: Optional[str],
    days_to_renewal: Optional[int],
    usage_pct: Optional[float],
) -> str:
    """Subscription lifecycle flag for Element 4/6 vendor management (prompt e)."""
    normalized = (status or "").strip().upper()
    if normalized in {"LAPSED", "EXPIRED", "CANCELLED", "CANCELED"}:
        return "DATA_GAP"
    if days_to_renewal is not None and days_to_renewal < 0:
        return "DATA_GAP"
    if days_to_renewal is not None and days_to_renewal <= 30:
        return "RENEWAL_DUE"
    if usage_pct is not None and usage_pct > 0.95:
        return "LICENSE_PRESSURE"
    if normalized in {"ACTIVE", "CURRENT"}:
        return "HEALTHY"
    return "WATCH"
