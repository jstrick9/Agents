"""Spark-optional transform helpers.

When pyspark is available (workspace / Connect), functions accept DataFrames.
Pure Python helpers remain in rules.py for local pytest.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional

from src.common.rules import (
    burn_rate,
    execution_risk,
    months_to_exhaustion,
    predicted_velocity,
    quality_score,
    trend_id,
    vendor_gap_status,
)


REQUIRED_GRANT_COLS = (
    "grant_id",
    "project_name",
    "performing_org",
    "onr_code",
    "tech_area",
    "start_date",
    "end_date",
    "award_amount",
    "status",
    "trl",
    "appropriation",
    "fiscal_year",
)

REQUIRED_FINANCIAL_COLS = (
    "transaction_id",
    "grant_id",
    "fiscal_year",
    "period",
    "budget_line",
    "appropriation",
    "budgeted",
    "obligated",
    "expended",
    "cost_center",
)

REQUIRED_VENDOR_COLS = (
    "subscription_id",
    "vendor_name",
    "dataset_name",
    "license_type",
    "renewal_date",
    "status",
    "annual_cost",
)


def normalize_grant_id(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).strip().upper()
    return cleaned or None


def is_valid_grant_id(value: Optional[str]) -> bool:
    if not value:
        return False
    return value.startswith("MOCK-ONR-") and len(value) >= 16


def is_valid_amount(value: Any) -> bool:
    try:
        return value is not None and float(value) >= 0
    except (TypeError, ValueError):
        return False


def row_quality_flags(row: dict) -> list[str]:
    flags = []
    if not is_valid_grant_id(normalize_grant_id(row.get("grant_id"))):
        flags.append("invalid_grant_id")
    if "award_amount" in row and not is_valid_amount(row.get("award_amount")):
        flags.append("invalid_award_amount")
    if "expended" in row and not is_valid_amount(row.get("expended")):
        flags.append("invalid_expended")
    if "obligated" in row and not is_valid_amount(row.get("obligated")):
        flags.append("invalid_obligated")
    return flags


def project_financial_row(
    award_amount: Optional[float],
    expended: Optional[float],
    obligated: Optional[float],
    months_elapsed: Optional[float],
    remaining_months: Optional[float],
    prior_burn: Optional[float] = None,
) -> dict:
    monthly = burn_rate(expended, months_elapsed)
    projected_remaining = predicted_velocity(expended, months_elapsed, remaining_months)
    projected_total = None
    if expended is not None and projected_remaining is not None:
        projected_total = float(expended) + float(projected_remaining)
    elif expended is not None:
        projected_total = float(expended)
    risk = execution_risk(projected_total, award_amount, remaining_months, obligated)
    mte = months_to_exhaustion(award_amount, expended, monthly)
    return {
        "monthly_burn": monthly,
        "projected_remaining": projected_remaining,
        "projected_total": projected_total,
        "months_to_exhaustion": mte,
        "risk_class": risk,
        "trend_id": trend_id(monthly, prior_burn, risk),
        "predicted_velocity": monthly,
    }


def dataset_health(
    total_rows: int,
    null_key_rows: int,
    rejected_rows: int,
    freshness_hours: Optional[float],
    schema_drift_count: int = 0,
) -> dict:
    if total_rows <= 0:
        return {
            "row_count": 0,
            "null_rate": 1.0,
            "reject_rate": 1.0,
            "health_score": 0.0,
            "health_band": "CRITICAL",
        }
    null_rate = null_key_rows / total_rows
    reject_rate = rejected_rows / max(total_rows + rejected_rows, 1)
    score = quality_score(null_rate, reject_rate, freshness_hours, schema_drift_count)
    if score >= 90:
        band = "HEALTHY"
    elif score >= 75:
        band = "WATCH"
    elif score >= 50:
        band = "DEGRADED"
    else:
        band = "CRITICAL"
    return {
        "row_count": total_rows,
        "null_rate": round(null_rate, 4),
        "reject_rate": round(reject_rate, 4),
        "health_score": score,
        "health_band": band,
    }


def missing_columns(present: Iterable[str], required: Iterable[str]) -> list[str]:
    have = {c.lower() for c in present}
    return [c for c in required if c.lower() not in have]


# Re-export for notebooks that import a single module
__all__ = [
    "REQUIRED_GRANT_COLS",
    "REQUIRED_FINANCIAL_COLS",
    "REQUIRED_VENDOR_COLS",
    "normalize_grant_id",
    "is_valid_grant_id",
    "is_valid_amount",
    "row_quality_flags",
    "project_financial_row",
    "dataset_health",
    "missing_columns",
    "vendor_gap_status",
    "quality_score",
]
