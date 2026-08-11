from src.common.rules import (
    burn_rate,
    execution_risk,
    months_to_exhaustion,
    predicted_velocity,
    quality_score,
    trend_id,
    vendor_gap_status,
)


def test_burn_rate_basic():
    assert burn_rate(1200, 4) == 300
    assert burn_rate(100, 0) == 0.0
    assert burn_rate(None, 3) is None


def test_predicted_velocity():
    assert predicted_velocity(1000, 4, 2) == 500
    assert predicted_velocity(1000, 4, 0) == 0.0


def test_months_to_exhaustion():
    assert months_to_exhaustion(1000, 400, 200) == 3
    assert months_to_exhaustion(1000, 400, 0) is None


def test_execution_risk_bands():
    assert execution_risk(1200, 1000) == "OVERRUN"
    assert execution_risk(980, 1000) == "AT_RISK"
    assert execution_risk(700, 1000, months_remaining=2) == "UNDER_EXEC"
    assert execution_risk(800, 1000, months_remaining=10) == "ON_TRACK"
    assert execution_risk(None, 1000) == "UNKNOWN"


def test_trend_id():
    assert trend_id(120, 100, "OVERRUN") == "TRD-OVERRUN-ACCEL"
    assert trend_id(80, 100, "ON_TRACK") == "TRD-ON_TRACK-DECEL"
    assert trend_id(100, 100, "ON_TRACK") == "TRD-ON_TRACK-FLAT"
    assert trend_id(None, 10, "ON_TRACK") == "TRD-UNKNOWN"


def test_quality_score_healthy():
    score = quality_score(null_rate=0.0, reject_rate=0.0, freshness_hours=0.5)
    assert score >= 90


def test_quality_score_degraded():
    score = quality_score(null_rate=0.5, reject_rate=0.4, freshness_hours=200, schema_drift_count=5)
    assert score < 50


def test_vendor_gap_status():
    assert vendor_gap_status("Lapsed", 10, 0.1) == "DATA_GAP"
    assert vendor_gap_status("Active", -1, 0.1) == "DATA_GAP"
    assert vendor_gap_status("Active", 10, 0.5) == "RENEWAL_DUE"
    assert vendor_gap_status("Active", 90, 0.99) == "LICENSE_PRESSURE"
    assert vendor_gap_status("Active", 90, 0.4) == "HEALTHY"
