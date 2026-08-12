from src.common.transforms import (
    REQUIRED_GRANT_COLS,
    dataset_health,
    is_valid_amount,
    is_valid_grant_id,
    missing_columns,
    normalize_grant_id,
    project_financial_row,
    row_quality_flags,
)


def test_normalize_and_validate_grant_id():
    assert normalize_grant_id(" mock-onr-n00014-26-c-0001 ") == "MOCK-ONR-N00014-26-C-0001"
    assert is_valid_grant_id("MOCK-ONR-N00014-26-C-0001")
    assert not is_valid_grant_id("BAD-ID-0001")
    assert not is_valid_grant_id(None)


def test_is_valid_amount():
    assert is_valid_amount(0)
    assert is_valid_amount(10.5)
    assert not is_valid_amount(-1)
    assert not is_valid_amount(None)


def test_row_quality_flags():
    flags = row_quality_flags({"grant_id": None, "award_amount": -5, "expended": 10})
    assert "invalid_grant_id" in flags
    assert "invalid_award_amount" in flags


def test_project_financial_row_overrun():
    out = project_financial_row(
        award_amount=1000,
        expended=800,
        obligated=900,
        months_elapsed=4,
        remaining_months=4,
    )
    assert out["monthly_burn"] == 200
    assert out["projected_total"] == 1600
    assert out["risk_class"] == "OVERRUN"
    assert out["trend_id"].startswith("TRD-OVERRUN-")


def test_dataset_health_empty():
    h = dataset_health(0, 0, 0, None)
    assert h["health_band"] == "CRITICAL"
    assert h["health_score"] == 0.0


def test_dataset_health_ok():
    h = dataset_health(100, 0, 0, 1.0)
    assert h["health_band"] == "HEALTHY"
    assert h["row_count"] == 100


def test_missing_columns():
    missing = missing_columns(["grant_id", "project_name"], REQUIRED_GRANT_COLS)
    assert "award_amount" in missing
    assert "grant_id" not in missing
