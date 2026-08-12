from src.common.mock_data import (
    build_demo_drop_file,
    build_financial,
    build_grants,
    build_grants_schema_evolution,
    build_vendors,
)


def test_grants_are_mock_prefixed():
    rows = build_grants(include_bad_rows=False)
    assert len(rows) >= 30
    assert all(r["grant_id"].startswith("MOCK-ONR-") for r in rows)
    assert all(r["classification"] == "MOCK_UNCLASSIFIED" for r in rows)
    # No obvious PII fields
    forbidden = {"ssn", "email", "phone", "home_address"}
    assert not (forbidden & set(rows[0]))


def test_seeded_bad_rows_exist_for_quality_demo():
    rows = build_grants(include_bad_rows=True)
    assert any(r["grant_id"] is None for r in rows)
    assert any((r.get("award_amount") or 0) < 0 for r in rows)


def test_schema_evolution_adds_columns():
    extra = build_grants_schema_evolution()
    assert extra
    assert "collaboration_flag" in extra[0]
    assert "international_partner" in extra[0]


def test_financial_references_real_grants_only():
    grants = build_grants(include_bad_rows=True)
    fin = build_financial(grants, include_bad_rows=False)
    grant_ids = {g["grant_id"] for g in grants if g.get("grant_id")}
    assert fin
    assert all(r["grant_id"] in grant_ids for r in fin)


def test_vendors_include_a_data_gap():
    vendors = build_vendors()
    assert any(v["status"] == "Lapsed" for v in vendors)
    assert any(v["days_to_renewal"] <= 30 for v in vendors)


def test_live_drop_marker():
    drop = build_demo_drop_file()
    assert drop["demo_marker"] == "ELEMENT_3_LIVE_DROP"
    assert drop["grant_id"].startswith("MOCK-ONR-")
