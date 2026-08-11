"""Generate sanitized mock ONR S&T registry, financial ERP, and vendor subscription data.

All records are fictitious. Identifiers use the MOCK-ONR- prefix.
No CUI, PII, or classified content is produced.
"""

from __future__ import annotations

import csv
import json
import random
from datetime import date, datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Any

SEED = 20260811
TODAY = date(2026, 8, 11)

ONR_CODES = ["31", "32", "33", "34", "35", "08"]
TECH_AREAS = [
    "Autonomy",
    "Ocean Sensing",
    "Power and Energy",
    "Materials",
    "Human Systems",
    "C4ISR",
    "Undersea Warfare",
    "Data and Analytics",
]
APPROPRIATIONS = ["6.1", "6.2", "6.3"]
ORGS = [
    "Mock Naval Postgraduate Lab",
    "Mock Fleet Experimentation Center",
    "Mock University Affiliated Lab",
    "Mock Warfare Center Detachment",
    "Mock Applied Ocean Institute",
]
STATUSES = ["Active", "Active", "Active", "Pending", "Completed"]
BUDGET_LINES = [
    "Basic Research",
    "Applied Research",
    "Advanced Technology Dev",
    "RDT&E Management Support",
]
COST_CENTERS = ["ONR-08-DA", "ONR-08-OPS", "ONR-08-FIN", "ONR-31-SNT"]
VENDORS = [
    {
        "subscription_id": "SUB-MOCK-014",
        "vendor_name": "OceanMetrics Analytics (Mock)",
        "dataset_name": "Global S&T Publication Index",
        "license_type": "Seat",
        "seats": 25,
        "start_date": "2026-01-01",
        "renewal_date": "2026-12-31",
        "annual_cost": 185000.0,
        "status": "Active",
        "usage_pct": 0.72,
        "quality_sla": 0.995,
        "feeds_gold_table": "gold_grant_portfolio",
    },
    {
        "subscription_id": "SUB-MOCK-021",
        "vendor_name": "HarborLedger Research (Mock)",
        "dataset_name": "Federal Award Crosswalk",
        "license_type": "Enterprise",
        "seats": 0,
        "start_date": "2025-10-01",
        "renewal_date": "2026-09-30",
        "annual_cost": 94000.0,
        "status": "Active",
        "usage_pct": 0.41,
        "quality_sla": 0.99,
        "feeds_gold_table": "gold_grant_portfolio",
    },
    {
        "subscription_id": "SUB-MOCK-033",
        "vendor_name": "Tidewatch Market Data (Mock)",
        "dataset_name": "Defense Industrial Base Pricing",
        "license_type": "Seat",
        "seats": 10,
        "start_date": "2025-08-15",
        "renewal_date": "2026-08-14",
        "annual_cost": 210000.0,
        "status": "Active",
        "usage_pct": 0.97,
        "quality_sla": 0.98,
        "feeds_gold_table": "gold_financial_execution",
    },
    {
        "subscription_id": "SUB-MOCK-044",
        "vendor_name": "Northwater NewsWire (Mock)",
        "dataset_name": "S&T Open Source Digest",
        "license_type": "Enterprise",
        "seats": 0,
        "start_date": "2025-03-01",
        "renewal_date": "2026-02-28",
        "annual_cost": 62000.0,
        "status": "Lapsed",
        "usage_pct": 0.0,
        "quality_sla": 0.90,
        "feeds_gold_table": "gold_executive_kpis",
    },
    {
        "subscription_id": "SUB-MOCK-055",
        "vendor_name": "BlueRidge ERP Extractor (Mock)",
        "dataset_name": "Command Financial Actuals Feed",
        "license_type": "Connector",
        "seats": 1,
        "start_date": "2026-01-01",
        "renewal_date": "2027-01-01",
        "annual_cost": 48000.0,
        "status": "Active",
        "usage_pct": 0.55,
        "quality_sla": 0.999,
        "feeds_gold_table": "gold_financial_execution",
    },
    {
        "subscription_id": "SUB-MOCK-066",
        "vendor_name": "Keelstone Geospatial (Mock)",
        "dataset_name": "Test Range Environmental Index",
        "license_type": "Seat",
        "seats": 8,
        "start_date": "2026-04-01",
        "renewal_date": "2026-09-01",
        "annual_cost": 73000.0,
        "status": "Active",
        "usage_pct": 0.33,
        "quality_sla": 0.97,
        "feeds_gold_table": "gold_grant_portfolio",
    },
]


def _rng() -> random.Random:
    return random.Random(SEED)


def build_grants(n: int = 36, include_bad_rows: bool = True) -> list[dict[str, Any]]:
    rng = _rng()
    rows: list[dict[str, Any]] = []
    for i in range(1, n + 1):
        start = date(2024, 10, 1) + timedelta(days=rng.randint(0, 400))
        duration_months = rng.choice([12, 18, 24, 36])
        end = start + timedelta(days=30 * duration_months)
        award = float(rng.choice([350_000, 750_000, 1_250_000, 2_400_000, 4_100_000]))
        fy = start.year if start.month >= 10 else start.year
        # Fiscal year of award start (Oct-Sep)
        fiscal_year = start.year + 1 if start.month >= 10 else start.year
        row = {
            "grant_id": f"MOCK-ONR-N00014-{str(fy)[-2:]}-C-{i:04d}",
            "project_name": f"Mock {rng.choice(TECH_AREAS)} Initiative {i:03d}",
            "performing_org": rng.choice(ORGS),
            "onr_code": rng.choice(ONR_CODES),
            "tech_area": rng.choice(TECH_AREAS),
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "award_amount": award,
            "status": STATUSES[i % len(STATUSES)],
            "trl": rng.randint(2, 7),
            "appropriation": rng.choice(APPROPRIATIONS),
            "fiscal_year": fiscal_year,
            "investigator_id": f"INV-{i:04d}",
            "source_system": "legacy_da_portal_mock",
            "classification": "MOCK_UNCLASSIFIED",
        }
        rows.append(row)

    if include_bad_rows:
        # Intentional quality failures for Element 3/4 demo
        rows.append(
            {
                "grant_id": None,
                "project_name": "Malformed row — missing grant id",
                "performing_org": ORGS[0],
                "onr_code": "31",
                "tech_area": "Autonomy",
                "start_date": "2026-01-01",
                "end_date": "2027-01-01",
                "award_amount": 100000.0,
                "status": "Active",
                "trl": 3,
                "appropriation": "6.2",
                "fiscal_year": 2026,
                "investigator_id": "INV-BAD1",
                "source_system": "legacy_da_portal_mock",
                "classification": "MOCK_UNCLASSIFIED",
            }
        )
        rows.append(
            {
                "grant_id": "BAD-ID-0001",
                "project_name": "Malformed row — negative award",
                "performing_org": ORGS[1],
                "onr_code": "32",
                "tech_area": "Materials",
                "start_date": "2026-02-01",
                "end_date": "2026-12-01",
                "award_amount": -25000.0,
                "status": "Pending",
                "trl": 4,
                "appropriation": "6.1",
                "fiscal_year": 2026,
                "investigator_id": "INV-BAD2",
                "source_system": "legacy_da_portal_mock",
                "classification": "MOCK_UNCLASSIFIED",
            }
        )
    return rows


def build_grants_schema_evolution(base_rows: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """Second landing file with two extra columns — Auto Loader addNewColumns demo."""
    rng = _rng()
    extras = []
    for i in range(37, 43):
        extras.append(
            {
                "grant_id": f"MOCK-ONR-N00014-26-C-{i:04d}",
                "project_name": f"Mock Collaboration Pilot {i:03d}",
                "performing_org": rng.choice(ORGS),
                "onr_code": rng.choice(ONR_CODES),
                "tech_area": rng.choice(TECH_AREAS),
                "start_date": "2026-06-01",
                "end_date": "2028-05-31",
                "award_amount": 980000.0,
                "status": "Active",
                "trl": 5,
                "appropriation": "6.2",
                "fiscal_year": 2026,
                "investigator_id": f"INV-{i:04d}",
                "source_system": "legacy_da_portal_mock",
                "classification": "MOCK_UNCLASSIFIED",
                "collaboration_flag": True,
                "international_partner": rng.choice(["NONE", "FVEY_MOCK", "NONE"]),
            }
        )
    return extras


def build_financial(grants: list[dict[str, Any]], include_bad_rows: bool = True) -> list[dict[str, Any]]:
    rng = _rng()
    rows: list[dict[str, Any]] = []
    txn = 1
    for g in grants:
        gid = g.get("grant_id")
        if not gid or not str(gid).startswith("MOCK-ONR-"):
            continue
        award = float(g["award_amount"])
        start = date.fromisoformat(g["start_date"])
        # 8 monthly postings from FY26
        for m in range(8):
            period_date = date(2026, 1, 1) + timedelta(days=30 * m)
            if period_date > TODAY:
                break
            months_into = max(1, (period_date.year - start.year) * 12 + (period_date.month - start.month) + 1)
            # Some grants burn hot, some lag — creates forecast signal
            pace = 0.7 + (hash(gid) % 7) * 0.08
            monthly = (award / 24.0) * pace
            budgeted = award / 12.0
            obligated = monthly * 1.05
            expended = monthly * (0.75 + (m % 3) * 0.08)
            rows.append(
                {
                    "transaction_id": f"ERP-MOCK-2026-{txn:05d}",
                    "grant_id": gid,
                    "fiscal_year": 2026,
                    "period": period_date.strftime("%Y-%m"),
                    "period_date": period_date.isoformat(),
                    "budget_line": rng.choice(BUDGET_LINES),
                    "appropriation": g["appropriation"],
                    "budgeted": round(budgeted, 2),
                    "obligated": round(obligated, 2),
                    "expended": round(expended, 2),
                    "cost_center": rng.choice(COST_CENTERS),
                    "source_system": "mock_erp",
                    "classification": "MOCK_UNCLASSIFIED",
                }
            )
            txn += 1

    if include_bad_rows:
        rows.append(
            {
                "transaction_id": "ERP-MOCK-BAD-00001",
                "grant_id": "MOCK-ONR-N00014-26-C-0001",
                "fiscal_year": 2026,
                "period": "2026-03",
                "period_date": "2026-03-01",
                "budget_line": "Applied Research",
                "appropriation": "6.2",
                "budgeted": 10000.0,
                "obligated": 10000.0,
                "expended": -500.0,
                "cost_center": "ONR-08-FIN",
                "source_system": "mock_erp",
                "classification": "MOCK_UNCLASSIFIED",
            }
        )
    return rows


def build_financial_schema_variant(financial: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """CSV variant with an extra column (program_element) for schema-evolution demo."""
    out = []
    for row in financial[:12]:
        clone = dict(row)
        clone["program_element"] = "0601153N"
        out.append(clone)
    return out


def build_vendors() -> list[dict[str, Any]]:
    rows = []
    for v in VENDORS:
        renewal = date.fromisoformat(v["renewal_date"])
        days = (renewal - TODAY).days
        row = dict(v)
        row["days_to_renewal"] = days
        row["classification"] = "MOCK_UNCLASSIFIED"
        row["source_system"] = "vendor_lifecycle_mock"
        rows.append(row)
    return rows


def build_demo_drop_file() -> dict[str, Any]:
    """Single new grant used during the live Element 3 file-drop."""
    return {
        "grant_id": "MOCK-ONR-N00014-26-C-0901",
        "project_name": "Mock Near-Real-Time Ingest Demonstration",
        "performing_org": "Mock Fleet Experimentation Center",
        "onr_code": "08",
        "tech_area": "Data and Analytics",
        "start_date": "2026-08-01",
        "end_date": "2027-07-31",
        "award_amount": 615000.0,
        "status": "Active",
        "trl": 6,
        "appropriation": "6.3",
        "fiscal_year": 2026,
        "investigator_id": "INV-0901",
        "source_system": "legacy_da_portal_mock",
        "classification": "MOCK_UNCLASSIFIED",
        "collaboration_flag": False,
        "international_partner": "NONE",
        "demo_marker": "ELEMENT_3_LIVE_DROP",
    }


def write_json(path: Path, rows: list[dict[str, Any]] | dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, indent=2), encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row) + "\n")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    seen = set()
    for row in rows:
        for k in row:
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def csv_text(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    fieldnames: list[str] = []
    seen = set()
    for row in rows:
        for k in row:
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)
    buf = StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


def write_all(root: Path) -> dict[str, Path]:
    grants = build_grants()
    evolved = build_grants_schema_evolution()
    financial = build_financial(grants)
    financial_variant = build_financial_schema_variant(financial)
    vendors = build_vendors()
    drop = build_demo_drop_file()

    paths = {
        "grants": root / "grants" / "batch_001.jsonl",
        "grants_evolved": root / "grants" / "batch_002_schema_evolution.jsonl",
        "grants_drop": root / "grants" / "live_drop_element3.json",
        "financial": root / "financial" / "fy26_execution.csv",
        "financial_variant": root / "financial" / "fy26_execution_variant.csv",
        "vendors": root / "vendors" / "subscriptions.json",
    }
    write_jsonl(paths["grants"], grants)
    write_jsonl(paths["grants_evolved"], evolved)
    write_json(paths["grants_drop"], drop)
    write_csv(paths["financial"], financial)
    write_csv(paths["financial_variant"], financial_variant)
    write_json(paths["vendors"], vendors)
    return paths


if __name__ == "__main__":
    here = Path(__file__).resolve().parents[2] / "data" / "mock"
    written = write_all(here)
    print(f"Wrote mock datasets to {here}")
    for name, path in written.items():
        print(f"  {name}: {path}")
