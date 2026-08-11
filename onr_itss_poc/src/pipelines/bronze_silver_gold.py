"""ONR ITSS POC — Lakeflow Declarative Pipeline (bronze → silver → gold).

Element 3: Auto Loader file detection, quality expectations, schema evolution.
Element 4: Catalog comments/tags, quality metrics, lineage (UC automatic).
Element 5/6: Gold decision-support + anomaly + vendor lifecycle tables.

Streaming tables for incremental landing files. Materialized views for gold.
Kinesis is documented, not required, per demo scope.

Configuration (pipeline settings / DAB):
  onr.catalog, onr.schema  — used to resolve Volume paths
"""

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import functions as F


def _cfg(key: str, default: str) -> str:
    try:
        return spark.conf.get(key, default)  # noqa: F821
    except Exception:
        try:
            return spark.conf.get(key)  # noqa: F821
        except Exception:
            return default


CATALOG = _cfg("onr.catalog", "onr_itss_dev")
SCHEMA = _cfg("onr.schema", "da_platform")
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
SCHEMAS = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/schemas"


def _autoloader(path: str, fmt: str, schema_name: str):
    reader = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", fmt)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{SCHEMAS}/{schema_name}")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    )
    if fmt == "csv":
        reader = reader.option("header", "true")
    # JSON is JSONL (one object per line). Do not set multiLine=true — that treats the
    # whole file as a single document and breaks batch_001.jsonl.
    return (
        reader.load(path)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


# ---------------------------------------------------------------------------
# BRONZE — append-only raw
# ---------------------------------------------------------------------------

@dp.table(
    name="bronze_grants",
    comment="Raw mock S&T grant registry (Auto Loader JSON). MOCK_UNCLASSIFIED. Element 3.",
    table_properties={
        "quality": "bronze",
        "classification": "MOCK_UNCLASSIFIED",
        "cui": "false",
        "pii": "false",
        "pipelines.autoOptimize.managed": "true",
    },
    cluster_by=["grant_id"],
)
@dp.expect("has_source_file", "_source_file IS NOT NULL")
def bronze_grants():
    return _autoloader(f"{LANDING}/grants", "json", "bronze_grants")


@dp.table(
    name="bronze_financial",
    comment="Raw mock financial ERP postings (Auto Loader CSV). MOCK_UNCLASSIFIED. Element 3 / prompt b.",
    table_properties={
        "quality": "bronze",
        "classification": "MOCK_UNCLASSIFIED",
        "cui": "false",
        "pii": "false",
    },
    cluster_by=["grant_id"],
)
@dp.expect("has_source_file", "_source_file IS NOT NULL")
def bronze_financial():
    return _autoloader(f"{LANDING}/financial", "csv", "bronze_financial")


@dp.table(
    name="bronze_vendors",
    comment="Raw mock commercial data subscriptions. MOCK_UNCLASSIFIED. Prompt e.",
    table_properties={
        "quality": "bronze",
        "classification": "MOCK_UNCLASSIFIED",
        "cui": "false",
        "pii": "false",
    },
)
def bronze_vendors():
    return _autoloader(f"{LANDING}/vendors", "json", "bronze_vendors")


# ---------------------------------------------------------------------------
# SILVER — cleansed, typed, quality-enforced
# ---------------------------------------------------------------------------

@dp.table(
    name="silver_grants",
    comment="Cleansed grant registry. Invalid ids/amounts dropped and scored. Element 3/4.",
    table_properties={
        "quality": "silver",
        "classification": "MOCK_UNCLASSIFIED",
        "cui": "false",
        "pii": "false",
    },
    cluster_by=["onr_code", "fiscal_year"],
)
@dp.expect_or_drop("valid_grant_id", "grant_id IS NOT NULL AND grant_id LIKE 'MOCK-ONR-%'")
@dp.expect_or_drop("non_negative_award", "award_amount IS NOT NULL AND award_amount >= 0")
@dp.expect("known_onr_code", "onr_code IN ('31','32','33','34','35','08')")
@dp.expect("valid_trl", "trl IS NULL OR (trl >= 1 AND trl <= 9)")
def silver_grants():
    return (
        spark.readStream.table("bronze_grants")  # noqa: F821
        .withColumn("grant_id", F.upper(F.trim(F.col("grant_id"))))
        .withColumn("award_amount", F.col("award_amount").cast("double"))
        .withColumn("trl", F.col("trl").cast("int"))
        .withColumn("fiscal_year", F.col("fiscal_year").cast("int"))
        .withColumn("start_date", F.to_date("start_date"))
        .withColumn("end_date", F.to_date("end_date"))
        .withColumn("onr_code", F.trim(F.col("onr_code").cast("string")))
        .dropDuplicates(["grant_id"])
    )


@dp.table(
    name="silver_financial",
    comment="Cleansed ERP actuals. Negative expended dropped. Element 3 / prompt b.",
    table_properties={
        "quality": "silver",
        "classification": "MOCK_UNCLASSIFIED",
        "cui": "false",
        "pii": "false",
    },
    cluster_by=["grant_id", "period"],
)
@dp.expect_or_drop("valid_txn", "transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_grant_fk", "grant_id IS NOT NULL AND grant_id LIKE 'MOCK-ONR-%'")
@dp.expect_or_drop("non_negative_expended", "expended IS NULL OR expended >= 0")
@dp.expect("non_negative_budgeted", "budgeted IS NULL OR budgeted >= 0")
@dp.expect("non_negative_obligated", "obligated IS NULL OR obligated >= 0")
def silver_financial():
    return (
        spark.readStream.table("bronze_financial")  # noqa: F821
        .withColumn("grant_id", F.upper(F.trim(F.col("grant_id"))))
        .withColumn("budgeted", F.col("budgeted").cast("double"))
        .withColumn("obligated", F.col("obligated").cast("double"))
        .withColumn("expended", F.col("expended").cast("double"))
        .withColumn("fiscal_year", F.col("fiscal_year").cast("int"))
        .withColumn(
            "period_date",
            F.coalesce(F.to_date("period_date"), F.to_date(F.concat(F.col("period"), F.lit("-01")))),
        )
        .dropDuplicates(["transaction_id"])
    )


@dp.table(
    name="silver_vendors",
    comment="Cleansed vendor / subscription registry with renewal math. Prompt e.",
    table_properties={
        "quality": "silver",
        "classification": "MOCK_UNCLASSIFIED",
        "cui": "false",
        "pii": "false",
    },
)
@dp.expect_or_drop("valid_subscription", "subscription_id IS NOT NULL")
@dp.expect("has_vendor_name", "vendor_name IS NOT NULL")
def silver_vendors():
    return (
        spark.readStream.table("bronze_vendors")  # noqa: F821
        .withColumn("renewal_date", F.to_date("renewal_date"))
        .withColumn("start_date", F.to_date("start_date"))
        .withColumn("annual_cost", F.col("annual_cost").cast("double"))
        .withColumn("usage_pct", F.col("usage_pct").cast("double"))
        .withColumn("days_to_renewal", F.datediff(F.col("renewal_date"), F.current_date()))
        .withColumn(
            "gap_status",
            F.when(F.upper(F.col("status")).isin("LAPSED", "EXPIRED", "CANCELLED", "CANCELED"), "DATA_GAP")
            .when(F.col("days_to_renewal") < 0, "DATA_GAP")
            .when(F.col("days_to_renewal") <= 30, "RENEWAL_DUE")
            .when(F.col("usage_pct") > 0.95, "LICENSE_PRESSURE")
            .when(F.upper(F.col("status")) == "ACTIVE", "HEALTHY")
            .otherwise("WATCH"),
        )
        .dropDuplicates(["subscription_id"])
    )


# ---------------------------------------------------------------------------
# GOLD — decision support, automation, vendor health
# ---------------------------------------------------------------------------

@dp.materialized_view(
    name="gold_grant_portfolio",
    comment="Portfolio rollup by code / tech area / FY. Leadership view. Element 5/6.",
    table_properties={
        "quality": "gold",
        "classification": "MOCK_UNCLASSIFIED",
        "cui": "false",
        "pii": "false",
    },
)
def gold_grant_portfolio():
    return (
        spark.read.table("silver_grants")  # noqa: F821
        .groupBy("fiscal_year", "onr_code", "tech_area", "appropriation", "status")
        .agg(
            F.count("*").alias("grant_count"),
            F.sum("award_amount").alias("total_awarded"),
            F.avg("trl").alias("avg_trl"),
        )
    )


@dp.materialized_view(
    name="gold_financial_execution",
    comment="Budget vs obligated vs expended by grant — financial execution tracking (prompt b).",
    table_properties={
        "quality": "gold",
        "classification": "MOCK_UNCLASSIFIED",
        "cui": "false",
        "pii": "false",
    },
)
def gold_financial_execution():
    fin = spark.read.table("silver_financial")  # noqa: F821
    grants = spark.read.table("silver_grants")  # noqa: F821
    agg = (
        fin.groupBy("grant_id", "fiscal_year")
        .agg(
            F.sum("budgeted").alias("budgeted"),
            F.sum("obligated").alias("obligated"),
            F.sum("expended").alias("expended"),
            F.count("*").alias("txn_count"),
            F.min("period_date").alias("first_period"),
            F.max("period_date").alias("last_period"),
        )
        .withColumn(
            "months_elapsed",
            F.greatest(
                F.lit(1.0),
                F.months_between(F.col("last_period"), F.col("first_period")) + F.lit(1.0),
            ),
        )
        .withColumn("monthly_burn", F.col("expended") / F.col("months_elapsed"))
    )
    return (
        agg.join(grants, "grant_id", "left")
        .withColumn(
            "remaining_months",
            F.greatest(F.lit(0.0), F.months_between(F.col("end_date"), F.current_date())),
        )
        .withColumn("projected_remaining", F.col("monthly_burn") * F.col("remaining_months"))
        .withColumn("projected_total", F.col("expended") + F.col("projected_remaining"))
        .withColumn(
            "months_to_exhaustion",
            F.when(F.col("monthly_burn") <= 0, F.lit(None).cast("double")).otherwise(
                (F.col("award_amount") - F.col("expended")) / F.col("monthly_burn")
            ),
        )
        .withColumn(
            "risk_class",
            F.when(F.col("award_amount").isNull() | (F.col("award_amount") <= 0), "UNKNOWN")
            .when(F.col("projected_total") / F.col("award_amount") > 1.05, "OVERRUN")
            .when(
                (F.col("remaining_months") < 3)
                & (F.col("projected_total") / F.col("award_amount") < 0.80),
                "UNDER_EXEC",
            )
            .when(F.col("projected_total") / F.col("award_amount") >= 0.95, "AT_RISK")
            .otherwise("ON_TRACK"),
        )
        .withColumn(
            "trend_id",
            F.concat(F.lit("TRD-"), F.col("risk_class"), F.lit("-BURN")),
        )
        .withColumn("predicted_velocity", F.col("monthly_burn"))
        .withColumn("as_of_ts", F.current_timestamp())
        .select(
            "grant_id",
            "project_name",
            "onr_code",
            "tech_area",
            "appropriation",
            "fiscal_year",
            "status",
            "award_amount",
            "budgeted",
            "obligated",
            "expended",
            "monthly_burn",
            "remaining_months",
            "projected_remaining",
            "projected_total",
            "months_to_exhaustion",
            "risk_class",
            "trend_id",
            "predicted_velocity",
            "txn_count",
            "last_period",
            "as_of_ts",
        )
    )


@dp.materialized_view(
    name="gold_anomalies",
    comment="Automated anomaly flags for leadership review / approval routing. Element 6.",
    table_properties={"quality": "gold", "classification": "MOCK_UNCLASSIFIED", "cui": "false", "pii": "false"},
)
def gold_anomalies():
    execu = spark.read.table("gold_financial_execution")  # noqa: F821
    overrun = (
        execu.filter(F.col("risk_class").isin("OVERRUN", "AT_RISK", "UNDER_EXEC"))
        .select(
            F.col("grant_id"),
            F.lit("FINANCIAL_EXECUTION").alias("anomaly_type"),
            F.col("risk_class").alias("severity"),
            F.concat(
                F.lit("Grant "),
                F.col("grant_id"),
                F.lit(" classified "),
                F.col("risk_class"),
                F.lit(" — projected "),
                F.round(F.col("projected_total"), 0).cast("string"),
                F.lit(" vs award "),
                F.round(F.col("award_amount"), 0).cast("string"),
            ).alias("description"),
            F.col("as_of_ts").alias("detected_ts"),
        )
    )
    vendors = (
        spark.read.table("silver_vendors")  # noqa: F821
        .filter(F.col("gap_status").isin("DATA_GAP", "RENEWAL_DUE", "LICENSE_PRESSURE"))
        .select(
            F.col("subscription_id").alias("grant_id"),
            F.lit("VENDOR_LIFECYCLE").alias("anomaly_type"),
            F.col("gap_status").alias("severity"),
            F.concat(
                F.col("vendor_name"),
                F.lit(" / "),
                F.col("dataset_name"),
                F.lit(" status="),
                F.col("gap_status"),
                F.lit(" renewal_in_days="),
                F.coalesce(F.col("days_to_renewal").cast("string"), F.lit("n/a")),
            ).alias("description"),
            F.current_timestamp().alias("detected_ts"),
        )
    )
    return overrun.unionByName(vendors)


@dp.materialized_view(
    name="gold_approval_queue",
    comment="Automated approval routing queue for flagged execution and vendor gaps. Element 6.",
    table_properties={"quality": "gold", "classification": "MOCK_UNCLASSIFIED", "cui": "false", "pii": "false"},
)
def gold_approval_queue():
    return (
        spark.read.table("gold_anomalies")  # noqa: F821
        .withColumn(
            "route_to",
            F.when(F.col("anomaly_type") == "VENDOR_LIFECYCLE", "data_vendor_manager").otherwise(
                "financial_execution_lead"
            ),
        )
        .withColumn("status", F.lit("PENDING"))
        .withColumn("sla_hours", F.when(F.col("severity") == "DATA_GAP", F.lit(8)).otherwise(F.lit(48)))
        .withColumn("opened_ts", F.col("detected_ts"))
        .select("grant_id", "anomaly_type", "severity", "description", "route_to", "status", "sla_hours", "opened_ts")
    )


@dp.materialized_view(
    name="gold_vendor_lifecycle",
    comment="Commercial subscription tracking, license pressure, and dashboard-gap risk. Prompt e.",
    table_properties={"quality": "gold", "classification": "MOCK_UNCLASSIFIED", "cui": "false", "pii": "false"},
)
def gold_vendor_lifecycle():
    return spark.read.table("silver_vendors").withColumn("as_of_ts", F.current_timestamp())  # noqa: F821


@dp.materialized_view(
    name="gold_data_quality_scores",
    comment="Catalog health scores per dataset (completeness / validity / freshness). Element 4.",
    table_properties={"quality": "gold", "classification": "MOCK_UNCLASSIFIED", "cui": "false", "pii": "false"},
)
def gold_data_quality_scores():
    def score(name: str, table: str, key_col: str, ts_col: str):
        df = spark.read.table(table)  # noqa: F821
        return df.agg(
            F.lit(name).alias("dataset"),
            F.count(F.lit(1)).alias("row_count"),
            (F.sum(F.when(F.col(key_col).isNull(), 1).otherwise(0)) / F.count(F.lit(1))).alias("null_rate"),
            (
                (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.max(F.col(ts_col)))) / 3600.0
            ).alias("freshness_hours"),
        ).select(
            "dataset",
            "row_count",
            "null_rate",
            "freshness_hours",
            (
                (F.lit(1.0) - F.col("null_rate")) * 40.0
                + F.lit(35.0)
                + F.when(F.col("freshness_hours") <= 24, F.lit(20.0)).otherwise(F.lit(10.0))
                + F.lit(5.0)
            ).alias("health_score"),
        ).withColumn(
            "health_band",
            F.when(F.col("health_score") >= 90, "HEALTHY")
            .when(F.col("health_score") >= 75, "WATCH")
            .otherwise("DEGRADED"),
        )

    parts = [
        score("bronze_grants", "bronze_grants", "grant_id", "_ingest_ts"),
        score("bronze_financial", "bronze_financial", "grant_id", "_ingest_ts"),
        score("silver_grants", "silver_grants", "grant_id", "_ingest_ts"),
        score("silver_financial", "silver_financial", "transaction_id", "_ingest_ts"),
        score("silver_vendors", "silver_vendors", "subscription_id", "_ingest_ts"),
        score("gold_financial_execution", "gold_financial_execution", "grant_id", "as_of_ts"),
    ]
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out.withColumn("computed_ts", F.current_timestamp())


@dp.materialized_view(
    name="gold_executive_kpis",
    comment="Single-row-per-dimension executive KPI set for Lakeview + App. Element 6 / prompt b.",
    table_properties={"quality": "gold", "classification": "MOCK_UNCLASSIFIED", "cui": "false", "pii": "false"},
)
def gold_executive_kpis():
    fin = spark.read.table("gold_financial_execution")  # noqa: F821
    vendors = spark.read.table("gold_vendor_lifecycle")  # noqa: F821
    kpis = fin.agg(
        F.count("*").alias("grant_count"),
        F.sum("award_amount").alias("total_awarded"),
        F.sum("expended").alias("total_expended"),
        F.sum("obligated").alias("total_obligated"),
        F.sum(F.when(F.col("risk_class") == "OVERRUN", 1).otherwise(0)).alias("overrun_count"),
        F.sum(F.when(F.col("risk_class") == "AT_RISK", 1).otherwise(0)).alias("at_risk_count"),
        F.sum(F.when(F.col("risk_class") == "UNDER_EXEC", 1).otherwise(0)).alias("under_exec_count"),
        F.sum(F.when(F.col("risk_class") == "ON_TRACK", 1).otherwise(0)).alias("on_track_count"),
        F.avg("predicted_velocity").alias("avg_monthly_velocity"),
    ).withColumn("execution_rate", F.col("total_expended") / F.col("total_awarded"))
    v = vendors.agg(
        F.sum(F.when(F.col("gap_status") == "DATA_GAP", 1).otherwise(0)).alias("vendor_data_gaps"),
        F.sum(F.when(F.col("gap_status") == "RENEWAL_DUE", 1).otherwise(0)).alias("vendor_renewals_due"),
        F.sum("annual_cost").alias("vendor_annual_cost"),
    )
    return kpis.crossJoin(v).withColumn("as_of_ts", F.current_timestamp()).withColumn(
        "classification", F.lit("MOCK_UNCLASSIFIED")
    )


@dp.materialized_view(
    name="gold_executive_summary",
    comment="Deterministic automated narrative for non-technical leaders. Element 6. No external LLM.",
    table_properties={"quality": "gold", "classification": "MOCK_UNCLASSIFIED", "cui": "false", "pii": "false"},
)
def gold_executive_summary():
    k = spark.read.table("gold_executive_kpis")  # noqa: F821
    return k.select(
        F.current_timestamp().alias("generated_ts"),
        F.concat(
            F.lit("FY26 mock portfolio: "),
            F.col("grant_count").cast("string"),
            F.lit(" awards totaling $"),
            F.format_number(F.col("total_awarded"), 0),
            F.lit(". Execution rate "),
            F.format_number(F.col("execution_rate") * 100, 1),
            F.lit("%. Risk: "),
            F.col("overrun_count").cast("string"),
            F.lit(" overrun, "),
            F.col("at_risk_count").cast("string"),
            F.lit(" at-risk, "),
            F.col("under_exec_count").cast("string"),
            F.lit(" under-executing, "),
            F.col("on_track_count").cast("string"),
            F.lit(" on-track. Vendor lifecycle: "),
            F.col("vendor_data_gaps").cast("string"),
            F.lit(" data-gap(s), "),
            F.col("vendor_renewals_due").cast("string"),
            F.lit(" renewal(s) due. Average monthly velocity $"),
            F.format_number(F.col("avg_monthly_velocity"), 0),
            F.lit("."),
        ).alias("summary_text"),
        F.lit("DETERMINISTIC_TEMPLATE").alias("generator"),
        F.lit("MOCK_UNCLASSIFIED").alias("classification"),
    )
