"""ONR ITSS POC — one Lakeflow pipeline (bronze → silver → gold)."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


def _cfg(key: str, default: str) -> str:
    try:
        return spark.conf.get(key, default)  # noqa: F821
    except Exception:
        return default


CATALOG = _cfg("onr.catalog", "onr_itss_poc")
SCHEMA = _cfg("onr.schema", "da_platform")
LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
SCHEMAS = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/schemas"


def _cloud_files(path: str, fmt: str, name: str):
    r = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", fmt)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{SCHEMAS}/{name}")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    )
    if fmt == "csv":
        r = r.option("header", "true")
    return (
        r.load(path)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(name="bronze_grants", comment="Raw mock S&T grants. Element 3.")
def bronze_grants():
    return _cloud_files(f"{LANDING}/grants", "json", "bronze_grants")


@dp.table(name="bronze_financial", comment="Raw mock ERP. Element 3.")
def bronze_financial():
    return _cloud_files(f"{LANDING}/financial", "csv", "bronze_financial")


@dp.table(name="bronze_vendors", comment="Raw mock subscriptions. Prompt e.")
def bronze_vendors():
    return _cloud_files(f"{LANDING}/vendors", "json", "bronze_vendors")


@dp.table(name="silver_grants", comment="Cleansed grants. Bad ids/amounts dropped.")
@dp.expect_or_drop("valid_grant_id", "grant_id IS NOT NULL AND grant_id LIKE 'MOCK-ONR-%'")
@dp.expect_or_drop("non_negative_award", "award_amount IS NOT NULL AND award_amount >= 0")
def silver_grants():
    return (
        spark.readStream.table("bronze_grants")  # noqa: F821
        .withColumn("grant_id", F.upper(F.trim("grant_id")))
        .withColumn("award_amount", F.col("award_amount").cast("double"))
        .withColumn("start_date", F.to_date("start_date"))
        .withColumn("end_date", F.to_date("end_date"))
        .dropDuplicates(["grant_id"])
    )


@dp.table(name="silver_financial", comment="Cleansed ERP. Negative expended dropped.")
@dp.expect_or_drop("valid_txn", "transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_grant", "grant_id LIKE 'MOCK-ONR-%'")
@dp.expect_or_drop("non_negative_expended", "expended IS NULL OR expended >= 0")
def silver_financial():
    return (
        spark.readStream.table("bronze_financial")  # noqa: F821
        .withColumn("grant_id", F.upper(F.trim("grant_id")))
        .withColumn("expended", F.col("expended").cast("double"))
        .withColumn("obligated", F.col("obligated").cast("double"))
        .withColumn("budgeted", F.col("budgeted").cast("double"))
        .withColumn("period_date", F.coalesce(F.to_date("period_date"), F.to_date(F.concat(F.col("period"), F.lit("-01")))))
        .dropDuplicates(["transaction_id"])
    )


@dp.table(name="silver_vendors", comment="Cleansed subscriptions.")
@dp.expect_or_drop("valid_sub", "subscription_id IS NOT NULL")
def silver_vendors():
    return (
        spark.readStream.table("bronze_vendors")  # noqa: F821
        .withColumn("renewal_date", F.to_date("renewal_date"))
        .withColumn("days_to_renewal", F.datediff(F.col("renewal_date"), F.current_date()))
        .withColumn(
            "gap_status",
            F.when(F.upper(F.col("status")).isin("LAPSED", "EXPIRED"), "DATA_GAP")
            .when(F.col("days_to_renewal") < 0, "DATA_GAP")
            .when(F.col("days_to_renewal") <= 30, "RENEWAL_DUE")
            .otherwise("HEALTHY"),
        )
        .dropDuplicates(["subscription_id"])
    )


@dp.materialized_view(name="gold_financial_execution", comment="Budget vs spend + risk. Elements 5/6.")
def gold_financial_execution():
    fin = spark.read.table("silver_financial")  # noqa: F821
    grants = spark.read.table("silver_grants")  # noqa: F821
    agg = fin.groupBy("grant_id").agg(
        F.sum("budgeted").alias("budgeted"),
        F.sum("obligated").alias("obligated"),
        F.sum("expended").alias("expended"),
        F.min("period_date").alias("first_period"),
        F.max("period_date").alias("last_period"),
    ).withColumn(
        "months_elapsed",
        F.greatest(F.lit(1.0), F.months_between("last_period", "first_period") + F.lit(1.0)),
    ).withColumn("monthly_burn", F.col("expended") / F.col("months_elapsed"))
    return (
        agg.join(grants, "grant_id", "left")
        .withColumn("remaining_months", F.greatest(F.lit(0.0), F.months_between("end_date", F.current_date())))
        .withColumn("projected_total", F.col("expended") + F.col("monthly_burn") * F.col("remaining_months"))
        .withColumn(
            "risk_class",
            F.when(F.col("award_amount").isNull() | (F.col("award_amount") <= 0), "UNKNOWN")
            .when(F.col("projected_total") / F.col("award_amount") > 1.05, "OVERRUN")
            .when((F.col("remaining_months") < 3) & (F.col("projected_total") / F.col("award_amount") < 0.80), "UNDER_EXEC")
            .when(F.col("projected_total") / F.col("award_amount") >= 0.95, "AT_RISK")
            .otherwise("ON_TRACK"),
        )
        .withColumn("trend_id", F.concat(F.lit("TRD-"), F.col("risk_class"), F.lit("-BURN")))
        .withColumn("predicted_velocity", F.col("monthly_burn"))
        .withColumn("as_of_ts", F.current_timestamp())
        .select(
            "grant_id", "project_name", "onr_code", "tech_area", "award_amount",
            "budgeted", "obligated", "expended", "monthly_burn", "remaining_months",
            "projected_total", "risk_class", "trend_id", "predicted_velocity", "as_of_ts",
        )
    )


@dp.materialized_view(name="gold_anomalies", comment="Flags + routing. Element 6.")
def gold_anomalies():
    fin = (
        spark.read.table("gold_financial_execution")  # noqa: F821
        .filter(F.col("risk_class").isin("OVERRUN", "AT_RISK", "UNDER_EXEC"))
        .select(
            F.col("grant_id"),
            F.lit("FINANCIAL").alias("anomaly_type"),
            F.col("risk_class").alias("severity"),
            F.concat(F.lit("Grant "), F.col("grant_id"), F.lit(" is "), F.col("risk_class")).alias("description"),
            F.lit("financial_execution_lead").alias("route_to"),
            F.current_timestamp().alias("detected_ts"),
        )
    )
    vend = (
        spark.read.table("silver_vendors")  # noqa: F821
        .filter(F.col("gap_status").isin("DATA_GAP", "RENEWAL_DUE"))
        .select(
            F.col("subscription_id").alias("grant_id"),
            F.lit("VENDOR").alias("anomaly_type"),
            F.col("gap_status").alias("severity"),
            F.concat(F.col("vendor_name"), F.lit(" "), F.col("gap_status")).alias("description"),
            F.lit("data_vendor_manager").alias("route_to"),
            F.current_timestamp().alias("detected_ts"),
        )
    )
    return fin.unionByName(vend)


@dp.materialized_view(name="gold_vendors", comment="Subscription health. Prompt e.")
def gold_vendors():
    return spark.read.table("silver_vendors").withColumn("as_of_ts", F.current_timestamp())  # noqa: F821


@dp.materialized_view(name="gold_data_quality", comment="Health scores. Element 4.")
def gold_data_quality():
    def score(name, table, key):
        return spark.read.table(table).agg(  # noqa: F821
            F.lit(name).alias("dataset"),
            F.count(F.lit(1)).alias("row_count"),
            (F.sum(F.when(F.col(key).isNull(), 1).otherwise(0)) / F.count(F.lit(1))).alias("null_rate"),
        ).withColumn("health_score", (F.lit(1.0) - F.col("null_rate")) * 100).withColumn(
            "health_band", F.when(F.col("health_score") >= 90, "HEALTHY").otherwise("WATCH")
        )

    parts = [
        score("bronze_grants", "bronze_grants", "grant_id"),
        score("silver_grants", "silver_grants", "grant_id"),
        score("silver_financial", "silver_financial", "transaction_id"),
        score("gold_financial_execution", "gold_financial_execution", "grant_id"),
    ]
    out = parts[0]
    for p in parts[1:]:
        out = out.unionByName(p)
    return out.withColumn("computed_ts", F.current_timestamp())


@dp.materialized_view(name="gold_executive_kpis", comment="KPI strip. Element 6.")
def gold_executive_kpis():
    fin = spark.read.table("gold_financial_execution")  # noqa: F821
    vend = spark.read.table("gold_vendors")  # noqa: F821
    k = fin.agg(
        F.count("*").alias("grant_count"),
        F.sum("award_amount").alias("total_awarded"),
        F.sum("expended").alias("total_expended"),
        F.sum(F.when(F.col("risk_class") == "OVERRUN", 1).otherwise(0)).alias("overrun_count"),
        F.sum(F.when(F.col("risk_class") == "AT_RISK", 1).otherwise(0)).alias("at_risk_count"),
        F.avg("predicted_velocity").alias("avg_monthly_velocity"),
    ).withColumn("execution_rate", F.col("total_expended") / F.col("total_awarded"))
    v = vend.agg(F.sum(F.when(F.col("gap_status") == "DATA_GAP", 1).otherwise(0)).alias("vendor_data_gaps"))
    return k.crossJoin(v).withColumn("as_of_ts", F.current_timestamp())


@dp.materialized_view(name="gold_executive_summary", comment="Automated narrative. Element 6.")
def gold_executive_summary():
    return spark.read.table("gold_executive_kpis").select(  # noqa: F821
        F.current_timestamp().alias("generated_ts"),
        F.concat(
            F.lit("Mock portfolio: "), F.col("grant_count").cast("string"),
            F.lit(" awards, $"), F.format_number("total_awarded", 0),
            F.lit(" awarded, "), F.format_number(F.col("execution_rate") * 100, 1),
            F.lit("% executed. Overruns: "), F.col("overrun_count").cast("string"),
            F.lit(". Vendor data-gaps: "), F.col("vendor_data_gaps").cast("string"), F.lit("."),
        ).alias("summary_text"),
    )
