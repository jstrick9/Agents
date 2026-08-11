# Databricks notebook source
# MAGIC %md
# MAGIC # Element 4 — Data Governance, Quality, and Cataloging
# MAGIC **Key Personnel lead.** Navigate the Unity Catalog registry — not a slide.
# MAGIC
# MAGIC **Show**
# MAGIC 1. How ingested datasets are registered / cataloged.
# MAGIC 2. Metadata (source, schema, lineage tags, ownership).
# MAGIC 3. Data quality / health scores.
# MAGIC 4. End-to-end lineage from landing → bronze → silver → gold → visualization.
# MAGIC
# MAGIC **Narrate prompt (e)** vendor metadata, **(c)** least-privilege, **(a)** legacy cataloging.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "default")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Catalog / registry navigation
# MAGIC This is the platform data-management registry (Unity Catalog). Open Catalog Explorer in another tab to show the same objects graphically.

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {catalog}.{schema}"))
display(spark.sql(f"SHOW VOLUMES IN {catalog}.{schema}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Metadata capture — comments, tags, columns, owner

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT table_name, comment, table_owner, created, last_altered
        FROM {catalog}.information_schema.tables
        WHERE table_schema = '{schema}'
        ORDER BY table_name
        """
    )
)

display(
    spark.sql(
        f"""
        SELECT table_name, column_name, full_data_type, comment
        FROM {catalog}.information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name IN ('silver_grants','gold_financial_execution','gold_vendor_lifecycle')
        ORDER BY table_name, ordinal_position
        """
    )
)

# COMMAND ----------

# Tags (classification, cui, pii, quality layer)
display(
    spark.sql(
        f"""
        SELECT catalog_name, schema_name, table_name, tag_name, tag_value
        FROM {catalog}.information_schema.table_tags
        WHERE schema_name = '{schema}'
        ORDER BY table_name, tag_name
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Apply / refresh portable tags so Catalog Explorer shows classification at a glance.

# COMMAND ----------

for tbl in [
    "bronze_grants",
    "silver_grants",
    "gold_financial_execution",
    "gold_executive_kpis",
    "gold_vendor_lifecycle",
    "gold_data_quality_scores",
]:
    fqn = f"{catalog}.{schema}.{tbl}"
    if spark.catalog.tableExists(fqn):
        spark.sql(
            f"""
            ALTER TABLE {fqn} SET TAGS (
              'classification' = 'MOCK_UNCLASSIFIED',
              'cui' = 'false',
              'pii' = 'false',
              'domain' = 'onr_snt_mock'
            )
            """
        )
print("Tags applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Data quality / health scores
# MAGIC `gold_data_quality_scores` is computed in the pipeline from null rates + freshness.
# MAGIC Pipeline **Expectations** tab is the live quality dashboard for drop/fail counts.

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{schema}.gold_data_quality_scores"):
    display(spark.table(f"{catalog}.{schema}.gold_data_quality_scores").orderBy("dataset"))
else:
    print("Run the medallion pipeline to populate gold_data_quality_scores.")

# COMMAND ----------

# MAGIC %md
# MAGIC Vendor quality is first-class catalog metadata (prompt e). A lapsed subscription is a *data gap*, not just a contract event.

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{schema}.gold_vendor_lifecycle"):
    display(
        spark.sql(
            f"""
            SELECT subscription_id, vendor_name, dataset_name, status, gap_status,
                   days_to_renewal, usage_pct, feeds_gold_table
            FROM {catalog}.{schema}.gold_vendor_lifecycle
            ORDER BY CASE gap_status
                       WHEN 'DATA_GAP' THEN 1
                       WHEN 'RENEWAL_DUE' THEN 2
                       WHEN 'LICENSE_PRESSURE' THEN 3
                       ELSE 4 END
            """
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 End-to-end lineage (visual + queryable)
# MAGIC Unity Catalog records table and column lineage automatically when the Lakeflow pipeline and Lakeview/App queries run.
# MAGIC
# MAGIC **In the UI:** Catalog Explorer → table → **Lineage** tab. Walk `landing` volume → `bronze_*` → `silver_*` → `gold_*` → dashboard / app.
# MAGIC
# MAGIC **In SQL:** `system.access.table_lineage` and `system.access.column_lineage` (available on GovCloud).

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT source_table_full_name, target_table_full_name, event_time, created_by
        FROM system.access.table_lineage
        WHERE target_table_full_name LIKE '{catalog}.{schema}.%'
           OR source_table_full_name LIKE '{catalog}.{schema}.%'
        ORDER BY event_time DESC
        LIMIT 50
        """
    )
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT source_table_full_name, source_column_name,
               target_table_full_name, target_column_name, event_time
        FROM system.access.column_lineage
        WHERE target_table_full_name LIKE '{catalog}.{schema}.gold_%'
        ORDER BY event_time DESC
        LIMIT 40
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.5 Least-privilege view of the catalog (prompt c)
# MAGIC Grants are group-based. Executives get `SELECT` on gold only — not bronze landing, not WRITE VOLUME.

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT privilege_type, grantee, table_name
        FROM {catalog}.information_schema.table_privileges
        WHERE table_schema = '{schema}'
        ORDER BY table_name, grantee
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lineage map the presenter draws
# MAGIC ```
# MAGIC Volume landing/grants/*.jsonl  ──► bronze_grants  ──► silver_grants ──┐
# MAGIC Volume landing/financial/*.csv ──► bronze_financial ► silver_financial ┼► gold_financial_execution
# MAGIC Volume landing/vendors/*.jsonl ──► bronze_vendors  ► silver_vendors  ──► gold_vendor_lifecycle
# MAGIC                                                                      ├► gold_anomalies / gold_approval_queue
# MAGIC                                                                      ├► gold_data_quality_scores
# MAGIC                                                                      └► gold_executive_kpis → Lakeview + App
# MAGIC ```
# MAGIC
# MAGIC **Prompt (a):** legacy portal tables are registered as *external* UC tables (read-only) during transition so they appear in the same catalog and lineage graph without moving bytes on day one.
# MAGIC
# MAGIC ### Element 4 complete
# MAGIC Next: `05_element_analytics_ml`.
