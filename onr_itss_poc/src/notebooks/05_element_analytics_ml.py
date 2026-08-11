# Databricks notebook source
# MAGIC %md
# MAGIC # Element 5 — Decision-Support Analytics and Modeling
# MAGIC **Key Personnel (Data Scientist) lead.** Live model run against mock data.
# MAGIC
# MAGIC **Action:** Trigger an analytical routine / ML model on the ingested sample.
# MAGIC
# MAGIC **Show**
# MAGIC - Model execution against real (mock) gold/silver rows
# MAGIC - Structured outputs: forecasting, predicted velocity, trend IDs, risk class
# MAGIC - How leadership uses the output (not just a score)
# MAGIC
# MAGIC **Narrate prompt (b)** financial / budgetary analytics.
# MAGIC
# MAGIC IL5 note: this uses **sklearn + MLflow on UC**, not `ai_query` foundation-model functions (not in the IL5 feature matrix). Custom model serving is available if you later publish an endpoint.

# COMMAND ----------

dbutils.widgets.text("catalog", "onr_itss_dev")
dbutils.widgets.text("schema", "da_platform")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier, GradientBoostingRegressor
from sklearn.metrics import accuracy_score, mean_absolute_error
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import LabelEncoder

import sys
from pathlib import Path

for parent in [Path.cwd(), *Path.cwd().parents]:
    if (parent / "src" / "common" / "rules.py").exists():
        sys.path.insert(0, str(parent))
        break

from src.common.rules import trend_id  # noqa: E402

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Feature set from gold financial execution (prompt b)

# COMMAND ----------

src = spark.table(f"{catalog}.{schema}.gold_financial_execution").toPandas()
assert len(src) > 0, "gold_financial_execution is empty — run the medallion pipeline"
print(f"Rows available for modeling: {len(src)}")
display(spark.createDataFrame(src.head(15)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Train two models live
# MAGIC 1. **Classifier** — execution risk class (OVERRUN / AT_RISK / UNDER_EXEC / ON_TRACK)
# MAGIC 2. **Regressor** — months-to-exhaustion (forecast horizon)
# MAGIC
# MAGIC Labels are computed from the same deterministic rules the pipeline uses, so the model learns the command's published policy — it does not invent a black box.

# COMMAND ----------

pdf = src.copy()
pdf["award_amount"] = pd.to_numeric(pdf["award_amount"], errors="coerce")
pdf["expended"] = pd.to_numeric(pdf["expended"], errors="coerce")
pdf["obligated"] = pd.to_numeric(pdf["obligated"], errors="coerce")
pdf["monthly_burn"] = pd.to_numeric(pdf["monthly_burn"], errors="coerce")
pdf["remaining_months"] = pd.to_numeric(pdf["remaining_months"], errors="coerce")
pdf["trl"] = pd.to_numeric(pdf.get("trl", 4), errors="coerce").fillna(4)
pdf = pdf.dropna(subset=["award_amount", "expended", "monthly_burn"])

# Encode appropriation as a simple numeric feature
pdf["approp_ord"] = pdf["appropriation"].astype(str).str.replace("6.", "", regex=False)
pdf["approp_ord"] = pd.to_numeric(pdf["approp_ord"], errors="coerce").fillna(2)

feature_cols = ["award_amount", "expended", "obligated", "monthly_burn", "remaining_months", "trl", "approp_ord"]
X = pdf[feature_cols].fillna(0)

y_class = pdf["risk_class"].fillna("UNKNOWN")
y_reg = pd.to_numeric(pdf["months_to_exhaustion"], errors="coerce").fillna(12.0)

X_train, X_test, yc_train, yc_test, yr_train, yr_test = train_test_split(
    X, y_class, y_reg, test_size=0.3, random_state=20260811
)

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Shared/onr_itss_poc/execution_models_{catalog}")

with mlflow.start_run(run_name="onr-execution-decision-support") as run:
    clf = GradientBoostingClassifier(random_state=20260811)
    reg = GradientBoostingRegressor(random_state=20260811)
    clf.fit(X_train, yc_train)
    reg.fit(X_train, yr_train)

    acc = accuracy_score(yc_test, clf.predict(X_test))
    mae = mean_absolute_error(yr_test, reg.predict(X_test))
    mlflow.log_params({"n_estimators_default": True, "features": ",".join(feature_cols)})
    mlflow.log_metric("risk_accuracy", float(acc))
    mlflow.log_metric("mte_mae_months", float(mae))

    mlflow.sklearn.log_model(
        clf,
        artifact_path="risk_classifier",
        registered_model_name=f"{catalog}.{schema}.onr_execution_risk",
    )
    mlflow.sklearn.log_model(
        reg,
        artifact_path="exhaustion_regressor",
        registered_model_name=f"{catalog}.{schema}.onr_months_to_exhaustion",
    )
    run_id = run.info.run_id

print(f"MLflow run {run_id}  accuracy={acc:.3f}  MAE(months)={mae:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Score the full portfolio — structured leadership output

# COMMAND ----------

pdf["model_risk_class"] = clf.predict(X)
pdf["model_months_to_exhaustion"] = np.clip(reg.predict(X), 0, 60)
pdf["model_predicted_velocity"] = pdf["monthly_burn"]
pdf["model_trend_id"] = [
    trend_id(b, None, r) for b, r in zip(pdf["monthly_burn"], pdf["model_risk_class"])
]
pdf["model_run_id"] = run_id

scored = spark.createDataFrame(
    pdf[
        [
            "grant_id",
            "project_name",
            "onr_code",
            "tech_area",
            "award_amount",
            "expended",
            "projected_total",
            "risk_class",
            "model_risk_class",
            "months_to_exhaustion",
            "model_months_to_exhaustion",
            "model_predicted_velocity",
            "model_trend_id",
            "model_run_id",
        ]
    ]
)

scored.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{catalog}.{schema}.gold_predictive_velocity"
)

spark.sql(
    f"""
    ALTER TABLE {catalog}.{schema}.gold_predictive_velocity SET TAGS (
      'classification' = 'MOCK_UNCLASSIFIED', 'cui' = 'false', 'pii' = 'false', 'element' = '5'
    )
    """
)

display(
    spark.sql(
        f"""
        SELECT grant_id, project_name, onr_code, award_amount,
               risk_class AS rule_risk, model_risk_class,
               ROUND(model_months_to_exhaustion, 1) AS forecast_months,
               ROUND(model_predicted_velocity, 0) AS predicted_velocity,
               model_trend_id
        FROM {catalog}.{schema}.gold_predictive_velocity
        ORDER BY CASE model_risk_class
                   WHEN 'OVERRUN' THEN 1 WHEN 'AT_RISK' THEN 2
                   WHEN 'UNDER_EXEC' THEN 3 ELSE 4 END,
                 award_amount DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.4 What leadership actually does with this
# MAGIC
# MAGIC | Output | Decision |
# MAGIC |---|---|
# MAGIC | `model_risk_class = OVERRUN` | Reprogram or plus-up before the next execution review |
# MAGIC | `UNDER_EXEC` + few `forecast_months` | Pull-forward work or reallocate before year-end lapse |
# MAGIC | `predicted_velocity` vs plan | Budget formulation input for the next FY (prompt b) |
# MAGIC | `model_trend_id` | Stable handle for the weekly battle-rhythm brief |
# MAGIC
# MAGIC The rule-based `risk_class` from the pipeline remains the system of record. The model is a *second opinion* with an MLflow run id for audit. Disagreements are themselves a briefing item.
# MAGIC
# MAGIC **Prompt (d):** scoring is a batch notebook — if the primary workspace is down, the same notebook runs in the failover workspace against the replica catalog. No online feature store is required.
# MAGIC
# MAGIC **Prompt (e):** if a vendor feed is `DATA_GAP`, `gold_financial_execution` freshness drops and the model is *not* silently retrained; `validate_gold` fails closed.

# COMMAND ----------

disagree = spark.sql(
    f"""
    SELECT COUNT(*) AS disagreements
    FROM {catalog}.{schema}.gold_predictive_velocity
    WHERE risk_class <> model_risk_class
    """
)
display(disagree)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Element 5 complete
# MAGIC Next: `06_element_dashboard_automation` (Lakeview + App).
