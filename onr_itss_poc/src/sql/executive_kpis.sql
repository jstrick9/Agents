-- ONR Executive KPI queries for Lakeview / DBSQL (Element 6).
-- Replace ${catalog} / ${schema} or use the warehouse default catalog.

-- KPI strip
SELECT
  grant_count,
  total_awarded,
  total_expended,
  total_obligated,
  execution_rate,
  overrun_count,
  at_risk_count,
  under_exec_count,
  on_track_count,
  avg_monthly_velocity,
  vendor_data_gaps,
  vendor_renewals_due,
  vendor_annual_cost,
  as_of_ts
FROM IDENTIFIER(:catalog || '.' || :schema || '.gold_executive_kpis');

-- Execution by ONR code
SELECT
  onr_code,
  COUNT(*) AS grants,
  SUM(award_amount) AS awarded,
  SUM(expended) AS expended,
  SUM(CASE WHEN risk_class = 'OVERRUN' THEN 1 ELSE 0 END) AS overruns,
  AVG(predicted_velocity) AS avg_velocity
FROM IDENTIFIER(:catalog || '.' || :schema || '.gold_financial_execution')
GROUP BY onr_code
ORDER BY awarded DESC;

-- Risk-ranked awards
SELECT
  grant_id,
  project_name,
  onr_code,
  tech_area,
  award_amount,
  expended,
  projected_total,
  risk_class,
  trend_id,
  months_to_exhaustion
FROM IDENTIFIER(:catalog || '.' || :schema || '.gold_financial_execution')
ORDER BY
  CASE risk_class
    WHEN 'OVERRUN' THEN 1
    WHEN 'AT_RISK' THEN 2
    WHEN 'UNDER_EXEC' THEN 3
    ELSE 4
  END,
  award_amount DESC;

-- Automated narrative
SELECT generated_ts, summary_text, generator
FROM IDENTIFIER(:catalog || '.' || :schema || '.gold_executive_summary');

-- Vendor lifecycle (prompt e)
SELECT
  subscription_id,
  vendor_name,
  dataset_name,
  status,
  gap_status,
  days_to_renewal,
  usage_pct,
  annual_cost,
  feeds_gold_table
FROM IDENTIFIER(:catalog || '.' || :schema || '.gold_vendor_lifecycle')
ORDER BY
  CASE gap_status
    WHEN 'DATA_GAP' THEN 1
    WHEN 'RENEWAL_DUE' THEN 2
    WHEN 'LICENSE_PRESSURE' THEN 3
    ELSE 4
  END,
  days_to_renewal;

-- Data health (Element 4)
SELECT dataset, row_count, null_rate, freshness_hours, health_score, health_band, computed_ts
FROM IDENTIFIER(:catalog || '.' || :schema || '.gold_data_quality_scores')
ORDER BY dataset;

-- Approval queue (Element 6 automation)
SELECT grant_id, anomaly_type, severity, description, route_to, status, sla_hours, opened_ts
FROM IDENTIFIER(:catalog || '.' || :schema || '.gold_approval_queue')
WHERE status = 'PENDING'
ORDER BY opened_ts DESC;
