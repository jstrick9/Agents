"""ONR ITSS POC — executive Databricks App (Element 6).

Non-technical leaders search, filter, extract, review anomalies, and route
approvals. No cluster, no notebook. Mock unclassified data only.

Auth: Databricks Apps injects the app service principal. Warehouse and table
grants are declared in resources/apps.yml.
"""

from __future__ import annotations

import io
import os
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

CATALOG = os.environ.get("ONR_CATALOG", "workspace")
SCHEMA = os.environ.get("ONR_SCHEMA", "default")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")


def _fqn(table: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{table}"


@st.cache_resource
def _connection():
    cfg = Config()
    http_path = f"/sql/1.0/warehouses/{WAREHOUSE_ID}"
    return sql.connect(
        server_hostname=cfg.host,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
    )


def query(statement: str) -> pd.DataFrame:
    with _connection().cursor() as cur:
        cur.execute(statement)
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=cols)


st.set_page_config(page_title="ONR Executive D&A", layout="wide")
st.title("ONR Code 08 — Executive Data & Analytics")
st.caption(
    "Mock unclassified demonstration. No CUI / PII. Search, filter, and extract "
    "without opening a notebook."
)

# ---------------------------------------------------------------------------
# Automated summary banner
# ---------------------------------------------------------------------------
try:
    summary = query(f"SELECT summary_text, generated_ts FROM {_fqn('gold_executive_summary')} LIMIT 1")
    if not summary.empty:
        st.info(summary.iloc[0]["summary_text"])
        st.caption(f"Automated summary generated {summary.iloc[0]['generated_ts']}")
except Exception as exc:
    st.warning(f"Summary unavailable (run the medallion pipeline): {exc}")

# ---------------------------------------------------------------------------
# KPI strip
# ---------------------------------------------------------------------------
try:
    kpis = query(f"SELECT * FROM {_fqn('gold_executive_kpis')} LIMIT 1")
except Exception as exc:
    st.error(f"Cannot read gold_executive_kpis: {exc}")
    st.stop()

if kpis.empty:
    st.error("gold_executive_kpis is empty. Run the medallion pipeline.")
    st.stop()

row = kpis.iloc[0]
c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Awards", f"{int(row['grant_count'])}")
c2.metric("Awarded", f"${row['total_awarded']:,.0f}")
c3.metric("Execution", f"{float(row['execution_rate']) * 100:.1f}%")
c4.metric("Overruns", int(row["overrun_count"]))
c5.metric("At risk", int(row["at_risk_count"]))
c6.metric("Vendor gaps", int(row["vendor_data_gaps"]))

# ---------------------------------------------------------------------------
# Sidebar filters (non-technical)
# ---------------------------------------------------------------------------
st.sidebar.header("Filter portfolio")
search = st.sidebar.text_input("Search project or grant id")
try:
    codes = query(f"SELECT DISTINCT onr_code FROM {_fqn('gold_financial_execution')} ORDER BY onr_code")
    code_opts = ["(all)"] + [str(x) for x in codes["onr_code"].dropna().tolist()]
except Exception:
    code_opts = ["(all)"]
code = st.sidebar.selectbox("ONR code", code_opts)
risk = st.sidebar.selectbox(
    "Risk class", ["(all)", "OVERRUN", "AT_RISK", "UNDER_EXEC", "ON_TRACK", "UNKNOWN"]
)
tech = st.sidebar.text_input("Tech area contains")

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_port, tab_anom, tab_appr, tab_vend, tab_qual, tab_extract = st.tabs(
    ["Portfolio", "Anomalies", "Approvals", "Vendors", "Data health", "Extract"]
)

where = ["1=1"]
if search:
    safe = search.replace("'", "''")
    where.append(f"(lower(project_name) LIKE '%{safe.lower()}%' OR lower(grant_id) LIKE '%{safe.lower()}%')")
if code != "(all)":
    where.append(f"onr_code = '{code}'")
if risk != "(all)":
    where.append(f"risk_class = '{risk}'")
if tech:
    safe = tech.replace("'", "''")
    where.append(f"lower(tech_area) LIKE '%{safe.lower()}%'")
where_sql = " AND ".join(where)

with tab_port:
    st.subheader("Financial execution")
    port = query(
        f"""
        SELECT grant_id, project_name, onr_code, tech_area, award_amount, expended,
               projected_total, risk_class, trend_id, months_to_exhaustion
        FROM {_fqn('gold_financial_execution')}
        WHERE {where_sql}
        ORDER BY award_amount DESC
        """
    )
    st.dataframe(port, use_container_width=True, hide_index=True)
    st.caption(f"{len(port)} awards after filter")

with tab_anom:
    st.subheader("Automated anomaly flags")
    anoms = query(
        f"""
        SELECT grant_id, anomaly_type, severity, description, detected_ts
        FROM {_fqn('gold_anomalies')}
        ORDER BY detected_ts DESC
        """
    )
    st.dataframe(anoms, use_container_width=True, hide_index=True)

with tab_appr:
    st.subheader("Approval routing queue")
    st.caption("SLA-backed routes. Status updates in this POC are a demo action — production would MERGE to a Delta CDC table.")
    queue = query(
        f"""
        SELECT grant_id, anomaly_type, severity, description, route_to, status, sla_hours, opened_ts
        FROM {_fqn('gold_approval_queue')}
        ORDER BY opened_ts DESC
        """
    )
    st.dataframe(queue, use_container_width=True, hide_index=True)
    if not queue.empty:
        pick = st.selectbox("Select item", queue["grant_id"].astype(str).tolist())
        decision = st.radio("Decision", ["Acknowledge", "Escalate", "Close"])
        if st.button("Record decision (demo)"):
            st.success(f"{decision} recorded locally for {pick}. Production writes an audited MERGE.")

with tab_vend:
    st.subheader("Commercial data subscriptions")
    vend = query(
        f"""
        SELECT subscription_id, vendor_name, dataset_name, status, gap_status,
               days_to_renewal, usage_pct, annual_cost, feeds_gold_table
        FROM {_fqn('gold_vendor_lifecycle')}
        ORDER BY CASE gap_status
                   WHEN 'DATA_GAP' THEN 1 WHEN 'RENEWAL_DUE' THEN 2
                   WHEN 'LICENSE_PRESSURE' THEN 3 ELSE 4 END
        """
    )
    st.dataframe(vend, use_container_width=True, hide_index=True)
    gaps = vend[vend["gap_status"] == "DATA_GAP"] if not vend.empty else vend
    if not gaps.empty:
        st.error(
            f"{len(gaps)} subscription(s) create a dashboard data gap. "
            "Renew or swap the feed before the next pipeline run."
        )

with tab_qual:
    st.subheader("Catalog health scores")
    qual = query(
        f"""
        SELECT dataset, row_count, null_rate, freshness_hours, health_score, health_band, computed_ts
        FROM {_fqn('gold_data_quality_scores')}
        ORDER BY dataset
        """
    )
    st.dataframe(qual, use_container_width=True, hide_index=True)

with tab_extract:
    st.subheader("Secure extract (CSV / JSON)")
    st.caption("Downloads stay in the authenticated App session. Bulk Parquet/CSV also land on the export Volume via notebook 07.")
    extract_df = query(
        f"""
        SELECT grant_id, project_name, onr_code, tech_area, award_amount, expended,
               projected_total, risk_class, trend_id
        FROM {_fqn('gold_financial_execution')}
        WHERE {where_sql}
        """
    )
    st.write(f"{len(extract_df)} filtered rows")
    csv_buf = extract_df.to_csv(index=False).encode("utf-8")
    json_buf = extract_df.to_json(orient="records", indent=2).encode("utf-8")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    st.download_button("Download CSV", data=csv_buf, file_name=f"onr_extract_{stamp}.csv", mime="text/csv")
    st.download_button("Download JSON", data=json_buf, file_name=f"onr_extract_{stamp}.json", mime="application/json")
    # Parquet via in-memory buffer
    pq = io.BytesIO()
    try:
        extract_df.to_parquet(pq, index=False)
        st.download_button(
            "Download Parquet",
            data=pq.getvalue(),
            file_name=f"onr_extract_{stamp}.parquet",
            mime="application/octet-stream",
        )
    except Exception:
        st.caption("Parquet download requires pyarrow in the App env; CSV/JSON always work.")
