"""ONR executive App — Element 6. Search, filter, extract. Mock data only."""

from __future__ import annotations

import io
import os
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk.core import Config

CATALOG = os.environ.get("ONR_CATALOG", "onr_itss_poc")
SCHEMA = os.environ.get("ONR_SCHEMA", "da_platform")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")


def fqn(table: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{table}"


@st.cache_resource
def _conn():
    cfg = Config()
    return sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        credentials_provider=lambda: cfg.authenticate,
    )


def query(sql_text: str) -> pd.DataFrame:
    with _conn().cursor() as cur:
        cur.execute(sql_text)
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=cols)


st.set_page_config(page_title="ONR Executive D&A", layout="wide")
st.title("ONR Code 08 — Executive Data & Analytics")
st.caption("Mock unclassified. Search / filter / extract — no notebook required.")

try:
    summary = query(f"SELECT summary_text, generated_ts FROM {fqn('gold_executive_summary')} LIMIT 1")
    if not summary.empty:
        st.info(summary.iloc[0]["summary_text"])
except Exception as exc:
    st.warning(f"Run the pipeline first: {exc}")

try:
    kpis = query(f"SELECT * FROM {fqn('gold_executive_kpis')} LIMIT 1")
except Exception as exc:
    st.error(f"Cannot read KPIs: {exc}")
    st.stop()
if kpis.empty:
    st.error("gold_executive_kpis is empty.")
    st.stop()

row = kpis.iloc[0]
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Awards", int(row["grant_count"]))
c2.metric("Awarded", f"${row['total_awarded']:,.0f}")
c3.metric("Execution", f"{float(row['execution_rate']) * 100:.1f}%")
c4.metric("Overruns", int(row["overrun_count"]))
c5.metric("Vendor gaps", int(row["vendor_data_gaps"]))

st.sidebar.header("Filter")
search = st.sidebar.text_input("Search project or grant id")
try:
    codes = query(f"SELECT DISTINCT onr_code FROM {fqn('gold_financial_execution')} ORDER BY onr_code")
    code_opts = ["(all)"] + [str(x) for x in codes["onr_code"].dropna().tolist()]
except Exception:
    code_opts = ["(all)"]
code = st.sidebar.selectbox("ONR code", code_opts)
risk = st.sidebar.selectbox("Risk", ["(all)", "OVERRUN", "AT_RISK", "UNDER_EXEC", "ON_TRACK"])

where = ["1=1"]
if search:
    s = search.replace("'", "''").lower()
    where.append(f"(lower(project_name) LIKE '%{s}%' OR lower(grant_id) LIKE '%{s}%')")
if code != "(all)":
    where.append(f"onr_code = '{code}'")
if risk != "(all)":
    where.append(f"risk_class = '{risk}'")
wh = " AND ".join(where)

tab_p, tab_a, tab_v, tab_e = st.tabs(["Portfolio", "Anomalies", "Vendors", "Extract"])

with tab_p:
    port = query(
        f"""SELECT grant_id, project_name, onr_code, tech_area, award_amount, expended,
                   projected_total, risk_class, trend_id
            FROM {fqn('gold_financial_execution')} WHERE {wh} ORDER BY award_amount DESC"""
    )
    st.dataframe(port, use_container_width=True, hide_index=True)
    st.caption(f"{len(port)} awards")

with tab_a:
    st.caption("Automated flags with a route_to owner (Element 6 process automation).")
    st.dataframe(
        query(f"SELECT grant_id, anomaly_type, severity, description, route_to, detected_ts FROM {fqn('gold_anomalies')}"),
        use_container_width=True,
        hide_index=True,
    )

with tab_v:
    vend = query(
        f"""SELECT subscription_id, vendor_name, dataset_name, status, gap_status, days_to_renewal
            FROM {fqn('gold_vendors')}
            ORDER BY CASE gap_status WHEN 'DATA_GAP' THEN 1 WHEN 'RENEWAL_DUE' THEN 2 ELSE 3 END"""
    )
    st.dataframe(vend, use_container_width=True, hide_index=True)
    if not vend.empty and (vend["gap_status"] == "DATA_GAP").any():
        st.error("A lapsed subscription is creating a dashboard data gap.")

with tab_e:
    ext = query(
        f"""SELECT grant_id, project_name, onr_code, award_amount, expended, projected_total, risk_class, trend_id
            FROM {fqn('gold_financial_execution')} WHERE {wh}"""
    )
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    st.download_button("CSV", ext.to_csv(index=False).encode(), f"onr_extract_{stamp}.csv", "text/csv")
    st.download_button("JSON", ext.to_json(orient="records", indent=2).encode(), f"onr_extract_{stamp}.json", "application/json")
    try:
        buf = io.BytesIO()
        ext.to_parquet(buf, index=False)
        st.download_button("Parquet", buf.getvalue(), f"onr_extract_{stamp}.parquet", "application/octet-stream")
    except Exception:
        pass
