import os
import yaml
import streamlit as st
from app.utils.db import table_exists, query_df
from app.utils.glossary import KPI_TOOLTIPS

st.set_page_config(page_title="Overview", layout="wide")

MODE = os.environ.get("MODE", "real")
CFG_PATH = "config/config.yaml"

def load_thresholds():
    try:
        with open(CFG_PATH, "r") as f:
            y = yaml.safe_load(f) or {}
        return y.get("thresholds", {})
    except Exception:
        return {}

TH = load_thresholds()
ONTIME_WARN = TH.get("ontime_warn_pct", 85)
REVIEW_WARN = TH.get("review_warn_score", 3.8)
FREIGHT_WARN = TH.get("freight_pct_warn", 0.20)

table = "mrt_kpis_daily_real" if MODE == "real" else "mrt_kpis_daily_synth"

st.title("Overview")

if not table_exists(table):
    st.warning(f"Expected table `{table}` not found. Run `make ingest dbt_build` and reload.")
    st.stop()

df = query_df(f"""
    SELECT * FROM {table}
    WHERE kpi_date >= current_date - INTERVAL 180 DAY
    ORDER BY kpi_date
""")

if df.empty:
    st.info("No KPI rows available.")
    st.stop()

last = df.iloc[-1].to_dict()

cols = st.columns(5)
def tile(col, label, value, tip_key=None, fmt="{:,.2f}", delta=None):
    with col:
        if tip_key and tip_key in KPI_TOOLTIPS:
            st.caption(f"{label} — {KPI_TOOLTIPS[tip_key]}")
        else:
            st.caption(label)
        if isinstance(value, (int, float)):
            st.metric(label="", value=fmt.format(value), delta=delta)
        else:
            st.metric(label="", value=value, delta=delta)

if MODE == "real":
    tile(cols[0], "Orders (last day)", last.get("orders_delivered", 0), None, "{:,.0f}")
    tile(cols[1], "GMV", last.get("gmv", 0.0), "GMV", "R$ {:,.0f}")
    tile(cols[2], "AOV", last.get("aov", 0.0), "AOV", "R$ {:,.0f}")
    tile(cols[3], "On-time %", 100 * last.get("on_time_pct", 0.0), "On-time %", "{:,.1f}%")
    tile(cols[4], "Freight % GMV", 100 * last.get("freight_pct_gmv", 0.0), "Freight % GMV", "{:,.1f}%")
else:
    tile(cols[0], "GMV", last.get("gmv", 0.0), "GMV", "£ {:,.0f}")
    tile(cols[1], "Net Revenue", last.get("net_revenue", 0.0), "Net Revenue", "£ {:,.0f}")
    tile(cols[2], "AOV", last.get("aov", 0.0), "AOV", "£ {:,.0f}")
    tile(cols[3], "CAC", last.get("cac", 0.0), "CAC", "£ {:,.0f}")
    tile(cols[4], "LTV (90d)", last.get("ltv_90d", 0.0), "LTV", "£ {:,.0f}")

with st.expander("Trend (last 180 days)", expanded=True):
    keep = [c for c in df.columns if c not in ("kpi_date",)]
    st.line_chart(df.set_index("kpi_date")[keep])

# ---- Simple alerts (real mode) ----
if MODE == "real":
    alerts = []
    if last.get("on_time_pct") is not None and last["on_time_pct"] * 100 < ONTIME_WARN:
        alerts.append(f"On-time % below {ONTIME_WARN}%")
    if last.get("freight_pct_gmv") is not None and last["freight_pct_gmv"] > FREIGHT_WARN:
        alerts.append(f"Freight % GMV above {int(FREIGHT_WARN*100)}%")
    # Pull latest daily review score from mrt_reviews if exists
    if table_exists("mrt_reviews"):
        rev = query_df("SELECT * FROM mrt_reviews ORDER BY as_of_date").tail(1)
        if not rev.empty and float(rev["avg_score"].iloc[0]) < REVIEW_WARN:
            alerts.append(f"Avg review score below {REVIEW_WARN}")
    if alerts:
        st.error(" • " + " | ".join(alerts))
    else:
        st.success("All KPI thresholds look healthy today.")
