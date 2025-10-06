import os
import time
from pathlib import Path

import streamlit as st


from app.utils.glossary import KPI_TOOLTIPS
from app.utils.db import get_con, table_exists, query_df, ensure_demo_db

# ensure a tiny demo DB exists when running in the cloud
ensure_demo_db()

APP_TITLE = "Nomad Commerce Analytics"
MODE = st.secrets.get("MODE", os.environ.get("MODE", "real"))
DUCKDB_PATH = st.secrets.get("DUCKDB_PATH", os.environ.get("DUCKDB_PATH", "warehouse/nomad.duckdb"))

st.set_page_config(page_title=APP_TITLE, layout="wide")

# ---- Header / status ----
with st.sidebar:
    st.markdown(f"### {APP_TITLE}")
    st.caption("DuckDB + dbt + Streamlit")
    st.write(f"**Mode:** `{MODE}`")
    st.write(f"**DB:** `{DUCKDB_PATH}`")
    run_checks = st.checkbox("Run quick health checks", value=True)
    if st.button("Refresh"):
        st.rerun()

st.title(APP_TITLE)
st.write("Use the left sidebar to switch pages. This home view shows a quick health/status summary.")

# ---- Health checks ----
def health() -> dict:
    checks = {}
    checks["db_file_present"] = Path(DUCKDB_PATH).exists()
    ks = ["mrt_kpis_daily_real", "mrt_kpis_daily_synth", "fct_orders", "fct_order_items"]
    checks["known_tables"] = {k: table_exists(k) for k in ks}
    return checks

if run_checks:
    with st.expander("Health checks", expanded=True):
        h = health()
        st.write(f"DB file present: **{h['db_file_present']}**")
        st.write("Known tables:")
        st.json(h["known_tables"])

# ---- KPI snapshot (mode-aware) ----
def render_kpis():
    table = "mrt_kpis_daily_real" if MODE == "real" else "mrt_kpis_daily_synth"
    if not table_exists(table):
        st.info(f"Waiting for dbt build… Expected table `{table}` not found yet.")
        return
    df = query_df(f"""
        SELECT *
        FROM {table}
        WHERE kpi_date >= current_date - INTERVAL 90 DAY
        ORDER BY kpi_date
    """)
    if df.empty:
        st.info("KPI table is empty.")
        return

    last = df.iloc[-1].to_dict()
    cols = st.columns(5)
    def tile(col, label, value, tooltip_key=None, fmt="{:,.2f}"):
        with col:
            if tooltip_key and tooltip_key in KPI_TOOLTIPS:
                st.caption(f"{label}  ⓘ")
                st.caption(KPI_TOOLTIPS[tooltip_key])
            else:
                st.caption(label)
            if isinstance(value, (int, float)):
                st.metric(label="", value=fmt.format(value))
            else:
                st.metric(label="", value=value)

    # Tiles differ slightly by mode
    if MODE == "real":
        tile(cols[0], "Orders (last day)", last.get("orders_delivered", 0), None, "{:,.0f}")
        tile(cols[1], "GMV", last.get("gmv", 0.0), "GMV", "R$ {:,.0f}")
        tile(cols[2], "AOV", last.get("aov", 0.0), "AOV", "R$ {:,.0f}")
        tile(cols[3], "On-time %", 100 * last.get("on_time_pct", 0.0), "On-time %", "{:,.1f}%")
        tile(cols[4], "Freight % GMV", 100 * last.get("freight_pct_gmv", 0.0), "Freight % GMV", "{:,.1f}%")
    else:
        # Synth mode has a richer KPI set; render a basic subset for now
        tile(cols[0], "GMV", last.get("gmv", 0.0), "GMV", "£ {:,.0f}")
        tile(cols[1], "Net Revenue", last.get("net_revenue", 0.0), "Net Revenue", "£ {:,.0f}")
        tile(cols[2], "AOV", last.get("aov", 0.0), "AOV", "£ {:,.0f}")
        tile(cols[3], "CAC", last.get("cac", 0.0), "CAC", "£ {:,.0f}")
        tile(cols[4], "LTV (90d)", last.get("ltv_90d", 0.0), "LTV", "£ {:,.0f}")

    with st.expander("KPI trend (last 90 days)", expanded=True):
        st.line_chart(df.set_index("kpi_date")[[c for c in df.columns if c not in ("kpi_date",)]])

render_kpis()

st.caption("Tip: run `make ingest dbt_build app` after placing Olist CSVs in `data/real/`.")
