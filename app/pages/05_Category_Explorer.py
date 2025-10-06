import os
import datetime as dt
import pandas as pd
import streamlit as st
from app.utils.db import table_exists, query_df

st.set_page_config(page_title="Category Explorer", layout="wide")

MODE = os.environ.get("MODE", "real")
st.title("Category Explorer")

if MODE != "real":
    st.info("This page uses the real Olist dataset.")
    st.stop()

# ---- Guards ----
need = ["fct_order_items", "fct_orders", "stg_products"]
missing = [t for t in need if not table_exists(t)]
if missing:
    st.warning(f"Missing tables: {', '.join(missing)}. Run `make dbt_build`.")
    st.stop()

# ---- Controls ----
c1, c2, c3, c4 = st.columns([1, 1, 1, 2])
with c1:
    days = st.slider("Lookback window (days)", min_value=30, max_value=365, value=180, step=15)
with c2:
    min_orders = st.number_input("Min delivered orders per category", min_value=5, value=25, step=5)
with c3:
    topn = st.slider("Show Top N by GMV", 5, 50, 20, step=5)
with c4:
    st.caption(
        "Proxy margin = line_net − freight_value (COGS not available). "
        "Velocity = units/day in window. Use filters to focus on meaningful categories."
    )

# ---- Pull data (windowed, delivered only) ----
sql = f"""
WITH windowed AS (
  SELECT
    oi.product_id,
    p.category,
    o.order_date,
    oi.qty,
    oi.line_gross,
    oi.line_net,
    oi.freight_value
  FROM fct_order_items oi
  JOIN fct_orders o USING (order_id)
  LEFT JOIN stg_products p USING (product_id)
  WHERE o.is_delivered
    AND o.order_date >= current_date - INTERVAL {days} DAY
),
by_cat AS (
  SELECT
    lower(coalesce(category, 'unknown'))                 AS category,
    COUNT(*)                                             AS lines,
    SUM(qty)::DOUBLE                                     AS units,
    SUM(line_gross)::DOUBLE                              AS gmv,
    SUM(line_net)::DOUBLE                                AS net_revenue,
    SUM(line_net - freight_value)::DOUBLE                AS proxy_margin,
    SUM(freight_value)::DOUBLE                           AS freight_total,
    AVG(CASE WHEN line_gross > 0 THEN freight_value/line_gross END)::DOUBLE AS avg_freight_pct
  FROM windowed
  GROUP BY 1
),
orders_per_cat AS (
  SELECT
    lower(coalesce(p.category, 'unknown')) AS category,
    COUNT(DISTINCT order_id)               AS orders_delivered
  FROM fct_order_items oi
  JOIN fct_orders o USING (order_id)
  LEFT JOIN stg_products p USING (product_id)
  WHERE o.is_delivered
    AND o.order_date >= current_date - INTERVAL {days} DAY
  GROUP BY 1
)
SELECT
  b.category,
  o.orders_delivered,
  b.lines,
  b.units,
  b.gmv,
  b.net_revenue,
  b.proxy_margin,
  b.freight_total,
  b.avg_freight_pct
FROM by_cat b
LEFT JOIN orders_per_cat o USING (category)
"""
df = query_df(sql)

if df.empty:
    st.info("No delivered orders in the selected window.")
    st.stop()

# ---- Derived metrics ----
df["days"] = days
df["velocity_units_per_day"] = (df["units"] / df["days"]).round(4)
df["margin_pct_of_gmv"] = (df["proxy_margin"] / df["gmv"]).where(df["gmv"] > 0).round(4)
df["avg_order_value"] = (df["gmv"] / df["orders_delivered"]).where(df["orders_delivered"] > 0).round(2)

# Apply minimum orders filter
df = df[(df["orders_delivered"] >= min_orders)].copy()
if df.empty:
    st.warning("No categories meet the minimum delivered orders threshold. Lower it and try again.")
    st.stop()

# Rankers
df["rank_gmv"] = df["gmv"].rank(ascending=False, method="dense")
df["rank_velocity"] = df["velocity_units_per_day"].rank(ascending=False, method="dense")
df["rank_margin_pct"] = df["margin_pct_of_gmv"].rank(ascending=False, method="dense")

# ---- Headline tables ----
left, right = st.columns([2, 1])

with left:
    st.subheader(f"Top {topn} categories by GMV (last {days}d)")
    cols_keep = [
        "category", "orders_delivered", "gmv", "avg_order_value",
        "proxy_margin", "margin_pct_of_gmv", "velocity_units_per_day", "avg_freight_pct",
    ]
    top_df = (
        df.sort_values(["gmv"], ascending=False)
          .head(topn)[cols_keep]
          .rename(columns={
              "gmv": "GMV",
              "proxy_margin": "Proxy Margin",
              "margin_pct_of_gmv": "Margin % of GMV",
              "velocity_units_per_day": "Units / day",
              "avg_freight_pct": "Avg Freight % of line",
          })
    )
    st.dataframe(top_df, use_container_width=True)

with right:
    st.subheader("Quick KPIs")
    st.metric("Categories analysed", f"{len(df):,}")
    st.metric("Delivered orders", f"{int(df['orders_delivered'].sum()):,}")
    st.metric("GMV (total)", f"R$ {df['gmv'].sum():,.0f}")
    st.metric("Proxy Margin (total)", f"R$ {df['proxy_margin'].sum():,.0f}")

# ---- Discontinue / Fix shortlist (exportable) ----
st.subheader("Discontinue / Fix candidates")
st.caption(
    "Heuristic: low velocity AND low margin %. Tweak thresholds below to tune aggressiveness."
)
d1, d2, d3 = st.columns(3)
with d1:
    vel_cut = st.number_input("Velocity ≤ (units/day)", min_value=0.0, value=0.5, step=0.1, format="%.2f")
with d2:
    margin_cut = st.number_input("Margin % ≤", min_value=0.0, max_value=1.0, value=0.05, step=0.01, format="%.2f")
with d3:
    max_rows = st.slider("Max rows", 10, 200, 50, step=10)

cand = (
    df[(df["velocity_units_per_day"] <= vel_cut) & (df["margin_pct_of_gmv"].fillna(0) <= margin_cut)]
    .sort_values(["gmv"], ascending=True)
    [["category", "orders_delivered", "units", "gmv", "proxy_margin", "margin_pct_of_gmv", "velocity_units_per_day"]]
    .head(max_rows)
    .rename(columns={
        "gmv": "GMV",
        "proxy_margin": "Proxy Margin",
        "margin_pct_of_gmv": "Margin % of GMV",
        "velocity_units_per_day": "Units / day",
    })
)

if cand.empty:
    st.success("No categories match the ‘discontinue/fix’ criteria at current thresholds.")
else:
    st.dataframe(cand, use_container_width=True)
    st.download_button(
        "Download discontinue list (CSV)",
        cand.to_csv(index=False).encode("utf-8"),
        file_name=f"discontinue_categories_{days}d.csv",
        mime="text/csv",
    )

# ---- How to read (embedded glossary) ----
with st.expander("How to read this page"):
    st.markdown("""
- **GMV**: Sum of item prices (delivered orders) for the selected window.
- **Proxy Margin**: `line_net − freight_value` aggregated to category. (COGS not available in Olist → proxy only.)
- **Margin % of GMV**: Proxy Margin / GMV (comparative, not absolute).
- **Units / day (Velocity)**: Units sold per day in the window — helps spot slow movers.
- **Avg Freight % of line**: Average `freight_value / line_gross` — high values suggest logistics-heavy categories.
- **Discontinue / Fix**: Low velocity + low margin%. Review pricing, bundling, or catalog hygiene before delisting.
""")
