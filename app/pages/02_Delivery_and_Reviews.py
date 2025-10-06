import os
import numpy as np
import streamlit as st
from app.utils.db import table_exists, query_df

st.set_page_config(page_title="Delivery & Reviews", layout="wide")

MODE = os.environ.get("MODE", "real")
st.title("Delivery & Reviews")

if MODE != "real":
    st.info("This page is available in Real-World (Olist) mode.")
    st.stop()

# ---------- Lead-time distribution ----------
if not table_exists("fct_deliveries"):
    st.warning("Expected table `fct_deliveries` not found. Run `make dbt_build`.")
    st.stop()

lead_df = query_df("""
    SELECT lead_time_days::INTEGER AS lead_time_days
    FROM fct_deliveries
    WHERE lead_time_days IS NOT NULL AND lead_time_days BETWEEN 0 AND 60
""")

left, right = st.columns([1,1])
with left:
    st.subheader("Lead-time distribution (days)")
    if lead_df.empty:
        st.info("No delivered orders found.")
    else:
        st.bar_chart(lead_df["lead_time_days"].value_counts().sort_index())

with right:
    st.subheader("On-time vs Late split")
    split = query_df("""
        SELECT
            SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END)::DOUBLE / COUNT(*) AS on_time_pct,
            SUM(CASE WHEN is_late THEN 1 ELSE 0 END)::DOUBLE  / COUNT(*) AS late_pct
        FROM fct_deliveries
        WHERE is_on_time IS NOT NULL
    """)
    if not split.empty:
        st.metric("On-time %", f"{split['on_time_pct'].iloc[0]*100:.1f}%")
        st.metric("Late %", f"{split['late_pct'].iloc[0]*100:.1f}%")

# ---------- Reviews impact of lateness ----------
if not table_exists("stg_reviews"):
    st.info("Reviews table not present.")
else:
    st.subheader("Impact of lateness on review score")
    reviews = query_df("""
        SELECT r.score, d.is_late
        FROM stg_reviews r
        JOIN fct_deliveries d USING (order_id)
        WHERE r.score IS NOT NULL
    """)
    if reviews.empty:
        st.info("No reviews joined to deliveries.")
    else:
        late_scores = reviews.loc[reviews["is_late"] == True, "score"].astype(float)
        ontime_scores = reviews.loc[reviews["is_late"] == False, "score"].astype(float)
        late_avg = float(late_scores.mean()) if not late_scores.empty else np.nan
        ontime_avg = float(ontime_scores.mean()) if not ontime_scores.empty else np.nan
        delta = ontime_avg - late_avg if np.isfinite(ontime_avg) and np.isfinite(late_avg) else np.nan

        c1, c2, c3 = st.columns(3)
        c1.metric("Avg score (on-time)", f"{ontime_avg:.2f}")
        c2.metric("Avg score (late)", f"{late_avg:.2f}")
        c3.metric("Delay penalty (Δ)", f"{delta:.2f}")

        with st.expander("Score distributions"):
            st.bar_chart(reviews["score"].value_counts().sort_index())

        st.caption("Definitions: Late if delivered date > estimated date; Delay penalty = on-time avg − late avg (positive means late deliveries reduce scores).")
