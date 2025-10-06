import os
import numpy as np
import pandas as pd
import altair as alt
import streamlit as st
from app.utils.db import table_exists, query_df

st.set_page_config(page_title="Freight & Distance", layout="wide")

MODE = os.environ.get("MODE", "real")
st.title("Freight & Distance")

if MODE != "real":
    st.info("This analysis is only available in Real-World (Olist) mode.")
    st.stop()

if not table_exists("fct_freight"):
    st.warning("Expected table `fct_freight` not found. Run `make dbt_build`.")
    st.stop()

# -------- Controls --------
left, right = st.columns([1, 2])
with left:
    level = st.radio(
        "Analysis level",
        ["Line items (fine-grained)", "Orders (aggregated)"],
        index=0,
        help=(
            "Line items: freight% per order line vs point-to-point distance.\n"
            "Orders: total freight / GMV per order vs typical ship distance."
        ),
    )
    max_km = st.slider("Max distance (km) to include", 50, 3000, 1500, step=50)
    max_pct = st.slider("Max freight % to include", 0.10, 2.50, 1.00, step=0.05)
    zcut = st.slider("Outlier threshold (|z|)", 1.5, 5.0, 2.5, step=0.1)
with right:
    st.caption(
        "Tip: set conservative bounds first to fit the base trend, then surface extreme deviations with the z-score threshold."
    )

# -------- Data pull --------
if level.startswith("Line"):
    df = query_df(
        f"""
        SELECT
          order_item_id, order_id, order_date, seller_id, product_id,
          distance_km, freight_pct_line AS freight_pct, line_gross
        FROM fct_freight
        WHERE distance_km IS NOT NULL
          AND distance_km BETWEEN 0 AND {max_km}
          AND freight_pct_line IS NOT NULL
          AND freight_pct_line BETWEEN 0 AND {max_pct}
          AND line_gross > 0
        """
    )
else:
    # Aggregate to order level: use freight% at order level and the median line distance as the order's typical lane
    df = query_df(
        f"""
        WITH per_line AS (
          SELECT order_id, distance_km, order_freight_pct AS freight_pct
          FROM fct_freight
          WHERE distance_km IS NOT NULL
            AND distance_km BETWEEN 0 AND {max_km}
            AND order_freight_pct IS NOT NULL
            AND order_freight_pct BETWEEN 0 AND {max_pct}
        ),
        agg AS (
          SELECT
            order_id,
            median(distance_km) AS distance_km,
            max(freight_pct)    AS freight_pct
          FROM per_line
          GROUP BY 1
        )
        SELECT
          order_id, NULL AS order_item_id, NULL AS seller_id, NULL AS product_id,
          NULL AS order_date, distance_km, freight_pct, NULL AS line_gross
        FROM agg
        """
    )

if df.empty:
    st.info("No rows after filters. Relax the bounds above.")
    st.stop()

# -------- Regression (OLS) --------
# y = freight_pct, x = distance_km
x = df["distance_km"].astype(float).to_numpy()
y = df["freight_pct"].astype(float).to_numpy()

# Guard against degenerate inputs
if np.nanstd(x) == 0 or np.nanstd(y) == 0:
    st.info("Not enough variation in filtered data to fit a regression. Adjust filters.")
    st.stop()

# Fit simple OLS: y = a + b*x
X = np.c_[np.ones_like(x), x]
beta, _, _, _ = np.linalg.lstsq(X, y, rcond=None)  # [a, b]
y_hat = X @ beta
residuals = y - y_hat
r2 = 1.0 - (np.sum((y - y_hat) ** 2) / np.sum((y - np.mean(y)) ** 2))
sigma = np.std(residuals, ddof=2)
z = residuals / (sigma if sigma > 0 else 1.0)

df_model = df.copy()
df_model["y_hat"] = y_hat
df_model["resid"] = residuals
df_model["z_resid"] = z
df_model["is_outlier"] = np.abs(z) > zcut

# -------- KPIs --------
k1, k2, k3, k4 = st.columns(4)
k1.metric("Rows analysed", f"{len(df_model):,}")
k2.metric("R² (fit quality)", f"{r2:.3f}")
k3.metric("Slope (Δ% per km)", f"{beta[1]*100:.4f}")
k4.metric("Outliers flagged", f"{int(df_model['is_outlier'].sum()):,}")

st.caption(
    "Model: OLS of freight% ~ distance(km). Outliers = |standardized residual| > threshold. "
    "Interpretation: positive slope ⇒ freight burden rises with distance; large positive residuals indicate lanes costlier than expected."
)

# -------- Plot --------
base = alt.Chart(df_model).mark_circle(opacity=0.3).encode(
    x=alt.X("distance_km:Q", title="Distance (km)"),
    y=alt.Y("freight_pct:Q", title="Freight as % of line/order value"),
    tooltip=[
        "order_id:N",
        "order_item_id:N",
        alt.Tooltip("seller_id:N", title="seller"),
        alt.Tooltip("product_id:N", title="product"),
        alt.Tooltip("distance_km:Q", format=".1f"),
        alt.Tooltip("freight_pct:Q", format=".3f"),
        alt.Tooltip("z_resid:Q", format=".2f", title="z-residual"),
    ],
)

line = alt.Chart(pd.DataFrame({
    "x": [float(np.nanmin(x)), float(np.nanmax(x))],
    "y": [float(beta[0] + beta[1]*np.nanmin(x)), float(beta[0] + beta[1]*np.nanmax(x))]
})).mark_line().encode(
    x="x:Q", y="y:Q"
)

outliers = alt.Chart(df_model[df_model["is_outlier"]]).mark_circle(size=60).encode(
    x="distance_km:Q",
    y="freight_pct:Q",
    color=alt.value("#d62728"),  # red-ish to pop
    tooltip=[
        "order_id:N",
        "order_item_id:N",
        alt.Tooltip("seller_id:N", title="seller"),
        alt.Tooltip("product_id:N", title="product"),
        alt.Tooltip("distance_km:Q", format=".1f"),
        alt.Tooltip("freight_pct:Q", format=".3f"),
        alt.Tooltip("z_resid:Q", format=".2f", title="z-residual"),
    ],
)

st.subheader("Freight% vs Distance (with fitted trend)")
st.altair_chart((base + line + outliers).interactive(), use_container_width=True)

# -------- Outlier table & download --------
st.subheader("Outlier lanes/items")
top_n = st.slider("Show top-N by |z|", 10, 200, 50, step=10)
df_out = (
    df_model[df_model["is_outlier"]]
    .assign(abs_z=lambda d: d["z_resid"].abs())
    .sort_values("abs_z", ascending=False)
    .head(top_n)[
        ["order_id", "order_item_id", "seller_id", "product_id",
         "distance_km", "freight_pct", "y_hat", "resid", "z_resid"]
    ]
)
if df_out.empty:
    st.info("No outliers at the current threshold.")
else:
    st.dataframe(df_out, use_container_width=True)
    st.download_button(
        "Download outliers as CSV",
        data=df_out.to_csv(index=False).encode("utf-8"),
        file_name="freight_distance_outliers.csv",
        mime="text/csv",
    )

# -------- Insights block (auto-generated) --------
with st.expander("Auto-insights", expanded=True):
    insight_lines = []
    insight_lines.append(f"• The model explains **{r2:.1%}** of variation in freight% across the filtered data.")
    slope_ppk = beta[1] * 100  # percentage points per km
    if slope_ppk > 0:
        insight_lines.append(f"• Freight burden **increases** with distance by ~**{slope_ppk:.4f} p.p. per km**.")
    else:
        insight_lines.append(f"• Freight burden **does not increase** meaningfully with distance (slope: {slope_ppk:.4f} p.p./km).")
    n_out = int(df_model["is_outlier"].sum())
    if n_out > 0:
        worst = df_out.iloc[0] if not df_out.empty else None
        if worst is not None:
            insight_lines.append(
                f"• Highest positive deviation: order `{worst['order_id']}` (z≈{worst['z_resid']:.1f}). "
                f"Investigate seller `{worst['seller_id']}` / product `{worst['product_id']}` or carrier zoning."
            )
    st.markdown("\n".join(insight_lines))
