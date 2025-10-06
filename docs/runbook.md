# Runbook — Nomad Commerce Analytics

Concise fixes for the most common issues when running locally or in CI.

---

## 1) First-time setup

```bash
make setup                # venv + deps + pre-commit
cp .env.example .env      # optional; defaults are OK

Folder checklist

    data/real/ → put the 8 Olist CSVs (exact filenames).

    warehouse/ → created on first run.

    dbt/ → profiles.yml already points at DUCKDB_PATH.

2) Ingest & build

make ingest MODE=real     # load CSVs → DuckDB
make quality              # fail fast if data contracts break
make dbt_build            # dbt models + tests
make app                  # Streamlit UI

Sanity queries (DuckDB CLI):

SELECT COUNT(*) FROM fct_orders;
SELECT * FROM mrt_kpis_daily_real ORDER BY kpi_date DESC LIMIT 5;

3) Common failures & fixes
A) FileNotFoundError: data/real/olist_*.csv

    Fix: Put the eight Olist CSVs in data/real/ with exact names.

    Tip: See README “Dataset” section.

B) quality_checks.py fails (contracts)

    Symptom: output lists violations (e.g., negative freight, NULL keys).

    Fix: Verify your CSVs; some Kaggle mirrors change headers/dtypes. Use the official Olist CSVs.

    Dev bypass (not recommended): narrow the check in scripts/quality_checks.py for exploration only.

C) dbt build errors

    profiles.yml not found: ensure DBT_PROFILES_DIR=./dbt in .env.

    Adapter missing: pip install -e .[dev] again to install dbt-duckdb.

    Model/test failures: run dbt ls to confirm model paths. Re-run make quality to catch raw data issues.

D) Streamlit shows “Expected table not found”

    Cause: app started before dbt_build.

    Fix: make dbt_build then refresh the app.

E) Windows / WSL quirks

    Use WSL2 for a smoother Python/dbt experience.

    Replace source .venv/bin/activate with the platform equivalent if running commands manually.

4) CI failures (GitHub Actions)

    Pre-commit: run pre-commit run --all-files locally; commit fixes.

    Ingest step: CI uses a tiny generated Olist subset; make sure workflow still creates all 8 CSVs.

    dbt tests: run locally with dbt build --vars 'mode: real' and inspect target/ logs.

    pytest: run pytest -q; check tests/e2e/test_end_to_end.py hints.

5) Performance

    Full build target: ≤ 3 minutes on a typical laptop.

    If slow:

        Ensure DuckDB is using multiple threads (default 4 in profiles.yml).

        Close other processes; DuckDB is in-process and shares CPU.

6) Troubleshooting commands

# Recreate DB from scratch
rm -f warehouse/nomad.duckdb && make ingest MODE=real && make dbt_build

# Inspect schema
duckdb warehouse/nomad.duckdb -c "PRAGMA show_tables;"

# Preview a model
duckdb warehouse/nomad.duckdb -c "SELECT * FROM fct_deliveries LIMIT 20;"

7) Support matrix

    Python 3.10+

    dbt-core 1.7 + dbt-duckdb 1.7

    DuckDB 0.9+

    Streamlit 1.25+


---

## 2) `app/pages/04_Payments.py`
```python
import os
import numpy as np
import pandas as pd
import streamlit as st
from app.utils.db import table_exists, query_df

st.set_page_config(page_title="Payments", layout="wide")

MODE = os.environ.get("MODE", "real")
st.title("Payments Analysis")

if MODE != "real":
    st.info("This page uses the real Olist dataset.")
    st.stop()

need = ["stg_payments", "fct_orders"]
missing = [t for t in need if not table_exists(t)]
if missing:
    st.warning(f"Missing tables: {', '.join(missing)}. Run `make dbt_build`.")
    st.stop()

# ---- Controls ----
c1, c2 = st.columns([1, 3])
with c1:
    days = st.slider("Lookback window (days)", min_value=30, max_value=365, value=180, step=15)
    min_orders = st.number_input("Min orders per payment method", min_value=5, value=25, step=5)

with c2:
    st.caption(
        "Explore installment behavior and payment methods. "
        "Olist allows split tenders; we summarize at order level for comparability."
    )

# ---- Pull order-level aggregates with payment rollups ----
sql = f"""
WITH pay AS (
  SELECT
    order_id,
    max(installments)                          AS installments,
    list_agg(DISTINCT method)                  AS methods_list
  FROM stg_payments
  GROUP BY 1
),
orders AS (
  SELECT
    o.order_id,
    o.order_date,
    o.status,
    o.is_delivered,
    o.is_canceled,
    o.gmv,
    o.payment_total
  FROM fct_orders o
  WHERE o.order_date >= current_date - INTERVAL {days} DAY
)
SELECT
  o.order_id,
  o.order_date,
  o.status,
  o.is_delivered,
  o.is_canceled,
  o.gmv,
  o.payment_total,
  p.installments,
  p.methods_list
FROM orders o
LEFT JOIN pay p USING (order_id)
"""
df = query_df(sql)
if df.empty:
    st.info("No orders in window.")
    st.stop()

# Derive a primary method label (first token) for simple grouping
def first_method(s: str) -> str:
    if not s:
        return "unknown"
    # list_agg may serialize like '["credit_card","voucher"]' or 'credit_card,voucher'
    s = str(s).strip()
    if s.startswith("["):
        try:
            import json
            a = json.loads(s)
            return (a[0] if a else "unknown") or "unknown"
        except Exception:
            pass
    return s.split(",")[0].strip()

df["primary_method"] = df["methods_list"].apply(first_method).str.lower().fillna("unknown")

# ---- KPI tiles ----
k1, k2, k3, k4 = st.columns(4)
k1.metric("Orders analysed", f"{len(df):,}")
k2.metric("Avg installments", f"{df['installments'].fillna(1).mean():.2f}")
k3.metric("Delivered %", f"{100*df['is_delivered'].mean():.1f}%")
k4.metric("Cancel %", f"{100*df['is_canceled'].mean():.1f}%")

# ---- Distribution of installments ----
st.subheader("Installments distribution")
inst = (
    df.assign(installments=df["installments"].fillna(1).clip(lower=1))
      .groupby("installments", as_index=False)
      .size()
      .rename(columns={"size": "orders"})
      .sort_values("installments")
)
if inst.empty:
    st.info("No installments data.")
else:
    st.bar_chart(inst.set_index("installments"))

# ---- AOV by payment method ----
st.subheader("Order value by payment method")
aov = (
    df.groupby("primary_method", as_index=False)
      .agg(orders=("order_id","count"), aov=("gmv","mean"))
      .query("orders >= @min_orders")
      .sort_values("aov", ascending=False)
)
if aov.empty:
    st.info("No methods meet the minimum orders threshold.")
else:
    st.dataframe(aov, use_container_width=True)

# ---- Cancellation proxy vs installments ----
st.subheader("Cancellation rate vs installments")
cancel = (
    df.assign(installments=df["installments"].fillna(1).clip(lower=1))
      .groupby("installments", as_index=False)
      .agg(
          orders=("order_id","count"),
          cancel_rate=("is_canceled","mean")
      )
      .sort_values("installments")
)
if cancel.empty:
    st.info("No cancellation data.")
else:
    cancel["cancel_rate_pct"] = (100 * cancel["cancel_rate"]).round(2)
    st.line_chart(cancel.set_index("installments")[["cancel_rate_pct"]])

# ---- Insights ----
with st.expander("Auto-insights", expanded=True):
    lines = []
    # Compare high vs low installments AOV
    low = df[df["installments"].fillna(1) <= 2]["gmv"].mean()
    high = df[df["installments"].fillna(1) >= 6]["gmv"].mean()
    if np.isfinite(low) and np.isfinite(high):
        delta = high - low
        lines.append(f"• Orders with ≥6 installments have an average GMV **{delta:+.2f}** higher than ≤2 installments.")
    # Cancellation monotonicity hint
    c_first = cancel["cancel_rate"].iloc[0] if not cancel.empty else np.nan
    c_last = cancel["cancel_rate"].iloc[-1] if not cancel.empty else np.nan
    if np.isfinite(c_first) and np.isfinite(c_last):
        trend = "higher" if c_last > c_first else "lower"
        lines.append(f"• Cancellation rate at the highest installment count is **{trend}** than at the lowest.")
    if not lines:
        lines.append("• Not enough variation to infer patterns in this window.")
    st.markdown("\n".join(lines))

# ---- How to read ----
with st.expander("How to read this page"):
    st.markdown("""
- **Installments**: maximum installments on any payment tied to the order.
- **Primary method**: first method observed when multiple tenders exist (simplification).
- **AOV by method**: compares average order value across payment methods (only methods with enough volume).
- **Cancellation vs installments**: monitors whether high-installment orders cancel more often.
""")