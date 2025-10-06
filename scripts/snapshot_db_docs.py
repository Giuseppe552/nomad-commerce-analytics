#!/usr/bin/env python3
"""
Create lightweight KPI screenshots as CI artifacts (no browser needed).
Outputs:
  artifacts/kpis_trend.png
  artifacts/freight_vs_time.png  (real mode only if data present)
"""
from __future__ import annotations

import os
from pathlib import Path
import duckdb
import pandas as pd
import matplotlib.pyplot as plt


DB = os.environ.get("DUCKDB_PATH", "warehouse/nomad.duckdb")
ART = Path("artifacts")
ART.mkdir(parents=True, exist_ok=True)

def _con():
    return duckdb.connect(DB, read_only=True)

def _read_df(sql: str) -> pd.DataFrame:
    try:
        with _con() as con:
            return con.execute(sql).fetchdf()
    except Exception:
        return pd.DataFrame()

def kpi_trend():
    # Prefer real; fallback to synth
    table = "mrt_kpis_daily_real"
    df = _read_df(f"SELECT * FROM {table} ORDER BY kpi_date")
    if df.empty:
        table = "mrt_kpis_daily_synth"
        df = _read_df(f"SELECT * FROM {table} ORDER BY kpi_date")
    if df.empty or "kpi_date" not in df.columns:
        print("[snapshot] No KPI table found.")
        return

    plt.figure(figsize=(8, 4.5))
    x = pd.to_datetime(df["kpi_date"])
    for col in [c for c in df.columns if c not in ("kpi_date",)]:
        try:
            plt.plot(x, df[col], label=col)
        except Exception:
            pass
    plt.title("KPI Trend")
    plt.xlabel("Date")
    plt.legend(loc="best", fontsize=8)
    out = ART / "kpis_trend.png"
    plt.tight_layout()
    plt.savefig(out)
    plt.close()
    print(f"[snapshot] Wrote {out}")

def freight_over_time():
    # Only meaningful in real mode; skip if missing
    df = _read_df("""
        SELECT order_date, avg(order_freight_pct) AS freight_pct
        FROM fct_freight
        WHERE order_date IS NOT NULL AND order_freight_pct IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """)
    if df.empty:
        print("[snapshot] fct_freight not available; skipping freight_vs_time.png")
        return

    plt.figure(figsize=(8, 4.5))
    plt.plot(pd.to_datetime(df["order_date"]), df["freight_pct"])
    plt.title("Freight % (order level) over time")
    plt.xlabel("Date")
    plt.ylabel("Freight % of GMV")
    out = ART / "freight_vs_time.png"
    plt.tight_layout()
    plt.savefig(out)
    plt.close()
    print(f"[snapshot] Wrote {out}")

def main():
    kpi_trend()
    freight_over_time()

if __name__ == "__main__":
    main()
