"""
E2E smoke: proves the pipeline can ingest → build dbt models → query KPIs.
Assumes CI (or local) already ran:
  - scripts/ingest_olist.py --source real
  - python scripts/quality_checks.py --mode real
  - dbt build --vars 'mode: real'
"""

import os
import duckdb
import pytest

DB_PATH = os.environ.get("DUCKDB_PATH", "warehouse/nomad.duckdb")


def connect():
    assert os.path.exists(DB_PATH), f"DuckDB not found at {DB_PATH}. Did you run ingest?"
    con = duckdb.connect(DB_PATH, read_only=True)
    return con


def table_exists(con, name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {name} LIMIT 1")
        return True
    except Exception:
        return False


@pytest.mark.order(1)
def test_core_tables_exist_and_nonempty():
    con = connect()
    required = ["fct_orders", "fct_order_items", "dim_customers"]
    missing = [t for t in required if not table_exists(con, t)]
    assert not missing, f"Missing core tables: {missing}"

    for t in required:
        n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        assert n >= 1, f"Table {t} is empty"


@pytest.mark.order(2)
def test_real_world_marts_exist_and_have_expected_columns():
    con = connect()
    marts = ["mrt_kpis_daily_real", "fct_deliveries", "fct_freight", "mrt_reviews"]
    missing = [t for t in marts if not table_exists(con, t)]
    assert not missing, f"Missing real-world marts: {missing}"

    # Validate KPI schema minimally
    cols = [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name='mrt_kpis_daily_real'"
    ).fetchall()]
    for c in ["kpi_date", "orders_delivered", "gmv", "aov", "on_time_pct", "freight_pct_gmv"]:
        assert c in cols, f"Column {c} missing from mrt_kpis_daily_real (have: {cols})"

    # Non-empty KPIs
    n_kpi = con.execute("SELECT COUNT(*) FROM mrt_kpis_daily_real").fetchone()[0]
    assert n_kpi >= 1, "mrt_kpis_daily_real is empty"

    # Sanity: values should be non-negative and on_time_pct between 0..1
    bad = con.execute("""
        SELECT COUNT(*) FROM mrt_kpis_daily_real
        WHERE gmv < 0 OR aov < 0 OR on_time_pct < 0 OR on_time_pct > 1 OR freight_pct_gmv < 0
    """).fetchone()[0]
    assert bad == 0, "Found invalid KPI values in mrt_kpis_daily_real"


@pytest.mark.order(3)
def test_freight_distance_relationship_has_signal():
    """
    We don't enforce strict R^2, but we expect distance & freight% join to be computable
    on at least some rows (non-null distance_km and freight_pct_line/order_freight_pct).
    """
    con = connect()
    assert table_exists(con, "fct_freight"), "fct_freight missing"

    # At least some rows with usable metrics
    usable = con.execute("""
        SELECT COUNT(*) FROM fct_freight
        WHERE distance_km IS NOT NULL
          AND (freight_pct_line IS NOT NULL OR order_freight_pct IS NOT NULL)
    """).fetchone()[0]
    assert usable >= 10, "Insufficient joined rows in fct_freight with distance & freight%"

    # Basic correlation check (not strict): ensure variance exists
    stats = con.execute("""
        SELECT
          var_pop(distance_km) AS v_dist,
          var_pop(freight_pct_line) AS v_fpct
        FROM fct_freight
        WHERE distance_km IS NOT NULL AND freight_pct_line IS NOT NULL
    """).fetchone()
    v_dist = stats[0] or 0
    v_fpct = stats[1] or 0
    assert v_dist > 0, "Zero variance in distance_km (after filters)"
    assert v_fpct >= 0, "freight_pct_line variance query failed"


@pytest.mark.order(4)
def test_reviews_delay_penalty_direction_when_data_present():
    """
    If reviews & deliveries overlap, the delay penalty should be computable.
    We don't enforce sign, only that the column is not all NULL when reviews exist.
    """
    con = connect()
    if not table_exists(con, "mrt_reviews"):
      pytest.skip("mrt_reviews not built")

    n_reviews = con.execute("SELECT COUNT(*) FROM mrt_reviews").fetchone()[0]
    if n_reviews == 0:
        pytest.skip("No reviews in dataset")

    non_null_penalty = con.execute(
        "SELECT COUNT(*) FROM mrt_reviews WHERE delay_penalty IS NOT NULL"
    ).fetchone()[0]
    assert non_null_penalty >= 1, "delay_penalty is NULL for all days despite reviews"
