#!/usr/bin/env python3
"""
Ingest real (Olist) or synthetic CSVs into DuckDB as raw_* tables.

Usage:
  python scripts/ingest_olist.py --source real --db warehouse/nomad.duckdb
  python scripts/ingest_olist.py --source synth --db warehouse/nomad.duckdb
Env (optional):
  OLIST_DATA_DIR (default: data/real)
  SYNTH_DATA_DIR (default: data/synth)
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
import duckdb

OLIST_FILES = {
    "olist_orders_dataset.csv": "raw_olist_orders",
    "olist_order_items_dataset.csv": "raw_olist_order_items",
    "olist_customers_dataset.csv": "raw_olist_customers",
    "olist_order_payments_dataset.csv": "raw_olist_payments",
    "olist_order_reviews_dataset.csv": "raw_olist_reviews",
    "olist_products_dataset.csv": "raw_olist_products",
    "olist_sellers_dataset.csv": "raw_olist_sellers",
    "olist_geolocation_dataset.csv": "raw_olist_geolocation",
}

SYNTH_FILES = {
    "orders.csv": "raw_orders",
    "order_items.csv": "raw_order_items",
    "customers.csv": "raw_customers",
    "refunds.csv": "raw_refunds",
    "inventory.csv": "raw_inventory",
    "marketing_spend.csv": "raw_marketing_spend",
}


def eprint(*a, **k):
    print(*a, file=sys.stderr, **k)


def connect(db_path: str) -> duckdb.DuckDBPyConnection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(db_path)
    # Enable useful extensions if available
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL json; LOAD json;")
    except Exception:
        pass
    # Pragmas for speed & stability on local dev
    con.execute("PRAGMA threads=4;")
    con.execute("PRAGMA enable_progress_bar=false;")
    return con


def read_csv_into_table(con: duckdb.DuckDBPyConnection, csv_path: Path, table: str) -> int:
    """
    Create or replace a DuckDB table from a CSV using read_csv_auto with robust options.
    Returns row count loaded.
    """
    # Use UNION_BY_NAME so missing columns don’t explode across months; ignore stray columns.
    q = f"""
    CREATE OR REPLACE TABLE {table} AS
    SELECT * FROM read_csv_auto(
        '{csv_path.as_posix()}',
        header = true,
        sample_size = -1,
        union_by_name = true,
        normalize_names = true,
        all_varchar = false
    );
    """
    con.execute(q)
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    return int(count)


def ingest_folder(con: duckdb.DuckDBPyConnection, folder: Path, mapping: dict[str, str]) -> list[tuple[str, int]]:
    if not folder.exists():
        raise FileNotFoundError(f"Data directory not found: {folder}")

    results: list[tuple[str, int]] = []
    for fname, table in mapping.items():
        fpath = folder / fname
        if not fpath.exists():
            raise FileNotFoundError(f"Expected file missing: {fpath}")
        eprint(f"→ Loading {fname} → {table}")
        rows = read_csv_into_table(con, fpath, table)
        results.append((table, rows))
    return results


def ensure_minimal_schema(con: duckdb.DuckDBPyConnection, source: str):
    """
    Create small convenience views that dbt staging expects to find consistently.
    (Optional, harmless if you prefer dbt to reference raw_* directly.)
    """
    if source == "real":
        con.execute("""
            CREATE OR REPLACE VIEW raw_orders AS SELECT * FROM raw_olist_orders;
            CREATE OR REPLACE VIEW raw_order_items AS SELECT * FROM raw_olist_order_items;
            CREATE OR REPLACE VIEW raw_customers AS SELECT * FROM raw_olist_customers;
            CREATE OR REPLACE VIEW raw_payments AS SELECT * FROM raw_olist_payments;
            CREATE OR REPLACE VIEW raw_reviews AS SELECT * FROM raw_olist_reviews;
            CREATE OR REPLACE VIEW raw_products AS SELECT * FROM raw_olist_products;
            CREATE OR REPLACE VIEW raw_sellers AS SELECT * FROM raw_olist_sellers;
            CREATE OR REPLACE VIEW raw_geolocation AS SELECT * FROM raw_olist_geolocation;
        """)
    else:
        # Synthetic names already align with raw_*; create no-ops if helpful
        for t in ["orders", "order_items", "customers", "refunds", "inventory", "marketing_spend"]:
            con.execute(f"CREATE OR REPLACE VIEW raw_{t} AS SELECT * FROM raw_{t};")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest CSVs into DuckDB")
    p.add_argument("--source", choices=["real", "synth"], required=True, help="Data source")
    p.add_argument("--db", default=os.environ.get("DUCKDB_PATH", "warehouse/nomad.duckdb"), help="DuckDB path")
    p.add_argument("--olist-dir", default=os.environ.get("OLIST_DATA_DIR", "data/real"), help="Olist CSV folder")
    p.add_argument("--synth-dir", default=os.environ.get("SYNTH_DATA_DIR", "data/synth"), help="Synthetic CSV folder")
    return p.parse_args()


def main():
    args = parse_args()
    con = connect(args.db)
    eprint(f"Connected to DuckDB at {args.db}")

    if args.source == "real":
        folder = Path(args.olist_dir)
        mapping = OLIST_FILES
    else:
        folder = Path(args.synth_dir)
        mapping = SYNTH_FILES

    results = ingest_folder(con, folder, mapping)
    ensure_minimal_schema(con, args.source)

    # Quick sanity: show a few column/type previews for key tables
    preview_tables = ["raw_orders", "raw_order_items", "raw_customers"]
    for t in preview_tables:
        try:
            info = con.execute(
                f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{t}' ORDER BY ordinal_position LIMIT 8"
            ).fetchall()
            eprint(f"[schema] {t}: " + ", ".join(f"{c}:{d}" for c, d in info))
        except Exception:
            pass

    # Summarize
    total = sum(r for _, r in results)
    longest = max((len(t) for t, _ in results), default=0)
    eprint("\n=== Ingest Summary ===")
    for t, r in results:
        eprint(f"{t.ljust(longest)}  rows={r:,}")
    eprint(f"TOTAL rows ingested: {total:,}")

    con.close()


if __name__ == "__main__":
    try:
        main()
    except FileNotFoundError as e:
        eprint(f"[ERROR] {e}")
        sys.exit(2)
    except Exception as e:
        eprint(f"[FATAL] {type(e).__name__}: {e}")
        sys.exit(1)
