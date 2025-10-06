#!/usr/bin/env python3
"""
Fast-fail data contracts & sanity checks before dbt.

Usage:
  python scripts/quality_checks.py --db warehouse/nomad.duckdb --mode real
  python scripts/quality_checks.py --db warehouse/nomad.duckdb --mode synth
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Iterable, Tuple

import duckdb
import yaml

CONFIG_PATH = Path("config/config.yaml")


def load_cfg() -> dict:
    if not CONFIG_PATH.exists():
        return {}
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f) or {}


def connect(db_path: str) -> duckdb.DuckDBPyConnection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(db_path)
    try:
        con.execute("INSTALL json; LOAD json;")
    except Exception:
        pass
    return con


def _run_count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    return int(con.execute(sql).fetchone()[0])


def _assert_zero(con: duckdb.DuckDBPyConnection, sql: str, msg: str, failures: list[str]) -> None:
    cnt = _run_count(con, sql)
    if cnt != 0:
        failures.append(f"{msg} (violations={cnt})")


def _assert_positive(con: duckdb.DuckDBPyConnection, sql: str, msg: str, failures: list[str]) -> None:
    cnt = _run_count(con, sql)
    if cnt <= 0:
        failures.append(f"{msg} (count={cnt})")


def check_real(con: duckdb.DuckDBPyConnection, cfg: dict) -> list[str]:
    """Contracts for Olist real-world dataset (raw_* views)."""
    failures: list[str] = []

    # ---------- presence ----------
    required_tables = [
        "raw_orders",
        "raw_order_items",
        "raw_customers",
        "raw_payments",
        "raw_reviews",
        "raw_products",
        "raw_sellers",
    ]
    for t in required_tables:
        _assert_positive(con, f"SELECT COUNT(*) FROM {t}", f"Missing or empty table: {t}", failures)

    # ---------- PK uniqueness ----------
    uniq_checks: Iterable[Tuple[str, str]] = [
        ("raw_orders", "order_id"),
        ("raw_order_items", "order_id, order_item_id"),
        ("raw_customers", "customer_id"),
        ("raw_reviews", "review_id"),
        ("raw_products", "product_id"),
        ("raw_sellers", "seller_id"),
    ]
    for table, cols in uniq_checks:
        _assert_zero(
            con,
            f"""
            WITH a AS (
              SELECT {cols}, COUNT(*) c
              FROM {table}
              GROUP BY {cols}
            )
            SELECT COUNT(*) FROM a WHERE c>1
            """,
            f"PK not unique on {table} ({cols})",
            failures,
        )
        _assert_zero(
            con,
            f"SELECT COUNT(*) FROM {table} WHERE { ' OR '.join([c.strip()+ ' IS NULL' for c in cols.split(',')]) }",
            f"PK contains NULLs on {table} ({cols})",
            failures,
        )

    # ---------- FK integrity ----------
    fk_checks: Iterable[Tuple[str, str, str]] = [
        ("raw_orders", "customer_id", "raw_customers(customer_id)"),
        ("raw_order_items", "order_id", "raw_orders(order_id)"),
        ("raw_order_items", "product_id", "raw_products(product_id)"),
        ("raw_order_items", "seller_id", "raw_sellers(seller_id)"),
        ("raw_payments", "order_id", "raw_orders(order_id)"),
        ("raw_reviews", "order_id", "raw_orders(order_id)"),
    ]
    for child, col, parent in fk_checks:
        _assert_zero(
            con,
            f"""
            SELECT COUNT(*) FROM {child} c
            LEFT JOIN {parent.split('(')[0]} p
              ON c.{col} = p.{parent.split('(')[1].split(')')[0]}
            WHERE p.{parent.split('(')[1].split(')')[0]} IS NULL
            """,
            f"FK missing: {child}.{col} → {parent}",
            failures,
        )

    # ---------- value constraints ----------
    _assert_zero(con, "SELECT COUNT(*) FROM raw_order_items WHERE price < 0 OR freight_value < 0",
                 "Negative price/freight_value in raw_order_items", failures)
    _assert_zero(con, "SELECT COUNT(*) FROM raw_payments WHERE payment_value < 0",
                 "Negative payment_value in raw_payments", failures)

    # ---------- time consistency ----------
    _assert_zero(
        con,
        """
        SELECT COUNT(*) FROM raw_orders
        WHERE order_purchase_timestamp IS NULL
           OR order_status IS NULL
           OR customer_id IS NULL
        """,
        "Essential NULLs in raw_orders (purchase ts/status/customer)", failures,
    )
    _assert_zero(
        con,
        """
        SELECT COUNT(*) FROM raw_orders
        WHERE delivered_customer_date IS NOT NULL
          AND order_purchase_timestamp IS NOT NULL
          AND delivered_customer_date < order_purchase_timestamp
        """,
        "Delivered before purchase in raw_orders", failures,
    )
    _assert_zero(
        con,
        """
        SELECT COUNT(*) FROM raw_orders
        WHERE estimated_delivery_date IS NOT NULL
          AND order_purchase_timestamp IS NOT NULL
          AND estimated_delivery_date < order_purchase_timestamp
        """,
        "Estimated delivery before purchase in raw_orders", failures,
    )

    return failures


def check_synth(con: duckdb.DuckDBPyConnection, cfg: dict) -> list[str]:
    """Contracts for synthetic generator schema (raw_* tables)."""
    failures: list[str] = []

    required_tables = [
        "raw_orders",
        "raw_order_items",
        "raw_customers",
        "raw_refunds",
        "raw_inventory",
        "raw_marketing_spend",
    ]
    for t in required_tables:
        _assert_positive(con, f"SELECT COUNT(*) FROM {t}", f"Missing or empty table: {t}", failures)

    # PK uniqueness
    uniq_checks = [
        ("raw_orders", "order_id"),
        ("raw_order_items", "order_item_id"),
        ("raw_customers", "customer_id"),
        ("raw_refunds", "refund_id"),
    ]
    for table, cols in uniq_checks:
        _assert_zero(
            con,
            f"WITH a AS (SELECT {cols}, COUNT(*) c FROM {table} GROUP BY {cols}) SELECT COUNT(*) FROM a WHERE c>1",
            f"PK not unique on {table} ({cols})",
            failures,
        )
        _assert_zero(
            con,
            f"SELECT COUNT(*) FROM {table} WHERE { ' OR '.join([c.strip()+ ' IS NULL' for c in cols.split(',')]) }",
            f"PK contains NULLs on {table} ({cols})",
            failures,
        )

    # FK integrity
    fk_checks = [
        ("raw_order_items", "order_id", "raw_orders(order_id)"),
        ("raw_refunds", "order_id", "raw_orders(order_id)"),
    ]
    for child, col, parent in fk_checks:
        _assert_zero(
            con,
            f"""
            SELECT COUNT(*) FROM {child} c
            LEFT JOIN {parent.split('(')[0]} p
              ON c.{col} = p.{parent.split('(')[1].split(')')[0]}
            WHERE p.{parent.split('(')[1].split(')')[0]} IS NULL
            """,
            f"FK missing: {child}.{col} → {parent}",
            failures,
        )

    # Value constraints
    _assert_zero(
        con,
        "SELECT COUNT(*) FROM raw_order_items WHERE qty < 1 OR unit_price < 0 OR discount_amount < 0 OR cogs_unit < 0",
        "Invalid qty/price/discount/cogs in raw_order_items",
        failures,
    )
    _assert_zero(
        con,
        "SELECT COUNT(*) FROM raw_order_items WHERE unit_price < discount_amount",
        "discount_amount exceeds unit_price in raw_order_items",
        failures,
    )
    _assert_zero(
        con,
        "SELECT COUNT(*) FROM raw_inventory WHERE on_hand < 0 OR lead_time_days < 0",
        "Invalid inventory on_hand/lead_time_days",
        failures,
    )
    _assert_zero(
        con,
        "SELECT COUNT(*) FROM raw_marketing_spend WHERE amount < 0",
        "Negative marketing spend",
        failures,
    )

    # Time consistency
    _assert_zero(
        con,
        """
        SELECT COUNT(*)
        FROM raw_orders o
        LEFT JOIN raw_customers c ON o.customer_id = c.customer_id
        WHERE o.order_ts IS NULL OR c.signup_ts IS NULL OR o.order_ts < c.signup_ts
        """,
        "Order time earlier than signup or essential NULLs (orders/customers)",
        failures,
    )

    return failures


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pre-dbt data quality checks")
    p.add_argument("--db", default=os.environ.get("DUCKDB_PATH", "warehouse/nomad.duckdb"))
    p.add_argument("--mode", choices=["real", "synth"], default=os.environ.get("MODE", "real"))
    return p.parse_args()


def main():
    args = parse_args()
    cfg = load_cfg()
    con = connect(args.db)

    print(f"[quality] DB={args.db} MODE={args.mode}")

    if args.mode == "real":
        failures = check_real(con, cfg)
    else:
        failures = check_synth(con, cfg)

    if failures:
        print("\n[QUALITY FAIL] One or more data contracts were violated:")
        for i, f in enumerate(failures, 1):
            print(f" {i:02d}. {f}")
        print("\nFix the above issues (or data files) and rerun.")
        sys.exit(2)

    print("[quality] All checks passed ✔")
    con.close()


if __name__ == "__main__":
    try:
        main()
    except duckdb.Error as e:
        print(f"[FATAL][DuckDB] {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"[FATAL] {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)
