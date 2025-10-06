import os
from functools import lru_cache
from typing import Any
import datetime as dt

import duckdb
import pandas as pd
import streamlit as st

# On Streamlit Cloud, /mount/data is writable during the session
DUCKDB_PATH = st.secrets.get("DUCKDB_PATH", os.environ.get("DUCKDB_PATH", "/mount/data/nomad.duckdb"))


@lru_cache(maxsize=1)
def _connect() -> duckdb.DuckDBPyConnection:
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH, read_only=False)
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL json; LOAD json;")
    except Exception:
        pass
    return con

def get_con() -> duckdb.DuckDBPyConnection:
    return _connect()

@st.cache_data(show_spinner=False)
def query_df(sql: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
    con = get_con()
    return con.execute(sql, params).fetchdf()

def table_exists(name: str) -> bool:
    con = get_con()
    try:
        con.execute(f"SELECT 1 FROM {name} LIMIT 1")
        return True
    except Exception:
        return False

def ensure_demo_db() -> None:
    """
    If the DuckDB file has no expected tables, create a SMALL demo dataset
    (~200 orders) with the exact tables the app expects. This avoids needing
    dbt or the real Olist CSVs on Streamlit Cloud.
    """
    con = get_con()
    # If core table already there, do nothing
    try:
        con.execute("SELECT 1 FROM fct_orders LIMIT 1")
        return
    except Exception:
        pass

    # ---------- Generate tiny Olist-like dataframes ----------
    n_orders = 200
    start = pd.Timestamp("2018-01-01")
    days = pd.date_range(start, periods=n_orders, freq="D")

    orders = pd.DataFrame({
        "order_id": [f"o{i:05d}" for i in range(1, n_orders+1)],
        "customer_id": [f"c{(i%80)+1:04d}" for i in range(1, n_orders+1)],
        "order_status": ["delivered"] * n_orders,
        "order_purchase_timestamp": days,
        "order_approved_at": days,
        "order_delivered_carrier_date": days + pd.Timedelta(days=2),
        "order_delivered_customer_date": days + pd.Timedelta(days=4),
        "order_estimated_delivery_date": days + pd.Timedelta(days=5),
    })

    items = pd.DataFrame({
        "order_id": [f"o{i:05d}" for i in range(1, n_orders+1) for _ in (0,1)],
        "order_item_id": [1,2]*n_orders,
        "product_id": [f"p{(i%50)+1:04d}" for i in range(1, 2*n_orders+1)],
        "seller_id": [f"s{(i%30)+1:03d}" for i in range(1, 2*n_orders+1)],
        "price": [100.0, 200.0] * n_orders,
        "freight_value": [10.0, 20.0] * n_orders,
        "qty": [1,1] * n_orders,
        "discount_amount": [0.0, 0.0] * n_orders,
    })

    customers = pd.DataFrame({
        "customer_id": [f"c{i:04d}" for i in range(1,81)],
        "customer_unique_id": [f"u{i:04d}" for i in range(1,81)],
        "customer_city": ["city"]*80,
        "customer_state": ["SP"]*80,
        "customer_zip_code_prefix": ["01000"]*80,
    })

    payments = pd.DataFrame({
        "order_id": orders["order_id"],
        "payment_sequential": [1]*n_orders,
        "payment_type": ["credit_card"]*n_orders,
        "payment_installments": [1]*n_orders,
        "payment_value": [300.0]*n_orders,
        "method": ["credit_card"]*n_orders,
        "installments": [1]*n_orders,
        "amount": [300.0]*n_orders,
    })

    reviews = pd.DataFrame({
        "review_id": [f"r{i:05d}" for i in range(1,n_orders+1)],
        "order_id": orders["order_id"],
        "review_score": [5]*n_orders,
        "review_creation_date": days + pd.Timedelta(days=5),
        "review_answer_timestamp": days + pd.Timedelta(days=6),
    })

    products = pd.DataFrame({
        "product_id": [f"p{i:04d}" for i in range(1,51)],
        "product_category_name": ["misc"]*50,
        "product_name_lenght": [10]*50,
        "product_description_lenght": [50]*50,
        "product_photos_qty": [1]*50,
        "product_weight_g": [100]*50,
        "product_length_cm": [10]*50,
        "product_height_cm": [5]*50,
        "product_width_cm": [5]*50,
    })

    sellers = pd.DataFrame({
        "seller_id": [f"s{i:03d}" for i in range(1,31)],
        "seller_city": ["city"]*30,
        "seller_state": ["SP"]*30,
        "seller_zip_code_prefix": ["02000"]*30,
    })

    geos = pd.DataFrame({
        "geolocation_zip_code_prefix": ["01000","02000"],
        "geolocation_city": ["city","city"],
        "geolocation_state": ["SP","SP"],
        "geolocation_lat": [-23.55,-23.50],
        "geolocation_lng": [-46.63,-46.60],
    })

    # ---------- Write raw_* tables ----------
    con.register("df_orders", orders)
    con.register("df_items", items)
    con.register("df_customers", customers)
    con.register("df_payments", payments)
    con.register("df_reviews", reviews)
    con.register("df_products", products)
    con.register("df_sellers", sellers)
    con.register("df_geos", geos)

    con.execute("CREATE OR REPLACE TABLE raw_orders AS SELECT * FROM df_orders")
    con.execute("CREATE OR REPLACE TABLE raw_order_items AS SELECT * FROM df_items")
    con.execute("CREATE OR REPLACE TABLE raw_customers AS SELECT * FROM df_customers")
    con.execute("CREATE OR REPLACE TABLE raw_payments AS SELECT * FROM df_payments")
    con.execute("CREATE OR REPLACE TABLE raw_reviews AS SELECT * FROM df_reviews")
    con.execute("CREATE OR REPLACE TABLE raw_products AS SELECT * FROM df_products")
    con.execute("CREATE OR REPLACE TABLE raw_sellers AS SELECT * FROM df_sellers")
    con.execute("CREATE OR REPLACE TABLE raw_geolocation AS SELECT * FROM df_geos")

    # ---------- Create the minimal modeled tables app expects ----------
    # Staging
    con.execute("""
        CREATE OR REPLACE VIEW stg_orders AS
        SELECT
          order_id,
          customer_id,
          lower(order_status) AS status,
          order_purchase_timestamp::TIMESTAMP AS order_ts,
          order_approved_at::TIMESTAMP AS approved_ts,
          order_delivered_carrier_date::TIMESTAMP AS delivered_carrier_ts,
          order_delivered_customer_date::TIMESTAMP AS delivered_customer_ts,
          order_estimated_delivery_date::TIMESTAMP AS estimated_delivery_ts,
          (order_purchase_timestamp)::DATE AS order_date,
          strftime(order_purchase_timestamp, '%Y-%m') AS order_ym,
          'BRL'::VARCHAR AS currency,
          (lower(order_status)='delivered') AS is_delivered,
          (lower(order_status)='canceled') AS is_canceled,
          false AS is_unavailable,
          CASE WHEN order_delivered_customer_date > order_estimated_delivery_date THEN true ELSE false END AS is_late
        FROM raw_orders;
    """)
    con.execute("""
        CREATE OR REPLACE VIEW stg_order_items AS
        SELECT
          order_id || '-' || lpad(cast(order_item_id as varchar),3,'0') AS order_item_id,
          order_id,
          order_item_id AS order_item_seq,
          product_id,
          seller_id,
          coalesce(qty,1)::INTEGER AS qty,
          price::DOUBLE AS unit_price,
          coalesce(discount_amount,0)::DOUBLE AS discount_amount,
          freight_value::DOUBLE AS freight_value,
          (coalesce(qty,1)*price)::DOUBLE AS line_gross,
          (coalesce(qty,1)*price - coalesce(discount_amount,0))::DOUBLE AS line_net
        FROM raw_order_items;
    """)
    con.execute("""
        CREATE OR REPLACE VIEW stg_customers AS
        SELECT
          customer_id,
          customer_unique_id,
          NULL::TIMESTAMP AS signup_ts,
          'BR'::VARCHAR AS country,
          customer_state AS state,
          customer_city AS city,
          customer_zip_code_prefix AS zip_prefix
        FROM raw_customers;
    """)
    con.execute("""
        CREATE OR REPLACE VIEW stg_products AS
        SELECT
          product_id,
          lower(product_category_name) AS category,
          product_name_lenght AS name_len,
          product_description_lenght AS desc_len,
          product_photos_qty AS photos_qty,
          product_weight_g AS weight_g,
          product_length_cm AS length_cm,
          product_height_cm AS height_cm,
          product_width_cm AS width_cm
        FROM raw_products;
    """)
    con.execute("""
        CREATE OR REPLACE VIEW stg_payments AS
        SELECT
          order_id,
          row_number() OVER (PARTITION BY order_id ORDER BY payment_value DESC) AS payment_seq,
          lower(coalesce(payment_type, method, 'unknown')) AS method,
          coalesce(payment_installments, installments, 1)::INTEGER AS installments,
          coalesce(payment_value, amount, 0)::DOUBLE AS value
        FROM raw_payments;
    """)
    con.execute("""
        CREATE OR REPLACE VIEW stg_reviews AS
        SELECT
          review_id,
          order_id,
          review_score AS score,
          review_creation_date::TIMESTAMP AS review_creation_ts,
          review_answer_timestamp::TIMESTAMP AS review_answer_ts,
          NULL::VARCHAR AS comment_title,
          NULL::VARCHAR AS comment_message
        FROM raw_reviews;
    """)
    con.execute("""
        CREATE OR REPLACE VIEW stg_sellers AS
        SELECT
          seller_id, seller_city AS city, seller_state AS state, seller_zip_code_prefix AS zip_prefix
        FROM raw_sellers;
    """)
    con.execute("""
        CREATE OR REPLACE VIEW stg_geolocation AS
        SELECT
          geolocation_zip_code_prefix AS zip_prefix,
          geolocation_city AS city,
          geolocation_state AS state,
          geolocation_lat AS lat,
          geolocation_lng AS lng
        FROM raw_geolocation
        GROUP BY 1,2,3,4,5;
    """)

    # Core facts/dims
    con.execute("""
        CREATE OR REPLACE TABLE dim_customers AS
        SELECT c.*, MIN(o.order_purchase_timestamp) AS first_order_ts
        FROM stg_customers c
        LEFT JOIN raw_orders o ON o.customer_id=c.customer_id
        GROUP BY ALL;
    """)
    con.execute("""
        CREATE OR REPLACE TABLE fct_order_items AS
        SELECT li.*, o.status, o.is_delivered, o.is_canceled, o.is_unavailable, o.is_late, o.order_ts
        FROM stg_order_items li
        LEFT JOIN stg_orders o USING (order_id);
    """)
    con.execute("""
        CREATE OR REPLACE TABLE fct_orders AS
        WITH items AS (
          SELECT order_id,
                 SUM(qty) AS items_qty,
                 COUNT(*) AS lines_count,
                 SUM(line_gross) AS gmv,
                 SUM(line_net) AS net_item_revenue,
                 SUM(freight_value) AS freight_total
          FROM stg_order_items GROUP BY 1
        ),
        pays AS (
          SELECT order_id, SUM(value) AS payment_total, MAX(installments) AS max_installments
          FROM stg_payments GROUP BY 1
        )
        SELECT
          o.order_id, o.customer_id, o.status, o.order_ts, o.approved_ts,
          o.delivered_carrier_ts, o.delivered_customer_ts, o.estimated_delivery_ts,
          o.order_date, o.order_ym, o.currency, o.is_delivered, o.is_canceled, o.is_unavailable, o.is_late,
          COALESCE(i.items_qty,0) AS items_qty,
          COALESCE(i.lines_count,0) AS lines_count,
          COALESCE(i.gmv,0)::DOUBLE AS gmv,
          COALESCE(i.net_item_revenue,0)::DOUBLE AS net_item_revenue,
          COALESCE(i.freight_total,0)::DOUBLE AS freight_total,
          COALESCE(p.payment_total,0)::DOUBLE AS payment_total,
          COALESCE(p.max_installments,0) AS max_installments
        FROM stg_orders o
        LEFT JOIN items i USING (order_id)
        LEFT JOIN pays p USING (order_id);
    """)

    # Real-world marts
    con.execute("""
        CREATE OR REPLACE TABLE fct_deliveries AS
        SELECT
          order_id, order_date, delivered_customer_ts, estimated_delivery_ts,
          datediff('day', order_date::TIMESTAMP, delivered_customer_ts) AS lead_time_days,
          CASE WHEN estimated_delivery_ts IS NOT NULL
               THEN datediff('day', estimated_delivery_ts, delivered_customer_ts) END AS eta_gap_days,
          CASE WHEN estimated_delivery_ts IS NULL THEN NULL ELSE (delivered_customer_ts <= estimated_delivery_ts) END AS is_on_time,
          CASE WHEN estimated_delivery_ts IS NULL THEN NULL ELSE (delivered_customer_ts > estimated_delivery_ts) END AS is_late
        FROM fct_orders
        WHERE is_delivered;
    """)
    # Build geo + distance + freight %
    con.execute("""
        CREATE OR REPLACE TABLE fct_freight AS
        WITH li AS (
          SELECT li.order_item_id, li.order_id, o.order_date, li.product_id, li.seller_id,
                 li.qty, li.unit_price, li.freight_value, li.line_gross
          FROM fct_order_items li
          JOIN fct_orders o USING (order_id)
          WHERE o.is_delivered
        ),
        ord_cust AS (
          SELECT o.order_id, c.zip_prefix AS cust_zip
          FROM fct_orders o
          LEFT JOIN dim_customers c USING (customer_id)
        ),
        geos AS (SELECT * FROM stg_geolocation),
        coords AS (
          SELECT li.*, oc.cust_zip, s.zip_prefix AS seller_zip,
                 cg.lat AS cust_lat, cg.lng AS cust_lng,
                 sg.lat AS seller_lat, sg.lng AS seller_lng
          FROM li
          LEFT JOIN ord_cust oc USING (order_id)
          LEFT JOIN stg_sellers s USING (seller_id)
          LEFT JOIN geos cg ON oc.cust_zip = cg.zip_prefix
          LEFT JOIN geos sg ON s.zip_prefix = sg.zip_prefix
        ),
        with_dist AS (
          SELECT *,
            CASE
              WHEN cust_lat IS NULL OR cust_lng IS NULL OR seller_lat IS NULL OR seller_lng IS NULL
                THEN NULL
              ELSE (2 * asin(sqrt(
                    pow(sin(radians((cust_lat - seller_lat)/2.0)),2) +
                    cos(radians(seller_lat)) * cos(radians(cust_lat)) *
                    pow(sin(radians((cust_lng - seller_lng)/2.0)),2)
                 )) * 6371.0)
            END AS distance_km
          FROM coords
        ),
        line_metrics AS (
          SELECT
            order_item_id, order_id, order_date, product_id, seller_id, qty, unit_price, freight_value, line_gross,
            CASE WHEN line_gross>0 THEN freight_value/line_gross ELSE NULL END AS freight_pct_line,
            distance_km
          FROM with_dist
        ),
        order_aggs AS (
          SELECT order_id, SUM(freight_value) AS order_freight_total, SUM(line_gross) AS order_gmv
          FROM line_metrics GROUP BY 1
        )
        SELECT
          l.*, oa.order_freight_total, oa.order_gmv,
          CASE WHEN oa.order_gmv>0 THEN oa.order_freight_total/oa.order_gmv ELSE NULL END AS order_freight_pct
        FROM line_metrics l
        LEFT JOIN order_aggs oa USING (order_id);
    """)
    con.execute("""
        CREATE OR REPLACE TABLE mrt_kpis_daily_real AS
        WITH delivered AS (
          SELECT order_id, order_date, gmv, freight_total
          FROM fct_orders WHERE is_delivered
        ),
        ontime AS (SELECT order_id, is_on_time, is_late FROM fct_deliveries)
        SELECT
          d.order_date AS kpi_date,
          COUNT(*) AS orders_delivered,
          SUM(d.gmv)::DOUBLE AS gmv,
          SUM(d.freight_total)::DOUBLE AS freight_total,
          AVG(CASE WHEN o.is_on_time THEN 1 ELSE 0 END) AS on_time_pct,
          AVG(CASE WHEN o.is_late THEN 1 ELSE 0 END) AS late_pct,
          CASE WHEN COUNT(*)>0 THEN SUM(d.gmv)/COUNT(*) ELSE NULL END AS aov,
          CASE WHEN SUM(d.gmv)>0 THEN SUM(d.freight_total)/SUM(d.gmv) ELSE NULL END AS freight_pct_gmv
        FROM delivered d LEFT JOIN ontime o USING (order_id)
        GROUP BY 1 ORDER BY 1;
    """)
    con.execute("""
        CREATE OR REPLACE TABLE mrt_reviews AS
        WITH rev AS (
          SELECT date_trunc('day', review_creation_date)::DATE AS as_of_date, order_id, review_score AS score
          FROM raw_reviews
        ),
        del AS (SELECT order_id, is_late, is_on_time FROM fct_deliveries),
        j AS (SELECT r.as_of_date, r.order_id, r.score, d.is_late, d.is_on_time FROM rev r LEFT JOIN del d USING (order_id))
        SELECT
          as_of_date,
          COUNT(*) AS reviews_count,
          AVG(score)::DOUBLE AS avg_score,
          AVG(CASE WHEN is_late THEN score END)::DOUBLE AS late_avg_score,
          AVG(CASE WHEN is_on_time THEN score END)::DOUBLE AS ontime_avg_score,
          CASE
            WHEN AVG(CASE WHEN is_late THEN score END) IS NOT NULL
             AND AVG(CASE WHEN is_on_time THEN score END) IS NOT NULL
            THEN AVG(CASE WHEN is_on_time THEN score END) - AVG(CASE WHEN is_late THEN score END)
          END AS delay_penalty
        FROM j GROUP BY 1 ORDER BY 1;
    """)

    print("[bootstrap] Demo DuckDB created with minimal marts.")
