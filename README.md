[![CI](https://github.com/YOUR_GH_USERNAME/YOUR_REPO/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_GH_USERNAME/YOUR_REPO/actions/workflows/ci.yml)

# Nomad Commerce Analytics (DuckDB + dbt + Streamlit)

**Real-world e-commerce analytics** using the public Olist dataset. One repo spins up ingestion → modeling → tests → a Streamlit app answering delivery, reviews, freight, payments, and category questions.

> **Why this matters:** demonstrates Analytics Engineering end-to-end: data contracts, dbt modeling, geospatial joins, statistical analysis, CI with artifacts, and an explorable UI.

---

## Features (employer highlights)

- **Real dataset** (Olist, ~100k orders): delivery timestamps, reviews, payments, products, sellers, geos.
- **Analytics you can act on:**
  - Delay → review impact (quantifies star drop when late).
  - Freight% vs distance with OLS regression + outlier lanes.
  - Category GMV, velocity & **proxy margin**; export discontinue list.
  - **Payments**: installments & method behavior (AOV and cancel proxy).
- **Solid engineering:** dbt models & tests, pre-dbt data contracts, CI workflow with **PNG chart artifacts**, reproducible local build.
- **Fast & local:** DuckDB; fresh build in minutes.

---

## Quickstart (3 minutes)

```bash
git clone <your-repo-url>
cd nomad-commerce-analytics
make setup                      # venv + deps + pre-commit
# Put Olist CSVs into data/real/  (see Dataset section below)
make ingest MODE=real           # load CSVs → DuckDB
make dbt_build                  # build + test models
make app                        # open Streamlit at localhost

```

Minimal run without full dataset (tiny sample):

# Creates a minimal Olist-like subset so you can smoke-test locally
python - <<'PY'
import pandas as pd, os
os.makedirs("data/real", exist_ok=True)
pd.DataFrame({
  "order_id":["o1"],
  "customer_id":["c1"],
  "order_status":["delivered"],
  "order_purchase_timestamp":["2018-01-01"],
  "order_approved_at":["2018-01-01"],
  "order_delivered_carrier_date":["2018-01-03"],
  "order_delivered_customer_date":["2018-01-05"],
  "order_estimated_delivery_date":["2018-01-06"]
}).to_csv("data/real/olist_orders_dataset.csv", index=False)
pd.DataFrame({
  "order_id":["o1","o1"],
  "order_item_id":[1,2],
  "product_id":["p1","p2"],
  "seller_id":["s1","s1"],
  "price":[100,200],
  "freight_value":[10,20]
}).to_csv("data/real/olist_order_items_dataset.csv", index=False)
pd.DataFrame({
  "customer_id":["c1"],
  "customer_unique_id":["u1"],
  "customer_city":["city"],
  "customer_state":["SP"],
  "customer_zip_code_prefix":["01000"]
}).to_csv("data/real/olist_customers_dataset.csv", index=False)
pd.DataFrame({
  "order_id":["o1"],
  "payment_sequential":[1],
  "payment_type":["credit_card"],
  "payment_installments":[1],
  "payment_value":[300]
}).to_csv("data/real/olist_order_payments_dataset.csv", index=False)
pd.DataFrame({
  "review_id":["r1"],
  "order_id":["o1"],
  "review_score":[5],
  "review_creation_date":["2018-01-06"],
  "review_answer_timestamp":["2018-01-07"]
}).to_csv("data/real/olist_order_reviews_dataset.csv", index=False)
pd.DataFrame({
  "product_id":["p1","p2"],
  "product_category_name":["misc","misc"],
  "product_name_lenght":[10,10],
  "product_description_lenght":[50,50],
  "product_photos_qty":[1,1],
  "product_weight_g":[100,100],
  "product_length_cm":[10,10],
  "product_height_cm":[5,5],
  "product_width_cm":[5,5]
}).to_csv("data/real/olist_products_dataset.csv", index=False)
pd.DataFrame({
  "seller_id":["s1"],
  "seller_city":["city"],
  "seller_state":["SP"],
  "seller_zip_code_prefix":["02000"]
}).to_csv("data/real/olist_sellers_dataset.csv", index=False)
pd.DataFrame({
  "geolocation_zip_code_prefix":["01000","02000"],
  "geolocation_city":["city","city"],
  "geolocation_state":["SP","SP"],
  "geolocation_lat":[-23.55,-23.50],
  "geolocation_lng":[-46.63,-46.60]
}).to_csv("data/real/olist_geolocation_dataset.csv", index=False)
PY

make ingest MODE=real && make dbt_build && make app

Dataset

Source: Brazilian E-Commerce Public Dataset by Olist (orders, items, customers, payments, reviews, products, sellers, geolocation).

Attribution: Olist / Kaggle. Use for analysis/education; raw data is not redistributed in this repo.

Placement: put the eight CSVs in data/real/ with the exact filenames:

olist_orders_dataset.csv

olist_order_items_dataset.csv

olist_customers_dataset.csv

olist_order_payments_dataset.csv

olist_order_reviews_dataset.csv

olist_products_dataset.csv

olist_sellers_dataset.csv

olist_geolocation_dataset.csv

What the app shows
Overview

Tiles for GMV, AOV, On-time %, Freight % GMV + 90–180 day trends. Alerts if thresholds are breached.
Delivery & Reviews

Lead-time distribution, On-time vs Late split, and delay penalty (avg star drop when late).
Freight & Distance

OLS of freight% vs haversine distance with outlier detection (z-residuals) and CSV export of costly lanes.
Payments

Installments distribution, AOV by payment method, and a cancel-rate proxy vs installments.
Category Explorer

GMV & velocity ranking, proxy margin (line_net − freight_value), discontinue/fix candidates with CSV export.
Architecture

    Warehouse: DuckDB (warehouse/nomad.duckdb)

    Ingestion: scripts/ingest_olist.py (creates raw_* tables/views), scripts/quality_checks.py (contracts)

    Modeling: dbt (staging/ → core/ → real_world/ marts)

    App: Streamlit (app/), mode-aware pages

    CI: GitHub Actions .github/workflows/ci.yml runs lint → ingest sample → quality → dbt build/tests → pytest → snapshot charts → upload artifacts

CSV → raw_* → stg_* → fct_* / dim_* → real_world marts → Streamlit

CI artifacts: Each run uploads artifacts/kpis_trend.png (+ freight_vs_time.png when available) and site/ (dbt docs). See the job’s Artifacts section.
Key KPIs (definitions)

    GMV = sum of item prices on delivered orders.

    AOV = GMV / delivered orders.

    On-time % = delivered_date ≤ estimated_date.

    Freight % GMV = Σ freight / Σ GMV.

    Delay penalty = Avg(review on-time) − Avg(review late).

(Verbose glossary in docs/kpi_dictionary.md; tooltips are embedded in the app.)
Commands you’ll actually use

make setup              # venv + deps + hooks
make ingest MODE=real   # load Olist CSVs
make quality            # pre-dbt contracts
make dbt_build          # build + test models
make app                # run the app
make ci                 # end-to-end on your machine

Troubleshooting / Runbook

See docs/runbook.md for common issues (missing CSVs, dbt profile, app tables, CI failures) and quick fixes.
Screenshots (add after first run)

    Overview tiles & trend

    Delivery vs Reviews (penalty metrics)

    Freight vs Distance (trend + outliers)

    Payments (installments & method KPIs)

    Category Explorer (Top N + candidates)

FAQ

Q: Do I need a database server?
A: No. DuckDB is an in-process OLAP DB—everything runs locally.

Q: Where do I put the data?
A: data/real/ (eight CSVs with the exact filenames above).

Q: How do I switch modes?
A: Set MODE=real (default). Synthetic mode is optional and can be added later.
License

MIT — see LICENSE.
