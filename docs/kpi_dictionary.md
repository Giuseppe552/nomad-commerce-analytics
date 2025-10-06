# KPI Dictionary

This glossary defines every metric shown in the app and dbt marts. Each entry includes the **where**, **how**, and **notes/edge cases**.

---

## GMV (Gross Merchandise Value)
- **Where:** `mrt_kpis_daily_real.gmv`
- **How:** Sum of item prices on **delivered** orders for the day.

GMV_d = Σ(line_gross) for orders with is_delivered = true on date d
line_gross = qty * unit_price

- **Notes:** Olist prices are BRL and tax handling is out-of-scope.

## AOV (Average Order Value)
- **Where:** `mrt_kpis_daily_real.aov`
- **How:**  

AOV_d = GMV_d / orders_delivered_d

- **Notes:** Uses **delivered** orders only (consistent with GMV).

## Orders Delivered
- **Where:** `mrt_kpis_daily_real.orders_delivered`
- **How:** Count of orders with `is_delivered = true` by `order_date`.

## Freight % GMV
- **Where:** `mrt_kpis_daily_real.freight_pct_gmv`
- **How:**  

freight_pct_gmv_d = Σ(freight_total_d) / GMV_d

where `freight_total` = Σ(`freight_value`) at the order level.
- **Notes:** Proxy for logistics intensity; higher = heavier shipping burden.

## On-time %
- **Where:** `mrt_kpis_daily_real.on_time_pct`
- **How:** Share of delivered orders where `delivered_customer_ts ≤ estimated_delivery_ts`.
- **Notes:** If estimated date is missing, record neither on-time nor late (excluded from ratio).

## Late %
- **Where:** `mrt_kpis_daily_real.late_pct`
- **How:** Share of delivered orders where `delivered_customer_ts > estimated_delivery_ts`.

## Lead Time (days)
- **Where:** `fct_deliveries.lead_time_days`
- **How:**  

lead_time_days = delivered_customer_ts - order_date (in days)


## ETA Gap (days)
- **Where:** `fct_deliveries.eta_gap_days`
- **How:**  

eta_gap_days = delivered_customer_ts - estimated_delivery_ts (days)

Positive = delivered after ETA; negative = early.

## Delay Penalty (reviews)
- **Where:** `mrt_reviews.delay_penalty`
- **How:**  

delay_penalty = avg_score_on_time − avg_score_late

- **Notes:** Positive means lateness correlates with lower review scores.

## Freight% (line level)
- **Where:** `fct_freight.freight_pct_line`
- **How:**  

freight_pct_line = freight_value / line_gross

- **Notes:** Undefined when `line_gross = 0` (treated as NULL).

## Distance (km)
- **Where:** `fct_freight.distance_km`
- **How:** Haversine distance between **seller ZIP prefix** and **customer ZIP prefix** centroids.
- **Notes:** If either ZIP centroid is missing, `distance_km` is NULL.

## Proxy Margin (Category Explorer)
- **Where:** computed in the app from `fct_order_items`
- **How:**  

proxy_margin = Σ(line_net − freight_value)
line_net = qty * unit_price − discount_amount

- **Notes:** Olist lacks COGS; this is a comparative proxy only.

## Velocity (units/day)
- **Where:** Category Explorer
- **How:**  

velocity = units_sold_in_window / window_days


---

# Data Contracts (selected)

- **Non-negative values:** `unit_price`, `freight_value`, `payment_value` ≥ 0  
- **PK uniqueness:** 
- Olist: `(order_id)`, `(order_id, order_item_id)`, `(customer_id)`, `(review_id)`, `(product_id)`, `(seller_id)`
- **FK integrity:** 
- `order_items.order_id → orders.order_id`
- `order_items.product_id → products.product_id`
- `order_items.seller_id → sellers.seller_id`
- `payments.order_id → orders.order_id`
- `reviews.order_id → orders.order_id`
- **Time sanity:** 
- `delivered_customer_date ≥ order_purchase_timestamp`
- `estimated_delivery_date ≥ order_purchase_timestamp`

---

# Interpretation Tips

- **On-time % ↔ Reviews:** If `delay_penalty > 0`, improving on-time rate should lift average stars.
- **Freight % vs Distance:** Trend should rise with distance; **outliers** above trend suggest carrier zoning or fulfillment issues.
- **Proxy Margin:** Use comparatively across categories; do **not** treat as true gross margin.
