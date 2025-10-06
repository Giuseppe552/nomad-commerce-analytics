{{ config(materialized='table', tags=['real']) }}

-- Freight & distance at line level with aggregations at order level.
-- Columns:
--   order_item_id, order_id, order_date, product_id, seller_id,
--   qty, unit_price, freight_value, line_gross,
--   freight_pct_line, distance_km,
--   order_freight_total, order_gmv, order_freight_pct

with li as (
  select
    li.order_item_id,
    li.order_id,
    o.order_date,
    li.product_id,
    li.seller_id,
    li.qty,
    li.unit_price,
    li.freight_value,
    li.line_gross
  from {{ ref('fct_order_items') }} li
  join {{ ref('fct_orders') }} o using (order_id)
  where o.is_delivered
),

-- Customer & seller zip prefixes
cust as (
  select customer_id, zip_prefix as cust_zip
  from {{ ref('dim_customers') }}
),
ord_cust as (
  select o.order_id, c.cust_zip
  from {{ ref('fct_orders') }} o
  left join cust c using (customer_id)
),

sel as (
  select seller_id, zip_prefix as seller_zip
  from {{ ref('stg_sellers') }}
),

-- Zip → lat/lng lookup
geo as (
  select zip_prefix, lat, lng from {{ ref('stg_geolocation') }}
),

-- Resolve coordinates
coords as (
  select
    li.*,
    oc.cust_zip,
    s.seller_zip,
    cg.lat  as cust_lat,
    cg.lng  as cust_lng,
    sg.lat  as seller_lat,
    sg.lng  as seller_lng
  from li
  left join ord_cust oc using (order_id)
  left join sel s using (seller_id)
  left join geo cg on oc.cust_zip = cg.zip_prefix
  left join geo sg on s.seller_zip = sg.zip_prefix
),

-- Haversine distance in KM (skip nulls)
with_dist as (
  select
    *,
    case
      when cust_lat is null or cust_lng is null or seller_lat is null or seller_lng is null
        then null
      else (
        2 * asin(sqrt(
          pow(sin(radians((cust_lat - seller_lat) / 2.0)), 2) +
          cos(radians(seller_lat)) * cos(radians(cust_lat)) *
          pow(sin(radians((cust_lng - seller_lng) / 2.0)), 2)
        )) * 6371.0
      )
    end as distance_km
  from coords
),

line_metrics as (
  select
    order_item_id,
    order_id,
    order_date,
    product_id,
    seller_id,
    qty,
    unit_price,
    freight_value,
    line_gross,
    case when line_gross > 0 then freight_value / line_gross else null end as freight_pct_line,
    distance_km
  from with_dist
),

order_aggs as (
  select
    order_id,
    sum(freight_value) as order_freight_total,
    sum(line_gross)    as order_gmv
  from line_metrics
  group by 1
)

select
  l.*,
  oa.order_freight_total,
  oa.order_gmv,
  case when oa.order_gmv > 0 then oa.order_freight_total / oa.order_gmv else null end as order_freight_pct
from line_metrics l
left join order_aggs oa using (order_id);
