{{ config(materialized='table', tags=['real']) }}

-- Delivery fact (one row per delivered order)
-- Columns:
--   order_id, order_date, delivered_customer_ts, estimated_delivery_ts,
--   lead_time_days, eta_gap_days, is_on_time, is_late

with delivered as (
  select
    order_id,
    order_date,
    delivered_customer_ts,
    estimated_delivery_ts
  from {{ ref('fct_orders') }}
  where is_delivered
)

select
  order_id,
  order_date,
  delivered_customer_ts,
  estimated_delivery_ts,
  -- Lead time from purchase to delivery (days)
  datediff('day', cast(order_date as timestamp), delivered_customer_ts)    as lead_time_days,
  -- Positive if delivered after ETA, negative if early
  case
    when estimated_delivery_ts is not null
    then datediff('day', estimated_delivery_ts, delivered_customer_ts)
    else null
  end                                                                     as eta_gap_days,
  case
    when estimated_delivery_ts is null then null
    else delivered_customer_ts <= estimated_delivery_ts
  end                                                                     as is_on_time,
  case
    when estimated_delivery_ts is null then null
    else delivered_customer_ts > estimated_delivery_ts
  end                                                                     as is_late
from delivered;
