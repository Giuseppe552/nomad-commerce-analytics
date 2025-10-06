{{ config(materialized='view', tags=['staging']) }}

-- Standardize orders from either Real-World (Olist) or Synthetic raw tables.
-- Output schema (stable across modes):
--   order_id, customer_id, status, order_ts, approved_ts,
--   delivered_carrier_ts, delivered_customer_ts, estimated_delivery_ts,
--   order_date, order_ym, currency, is_delivered, is_canceled, is_unavailable, is_late

with base as (

    -- NOTE: scripts/ingest_olist.py creates a view `raw_orders` for both modes.
    select
        -- --- Keys
        cast(order_id as varchar)                                        as order_id,
        cast(customer_id as varchar)                                     as customer_id,

        -- --- Status normalization
        -- Olist statuses include: created, approved, invoiced, processing, shipped, delivered, canceled, unavailable
        -- Synthetic may use: completed, cancelled, refunded
        lower(coalesce(order_status, status))                            as _status_raw,

        -- --- Timestamps (coalesce names across modes)
        cast(coalesce(order_purchase_timestamp, order_ts) as timestamp)  as order_ts,
        cast(coalesce(order_approved_at, approved_ts) as timestamp)      as approved_ts,
        cast(coalesce(order_delivered_carrier_date, delivered_carrier_ts) as timestamp)
                                                                         as delivered_carrier_ts,
        cast(coalesce(order_delivered_customer_date, delivered_customer_ts) as timestamp)
                                                                         as delivered_customer_ts,
        cast(coalesce(order_estimated_delivery_date, estimated_delivery_ts) as timestamp)
                                                                         as estimated_delivery_ts

    from {{ ref('raw_orders') if false else 'raw_orders' }} -- ref() placeholder; using view directly

),

normalized as (

    select
        order_id,
        customer_id,

        case
            when _status_raw in ('delivered', 'complete', 'completed') then 'delivered'
            when _status_raw in ('shipped', 'shipping')                then 'shipped'
            when _status_raw in ('approved')                           then 'approved'
            when _status_raw in ('invoiced')                           then 'invoiced'
            when _status_raw in ('processing', 'created')              then 'processing'
            when _status_raw in ('canceled', 'cancelled')              then 'canceled'
            when _status_raw in ('unavailable')                        then 'unavailable'
            when _status_raw in ('refunded')                           then 'refunded'
            when _status_raw is null                                   then 'unknown'
            else _status_raw
        end                                                             as status,

        order_ts,
        approved_ts,
        delivered_carrier_ts,
        delivered_customer_ts,
        estimated_delivery_ts,

        cast(order_ts as date)                                          as order_date,
        strftime(order_ts, '%Y-%m')                                     as order_ym,

        -- Olist has no currency column; use project-level default for consistency.
        cast({{ var('currency', 'BRL') }} as varchar)                   as currency

    from base
),

flags as (

    select
        *,
        (status = 'delivered')                                          as is_delivered,
        (status = 'canceled')                                           as is_canceled,
        (status = 'unavailable')                                        as is_unavailable,
        case
            when delivered_customer_ts is not null
             and estimated_delivery_ts is not null
             and delivered_customer_ts > estimated_delivery_ts
            then true else false
        end                                                             as is_late
    from normalized
)

select * from flags;
