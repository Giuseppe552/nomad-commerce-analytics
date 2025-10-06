{{ config(materialized='table', tags=['core']) }}

-- Order-level fact with totals, item counts, payment totals.

with o as (
    select
        order_id,
        customer_id,
        status,
        order_ts,
        approved_ts,
        delivered_carrier_ts,
        delivered_customer_ts,
        estimated_delivery_ts,
        order_date,
        order_ym,
        currency,
        is_delivered,
        is_canceled,
        is_unavailable,
        is_late
    from {{ ref('stg_orders') }}
),
items as (
    select
        order_id,
        sum(qty)                                  as items_qty,
        count(*)                                  as lines_count,
        sum(line_gross)                           as gmv,
        sum(line_net)                             as net_item_revenue,
        sum(freight_value)                        as freight_total
    from {{ ref('stg_order_items') }}
    group by 1
),
payments as (
    select
        order_id,
        sum(value)                                as payment_total,
        max(installments)                         as max_installments,
        list_agg(distinct method)                 as payment_methods
    from {{ ref('stg_payments') }}
    group by 1
)

select
    o.order_id,
    o.customer_id,
    o.status,
    o.order_ts,
    o.approved_ts,
    o.delivered_carrier_ts,
    o.delivered_customer_ts,
    o.estimated_delivery_ts,
    o.order_date,
    o.order_ym,
    o.currency,
    o.is_delivered,
    o.is_canceled,
    o.is_unavailable,
    o.is_late,

    coalesce(i.items_qty, 0)                      as items_qty,
    coalesce(i.lines_count, 0)                    as lines_count,
    coalesce(i.gmv, 0)::double                    as gmv,
    coalesce(i.net_item_revenue, 0)::double       as net_item_revenue,
    coalesce(i.freight_total, 0)::double          as freight_total,

    coalesce(p.payment_total, 0)::double          as payment_total,
    coalesce(p.max_installments, 0)               as max_installments,
    coalesce(p.payment_methods, '[]')             as payment_methods
from o
left join items i using (order_id)
left join payments p using (order_id);
