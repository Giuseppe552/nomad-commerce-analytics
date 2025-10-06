{{ config(materialized='table', tags=['core']) }}

-- Line-level fact joining order status flags for filtering.

with li as (
    select * from {{ ref('stg_order_items') }}
),
o as (
    select order_id, status, is_delivered, is_canceled, is_unavailable, is_late, order_ts
    from {{ ref('stg_orders') }}
)

select
    li.order_item_id,
    li.order_id,
    o.order_ts,
    o.status,
    o.is_delivered,
    o.is_canceled,
    o.is_unavailable,
    o.is_late,
    li.order_item_seq,
    li.product_id,
    li.seller_id,
    li.qty,
    li.unit_price,
    li.discount_amount,
    li.freight_value,
    li.line_gross,
    li.line_net
from li
left join o using (order_id);
