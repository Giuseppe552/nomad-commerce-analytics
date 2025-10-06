{{ config(materialized='view', tags=['staging']) }}

-- Normalize line items across Real-World (Olist) and Synthetic schemas.
-- Output columns:
--   order_item_id, order_id, order_item_seq, product_id, seller_id,
--   qty, unit_price, discount_amount, freight_value,
--   line_gross, line_net

with base as (
    select
        -- Keys
        cast(coalesce(order_id, orderid) as varchar)                                as order_id,
        -- In Olist this is an integer sequence per order; in Synth we also have a single id column.
        cast(coalesce(order_item_id, line_number, 1) as integer)                    as order_item_seq,

        -- Product/Seller
        cast(coalesce(product_id, sku) as varchar)                                   as product_id,
        cast(coalesce(seller_id, seller) as varchar)                                 as seller_id,

        -- Economics (coalesce names & default sensibly)
        cast(coalesce(unit_price, price, 0) as double)                               as unit_price,
        cast(coalesce(discount_amount, 0) as double)                                 as discount_amount,
        cast(coalesce(freight_value, shipping_fee, 0) as double)                     as freight_value,

        -- Quantity: Olist has one row per unit (no quantity column) → default 1
        cast(coalesce(qty, quantity, 1) as integer)                                  as qty

    from raw_order_items
),

derived as (
    select
        -- Stable line id across datasets: order-<seq zero-padded>
        order_id || '-' || lpad(cast(order_item_seq as varchar), 3, '0')            as order_item_id,
        order_id,
        order_item_seq,
        product_id,
        seller_id,
        qty,
        unit_price,
        discount_amount,
        freight_value,
        -- Totals
        cast(qty * unit_price as double)                                            as line_gross,
        cast(qty * unit_price - discount_amount as double)                           as line_net
    from base
)

select * from derived;
