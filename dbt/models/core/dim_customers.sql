{{ config(materialized='table', tags=['core']) }}

-- Customer dimension: one row per customer_id with first_order_ts and location fields.

with customers as (
    select
        customer_id,
        customer_unique_id,
        signup_ts,
        country,
        state,
        city,
        zip_prefix
    from {{ ref('stg_customers') }}
),

first_orders as (
    select
        customer_id,
        min(order_ts) as first_order_ts,
        min(case when is_delivered then order_ts end) as first_delivered_ts,
        count(*) as orders_count
    from {{ ref('stg_orders') }}
    group by 1
)

select
    c.customer_id,
    c.customer_unique_id,
    c.signup_ts,
    f.first_order_ts,
    f.first_delivered_ts,
    f.orders_count,
    c.country,
    c.state,
    c.city,
    c.zip_prefix
from customers c
left join first_orders f using (customer_id);
