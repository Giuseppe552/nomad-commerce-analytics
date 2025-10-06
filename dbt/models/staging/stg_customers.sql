{{ config(materialized='view', tags=['staging']) }}

-- Output (mode-agnostic):
--   customer_id, customer_unique_id, signup_ts, country, state, city, zip_prefix

with base as (
    select
        cast(coalesce(customer_id, customerid) as varchar)              as customer_id,
        -- Olist has both customer_id (hash per order stream) and customer_unique_id (true person key).
        cast(coalesce(customer_unique_id, customer_uuid, customer_id) as varchar)
                                                                       as customer_unique_id,
        cast(coalesce(signup_ts, first_order_ts, null) as timestamp)    as signup_ts,
        -- Geo fields (Olist)
        cast(coalesce(customer_state, state) as varchar)                 as state,
        cast(coalesce(customer_city, city) as varchar)                   as city,
        cast(coalesce(customer_zip_code_prefix, zip_prefix) as varchar)  as zip_prefix,
        -- Country not present in Olist (assume BR). Synthetic may set explicit ISO2.
        cast(coalesce(country, 'BR') as varchar)                         as country
    from raw_customers
)

select * from base;
