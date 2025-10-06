{{ config(materialized='view', tags=['staging']) }}

-- Seller directory from Olist (or synth if present).
-- Output:
--   seller_id, city, state, zip_prefix

with base as (
    select
        cast(seller_id as varchar)                          as seller_id,
        cast(coalesce(seller_city, city) as varchar)        as city,
        cast(coalesce(seller_state, state) as varchar)      as state,
        cast(coalesce(seller_zip_code_prefix, zip_prefix) as varchar) as zip_prefix
    from raw_sellers
)

select * from base;
