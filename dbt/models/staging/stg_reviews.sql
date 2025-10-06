{{ config(materialized='view', tags=['staging']) }}

-- Standardize product attributes (Olist has many physical dims; keep the most useful).
-- Output:
--   product_id, category, name_len, desc_len, photos_qty, weight_g, length_cm, height_cm, width_cm

with base as (
    select
        cast(product_id as varchar)                                      as product_id,
        lower(cast(coalesce(product_category_name, category) as varchar)) as category,
        cast(coalesce(product_name_lenght, name_len) as integer)          as name_len,
        cast(coalesce(product_description_lenght, desc_len) as integer)   as desc_len,
        cast(coalesce(product_photos_qty, photos_qty) as integer)         as photos_qty,
        cast(coalesce(product_weight_g, weight_g) as integer)             as weight_g,
        cast(coalesce(product_length_cm, length_cm) as integer)           as length_cm,
        cast(coalesce(product_height_cm, height_cm) as integer)           as height_cm,
        cast(coalesce(product_width_cm, width_cm) as integer)             as width_cm
    from raw_products
)

select * from base;
