{{ config(materialized='table', tags=['core']) }}

-- Product dimension (thin).

select
    product_id,
    category,
    name_len,
    desc_len,
    photos_qty,
    weight_g,
    length_cm,
    height_cm,
    width_cm
from {{ ref('stg_products') }};
