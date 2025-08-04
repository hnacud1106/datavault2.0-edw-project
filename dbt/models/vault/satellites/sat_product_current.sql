{{ config(
    materialized='incremental',
    unique_key=['product_hash_key', 'load_date'],
    incremental_strategy='append'
) }}

SELECT
    product_hash_key,
    product_hash_diff,
    product_name,
    product_description,
    category,
    brand,
    product_rating,
    product_image,
    shop_name,
    shop_link,
    price,
    load_date,
    record_source
FROM {{ ref("stg_product") }}

{% if is_incremental() %}
WHERE (product_hash_key, product_hash_diff, load_date) NOT IN (
    SELECT product_hash_key, product_hash_diff, load_date
    FROM {{ this }}
)
{% endif %}
