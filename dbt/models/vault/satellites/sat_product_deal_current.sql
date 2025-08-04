{{ config(
    materialized='incremental',
    unique_key=['product_deal_hash_key', 'load_date'],
    incremental_strategy='append'
) }}

SELECT
    product_deal_hash_key,
    cityHash64(concat(
        toString(revenue), '||',
        month, '||',
        toString(quantity)
    )) AS revenue_hash_diff,
    revenue,
    month,
    quantity,
    load_date,
    record_source
FROM {{ source('edw', 'stg_products') }}

{% if is_incremental() %}
WHERE (product_deal_hash_key, revenue_hash_diff, load_date) NOT IN (
    SELECT product_deal_hash_key, revenue_hash_diff, load_date
    FROM {{ this }}
)
{% endif %}
