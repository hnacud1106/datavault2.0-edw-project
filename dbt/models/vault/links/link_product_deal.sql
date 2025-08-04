{{ config(
    materialized='incremental',
    unique_key='product_deal_hash_key',
    incremental_strategy='append'
) }}

SELECT DISTINCT
    product_deal_hash_key,
    product_hash_key,
    deal_hash_key,
    load_date,
    record_source
FROM {{ ref('stg_product_deals') }}
WHERE product_hash_key IS NOT NULL
  AND deal_hash_key IS NOT NULL

{% if is_incremental() %}
AND product_deal_hash_key NOT IN (
    SELECT product_deal_hash_key
    FROM {{ this }}
)
{% endif %}
