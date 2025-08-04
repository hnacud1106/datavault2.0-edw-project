{{ config(
    materialized='incremental',
    unique_key='product_hash_key',
    incremental_strategy='append'
) }}

SELECT DISTINCT
    product_hash_key,
    product_base_id AS product_business_key,
    load_date,
    record_source
FROM {{ ref("stg_product") }}

{% if is_incremental() %}
WHERE product_hash_key NOT IN (
    SELECT product_hash_key
    FROM {{ this }}
)
{% endif %}
