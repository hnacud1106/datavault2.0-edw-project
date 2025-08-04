{{ config(
    materialized='incremental',
    unique_key='deal_hash_key',
    incremental_strategy='append'
) }}

WITH deal_data AS (
    SELECT DISTINCT
        deal_name,
        deal_hash_key,
        load_date,
        record_source
    FROM {{ ref('stg_product_deals') }}
    WHERE deal_name IS NOT NULL
)

SELECT
    deal_hash_key,
    deal_name AS deal_business_key,
    load_date,
    record_source
FROM deal_data

{% if is_incremental() %}
WHERE deal_hash_key NOT IN (
    SELECT deal_hash_key
    FROM {{ this }}
)
{% endif %}
