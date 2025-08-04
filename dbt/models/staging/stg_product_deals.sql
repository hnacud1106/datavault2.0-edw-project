{{ config(materialized='view') }}

SELECT DISTINCT
    product_base_id,
    deal_name,
    product_hash_key,
    deal_hash_key,
    product_deal_hash_key,
    max(load_date) AS load_date,
    max(record_source) AS record_source
FROM {{ source('edw', 'stg_products') }}
GROUP BY
    product_base_id, deal_name,
    product_hash_key, deal_hash_key, product_deal_hash_key
