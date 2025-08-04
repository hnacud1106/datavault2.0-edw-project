

WITH deal_data AS (
    SELECT DISTINCT
        deal_name,
        deal_hash_key,
        load_date,
        record_source
    FROM `edw`.`stg_product_deals`
    WHERE deal_name IS NOT NULL
)

SELECT
    deal_hash_key,
    deal_name AS deal_business_key,
    load_date,
    record_source
FROM deal_data


WHERE deal_hash_key NOT IN (
    SELECT deal_hash_key
    FROM `edw`.`hub_deal`
)
