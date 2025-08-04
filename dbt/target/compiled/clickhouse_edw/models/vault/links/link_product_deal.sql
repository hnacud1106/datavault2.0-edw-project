

SELECT DISTINCT
    product_deal_hash_key,
    product_hash_key,
    deal_hash_key,
    load_date,
    record_source
FROM `edw`.`stg_product_deals`
WHERE product_hash_key IS NOT NULL
  AND deal_hash_key IS NOT NULL


AND product_deal_hash_key NOT IN (
    SELECT product_deal_hash_key
    FROM `edw`.`link_product_deal`
)
