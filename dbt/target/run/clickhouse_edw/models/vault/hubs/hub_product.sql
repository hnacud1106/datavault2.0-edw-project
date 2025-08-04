
        insert into `edw`.`hub_product`
        ("product_hash_key", "product_business_key", "load_date", "record_source")

SELECT DISTINCT
    product_hash_key,
    product_base_id AS product_business_key,
    load_date,
    record_source
FROM `edw`.`stg_product`


WHERE product_hash_key NOT IN (
    SELECT product_hash_key
    FROM `edw`.`hub_product`
)

      