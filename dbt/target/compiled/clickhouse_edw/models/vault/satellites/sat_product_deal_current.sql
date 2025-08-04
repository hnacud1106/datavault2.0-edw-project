

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
FROM `edw`.`stg_products`

