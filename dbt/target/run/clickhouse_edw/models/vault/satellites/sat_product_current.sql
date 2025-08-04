
        insert into `edw`.`sat_product_current`
        ("product_hash_key", "product_hash_diff", "product_name", "product_description", "category", "brand", "product_rating", "product_image", "shop_name", "shop_link", "price", "load_date", "record_source")

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
FROM `edw`.`stg_product`


WHERE (product_hash_key, product_hash_diff, load_date) NOT IN (
    SELECT product_hash_key, product_hash_diff, load_date
    FROM `edw`.`sat_product_current`
)

      