
  
    
    
    
        
         


        insert into `edw`.`sat_product_history`
        ("product_hash_key", "product_hash_diff", "product_name", "product_description", "category", "brand", "product_rating", "product_image", "shop_name", "shop_link", "price", "load_date", "record_source", "load_end_date", "is_current")

WITH ranked_data AS (
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
        record_source,
        ROW_NUMBER() OVER (
            PARTITION BY product_hash_key
            ORDER BY load_date
        ) as rn,
        ROW_NUMBER() OVER (
            PARTITION BY product_hash_key
            ORDER BY load_date DESC
        ) as rn_desc
    FROM `edw`.`sat_product_current`
),

with_next_date AS (
    SELECT
        r1.product_hash_key,
        r1.product_hash_diff,
        r1.product_name,
        r1.product_description,
        r1.category,
        r1.brand,
        r1.product_rating,
        r1.product_image,
        r1.shop_name,
        r1.shop_link,
        r1.price,
        r1.load_date,
        r1.record_source,
        COALESCE(r2.load_date, toDateTime('9999-12-31 23:59:59')) AS load_end_date,
        CASE WHEN r1.rn_desc = 1 THEN 1 ELSE 0 END AS is_current
    FROM ranked_data r1
    LEFT JOIN ranked_data r2
        ON r1.product_hash_key = r2.product_hash_key
        AND r2.rn = r1.rn + 1
)

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
    record_source,
    load_end_date,
    is_current
FROM with_next_date
  