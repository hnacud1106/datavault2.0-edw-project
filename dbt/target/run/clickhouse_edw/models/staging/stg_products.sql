

  create or replace view `edw`.`stg_products` 
  
    
  
  
    
    
  as (
    

WITH source_data AS (
    SELECT DISTINCT
        product_base_id,
        product_name,
        product_description,
        category,
        brand,
        product_rating,
        product_image,
        shop_name,
        shop_link,
        revenue,
        month,
        price,
        quantity,
        deal_name,
        load_date,
        record_source,
        product_hash_key,
        deal_hash_key,
        product_deal_hash_key,
        product_hash_diff
    FROM `edw`.`stg_products`
),

normalized_products AS (
    SELECT
        product_base_id,
        product_name,
        product_description,
        category,
        brand,
        product_rating,
        product_image,
        shop_name,
        shop_link,
        revenue,
        month,
        price,
        quantity,
        product_hash_key,
        product_hash_diff,
        max(load_date) AS load_date,
        max(record_source) AS record_source
    FROM source_data
    GROUP BY
        product_base_id, product_name, product_description,
        category, brand, product_rating, product_image,
        shop_name, shop_link, revenue, month, price, quantity,
        product_hash_key, product_hash_diff
)

SELECT * FROM normalized_products
    
  )
      
      
                    -- end_of_sql
                    
                    