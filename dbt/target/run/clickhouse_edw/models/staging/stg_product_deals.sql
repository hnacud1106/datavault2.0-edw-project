

  create or replace view `edw`.`stg_product_deals` 
  
    
  
  
    
    
  as (
    

SELECT DISTINCT
    product_base_id,
    deal_name,
    product_hash_key,
    deal_hash_key,
    product_deal_hash_key,
    max(load_date) AS load_date,
    max(record_source) AS record_source
FROM `edw`.`stg_products`
GROUP BY
    product_base_id, deal_name,
    product_hash_key, deal_hash_key, product_deal_hash_key
    
  )
      
      
                    -- end_of_sql
                    
                    