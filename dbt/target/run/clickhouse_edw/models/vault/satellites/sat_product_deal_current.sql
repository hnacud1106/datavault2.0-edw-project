
        
  
    
    
    
        
         


        insert into `edw`.`sat_product_deal_current`
        ("product_deal_hash_key", "revenue_hash_diff", "revenue", "month", "quantity", "load_date", "record_source")

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


  
    