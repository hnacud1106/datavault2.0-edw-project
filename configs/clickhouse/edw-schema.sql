CREATE DATABASE IF NOT EXISTS edw;

-- Create raw source table for dbt
CREATE TABLE IF NOT EXISTS edw.raw_stg_products (
    product_base_id UInt64,
    product_name String,
    product_description String,
    category LowCardinality(String),
    brand Nullable(String),
    product_rating Nullable(Float32),
    product_image Nullable(String),
    shop_name LowCardinality(String),
    shop_link Nullable(String),
    revenue Decimal(18, 2),
    month String,
    price Decimal(18, 2),
    quantity UInt32,
    deal_name String,
    load_date DateTime,
    record_source String,
    product_hash_key UInt64,
    deal_hash_key UInt64,
    product_deal_hash_key UInt64,
    product_hash_diff UInt64
) ENGINE = MergeTree()
ORDER BY (product_base_id, deal_name)
PARTITION BY deal_name;

-- Staging table
CREATE TABLE IF NOT EXISTS edw.stg_products (
    product_base_id UInt64,
    product_name String,
    product_description String,
    category LowCardinality(String),
    brand Nullable(String),
    product_rating Nullable(Float32),
    product_image Nullable(String),
    shop_name LowCardinality(String),
    shop_link Nullable(String),
    revenue Decimal(18, 2),
    month String,
    price Decimal(18, 2),
    quantity UInt32,
    deal_name String,
    load_date DateTime,
    record_source String,
    product_hash_key UInt64,
    deal_hash_key UInt64,
    product_deal_hash_key UInt64,
    product_hash_diff UInt64
) ENGINE = MergeTree()
ORDER BY (product_base_id, deal_name)
PARTITION BY deal_name;

-- Hub tables
CREATE TABLE IF NOT EXISTS edw.hub_product (
    product_hash_key UInt64,
    product_business_key UInt64,
    load_date DateTime,
    record_source String
) ENGINE = MergeTree()
ORDER BY product_hash_key;

CREATE TABLE IF NOT EXISTS edw.hub_deal (
    deal_hash_key UInt64,
    deal_business_key String,
    load_date DateTime,
    record_source String
) ENGINE = MergeTree()
ORDER BY deal_hash_key;

-- Link table
CREATE TABLE IF NOT EXISTS edw.link_product_deal (
    product_deal_hash_key UInt64,
    product_hash_key UInt64,
    deal_hash_key UInt64,
    load_date DateTime,
    record_source String
) ENGINE = MergeTree()
ORDER BY product_deal_hash_key;

-- Satellite tables
CREATE TABLE IF NOT EXISTS edw.sat_product_current (
    product_hash_key UInt64,
    product_hash_diff UInt64,
    product_name String,
    product_description String,
    category LowCardinality(String),
    brand Nullable(String),
    product_rating Nullable(Float32),
    product_image Nullable(String),
    shop_name LowCardinality(String),
    shop_link Nullable(String),
    price Decimal(18, 2),
    load_date DateTime,
    record_source String
) ENGINE = ReplacingMergeTree(load_date)
ORDER BY (product_hash_key, product_hash_diff);