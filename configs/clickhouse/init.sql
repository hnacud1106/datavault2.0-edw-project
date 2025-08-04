-- Initialize source database
CREATE DATABASE IF NOT EXISTS staging;

-- Create source products table
CREATE TABLE IF NOT EXISTS staging.products (
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
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY product_base_id
PARTITION BY toYYYYMM(updated_at);

-- Insert sample data into products table
INSERT INTO staging.products (product_base_id, product_name, product_description, category, brand, product_rating, product_image, shop_name, shop_link, revenue, month, price, quantity) VALUES (1, 'Kem chống nắng Hapas SPF50', 'Kem chống nắng cao cấp từ Hapas', 'Skincare', 'Hapas', 4.5, 'hapas1.jpg', 'Hapas Official Store', 'https://hapas.com', 15000000.00, '2025-01', 250000.00, 60), (2, 'Serum Obagi Vitamin C', 'Serum trắng da chuyên nghiệp từ Obagi', 'Beauty', 'Obagi', 4.8, 'obagi1.jpg', 'Obagi Vietnam', 'https://obagi.vn', 25000000.00, '2025-01', 1200000.00, 21), (3, 'Sữa rửa mặt Loreal Paris', 'Sữa rửa mặt dịu nhẹ từ Loreal', 'Skincare', 'Loreal', 4.2, 'loreal1.jpg', 'Loreal Paris Store', 'https://loreal.com', 8000000.00, '2025-01', 180000.00, 44), (4, 'Kem dưỡng Hapas Anti-Aging', 'Kem dưỡng chống lão hóa', 'Skincare', 'Hapas', 4.6, 'hapas2.jpg', 'Hapas Official Store', 'https://hapas.com', 12000000.00, '2025-01', 320000.00, 38), (5, 'Toner Obagi Professional', 'Toner chuyên nghiệp cho da nhạy cảm', 'Skincare', 'Obagi', 4.7, 'obagi2.jpg', 'Obagi Vietnam', 'https://obagi.vn', 18000000.00, '2025-01', 890000.00, 20), (6, 'Mascara Loreal Volume', 'Mascara tạo volume tự nhiên', 'Makeup', 'Loreal', 4.3, 'loreal2.jpg', 'Loreal Paris Store', 'https://loreal.com', 6000000.00, '2025-01', 150000.00, 40);

-- Create CDC log table
CREATE TABLE IF NOT EXISTS staging.product_changes_log (
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
    operation_type FixedString(1),
    change_timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (product_base_id, change_timestamp);

-- Create materialized view for CDC
CREATE MATERIALIZED VIEW IF NOT EXISTS staging.product_changes_mv
TO staging.product_changes_log
AS SELECT
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
    'u' as operation_type,
    now() as change_timestamp
FROM staging.products;