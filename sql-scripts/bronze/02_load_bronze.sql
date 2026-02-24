/*
Script name: 02_reload_bronze.sql

Purpose:
    - Reloads all Bronze tables from the source (CSV files) using a full refresh strategy with truncate and bulk insert after.

Description:
    - Truncates existing data in all Bronze tables.
    - Loads raw CSV data into Bronze tables using BULK INSERT.
    - Preserves source data as it is with no transformations.

Execution:
    - Can be run multiple times.
    - Intended for local development and re-ingestion scenarios.
    - Must be executed after Bronze tables have been created.

Notes:
    - Uses a TRUNCATE + BULK INSERT pattern to refresh and load the data.
    - CSV files are expected to have headers.
    - Data is ingested exactly as provided in the source files.
    - The first row is just the header with column names, so we start from row 2.
    - We use UTF-8 encoding, where codepage = 65001 to get special characters loaded without error.
    - Tablock locks the table under the loading process and improves performance with less locking-overhead.
    - I cleaned the reviews CSV file, to load it correctly because of issues caused by commas and line shifts in comments.
*/

USE brazilian_ecommerce_dw;
GO

-- 1) Clear existing data (full refresh)
TRUNCATE TABLE bronze.olist_customers_dataset_raw;

-- 2) Load CSV data into Bronze table
BULK INSERT bronze.olist_customers_dataset_raw
FROM 'C:\Users\thoma\OneDrive\Desktop\data-warehouse-project-brazillian-ecommerce\Data\bronze\olist_customers_dataset.csv'
with(
    firstrow = 2,
    codepage = '65001',
    fieldterminator = ',',
    rowterminator = '0x0a',
    Tablock
);
GO

-- 1) Clear existing data (full refresh)
TRUNCATE TABLE bronze.olist_geolocation_dataset_raw;

-- 2) Load CSV data into Bronze table
BULK INSERT bronze.olist_geolocation_dataset_raw
FROM 'C:\Users\thoma\OneDrive\Desktop\data-warehouse-project-brazillian-ecommerce\Data\bronze\olist_geolocation_dataset.csv'
with(
    firstrow = 2,
    codepage = '65001',
    fieldterminator = ',',
    rowterminator = '0x0a',
    Tablock
);
GO

-- 1) Clear existing data (full refresh)
TRUNCATE TABLE bronze.olist_order_items_dataset_raw;

-- 2) Load CSV data into Bronze table
BULK INSERT bronze.olist_order_items_dataset_raw
FROM 'C:\Users\thoma\OneDrive\Desktop\data-warehouse-project-brazillian-ecommerce\Data\bronze\olist_order_items_dataset.csv'
with(
    firstrow = 2,
    codepage = '65001',
    fieldterminator = ',',
    rowterminator = '0x0a',
    Tablock
);
GO

-- 1) Clear existing data (full refresh)
TRUNCATE TABLE bronze.olist_order_payments_dataset_raw;

-- 2) Load CSV data into Bronze table
BULK INSERT bronze.olist_order_payments_dataset_raw
FROM 'C:\Users\thoma\OneDrive\Desktop\data-warehouse-project-brazillian-ecommerce\Data\bronze\olist_order_payments_dataset.csv'
with(
    firstrow = 2,
    codepage = '65001',
    fieldterminator = ',',
    rowterminator = '0x0a',
    Tablock
);
GO

--1) Clear existing data (full refresh)
TRUNCATE TABLE bronze.olist_order_reviews_dataset_raw;

BULK INSERT bronze.olist_order_reviews_dataset_raw
FROM 'C:\Users\thoma\OneDrive\Desktop\data-warehouse-project-brazillian-ecommerce\Data\bronze\olist_order_reviews_dataset.bulk.csv'
with (
    firstrow = 2,
    codepage = '65001',
    fieldterminator = '|',
    rowterminator = '0x0a',
    tablock
);
GO


-- 1) Clear existing data (full refresh)
TRUNCATE TABLE bronze.olist_orders_dataset_raw;

-- 2) Load CSV data into Bronze table
BULK INSERT bronze.olist_orders_dataset_raw
FROM 'C:\Users\thoma\OneDrive\Desktop\data-warehouse-project-brazillian-ecommerce\Data\bronze\olist_orders_dataset.csv'
with(
    firstrow = 2,
    codepage = '65001',
    fieldterminator = ',',
    rowterminator = '0x0a',
    Tablock
);
GO

-- 1) Clear existing data (full refresh)
TRUNCATE TABLE bronze.olist_products_dataset_raw;

-- 2) Load CSV data into Bronze table
BULK INSERT bronze.olist_products_dataset_raw
FROM 'C:\Users\thoma\OneDrive\Desktop\data-warehouse-project-brazillian-ecommerce\Data\bronze\olist_products_dataset.csv'
with(
    firstrow = 2,
    codepage = '65001',
    fieldterminator = ',',
    rowterminator = '0x0a',
    Tablock
);
GO

-- 1) Clear existing data (full refresh)
TRUNCATE TABLE bronze.olist_sellers_dataset_raw;

-- 2) Load CSV data into Bronze table
BULK INSERT bronze.olist_sellers_dataset_raw
FROM 'C:\Users\thoma\OneDrive\Desktop\data-warehouse-project-brazillian-ecommerce\Data\bronze\olist_sellers_dataset.csv'
with(
    firstrow = 2,
    codepage = '65001',
    fieldterminator = ',',
    rowterminator = '0x0a',
    Tablock
);
GO

-- 1) Clear existing data (full refresh)
TRUNCATE TABLE bronze.olist_product_category_name_translation_raw;

-- 2) Load CSV data into Bronze table
BULK INSERT bronze.olist_product_category_name_translation_raw
FROM 'C:\Users\thoma\OneDrive\Desktop\data-warehouse-project-brazillian-ecommerce\Data\bronze\product_category_name_translation.csv'
with(
    firstrow = 2,
    codepage = '65001',
    fieldterminator = ',',
    rowterminator = '0x0a',
    Tablock
);

















