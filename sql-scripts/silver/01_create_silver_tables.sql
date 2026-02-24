/*
Script name: 01_create_silver_tables.sql

Purpose: 
    - Creates Silver tables with cleaned structure and proper data types.
    - Defines typed schemas based on Bronze raw tables using DDL.

Description:
    - Converts raw VARCHAR columns from Bronze into appropriate SQL data types.
    - Standardizes column names where necessary.
    - Prepares data for analytical modeling in the Gold layer.
    - Applies primary keys and essential NOT NULL constraints where safe.

Execution:
    - Must be executed after Bronze tables are created.
    - Run once unless schema changes are required.

Notes:
    - Silver tables contains cleaned and typed data.
    - No business aggregations are preformed at this stage.
    - Data transformations are created in 02_load_silver.sql
    - We create PK's to secure zero duplicates, zero NULLs and to better preformance through clustered indexing. 
    - We wait with adding FK's till the gold layer because of the data quality issues. One mismatch stops the load.
    - Use CHAR(n) for fixed lengths for better join-preformance through quicker comparison and same size in memory.
    - We remove the 'dataset' in the names.
*/

USE brazilian_ecommerce_dw;
GO

CREATE TABLE silver.olist_customers_clean
(
    customer_id CHAR(32) NOT NULL,
    customer_unique_id CHAR(32) NOT NULL,
    customer_zip_code_prefix CHAR(5) NULL,
    customer_city VARCHAR(100) NULL,
    customer_state CHAR(2) NULL,

    CONSTRAINT pk_olist_customers_clean 
        PRIMARY KEY (customer_id)
);
GO

CREATE TABLE silver.olist_geolocation_clean
(
    geolocation_zip_code_prefix CHAR(5) NOT NULL,
    geolocation_lat FLOAT NULL,
    geolocation_lng FLOAT NULL,
    geolocation_city VARCHAR(50) NULL,
    geolocation_state CHAR(2) NULL,

    CONSTRAINT pk_olist_geolocation_clean 
        PRIMARY KEY (geolocation_zip_code_prefix)
    -- Duplicates are removed under loading.
);
GO

CREATE TABLE silver.olist_order_items_clean
(
    order_id CHAR(32) NOT NULL,
    order_item_id INT NOT NULL,
    product_id CHAR(32) NOT NULL,
    seller_id CHAR(32) NOT NULL,
    shipping_limit_date DATETIME2(0) NULL,
    price DECIMAL(10,2) NULL,
    freight_value DECIMAL(10,2) NULL,

    CONSTRAINT pk_olist_order_items_clean PRIMARY KEY (order_id, order_item_id)
);
GO

--Index for joining olist_selles
CREATE NONCLUSTERED INDEX ix_olist_order_items_clean_seller_id 
ON silver.olist_order_items_clean (seller_id);
GO


CREATE TABLE silver.olist_order_payments_clean
(
    order_id CHAR(32) NOT NULL,
    payment_sequential INT NOT NULL,
    payment_type VARCHAR(30) NULL,
    payment_installments INT NULL,
    payment_value DECIMAL(10,2) NULL,

    CONSTRAINT pk_olist_order_payments_clean PRIMARY KEY (order_id, payment_sequential)
);
GO

--Index for joining with orders
CREATE NONCLUSTERED INDEX ix_olist_order_payments_clean_order_id
ON silver.olist_order_payments_clean (order_id);
GO


CREATE TABLE silver.olist_order_reviews_clean
(
    review_id CHAR(32) NOT NULL,
    order_id CHAR(32) NOT NULL,
    review_score TINYINT NULL,
    review_comment_title VARCHAR(255) NULL,
    review_comment_message VARCHAR(MAX) NULL,
    review_creation_date DATETIME2(0) NULL,
    review_answer_timestamp DATETIME2(0) NULL,

    CONSTRAINT pk_olist_order_reviews_clean PRIMARY KEY (review_id)
);
GO

--Index for joining orders and reviews
CREATE NONCLUSTERED INDEX ix_olist_order_reviews_clean_order_id
ON silver.olist_order_reviews_clean (order_id);
GO


CREATE TABLE silver.olist_orders_clean
(
    order_id CHAR(32) NOT NULL,
    customer_id CHAR(32) NOT NULL,
    order_status VARCHAR(20) NULL,
    order_purchase_timestamp DATETIME2(0) NULL,
    order_approved_at DATETIME2(0) NULL,
    order_delivered_carrier_date DATETIME2(0) NULL,
    order_delivered_customer_date DATETIME2(0) NULL,
    order_estimated_delivery_date DATE NULL,

    CONSTRAINT pk_olist_orders_clean
        PRIMARY KEY (order_id)
);
GO

-- Silver indexing: index the FK/join column for fast joins to customers
CREATE NONCLUSTERED INDEX ix_olist_orders_clean_customer_id
ON silver.olist_orders_clean(customer_id);
GO

CREATE TABLE silver.olist_products_clean
(
    product_id CHAR(32) NOT NULL,
    product_category_name VARCHAR(50) NULL,
    product_name_length INT NULL,
    --corrected name length
    product_description_length INT NULL,
    -- corrected name length
    product_photos_qty TINYINT NULL,
    product_weight_g INT NULL,
    product_length_cm INT NULL,
    product_height_cm INT NULL,
    product_width_cm INT NULL,

    CONSTRAINT pk_olist_products_clean 
        PRIMARY KEY (product_id)
); 
GO

--Index for joins with category translation table
CREATE NONCLUSTERED INDEX ix_olist_products_clean_product_category_name 
ON silver.olist_products_clean(product_category_name);
GO

CREATE TABLE silver.olist_sellers_clean
(
    seller_id CHAR(32) NOT NULL,
    seller_zip_code_prefix CHAR(5) NOT NULL,
    seller_city VARCHAR(50) NULL,
    seller_state CHAR(2) NULL,

    CONSTRAINT pk_olist_sellers_clean PRIMARY KEY (seller_id)
);
GO

-- Index for faster joins with geolocations
CREATE NONCLUSTERED INDEX ix_olist_sellers_clean_seller_zip_code_prefix
ON silver.olist_sellers_clean(seller_zip_code_prefix);
GO

CREATE TABLE silver.olist_product_category_name_translation_clean
(
    product_category_name VARCHAR(50) NOT NULL,
    product_category_name_english VARCHAR(50) NOT NULL,

    CONSTRAINT pk_olist_product_category_name_translation_clean PRIMARY KEY (product_category_name)
);




















