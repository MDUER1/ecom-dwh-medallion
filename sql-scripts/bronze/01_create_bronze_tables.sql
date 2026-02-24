/*
script_name: 01_create_bronze_tables.sql

Purpose:
    - Creates Bronze tables that mirror the input CSV files 1:1 through DDL.

Notes:
    - Bronze tables store the raw data, and prioritizes data ingestion, not performance.
    - All data types are varchars to keep the data 1:1 and avoid loading data incorrectly.
    - No transformations or constraints + We use USE <database> to make sure the tables load into the correct database.
    - The columns default to NULL.
    - Length is spelled wrong as lenght, which we change in the silver layer, to keept it 1:1 with the CSV files.
*/

USE brazilian_ecommerce_dw;
GO

CREATE TABLE bronze.olist_customers_dataset_raw
(
    customer_id VARCHAR(100),
    customer_unique_id VARCHAR(100),
    customer_zip_code_prefix VARCHAR(100),
    customer_city VARCHAR(100),
    customer_state VARCHAR(100)
);
GO

CREATE TABLE bronze.olist_orders_dataset_raw
(
    order_id VARCHAR(100),
    customer_id VARCHAR(100),
    order_status VARCHAR(100),
    order_purchase_timestamp VARCHAR(100),
    order_approved_at VARCHAR(100),
    order_delivered_carrier_date VARCHAR(100),
    order_delivered_customer_date VARCHAR(100),
    order_estimated_delivery_date VARCHAR(100)
);
GO

CREATE TABLE bronze.olist_order_items_dataset_raw
(
    order_id VARCHAR(100),
    order_item_id VARCHAR(100),
    product_id VARCHAR(100),
    seller_id VARCHAR(100),
    shipping_limit_date VARCHAR(100),
    price VARCHAR(100),
    freight_value VARCHAR(100)
);
GO

CREATE TABLE bronze.olist_order_reviews_dataset_raw
(
    review_id VARCHAR(100),
    order_id VARCHAR(100),
    review_score VARCHAR(100),
    review_comment_title VARCHAR(100),
    review_comment_message VARCHAR(MAX),
    review_creation_date VARCHAR(100),
    review_answer_timestamp VARCHAR(100)
);
GO

CREATE TABLE bronze.olist_order_payments_dataset_raw
(
    order_id VARCHAR(100),
    payment_sequential VARCHAR(100),
    payment_type VARCHAR(100),
    payment_installments VARCHAR(100),
    payment_value VARCHAR(100)
);
GO

CREATE TABLE bronze.olist_sellers_dataset_raw
(
    seller_id VARCHAR(100),
    seller_zip_code_prefix VARCHAR(100),
    seller_city VARCHAR(100),
    seller_state VARCHAR(100)
);
GO

CREATE TABLE bronze.olist_products_dataset_raw
(
    product_id VARCHAR(100),
    product_category_name VARCHAR(100),
    product_name_lenght VARCHAR(100),
    product_description_lenght VARCHAR(MAX),
    product_photos_qty VARCHAR(100),
    product_weight_g VARCHAR(100),
    product_length_cm VARCHAR(100),
    product_height_cm VARCHAR(100),
    product_width_cm VARCHAR(100)
);
GO

CREATE TABLE bronze.olist_geolocation_dataset_raw
(
    geolocation_zip_code_prefix VARCHAR(100),
    geolocation_lat VARCHAR(100),
    geolocation_lng VARCHAR(100),
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(100)
);
GO

CREATE TABLE bronze.olist_product_category_name_translation_raw
(
    product_category_name VARCHAR(100),
    product_category_name_english VARCHAR(100)
);





































