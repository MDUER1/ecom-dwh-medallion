/*
Script Name: 03_sanity_checks.sql

Purpose:
    - Make sure the rows are actually loaded into the tables, by counting them.

Description:
    - Uses a simple select query with the * to count all rows.

Notes:
    - Usually we refrain from using * in our scripts, as stated in the naming conventions, but this is an exception.
*/

USE brazilian_ecommerce_dw;

    SELECT 'customers' AS t, COUNT(*) AS rows
    FROM bronze.olist_customers_dataset_raw
UNION ALL
    SELECT 'geolocation', COUNT(*)
    FROM bronze.olist_geolocation_dataset_raw
UNION ALL
    SELECT 'order_items', COUNT(*)
    FROM bronze.olist_order_items_dataset_raw
UNION ALL
    SELECT 'payments', COUNT(*)
    FROM bronze.olist_order_payments_dataset_raw
UNION ALL
    SELECT 'reviews', COUNT(*)
    FROM bronze.olist_order_reviews_dataset_raw
UNION ALL
    SELECT 'orders', COUNT(*)
    FROM bronze.olist_orders_dataset_raw
UNION ALL
    SELECT 'products', COUNT(*)
    FROM bronze.olist_products_dataset_raw
UNION ALL
    SELECT 'sellers', COUNT(*)
    FROM bronze.olist_sellers_dataset_raw
UNION ALL
    SELECT 'category_translation', COUNT(*)
    FROM bronze.olist_product_category_name_translation_raw;























