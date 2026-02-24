/*
Script name: 03_silver_quality_checks.sql

Purpose:
    - To check the data quality of the Silver Layer after 02_load_silver.
    - Act as a quality gate before progressing to the Gold Layer.

Description:
    - Core structural checks:
        - PK's uniqueness
        - NULL violations in NOT NULL columns
        - Rowcount comparison (BRONZE vs Silver)
    
    - Relational integrity checks:
        - Fact-to-Dimension mismatches (orphan records)
        - Make sure relations between tables are correct

    - Domain / Sanity checks:
        - Negative numeric values
        - Out-of-range values (like review_score)
        - Date logic consistency

    Critical issues set @failure = 1 and will terminate the script using THROW.
    Non-critical issues are reported via PRINT for monitoring.
    The script does not modify data.

Execution:
    - Must be executed after 02_load_silver.sql
    - If a critical check fails, the batch will terminate with an error
    - Intended for scheduled ELT pipelines.
*/


/*
Script: 03_quality_gate.sql
Purpose:
  - Structural + referential integrity checks for Silver layer
  - Fail-fast: any issue throws an error and stops downstream (Gold)
*/

USE brazilian_ecommerce_dw;
GO

/* ===========================
   1) TABLE EXISTS (FAIL)
   =========================== */

IF OBJECT_ID('silver.olist_customers_clean', 'U') IS NULL
    THROW 53000, 'DQ FAIL: Missing table silver.olist_customers_clean', 1;

IF OBJECT_ID('silver.olist_geolocation_clean', 'U') IS NULL
    THROW 53001, 'DQ FAIL: Missing table silver.olist_geolocation_clean', 1;

IF OBJECT_ID('silver.olist_order_items_clean', 'U') IS NULL
    THROW 53002, 'DQ FAIL: Missing table silver.olist_order_items_clean', 1;

IF OBJECT_ID('silver.olist_order_payments_clean', 'U') IS NULL
    THROW 53003, 'DQ FAIL: Missing table silver.olist_order_payments_clean', 1;

IF OBJECT_ID('silver.olist_order_reviews_clean', 'U') IS NULL
    THROW 53004, 'DQ FAIL: Missing table silver.olist_order_reviews_clean', 1;

IF OBJECT_ID('silver.olist_orders_clean', 'U') IS NULL
    THROW 53005, 'DQ FAIL: Missing table silver.olist_orders_clean', 1;

IF OBJECT_ID('silver.olist_products_clean', 'U') IS NULL
    THROW 53006, 'DQ FAIL: Missing table silver.olist_products_clean', 1;

IF OBJECT_ID('silver.olist_sellers_clean', 'U') IS NULL
    THROW 53007, 'DQ FAIL: Missing table silver.olist_sellers_clean', 1;

IF OBJECT_ID('silver.olist_product_category_name_translation_clean', 'U') IS NULL
    THROW 53008, 'DQ FAIL: Missing table silver.olist_product_category_name_translation_clean', 1;

GO

/* ===========================
   2) NOT NULL COLUMNS (FAIL)
   (structural columns marked NOT NULL in your DDL)
   =========================== */

-- customers
IF EXISTS (SELECT 1
FROM silver.olist_customers_clean
WHERE customer_id IS NULL OR customer_unique_id IS NULL)
    THROW 53100, 'DQ FAIL: NULL in NOT NULL columns (olist_customers_clean)', 1;

-- geolocation
IF EXISTS (SELECT 1
FROM silver.olist_geolocation_clean
WHERE geolocation_zip_code_prefix IS NULL)
    THROW 53101, 'DQ FAIL: NULL in NOT NULL columns (olist_geolocation_clean)', 1;

-- order_items
IF EXISTS (SELECT 1
FROM silver.olist_order_items_clean
WHERE order_id IS NULL OR order_item_id IS NULL OR product_id IS NULL OR seller_id IS NULL)
    THROW 53102, 'DQ FAIL: NULL in NOT NULL columns (olist_order_items_clean)', 1;

-- payments
IF EXISTS (SELECT 1
FROM silver.olist_order_payments_clean
WHERE order_id IS NULL OR payment_sequential IS NULL)
    THROW 53103, 'DQ FAIL: NULL in NOT NULL columns (olist_order_payments_clean)', 1;

-- reviews
IF EXISTS (SELECT 1
FROM silver.olist_order_reviews_clean
WHERE review_id IS NULL OR order_id IS NULL)
    THROW 53104, 'DQ FAIL: NULL in NOT NULL columns (olist_order_reviews_clean)', 1;

-- orders
IF EXISTS (SELECT 1
FROM silver.olist_orders_clean
WHERE order_id IS NULL OR customer_id IS NULL)
    THROW 53105, 'DQ FAIL: NULL in NOT NULL columns (olist_orders_clean)', 1;

-- products
IF EXISTS (SELECT 1
FROM silver.olist_products_clean
WHERE product_id IS NULL)
    THROW 53106, 'DQ FAIL: NULL in NOT NULL columns (olist_products_clean)', 1;

-- sellers
IF EXISTS (SELECT 1
FROM silver.olist_sellers_clean
WHERE seller_id IS NULL OR seller_zip_code_prefix IS NULL)
    THROW 53107, 'DQ FAIL: NULL in NOT NULL columns (olist_sellers_clean)', 1;

-- translation
IF EXISTS (SELECT 1
FROM silver.olist_product_category_name_translation_clean
WHERE product_category_name IS NULL OR product_category_name_english IS NULL)
    THROW 53108, 'DQ FAIL: NULL in NOT NULL columns (translation_clean)', 1;

GO

/* ===========================
   3) PK DUPLICATES (FAIL)
   (redundant if PK constraints always enforced, but good as a gate)
   =========================== */

-- customers PK (customer_id)
IF EXISTS (
    SELECT customer_id
FROM silver.olist_customers_clean
GROUP BY customer_id
HAVING COUNT(*) > 1
)
    THROW 53200, 'DQ FAIL: Duplicate PK in olist_customers_clean (customer_id)', 1;

-- geolocation PK (geolocation_zip_code_prefix)
IF EXISTS (
    SELECT geolocation_zip_code_prefix
FROM silver.olist_geolocation_clean
GROUP BY geolocation_zip_code_prefix
HAVING COUNT(*) > 1
)
    THROW 53201, 'DQ FAIL: Duplicate PK in olist_geolocation_clean (geolocation_zip_code_prefix)', 1;

-- order_items PK (order_id, order_item_id)
IF EXISTS (
    SELECT order_id, order_item_id
FROM silver.olist_order_items_clean
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1
)
    THROW 53202, 'DQ FAIL: Duplicate PK in olist_order_items_clean (order_id, order_item_id)', 1;

-- payments PK (order_id, payment_sequential)
IF EXISTS (
    SELECT order_id, payment_sequential
FROM silver.olist_order_payments_clean
GROUP BY order_id, payment_sequential
HAVING COUNT(*) > 1
)
    THROW 53203, 'DQ FAIL: Duplicate PK in olist_order_payments_clean (order_id, payment_sequential)', 1;

-- reviews PK (review_id)
IF EXISTS (
    SELECT review_id
FROM silver.olist_order_reviews_clean
GROUP BY review_id
HAVING COUNT(*) > 1
)
    THROW 53204, 'DQ FAIL: Duplicate PK in olist_order_reviews_clean (review_id)', 1;

-- orders PK (order_id)
IF EXISTS (
    SELECT order_id
FROM silver.olist_orders_clean
GROUP BY order_id
HAVING COUNT(*) > 1
)
    THROW 53205, 'DQ FAIL: Duplicate PK in olist_orders_clean (order_id)', 1;

-- products PK (product_id)
IF EXISTS (
    SELECT product_id
FROM silver.olist_products_clean
GROUP BY product_id
HAVING COUNT(*) > 1
)
    THROW 53206, 'DQ FAIL: Duplicate PK in olist_products_clean (product_id)', 1;

-- sellers PK (seller_id)
IF EXISTS (
    SELECT seller_id
FROM silver.olist_sellers_clean
GROUP BY seller_id
HAVING COUNT(*) > 1
)
    THROW 53207, 'DQ FAIL: Duplicate PK in olist_sellers_clean (seller_id)', 1;

-- translation PK (product_category_name)
IF EXISTS (
    SELECT product_category_name
FROM silver.olist_product_category_name_translation_clean
GROUP BY product_category_name
HAVING COUNT(*) > 1
)
    THROW 53208, 'DQ FAIL: Duplicate PK in translation_clean (product_category_name)', 1;

GO

/* ===========================
   4) ORPHANS (FAIL)
   (manual FK checks)
   =========================== */

-- orders.customer_id -> customers.customer_id
IF EXISTS (
    SELECT 1
FROM silver.olist_orders_clean o
WHERE NOT EXISTS (
        SELECT 1
FROM silver.olist_customers_clean c
WHERE c.customer_id = o.customer_id
    )
)
    THROW 53300, 'DQ FAIL: Orphan orders -> customers (customer_id missing)', 1;

-- order_items.order_id -> orders.order_id
IF EXISTS (
    SELECT 1
FROM silver.olist_order_items_clean oi
WHERE NOT EXISTS (
        SELECT 1
FROM silver.olist_orders_clean o
WHERE o.order_id = oi.order_id
    )
)
    THROW 53301, 'DQ FAIL: Orphan order_items -> orders (order_id missing)', 1;

-- order_items.product_id -> products.product_id
IF EXISTS (
    SELECT 1
FROM silver.olist_order_items_clean oi
WHERE NOT EXISTS (
        SELECT 1
FROM silver.olist_products_clean p
WHERE p.product_id = oi.product_id
    )
)
    THROW 53302, 'DQ FAIL: Orphan order_items -> products (product_id missing)', 1;

-- order_items.seller_id -> sellers.seller_id
IF EXISTS (
    SELECT 1
FROM silver.olist_order_items_clean oi
WHERE NOT EXISTS (
        SELECT 1
FROM silver.olist_sellers_clean s
WHERE s.seller_id = oi.seller_id
    )
)
    THROW 53303, 'DQ FAIL: Orphan order_items -> sellers (seller_id missing)', 1;

-- payments.order_id -> orders.order_id
IF EXISTS (
    SELECT 1
FROM silver.olist_order_payments_clean op
WHERE NOT EXISTS (
        SELECT 1
FROM silver.olist_orders_clean o
WHERE o.order_id = op.order_id
    )
)
    THROW 53304, 'DQ FAIL: Orphan payments -> orders (order_id missing)', 1;

-- reviews.order_id -> orders.order_id
IF EXISTS (
    SELECT 1
FROM silver.olist_order_reviews_clean r
WHERE NOT EXISTS (
        SELECT 1
FROM silver.olist_orders_clean o
WHERE o.order_id = r.order_id
    )
)
    THROW 53305, 'DQ FAIL: Orphan reviews -> orders (order_id missing)', 1;

GO

PRINT 'DQ PASS: Silver structural + orphan checks passed.';
GO







