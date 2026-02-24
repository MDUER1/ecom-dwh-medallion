/*
Script name: 02_load_silver.sql

Purpose:
    - Load data from Bronze tables into Silver tables.
    - Apply “clean + typed” transformations so Silver becomes the trusted structured layer.

What this script does:
    1) Resets Silver tables for dev-friendly reruns
       - TRUNCATE each Silver table in a safe load order.

    2) Inserts from Bronze -> Silver using INSERT...SELECT
       - Explicit typing with TRY_CAST / TRY_CONVERT to match Silver schema.
       - Basic cleaning: TRIM whitespace, NULLIF for empty strings, UPPER where useful (states).
       - Keeps only rows that satisfy Silver PK/NOT NULL requirements (or fixes them safely).

    3) Handles duplicates where Bronze is not unique but Silver has PK constraints
       - Uses ROW_NUMBER() to pick one deterministic “best row” per business key
         (e.g., geolocation per zip_code_prefix, where Bronze contains multiple rows).

    4) Adds lightweight validation after each load (optional but recommended)
       - Row counts inserted.
       - Quick duplicate checks against Silver PK columns.

Execution:
    - Run after:
        01_create_bronze_tables.sql + bronze load (raw ingestion)
        01_create_silver_tables.sql (Silver DDL)
    - Safe to rerun in dev if using TRUNCATE + INSERT approach.

Notes:
    - Silver is not for business logic or aggregations (that belongs in Gold).
    - We intentionally avoid adding Foreign Keys here due to known source quality issues.
      One mismatched key would stop the load; instead we validate relationships in Gold.
*/


USE brazilian_ecommerce_dw;
GO

SET NOCOUNT ON;
-- Cleaner output
SET XACT_ABORT ON;
-- Runtime error => transaction becomes uncommittable

DECLARE @run_id UNIQUEIDENTIFIER = NEWID();
-- Gives a unique ID for the execution
DECLARE @load_dts DATETIME2(0) = SYSDATETIME();
-- Timestamp for the execution

PRINT '02_loading_silver starting...';
PRINT CONCAT('run_id=', CONVERT(VARCHAR(36), @run_id),
             ' load_dts=', CONVERT(VARCHAR(19), @load_dts, 120));

BEGIN TRY
    BEGIN TRAN;

    /*
    All tables get a baseline cleaning + extra if errors are found under profiling the data
    Baseline cleaning:
        1) TRIM whitespace
        2) Remove double-quotes from columns
        3) Empty strings -> NULLs
        4) Casting data to the correct datatypes as required
    */

    -- Dim: customers
    TRUNCATE TABLE silver.olist_customers_clean;

    INSERT INTO silver.olist_customers_clean
    (
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
    )
SELECT
    CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.customer_id)), '"', ''), '') AS CHAR(32)) AS customer_id,
    CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.customer_unique_id)), '"', ''), '') AS CHAR(32)) AS customer_unique_id,
    CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.customer_zip_code_prefix)), '"', ''), '') AS CHAR(5)) AS customer_zip_code_prefix,
    CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.customer_city)), '"', ''), '') AS VARCHAR(100)) AS customer_city,
    CAST(NULLIF(REPLACE(UPPER(LTRIM(RTRIM(b.customer_state))), '"', ''), '') AS CHAR(2)) AS customer_state
-- Upper Case only
FROM bronze.olist_customers_dataset_raw b
WHERE
        NULLIF(REPLACE(LTRIM(RTRIM(b.customer_id)), '"', ''), '') IS NOT NULL
    AND NULLIF(REPLACE(LTRIM(RTRIM(b.customer_unique_id)), '"', ''), '') IS NOT NULL;
    
    DECLARE @rows_customers INT = @@ROWCOUNT;
    PRINT CONCAT('Customers dim inserted: ', @rows_customers); 


    -- Dim: Sellers
    TRUNCATE TABLE silver.olist_sellers_clean;

    INSERT INTO silver.olist_sellers_clean
    (
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
    )
SELECT
    CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.seller_id)), '"',''), '') AS CHAR(32)) AS seller_id,
    CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.seller_zip_code_prefix)), '"', ''), '') AS CHAR(5)) AS seller_zip_code_prefix,
    CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.seller_city)), '"', ''), '') AS VARCHAR(50)) AS seller_city,
    CAST(NULLIF(REPLACE(UPPER(LTRIM(RTRIM(b.seller_state))), '"', ''), '') AS CHAR(2)) AS seller_state
FROM bronze.olist_sellers_dataset_raw b
WHERE NULLIF(REPLACE(LTRIM(RTRIM(b.seller_id)), '"', ''), '') IS NOT NULL
    AND NULLIF(REPLACE(LTRIM(RTRIM(b.seller_zip_code_prefix)), '"', ''), '') IS NOT NULL;

    DECLARE @rows_sellers INT = @@ROWCOUNT;
    PRINT CONCAT('Sellers dim inserted: ', @rows_sellers);

    -- Dim products
    TRUNCATE TABLE silver.olist_products_clean;

    INSERT INTO silver.olist_products_clean
    (
    product_id,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
    )
SELECT
    CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.product_id)), '"', ''), '') AS CHAR(32)) AS product_id,
    CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.product_category_name)), '"', ''), '') AS VARCHAR(50)) AS product_category_name,
    TRY_CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.product_name_lenght)), '"', ''), '') AS INT) AS product_name_length,
    TRY_CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.product_description_lenght)), '"', ''), '') AS INT) AS product_description_length,
    TRY_CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.product_photos_qty)), '"', ''), '') AS TINYINT) AS product_photos_qty,
    TRY_CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.product_weight_g)), '"', ''), '') AS INT) AS product_weight_g,
    TRY_CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.product_length_cm)), '"', ''), '') AS INT) AS product_length_cm,
    TRY_CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.product_height_cm)), '"', ''), '') AS INT) AS product_height_cm,
    TRY_CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.product_width_cm)), '"', ''), '') AS INT) AS product_width_cm
FROM bronze.olist_products_dataset_raw b
WHERE NULLIF(REPLACE(LTRIM(RTRIM(b.product_id)), '"', ''), '') IS NOT NULL;

    DECLARE @row_products INT = @@ROWCOUNT;
    PRINT CONCAT('Products dim inserted:', @row_products);
    
    -- geolocation (dedup by zip_code_prefix)
TRUNCATE TABLE silver.olist_geolocation_clean;

;WITH
    geo
    AS
    (
        SELECT
            CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.geolocation_zip_code_prefix)), '"', ''), '') AS CHAR(5)) AS geolocation_zip_code_prefix,
            TRY_CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.geolocation_lat)), '"', ''), '') AS FLOAT) AS geolocation_lat,
            TRY_CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.geolocation_lng)), '"', ''), '') AS FLOAT) AS geolocation_lng,
            CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.geolocation_city)), '"', ''), '') AS VARCHAR(50)) AS geolocation_city,
            CAST(UPPER(NULLIF(REPLACE(LTRIM(RTRIM(b.geolocation_state)), '"', ''), '')) AS CHAR(2)) AS geolocation_state,
            ROW_NUMBER() OVER (
            PARTITION BY NULLIF(REPLACE(LTRIM(RTRIM(b.geolocation_zip_code_prefix)), '"', ''), '')
            ORDER BY
                CASE WHEN b.geolocation_lat IS NOT NULL AND b.geolocation_lng IS NOT NULL THEN 0 ELSE 1 END,
                b.geolocation_city
        ) AS rn
        FROM bronze.olist_geolocation_dataset_raw b
        WHERE NULLIF(REPLACE(LTRIM(RTRIM(b.geolocation_zip_code_prefix)), '"', ''), '') IS NOT NULL
    )
INSERT INTO silver.olist_geolocation_clean
    (
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
    )
SELECT
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state
FROM geo
WHERE rn = 1;

DECLARE @row_geolocation INT = @@ROWCOUNT;
PRINT CONCAT('Geolocation dim inserted: ', @row_geolocation);

    -- Product_category_name_translation_clean 
    TRUNCATE TABLE silver.olist_product_category_name_translation_clean;

    INSERT INTO silver.olist_product_category_name_translation_clean
    (
    product_category_name,
    product_category_name_english
    )
SELECT
    CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.product_category_name)), '"', ''), '') AS VARCHAR(50)) AS product_category_name,
    CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.product_category_name_english)), '"', ''), '') AS VARCHAR(50)) AS product_category_name_english
FROM bronze.olist_product_category_name_translation_raw b
WHERE NULLIF(REPLACE(LTRIM(RTRIM(b.product_category_name)), '"', ''), '') IS NOT NULL
    AND NULLIF(REPLACE(LTRIM(RTRIM(b.product_category_name_english)), '"', ''), '') IS NOT NULL;

    DECLARE @rows_translation INT = @@ROWCOUNT;
    PRINT CONCAT('Translation dim inserted: ', @rows_translation);

--Facts

--Orders
TRUNCATE TABLE silver.olist_orders_clean;

INSERT INTO silver.olist_orders_clean
    (
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date
    )
SELECT
    CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.order_id)), '"', ''), '') AS CHAR(32))     AS order_id,
    CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.customer_id)), '"', ''), '') AS CHAR(32))  AS customer_id,
    CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.order_status)), '"', ''), '') AS VARCHAR(20)) AS order_status,
    TRY_CONVERT(DATETIME2(0), NULLIF(REPLACE(LTRIM(RTRIM(b.order_purchase_timestamp)), '"', ''), '')) AS order_purchase_timestamp,
    TRY_CONVERT(DATETIME2(0), NULLIF(REPLACE(LTRIM(RTRIM(b.order_approved_at)), '"', ''), ''))        AS order_approved_at,
    TRY_CONVERT(DATETIME2(0), NULLIF(REPLACE(LTRIM(RTRIM(b.order_delivered_carrier_date)), '"', ''), '')) AS order_delivered_carrier_date,
    TRY_CONVERT(DATETIME2(0), NULLIF(REPLACE(LTRIM(RTRIM(b.order_delivered_customer_date)), '"', ''), '')) AS order_delivered_customer_date,
    TRY_CONVERT(DATE, NULLIF(REPLACE(LTRIM(RTRIM(b.order_estimated_delivery_date)), '"', ''), '')) AS order_estimated_delivery_date
FROM bronze.olist_orders_dataset_raw b
WHERE
    -- enforce Silver NOT NULL (PK + required FK)
    NULLIF(REPLACE(LTRIM(RTRIM(b.order_id)), '"', ''), '') IS NOT NULL
    AND NULLIF(REPLACE(LTRIM(RTRIM(b.customer_id)), '"', ''), '') IS NOT NULL;

DECLARE @rows_orders INT = @@ROWCOUNT;
PRINT CONCAT('Orders fact inserted: ', @rows_orders);


--Order Items
TRUNCATE TABLE silver.olist_order_items_clean;

INSERT INTO silver.olist_order_items_clean
    (
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value
    )
SELECT
    CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.order_id)), '"', ''), '') AS CHAR(32))    AS order_id,
    TRY_CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.order_item_id)), '"', ''), '') AS INT) AS order_item_id,
    CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.product_id)), '"', ''), '') AS CHAR(32))  AS product_id,
    CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.seller_id)), '"', ''), '') AS CHAR(32))   AS seller_id,

    TRY_CONVERT(DATETIME2(0), NULLIF(REPLACE(LTRIM(RTRIM(b.shipping_limit_date)), '"', ''), '')) AS shipping_limit_date,

    TRY_CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.price)), '"', ''), '') AS DECIMAL(10,2))        AS price,
    TRY_CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.freight_value)), '"', ''), '') AS DECIMAL(10,2)) AS freight_value
FROM bronze.olist_order_items_dataset_raw b
WHERE
    NULLIF(REPLACE(LTRIM(RTRIM(b.order_id)), '"', ''), '') IS NOT NULL
    AND TRY_CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.order_item_id)), '"', ''), '') AS INT) IS NOT NULL
    AND NULLIF(REPLACE(LTRIM(RTRIM(b.product_id)), '"', ''), '') IS NOT NULL
    AND NULLIF(REPLACE(LTRIM(RTRIM(b.seller_id)), '"', ''), '') IS NOT NULL;

DECLARE @rows_order_items INT = @@ROWCOUNT;
PRINT CONCAT('Order items fact inserted: ', @rows_order_items);


--Payments
TRUNCATE TABLE silver.olist_order_payments_clean;

INSERT INTO silver.olist_order_payments_clean
    (
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
    )
SELECT
    CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.order_id)), '"', ''), '') AS CHAR(32)) AS order_id,
    TRY_CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.payment_sequential)), '"', ''), '') AS INT) AS payment_sequential,

    CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.payment_type)), '"', ''), '') AS VARCHAR(30)) AS payment_type,
    TRY_CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.payment_installments)), '"', ''), '') AS INT) AS payment_installments,
    TRY_CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.payment_value)), '"', ''), '') AS DECIMAL(10,2)) AS payment_value
FROM bronze.olist_order_payments_dataset_raw b
WHERE
    NULLIF(REPLACE(LTRIM(RTRIM(b.order_id)), '"', ''), '') IS NOT NULL
    AND TRY_CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.payment_sequential)), '"', ''), '') AS INT) IS NOT NULL;

DECLARE @rows_payments INT = @@ROWCOUNT;
PRINT CONCAT('Order payments fact inserted: ', @rows_payments);


-- Reviews (dedup by review_id)
TRUNCATE TABLE silver.olist_order_reviews_clean;

;WITH
    r
    AS
    (
        SELECT
            CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.review_id)), '"', ''), '') AS CHAR(32)) AS review_id,
            CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.order_id)), '"', ''), '') AS CHAR(32))  AS order_id,

            TRY_CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.review_score)), '"', ''), '') AS TINYINT) AS review_score,

            CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.review_comment_title)), '"', ''), '') AS VARCHAR(255)) AS review_comment_title,
            CAST(NULLIF(REPLACE(LTRIM(RTRIM(b.review_comment_message)), '"', ''), '') AS VARCHAR(MAX)) AS review_comment_message,

            TRY_CONVERT(DATETIME2(0), NULLIF(REPLACE(LTRIM(RTRIM(b.review_creation_date)), '"', ''), '')) AS review_creation_date,
            TRY_CONVERT(DATETIME2(0), NULLIF(REPLACE(LTRIM(RTRIM(b.review_answer_timestamp)), '"', ''), '')) AS review_answer_timestamp,

            ROW_NUMBER() OVER (
            PARTITION BY NULLIF(REPLACE(LTRIM(RTRIM(b.review_id)), '"', ''), '')
            ORDER BY
                CASE WHEN b.review_answer_timestamp IS NOT NULL AND LTRIM(RTRIM(b.review_answer_timestamp)) <> '' THEN 0 ELSE 1 END,
                TRY_CONVERT(DATETIME2(0), NULLIF(REPLACE(LTRIM(RTRIM(b.review_answer_timestamp)), '"', ''), '')) DESC,
                TRY_CONVERT(DATETIME2(0), NULLIF(REPLACE(LTRIM(RTRIM(b.review_creation_date)), '"', ''), '')) DESC
        ) AS rn
        FROM bronze.olist_order_reviews_dataset_raw b
        WHERE
        NULLIF(REPLACE(LTRIM(RTRIM(b.review_id)), '"', ''), '') IS NOT NULL
            AND NULLIF(REPLACE(LTRIM(RTRIM(b.order_id)), '"', ''), '') IS NOT NULL
    )
INSERT INTO silver.olist_order_reviews_clean
    (
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
    )
SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp
FROM r
WHERE rn = 1;

DECLARE @rows_reviews INT = @@ROWCOUNT;
PRINT CONCAT('Order reviews fact inserted (dedup): ', @rows_reviews);


    COMMIT;
    PRINT '02_load_silver completed succesfully';
 END TRY 
 BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK;

    -- Error-messages for debugging
    PRINT '02_load_silver FAILED';
    PRINT CONCAT('ErrorNumber = ', ERROR_NUMBER(),
                 ' Severity = ', ERROR_SEVERITY(),
                 ' State = ', ERROR_STATE(),
                 ' Line = ', ERROR_LINE(),
                 ' Procedure = ', COALESCE(ERROR_PROCEDURE(), '(ad-hoc)'));
    PRINT CONCAT('Message = ', ERROR_MESSAGE());

    THROW; -- Make the failure visible
END CATCH;















































