/*
Script name: 03_views.sql

Purpose:
    - Create business-facing Gold views for BI and analytics consumption.

Description:
    - Exposes curated datasets on top of Gold star schema.
    - Hides surrogate-key complexity from end users.
    - Standardizes key KPIs for downstream dashboards.

Execution:
    - Run after 01_create_gold_tables.sql and 02_load_gold.sql.
    - Safe to rerun (uses create or alter view).
*/

use brazilian_ecommerce_dw;
go

create or alter view gold.vw_order_line_metrics
as
    select
        f.order_id,
        f.order_item_id,
        dd.full_date as order_date,
        dd.[year] as order_year,
        dd.[month] as order_month,
        dd.month_name,
        f.order_status,
        c.customer_id,
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        p.product_id,
        p.product_category_name,
        p.product_category_name_english,
        s.seller_id,
        s.seller_city,
        s.seller_state,
        f.quantity,
        f.item_amount,
        f.freight_amount,
        f.total_amount,
        f.allocated_payment_amount,
        f.review_score
    from gold.fact_orders f
        inner join gold.dim_date dd
        on dd.date_key = f.order_date_key
        inner join gold.dim_customers c
        on c.customer_key = f.customer_key
        inner join gold.dim_products p
        on p.product_key = f.product_key
        inner join gold.dim_sellers s
        on s.seller_key = f.seller_key;
go

create or alter view gold.vw_customer_metrics
as
    select
        c.customer_id,
        c.customer_unique_id,
        c.customer_city,
        c.customer_state,
        count(distinct f.order_id) as order_count,
        count(*) as order_line_count,
        sum(coalesce(f.total_amount, 0.00)) as gross_sales_amount,
        avg(cast(f.total_amount as decimal(12,2))) as avg_order_line_amount,
        avg(cast(f.review_score as decimal(5,2))) as avg_review_score
    from gold.fact_orders f
        inner join gold.dim_customers c
        on c.customer_key = f.customer_key
    group by
    c.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state;
go

create or alter view gold.vw_product_metrics
as
    select
        p.product_id,
        p.product_category_name,
        p.product_category_name_english,
        count(*) as order_line_count,
        sum(coalesce(f.quantity, 0)) as units_sold,
        sum(coalesce(f.total_amount, 0.00)) as gross_sales_amount,
        avg(cast(f.total_amount as decimal(12,2))) as avg_order_line_amount,
        avg(cast(f.review_score as decimal(5,2))) as avg_review_score
    from gold.fact_orders f
        inner join gold.dim_products p
        on p.product_key = f.product_key
    group by
    p.product_id,
    p.product_category_name,
    p.product_category_name_english;
go
