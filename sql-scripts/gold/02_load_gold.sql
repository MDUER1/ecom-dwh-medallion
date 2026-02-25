/*
Script name: 02_load_gold.sql

Purpose:
    - Load Gold dimensions and fact table from Silver cleaned data.

Description:
    - Reloads dimensions using business keys from Silver.
    - Builds dim_date from all operational dates used by facts.
    - Loads fact_orders at order-line grain and resolves surrogate keys.
    - Allocates order-level payment amount across order lines.
    - Geolocation is not included

Execution:
    - Run after 01_create_gold_tables.sql and successful Silver quality checks.
    - Safe for development reruns (delete + insert pattern).
*/

use brazilian_ecommerce_dw;
go

set nocount on;
-- Stops row-counting message spam
set xact_abort on;
-- If a runtime-error happens, the whole transaction will be rolled back

begin try -- Looks for if a runtime-error occurs, it jumps to CATCH
    begin tran; -- SQL keeps changes temporary until COMMIT, for atomicitet (everything or nothing)
    
    -- Keep the pipeline idempotent where TRUNCATE is not usable because dim_date is refered by a FK.
    delete from gold.fact_orders;
    delete from gold.dim_date;
    delete from gold.dim_customers;
    delete from gold.dim_products;
    delete from gold.dim_sellers;
    dbcc checkident ('gold.fact_orders', reseed, 0);
    dbcc checkident ('gold.dim_customers', reseed, 0);
    dbcc checkident ('gold.dim_products', reseed, 0);
    dbcc checkident ('gold.dim_sellers', reseed, 0);

    ;with
    date_source
    --Collects all dates from silver into ONE big column (full_date), with multiple row values
    as
    (
        -- Select ALL not NULL order_purchase_timestamps
                                            select cast(o.order_purchase_timestamp as date) as full_date
            from silver.olist_orders_clean o
            where o.order_purchase_timestamp is not null

        union
            -- Puts the next dataset of a different type of date under and auto-removes duplicates


            select cast(o.order_approved_at as date) as full_date
            from silver.olist_orders_clean o
            where o.order_approved_at is not null

        union

            select cast(o.order_delivered_customer_date as date) as full_date
            from silver.olist_orders_clean o
            where o.order_delivered_customer_date is not null

        union

            select cast(o.order_estimated_delivery_date as date) as full_date
            from silver.olist_orders_clean o
            where o.order_estimated_delivery_date is not null

        union

            select cast(oi.shipping_limit_date as date) as full_date
            from silver.olist_order_items_clean oi
            where oi.shipping_limit_date is not null
    )
insert into gold.dim_date
    (
    date_key,
    full_date,
    [year],
    [month],
    [day],
    month_name,
    day_name,
    quarter_number,
    is_weekend
    )
select --Runs for every row in the one-column CTE, and spits a unique row for each row with the selected column.
    cast(convert(char(8), ds.full_date, 112) as int) as date_key, --PK used for joins
    ds.full_date,
    datepart(year, ds.full_date) as [year],
    datepart(month, ds.full_date) as [month],
    datepart(day, ds.full_date) as [day],
    datename(month, ds.full_date) as month_name,
    datename(weekday, ds.full_date) as day_name,
    datepart(quarter, ds.full_date) as quarter_number,
    case when datename(weekday, ds.full_date) in ('Saturday', 'Sunday') then 1 else 0 end as is_weekend
from date_source ds;

    insert into gold.dim_customers
    (
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
    )
select
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state
from silver.olist_customers_clean c;

    insert into gold.dim_products
    (
    product_id,
    product_category_name,
    product_category_name_english,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
    )
select
    p.product_id,
    p.product_category_name,
    t.product_category_name_english,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
from silver.olist_products_clean p
    -- LEFT JOIN to enrich the data with the translation name
    left join silver.olist_product_category_name_translation_clean t
    on t.product_category_name = p.product_category_name;

    insert into gold.dim_sellers
    (
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
    )
select
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state
from silver.olist_sellers_clean s;

    -- The CTE's are used to create order-level calculations, to be used in fact_orders to match the grain
    ;with
    order_item_counts
    -- Counts the amount of items per order.
    as
    (
        select
            oi.order_id,
            count(*) as order_item_count
        from silver.olist_order_items_clean oi
        group by oi.order_id
    ),
    order_payments
    -- Sums up the order_payment_amount
    as
    (
        select
            op.order_id,
            sum(coalesce(op.payment_value, 0.00)) as order_payment_amount
        from silver.olist_order_payments_clean op
        group by op.order_id
    ),
    order_reviews
    -- Calculates the avg review score per order
    as
    (
        select
            r.order_id,
            avg(cast(r.review_score as decimal(5,2))) as avg_review_score
        from silver.olist_order_reviews_clean r
        where r.review_score is not null
        group by r.order_id
    )
insert into gold.fact_orders
    (
    order_id,
    order_item_id,
    customer_key,
    product_key,
    seller_key,
    order_date_key,
    shipping_limit_date_key,
    approved_date_key,
    delivered_date_key,
    estimated_delivery_date_key,
    order_status,
    quantity,
    item_amount,
    freight_amount,
    total_amount,
    allocated_payment_amount,
    review_score
    )
select
    oi.order_id,
    oi.order_item_id,
    dc.customer_key,
    dp.product_key,
    ds.seller_key,
    dd_order.date_key,
    dd_ship.date_key,
    dd_approved.date_key,
    dd_delivered.date_key,
    dd_estimated.date_key,
    o.order_status,
    1 as quantity,
    oi.price as item_amount,
    oi.freight_value as freight_amount,
    coalesce(oi.price, 0.00) + coalesce(oi.freight_value, 0.00) as total_amount,
    case
            when oic.order_item_count is null or oic.order_item_count = 0 then null
            else cast(op.order_payment_amount / oic.order_item_count as decimal(12,2))
        end as allocated_payment_amount,
    orv.avg_review_score as review_score
from silver.olist_order_items_clean oi -- We are using JOINs to get the surrogate FKs needed to relate to the dims.
    -- We use inner JOINs when the relation is mandatory, like order_id, customer_id for each orderline.
    inner join silver.olist_orders_clean o
    on o.order_id = oi.order_id
    inner join gold.dim_customers dc
    on dc.customer_id = o.customer_id
    inner join gold.dim_products dp
    on dp.product_id = oi.product_id
    inner join gold.dim_sellers ds
    on ds.seller_id = oi.seller_id
    inner join gold.dim_date dd_order
    on dd_order.full_date = cast(o.order_purchase_timestamp as date)
    -- We use left JOINs when the missing data is okay, so not a must-have like dates.
    left join gold.dim_date dd_ship
    on dd_ship.full_date = cast(oi.shipping_limit_date as date)
    left join gold.dim_date dd_approved
    on dd_approved.full_date = cast(o.order_approved_at as date)
    left join gold.dim_date dd_delivered
    on dd_delivered.full_date = cast(o.order_delivered_customer_date as date)
    left join gold.dim_date dd_estimated
    on dd_estimated.full_date = cast(o.order_estimated_delivery_date as date)
    left join order_item_counts oic
    on oic.order_id = oi.order_id
    left join order_payments op
    on op.order_id = oi.order_id
    left join order_reviews orv
    on orv.order_id = oi.order_id;

    commit; --Make changes permanent
    print '02_load_gold completed successfully.';
end try
begin catch --Runs if runtime-error has been detected in TRY
    if @@trancount > 0 rollback; --Rolls the temporary transaction back

    print concat(
        '02_load_gold failed. ErrorNumber=', error_number(),
        ' Line=', error_line(),
        ' Message=', error_message()
    );

    throw; -- Stops silent failures, be showing the runtime-eror and stops execution marking the job as a failure.
end catch;
go


