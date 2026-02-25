/*
Script name: 01_create_gold_tables.sql

Purpose:
    - Create Gold layer dimensional model objects (Kimball star schema).
    - Define conformed dimensions and fact table for analytics workloads.

Description:
    - Creates dimension tables with surrogate keys and business keys, because SK's are faster at joining (small size).
    - Creates fact_orders at order-line grain (one row per order_id + order_item_id).
    - Adds PK, FK, unique constraints and join indexes.
    - Adds unique constraints to make sure BK's doesn't create duplicates and ensures correct grain.
    - We add automatic idexes, behind UNIQUE constraints
    - Geolocation is not included

Execution:
    - Run after Silver layer is created and loaded.
    - Safe to rerun in development; existing Gold objects are dropped and recreated.
*/

use brazilian_ecommerce_dw;
go

-- Creates an idempotent pipline, be creating a rerunning mechanism by deleting the existing tables before it runs.
if object_id('gold.fact_orders', 'U') is not null drop table gold.fact_orders;
if object_id('gold.dim_date', 'U') is not null drop table gold.dim_date;
if object_id('gold.dim_customers', 'U') is not null drop table gold.dim_customers;
if object_id('gold.dim_products', 'U') is not null drop table gold.dim_products;
if object_id('gold.dim_sellers', 'U') is not null drop table gold.dim_sellers;
go

-- We are creating a date-dimension, gets filled by a CTE in date_source.
create table gold.dim_date
(
    date_key int not null,
    full_date date not null,
    [year] smallint not null,
    [month] tinyint not null,
    [day] tinyint not null,
    month_name varchar(15) not null,
    day_name varchar(15) not null,
    quarter_number tinyint not null,
    is_weekend bit not null,

    constraint pk_dim_date primary key (date_key),
    constraint ux_dim_date_full_date unique (full_date)
);
go

create table gold.dim_customers
(
    customer_key int identity(1,1) not null,
    -- Identity(1,1) auto-generates the key, by adding 1's.
    customer_id char(32) not null,
    customer_unique_id char(32) not null,
    customer_zip_code_prefix char(5) null,
    customer_city varchar(100) null,
    customer_state char(2) null,

    constraint pk_dim_customers primary key (customer_key),
    --SK's for joins
    constraint ux_dim_customers_customer_id unique (customer_id)
);
go

create table gold.dim_products
(
    product_key int identity(1,1) not null,
    product_id char(32) not null,
    product_category_name varchar(50) null,
    product_category_name_english varchar(50) null,
    product_name_length int null,
    product_description_length int null,
    product_photos_qty tinyint null,
    product_weight_g int null,
    product_length_cm int null,
    product_height_cm int null,
    product_width_cm int null,

    constraint pk_dim_products primary key (product_key),
    constraint ux_dim_products_product_id unique (product_id)
);
go

create table gold.dim_sellers
(
    seller_key int identity(1,1) not null,
    seller_id char(32) not null,
    seller_zip_code_prefix char(5) null,
    seller_city varchar(50) null,
    seller_state char(2) null,

    constraint pk_dim_sellers primary key (seller_key),
    constraint ux_dim_sellers_seller_id unique (seller_id)
);
go

-- Grain is a composite key (order_id, order_item_id),
create table gold.fact_orders
(
    fact_order_key bigint identity(1,1) not null,
    order_id char(32) not null,
    order_item_id int not null,

    customer_key int not null,
    product_key int not null,
    seller_key int not null,

    order_date_key int not null,
    shipping_limit_date_key int null,
    approved_date_key int null,
    delivered_date_key int null,
    estimated_delivery_date_key int null,

    order_status varchar(20) null,
    quantity int not null,
    item_amount decimal(12,2) null,
    freight_amount decimal(12,2) null,
    total_amount decimal(12,2) null,
    allocated_payment_amount decimal(12,2) null,
    review_score decimal(5,2) null,

    constraint pk_fact_orders primary key (fact_order_key),
    constraint ux_fact_orders_order_line unique (order_id, order_item_id),
    --The composite key

    -- Creates a FK constraint, and refrences the PK for joins with dimensions
    constraint fk_fact_orders_dim_customers foreign key (customer_key)
        references gold.dim_customers(customer_key),
    constraint fk_fact_orders_dim_products foreign key (product_key)
        references gold.dim_products(product_key),
    constraint fk_fact_orders_dim_sellers foreign key (seller_key)
        references gold.dim_sellers(seller_key),
    constraint fk_fact_orders_dim_date_order_date foreign key (order_date_key)
        references gold.dim_date(date_key),
    constraint fk_fact_orders_dim_date_shipping_date foreign key (shipping_limit_date_key)
        references gold.dim_date(date_key),
    constraint fk_fact_orders_dim_date_approved_date foreign key (approved_date_key)
        references gold.dim_date(date_key),
    constraint fk_fact_orders_dim_date_delivered_date foreign key (delivered_date_key)
        references gold.dim_date(date_key),
    constraint fk_fact_orders_dim_date_estimated_delivery_date foreign key (estimated_delivery_date_key)
        references gold.dim_date(date_key)
);
go

-- Creates a non-clustered index for the FK's for faster joins.
create nonclustered index ix_fact_orders_customer_key
on gold.fact_orders (customer_key);
go

create nonclustered index ix_fact_orders_product_key
on gold.fact_orders (product_key);
go

create nonclustered index ix_fact_orders_seller_key
on gold.fact_orders (seller_key);
go

create nonclustered index ix_fact_orders_order_date_key
on gold.fact_orders (order_date_key);
go











