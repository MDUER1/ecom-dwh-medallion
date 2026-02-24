# Naming conventions

This document defines naming conventions used across the data warehouse, to ensure consistency, readability and maintainability.

## Datawarehouse Structure

This project follows the Medallion Architecture with three layers consisting of bronze -> silver -> gold:
* Bronze: Raw ingested data (no transformation)
* Silver: Cleaned and transformed data
* Gold: Finished, analytics-ready and usable data

## Table names

### Raw tables (bronze)
* Format: `[name]_raw`
* Example: `orders_raw`

### Clean tables (silver)
* Format: `[name]_clean`
* Example: `orders_clean`

### Fact tables (gold)
* Format: `fact_[name]`
* Example: `fact_orders`

### Dimension tables (gold)
* Format: `dim_[name]`
* Examples: `dim_customers`, `dim_products`, `dim_date`

### Views (gold)
* Format: `vw_[name]`
* Example: `vw_customer_metrics`

### Bridge / junction tables (gold)
(Used for many-to-many relationships)
* Format: `bridge_[name1]_[name2]`
* Example: `bridge_product_categories`

## Column names

### Primary keys
* Format: `[name]_id`
* Example: `order_id`

### Foreign keys
* Format: same as referenced dimension primary key
* Examples: `customer_id`, `product_id`

### Date & timestamps
* Dates: `[name]_date`
* Timestamps: `[name]_at`
* Examples: `order_date`, `created_at`

### Amounts & metrics
* Amounts: `[name]_amount`
* Counts: `[name]_count`
* Examples: `order_amount`, `order_count`

### Booleans
* Format: `is_[state]`
* Examples: `is_active`, `is_delivered`

### Indexes; non-clustered & unique
One Column Format: `ix_<table>_<column>`
Example: ix_olist_customers_clean_order_id

Composite Format: `ix_<table>_<col1>_<col1>`
Example: ix_olist_order_items_clean_order_id_product_id

Unique Index: `ux_<table>_<column>`

### Constraints
Primary Key Format: `pk_<table>`
Example: pk_olist_customers_clean

Foreign Key Format: `fk_<child_table>_<parent_table>`
Example: fk_olist_orders_clean_customer



## SQL style
- `snake_case` is used for all identifiers (e.g. `order_id`, `customer_id`, `order_amount`)
- SQL keywords are written in lowercase for readability
- Always use explicit table aliases, especially when joining tables
- Avoid using `select *` in final queries
- Window functions should always include explicit `order by`
- Refrence Primary Keys as PK and Foreign Keys as FK.
- Start all scripts with a good description.

## Other
- All empty strings are converted to NULLs to keep the representation for missing data consistent.
