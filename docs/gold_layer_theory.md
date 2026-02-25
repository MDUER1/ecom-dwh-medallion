
# Gold layer theory (Kimball)
The Gold Layer is the analytics layer, where its job is to present data in a business-friendly, query-efficient structure (typically a star schema) that supports BI, reporting, dashboards and ad-hoc analysis.

## Kimball Dimensional Modeling
Kimball data modeling is a dimensional modeling approach for building data warehouses, introduced by Ralph Kimball.

**It structures data into**
- Fact tables -> numeric measurements of business processes
- Dimension tables -> descriptive context for those measurements

This is what creates a star schema, where the fact table sits in the center, with dimensions that describes it surrounds it like a star. Here the goal is analytical preformance, simplicity and business clarity.

## Philosophy Behind Kimball
We can separate Kimball's ideas into four core ideas, based on:

### 1. Model Around Business Processes
Instead of modeling data based on source systems like CRMs, ERPs and so on, we are modeling after what business event we are measuring. Good examples could be sales, reviews, shipment and so on. Each of these business process becomes a fact table.

### 2. Declare the Grain First
Here the grain means the atomic level of measurement, which for us is what exactly does one row in this table represent? This question needs to be answered before we begin designing the columns, measurements and create keys. We need to know this in order to do aggregations correctly.

### 3. Separate Facts and Dimensions
Fact tables contain numeric measurements like price, quantity, revenue and foreign keys to dimensions. They answer questions like how much, how many and how long.
Dimension tables contain descriptive attributes like names, categories, locations and so on. These answer questions like who, what, where and when. 

This separation creates simpler joins, and clear semantics that make analytics easier.

### 4. Use Surrogate Keys
Dimensions use a surrogate key, which is an internal dwh key, and a business key from the source. It does this because business keys like order_id, customer_id and so on changes and surrogate keys created in the dwh for joins allow for history tracking, stable joins and preformance optimization.

## What is Kimball used for?
We typically use Kimball Dimensional Modeling for businesses and others that need their data to be displayed by dashboards or through reporting and BI systems. This is the reason why i chose Kimball Dimensional Modelling, because it creates the optimal foundation for analytical queries, revenue metrics and customer behavior analysis. This would not be optimal for stuff like ML or OLTP.

## Conformed Dimensions
A conformed dimension means, that we have the same surrogate key, same business definition, attributes and it is reused across multiple fact tables like dim_customers is used to describe fact tables like fact_payment, fact_review, fact_order and so on. This makes the data modeling scalable because we can add new facts without redesigning dimensions, thus giving us the ability to integrate new data sources and maintaining consistent analytics.

## Keys in Gold
In Kimball, dimensions have
- a surrogate key: customer_key INT IDENTITY
- a business key: customer_id (from source)

Facts store surrogate keys, not raw source ids like
- customer_key
- product_key
- ...

These we will use as said before, for stable joins and better preformance.

## Slowly Changing Dimensions (SCD)
For dimensions, you must decide if you need history or not, because they have a problem with them. They change over time, as customers, products, geography and so on changes. So by always overwriting old values we loose history, which breaks historical analysis. We have two main ways of dealing with them, being

Type 1 - Overwrite (No history)
- When attributes changes, we need to update the row and overwrite the old value with the new. The pros are that it is simple, with easy joins and smaller dimensions. The con being we have no history.

Type 2 - Full History (Pro)
- Instead of updating, we expire the old row and insert a new row. We typically do this by adding a valid_from, valid_to, is_current to a row, to link facts to the correct version of dimensions based on date, which keeps the history. This allows for more creative and complex analytical queries, but is more complicated and bigger. But is preferred by the pros.

We are in this project, going to focus on simplicity and use Type 1.


## Views vs Tables
A view is a stored SELECT statement, and it does not store data by default. You typically use views in the Gold Layer because views are superior in many cases. These being security, as you can deny access to fact tables and only grant access to views for BI users. 

This protects the columns, metadata and so on. When facts and so on changes, you can ust update the view and the dashboards dont break. By creating certain SELECT statements, like calculating revenue, you allow everyone to use the select statement without being comfortable with SQL which is used alot in big companies.

You can also determine what columns and in general just data you want to display for the end users, which is one of the biggest security reasons. This is the reason why i use views.

## Gold Checks
We are going to be adding sanity checks for the Gold Layers aswell as we have done for both bronze and silver. Here we are going to check if fact rows have valid foreign keys to ensure we dont have orphan rows, uniqueness of BK, row counts and stuff like non-negative revenue.

## Perfomance
We are also going to look for performance boosting our queries by using clustered PKs, indexes, partitioning and so on, even though we have taken care of most of it in the silver layer.






















