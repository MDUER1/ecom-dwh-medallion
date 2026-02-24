
# Silver Layer Theory - Creation of the Silver Tables

In this document i discuss the relevant theory, and the theory behind my silver layer.

## Purpose of The Silver Layer

The purpose of the silver layer is to represent the cleaned, and structurally validated version of the raw bronze level data.

Some of the primary things we do are
- Correcting data types
- Columns are standardized
- Primary keys are enforced
- Basic data integrity rules are applied
- Duplication protection is implemented where relevant

In this layer we are not going to do any dimensional modeling, that happens in the gold layer.

## Discussing General Theory

### Parent and Child Tables

A parent table is a table whose primary key is referenced by another table.

A child table is a table that contains a foreign key referencing a parent table.

Example: customers (parent) -> orders (child of customers, parent of order_items) -> order_items (child)

The important take-away is that relationships are relative. Thus a parent in one relationship, can be a child in another.

### 1:M Relationships

A 1:M (one-to-many) relationship means, that one row in the parent table can be associated with multiple rows in the child table.

Example: One order can have multiple order items. This is the reason why child tables require composite natural keys.

### Primary Key

A Primary Key (PK) is a constraint that uniquely identifies each row in a table. It's properties is that it must be unique, cannot by null, one one PK per table, can consist of one or more columns (composite key) and it automatically creates a unique index.

### Natural Keys vs Composite Keys

Natural Keys: A column that uniquely identifies a row using business logic. Typically customer_id, order_id, review_id and so on.

Natural keys are already sufficient to be labeled as primary keys, as they are unique and identify the rows without confusion.

Composite Key: This is a key that is formed by combining multiple columns to uniquely identify a row. Example (order_id, order_item_id). Neither column alone is unique but together they form unique combinations that identify rows.

This key dows not create new columns, but create an index enforcing uniqueness. This protects the data integrity, and is typically combined with a surrogate key.

### Surrogate Keys

A surrogate key (SK) is an artifical generated key used as the primary key. Example order_items_sk INT IDENTITY(1,1) PRIMARY KEY

This key has no business meaning, and is purely used for technical purpose and perfomance benefits because of its small size.

SK's are used in Kimball-style data warehousing and its advantages are smaller indexes, because it uses INT instead of other datatypes like VARCHAR with larger sizes. This creates faster joins, stable architecture and cleaner dimensional modeling later in the gold layer. To prevent duplicates we combine it with a composite key.

## Index Theory

An index is a datastructure (typically a B-tree) that makes it possible for SQL to search, sort and make JOINs faster since indexes prevents SQL from scanning the whole table, but instead seeks directly to the sought after value.

### Clustered index

"Clustered" means that the table itself is the index. The rows are physically sorted after the clustered key. This is why you can only have one clustered index per tabel, because it orders the data in a specific order. Primary Keys create a clustered index with it being the clustered key where the table is ordered after it. This means that SQL can go directly down the B-tree structure and find the row your are looking for when doing a WHERE clause. 

Clustered is useful on columns used for JOINs, that are unique and values that dont change. A bad clustered key would be a composite key since this needs more memory with indexes becoming larger, therefor SK's are often more practical.

### Nonclustered index

A non-clustered index, is a separate datastructure that points to the correct row. It contains an index key and points to a clustered key. You can have multiple non-clustered indexes.

## B-Tree

All indexes in SQL are B-Trees, and they act like a "table-of-contents" where instead of reading every row/page it searches up in alphabetical order, finds the section, then page and name. This is way faster than scanning all pages with the phone book analogy. All non-clustered indexes includes their key and the clustered key as a pointer. Thus if the clustered key is broad, like a composite key, then all indexes are broader since the pointer now includes multiple columns.

You can view a non-clustered index as a separate B-Tree with an index key and pointer to the clustered key. The index key is what the index is sorted by "CREATE NONCLUSTERED INDEX ix_order_items_order_id ON order_items(order_id)" where order_id is the index key. This means the B-Tree is built and sorted on order_id. This is what the index organizes after, and acts like the search column. This is what enables SQL to do quicker joins and find specific order_id's. The pointer tells SQL where the actual row is for the value found, since a nonclustered index does not store the full row. It only stores the index key and pointer, so the index key finds the value sought after, and the pointer points the row for that value, which makes it super important that the pointer is unique. Thus the pointer is the clustered key value if the table has a clustered index, and thus typically the PK.

If the table is a heap (no clustered index), then the pointer is a physical row locator. Thus the process goes; Step 1: Seek in the non-clustered index to find matching keys, Step 2: Use pointer (clustered key) to go to clustered index and step 3: Retrieve the full row. Step 2 through 3 is called a Key Lookup.

### Covering Index

A covering index is a non-clustered index, that has all the columns a query needs, so SQL doesn't have to do a key lookup. Normaly the process go -> seek in nonclustered index, and after it does a lookup in the clustered index to get the rest. But a covering index only seeks, and then it's done. This means a covering index is optimized for a specific query pattern, and thus we use it selectively in the gold layer instead for better preformance when doing specific queries for analytics.

### Silver Index Strategy

For Parent-tables we always create a PK on the natural key, which creates a clustered index around it, as the clustered key.

We may also do a non-clustered index on the columns that we know may be used for joining.

For Child-tables that have a 1:M relation to their Parent-table, we always create a non-clustered index for the FK, because we use it for joins. We chose the Pk to a composite key or SK combined with the unique natural composite key. Remember we typically use a SK as a FK because it is small, so faster preformance.

### Silver Design Strategy

Silver is not yet dimensional modeling, therefor Parent tables use their natural primary keys, and child tables use a SK as their PK with a UNIQUE constraint on natural composite key to prevent duplicates.


# Silver Layer Theory - Loading the Silver Layer

As a rule of thumb, when we are going to load the Silver Layer we are going to clean up the data by
- Fixing whitespace/empty strings
- Casting/standardization
- Safe casting
- Dedup
- Format-normalizing

80% of loading failures can be explained by bad casts, missing keys or PK duplicates. The script needs to be stable and reusable, which is we we use at TRUNCATE + LOADING strategy as a reloading mechanism, and use TRAN, COMMIT and ROLLBACK to make sure if one table fails to load, the whole load is rolled back to not get inconsistent loads where only some of the tables get loaded. To Catch these errors we use TRY and CATCH, which tries to create bugs and errors to check if the load is stable.

When standardizing we typically go through a protocol of TRIM, NULLIF, UPPER/LOWER, and making sure IDs are the same length, all to avoid JOIN-errors, and different types of ways to express the same data, like 'sp' 'Sp' 'SP' and better preformance a bit.

We use the 'SET NOCOUNT' to avoid getting the 'X rows affected' message and 'SET XACT_ABORT' that makes sure if a statement fails the whole transaction stops automatically, to make the system more robust.

We run metadata like declaring run_id, load_dts to get better tracking, debugging and logging capabilities. 

We load the dimensions before facts, bevause facts refer to dims and it thus follows a more logical structure.

We follow the ACID-principle
- Atomicity (All-or-Nothing)
- Consistency
- Isolation
- Durability

## The Professional ELT Transaction Pattern to insure ACID
It looks like:

SET NOCOUNT ON;        -- Prevents "(X rows affected)" noise

SET XACT_ABORT ON;     -- Automatically invalidates transaction on runtime error

BEGIN TRY

    BEGIN TRAN;        -- Start atomic unit of work

    ----------------------------------------------------------
    -- 🔹 ETL LOGIC START
    ----------------------------------------------------------

    -- Example:
    -- INSERT INTO silver.table1 ...
    -- INSERT INTO silver.table2 ...
    -- UPDATE silver.table3 ...

    ----------------------------------------------------------
    -- 🔹 ETL LOGIC END
    ----------------------------------------------------------

    COMMIT;            -- Persist changes if no error occurred

END TRY

BEGIN CATCH

    -- If a transaction is still open, roll it back
    IF @@TRANCOUNT > 0
        ROLLBACK;

    -- Re-throw the original error to fail the job properly
    THROW;

END CATCH;

Explained: 

1. SET XACT_ABORT ON: This tells SQL that if there is a runtime error, to automatically mark the transaction as aborted, so that i can't be commited. This works mostly as a safety net, because the pattern already takes care of the runtime error.

2. BEGIN TRY: This starts the error-handling, and if a runtime error happens inside the transaction, it jumps to execute CATCH. This change the control flows behavior, and not directly the transaction like XACT_ABORT does.

3. BEGIN TRAN: This starts the transaction, and in here alle data changes are temporary and not permanent until COMMIT. This means they can be undone with ROLLBACK.

4. Now comes the T-logic with INSERT, UPDATE, DELETE and so on through BEGIN TRAN. Here everything executes normally, but if a runtime error happens, then TRY makes the CONTROL FLOW jump to catch and execute it, with XACT_ABORT marking the transaction as invalid.

5. COMMIT: This only runs if there occurs no errors in TRY, and makes all the changes go from temporary to permanent. After commit, the transaction ends, and no ROLLBACK is possible. If XACT_ABORT has marked the transaction as doomed, then the COMMIT fails.

6. END TRY: This marks the end of the protected logic. If the TRY does not catch any erros, then the execution ends here, and skips BEGIN CATCH. But if a runtime error happens, then begin catch activates. 

7. BEGIN CATCH: This runs only if a runtime error occured inside TRY, and puts the control flow in error handling mode.

8. @@TRANCOUNT: This is critical, and is a system variable that tells you how many active active transactions exist. So we check to see if a transaction is stille open, and if it is, it must be rolled back because it thus contains an error. We do this because we can't throw back a transaction that ins't there. This is defensive programming.

9. ROLLBACK: If a transaction is open, then undo all changes since BEGIN TRAN. This restores database consistency. Even with XACT_ABORT ON you still need to ROLLBACK to close the faulty transaction.

10. THROW: This is extremely important, as it Re-raises the original error. When an ERROR is caught by TRY, then CATCH runs which activates THROW which is the command that lets you know that you ran into an error. Without it the failure wouldn't be recognized and the control flow would just end like normal.

11. Meta-Data: We declare a local variable @run_id followed by its type UNIQUEIDENTIFIER which we equal to the function NEWID() that generates a new unique value. Thus we create a unique ID for the specific execution of the script. This makes it, so we can log it and track it. We also delcare a local variabel @load_dts of type DATETIME2(0) which we equal to SYSDATETIME(); which is a system function that stores the exact timestamp when the load started. This gives the execution a timestamp so we can track it.

In general, we load dimensions before, which are the tables that refer to the facts, where facts are tables with actual data and measurements while descriptions like city, state and so on are dimensions, because when we have loaded dims, then we can check for orphans, like orders without customers.


# Silver Data Quality Checks

This is where we make sure that the quality of the data is good, before moving to the Gold Layer. Here we do no data transformations, only measurements and stop the pipeline if one of the critical checks fail, to prevent bad data from reaching the gold layer.

The main things we check are the structure, relational integrity, and if all the data has been loaded from the silver layer.

### Critical checks (0-tolerance)

This goes for almost all tables, and these checks are for PK uniqueness, if NOT NULL columns are NOT NULL and checking if all the rows have been loaded from the bronze to silver layer probably. 

### Relationship Checks

Here we check if the relationship between the tables are good, like orders to customers to see if we have any missing customers.

### Sanity checks

These checks are only for relevant columns, and are checks for negative values, out-of-range numbers, date logic and so on.

## How the loading and checking of silver layers co-exist

Before loading the data, we are going to understand it first by querying it and looking at it. This is called profiling the data, and here we are going to look for the classics like whitespace, duplicates, NULLs and so on. We use this to decide what we are going to correct in the silver layer, and we are going to check the more structural and critical thing that is a must to fix before continuing to the gold layer.

### Baseline-cleaning & profiling data

When loading the silver tables, we are always doing a baseline cleaning to make sure to standardize and format the data.

Baselinecleaning typically consists of TRIM, removing quotes when handling csv-files, NULLIF empty strings and CAST to match the data to the datatypes we implemented in 01 when we defined the table-structure. We typically use TRY_CAST in silver, so if we have a value that cant be casted, then it just becomes a NULL instead of rolling back the entire transaction by using CAST.

We also filter any potential rows away, that have NULLs in NOT NULL rows, like PKs.





















