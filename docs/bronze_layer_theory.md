# Bronze Layer Theory

Here i'm going to discuss the Bronze Layer theory that went into creating the Bronze Layer for the Medallion Structure.

## What the Bronze Layer is responsible for
The bronze layers job is to ingest the raw data, and thus to keep it as it is from the source. We are doing this be creating all datatypes as long VARCHARS to not alter the data, and thus preserve the traceability of it if a problem occurs. 

## Creation and depth of the bronze layer
We create the bronze layer, by creating a bronze schema to seperate the layers adding security and structure to the dwh. We have three sql-scripts, with one initializing the tables into the dwh, while the other loads it afterwards. The last makes a simple row-count to make sure every row has been loaded in properly. 

## Keeping the pipeline idempotent
Idempotency is critical for pipelines, which means that it creates the same result, despite running multiple times. We keep the pipeline idempotent by using TRUNCATE + BULK INSERT as a data reloading strategy, thus keeping the bronze layer idempotent.

















































