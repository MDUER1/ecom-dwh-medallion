/*
Script name: 01_create_database_and_schemas.sql

Purpose: 
    This script uses DDL to initialize the data warehouse database.

Description:
    - Initializes the data warehouse database, to house everything in the data warehouse.
    - Create schemas for the Medallion architecture, to separate data by maturity level, improve security and stay organized.  
    - Provides the foundation for all subsequent DDL and DML scripts.

Execution:
    - Run once during initial setup.
    - Must be executed before any bronze ingestion scripts.

Notes:
    - This script only contains DDL statements.
*/

--  Create database
CREATE database brazilian_ecommerce_dw;
GO

-- Switch to the new database
USE brazilian_ecommerce_dw;
GO

-- Create Medallion Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;


































