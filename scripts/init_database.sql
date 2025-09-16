/*
=========================================================================
CREATE DATABASE AND SCHEMAS
=========================================================================
Script Purpose:
	This scripts creates new schemas for database named "datawarehouse".
Warning:
  The "datawarehouse" database must have been previously created using the pgadmin tool.
  To avoid errors, after create the database, query tool must be execute right-clicking the database
*/

-- Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
DROP SCHEMA public;
