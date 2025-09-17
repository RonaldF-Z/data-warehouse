/*
=========================================================================
STORE PROCEDURE: LOAD BRONZE LAYER DATA FROM CSV FILES
=========================================================================
Script Purpose:
	This scripts creates the store procedure wich function is load all data stored in external CSV files following the steps:
		- Truncate the bronze table
		- Load the data into the corresponding table
Parameters: 
	None, this store procedure doesn't accept any parameters or return any values
Usage: 
	CALL bronze.load_bronze();
Note:
	Use clause SELECT to check data tables (e.g.: select * from bronze.crm_cust_info) 
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
		start_time timestamp;
		end_time timestamp;
		batch_start_time timestamp;
		batch_end_time timestamp;
BEGIN
	
	batch_start_time := clock_timestamp();
	
	RAISE INFO '===================================';
	RAISE INFO 'Loading the Bronze Layer';
	RAISE INFO '===================================';

	RAISE INFO '----------------------------';
	RAISE INFO 'Loading CRM Tables';
	RAISE INFO '----------------------------';

	start_time := clock_timestamp();
	RAISE INFO '>>Truncating Table: bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;
	RAISE INFO '>>Inserting Table: bronze.crm_cust_info';
	copy bronze.crm_cust_info
	FROM 'C:\temp\dataset\source_crm\cust_info.csv'
	DELIMITER ','
	CSV HEADER;
	end_time := clock_timestamp();
	RAISE INFO '>>Load duration for Table bronze.crm_cust_info: %ms',EXTRACT(EPOCH FROM (end_time-start_time))*1000;

	start_time := clock_timestamp();
	RAISE INFO '>>Truncating Table: bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;
	RAISE INFO '>>Inserting Table: bronze.crm_prd_info';
	copy bronze.crm_prd_info
	FROM 'C:\temp\dataset\source_crm\prd_info.csv'
	DELIMITER ','
	CSV HEADER;
	end_time := clock_timestamp();
	RAISE INFO '>>Load duration for Table bronze.crm_prd_info: %ms',EXTRACT(EPOCH FROM (end_time-start_time))*1000;

	start_time := clock_timestamp();
	RAISE INFO '>>Truncating Table: bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;
	RAISE INFO '>>Inserting Table: bronze.crm_sales_details';
	copy bronze.crm_sales_details
	FROM 'C:\temp\dataset\source_crm\sales_details.csv'
	DELIMITER ','
	CSV HEADER;
	end_time := clock_timestamp();
	RAISE INFO '>>Load duration for Table bronze.crm_sales_details: %ms',EXTRACT(EPOCH FROM (end_time-start_time))*1000;


	RAISE INFO '----------------------------';
	RAISE INFO 'Loading ERP Tables';
	RAISE INFO '----------------------------';

	start_time := clock_timestamp();
	RAISE INFO '>>Truncating Table: bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;
	RAISE INFO '>>Inserting Table: bronze.erp_cust_az12';
	copy bronze.erp_cust_az12
	FROM 'C:\temp\dataset\source_erp\cust_az12.csv'
	DELIMITER ','
	CSV HEADER;
	end_time := clock_timestamp();
	RAISE INFO '>>Load duration for Table bronze.erp_cust_az12: %ms',EXTRACT(EPOCH FROM (end_time-start_time))*1000;

	start_time := clock_timestamp();
	RAISE INFO '>>Truncating Table: bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;
	RAISE INFO '>>Inserting Table: bronze.erp_loc_a101';
	copy bronze.erp_loc_a101
	FROM 'C:\temp\dataset\source_erp\loc_a101.csv'
	DELIMITER ','
	CSV HEADER;
	end_time := clock_timestamp();
	RAISE INFO '>>Load duration for Table bronze.erp_loc_a101: %ms',EXTRACT(EPOCH FROM (end_time-start_time))*1000;

	start_time := clock_timestamp();
	RAISE INFO '>>Truncating Table: bronze.erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	RAISE INFO '>>Inserting Table: bronze.erp_px_cat_g1v2';
	copy bronze.erp_px_cat_g1v2
	FROM 'C:\temp\dataset\source_erp\px_cat_g1v2.csv'
	DELIMITER ','
	CSV HEADER;
	end_time := clock_timestamp();
	RAISE INFO '>>Load duration for Table bronze.erp_px_cat_g1v2: %ms',EXTRACT(EPOCH FROM (end_time-start_time))*1000;

	batch_end_time := clock_timestamp();
	RAISE INFO '>>Load duration for Bronze Layer: %ms',EXTRACT(EPOCH FROM (batch_end_time-batch_start_time))*1000;
	
EXCEPTION
	WHEN others THEN
		RAISE NOTICE '---------------------------';
		RAISE NOTICE '❌ERROR OCCURED DURING LOADING BRONZE LAYER';
		RAISE NOTICE 'SQL STATE: %, SQL MESSAGE: %',SQLSTATE,SQLERRM;
		RAISE NOTICE 'EXECUTING ROLLBACK';
		RAISE NOTICE '---------------------------';

END;
$$;

CALL bronze.load_bronze();

select * from bronze.crm_cust_info;
select * from bronze.crm_prd_info;
select * from bronze.crm_sales_details;
select * from bronze.erp_cust_az12;
select * from bronze.erp_loc_a101;
select * from bronze.erp_px_cat_g1v2;
