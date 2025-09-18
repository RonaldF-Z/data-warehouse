CREATE OR REPLACE PROCEDURE silver.load_silver()
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
	RAISE INFO 'Loading the Silver Layer';
	RAISE INFO '===================================';

	RAISE INFO '----------------------------';
	RAISE INFO 'Loading CRM Tables';
	RAISE INFO '----------------------------';

	start_time := clock_timestamp();
	RAISE INFO '>>INSERTING DATA INTO silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	with row_cte as (
		select *, row_number() over(partition by cst_id order by cst_create_date desc) flag_times 
		from bronze.crm_cust_info
		where cst_id is not null
	)
	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)
	select cst_id,
		cst_key,
		trim(cst_firstname) cst_firstname,
		trim(cst_lastname) cst_lastname,
		case when UPPER(TRIM(cst_marital_status))='S' then 'Single'
			when UPPER(TRIM(cst_marital_status))='M' then 'Married'
			else 'n/a' end cst_marital_status,
		case when UPPER(TRIM(cst_gndr))='F' then 'Female'
			when UPPER(TRIM(cst_gndr))='M' then 'Male'
			else 'n/a' end cst_gndr,
		cst_create_date
	from row_cte where flag_times=1;
	end_time := clock_timestamp();
	RAISE INFO '>>Load duration for Table silver.crm_cust_info: %ms',EXTRACT(EPOCH FROM (end_time-start_time))*1000;
	--------------------------------------------------------------------------------------------------
	start_time := clock_timestamp();
	RAISE INFO '>>INSERTING DATA INTO silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	select prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') cat_id,
		SUBSTRING(prd_key,7,LENGTH(prd_key)) prd_key,
		prd_nm,
		COALESCE(prd_cost,0) prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'T' THEN 'Touring'
			WHEN 'S' THEN 'Other Sales'
			ELSE 'n/a' end prd_line,
		prd_start_dt,
		lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)-1 prd_end_dt_fix
	from bronze.crm_prd_info;
	end_time := clock_timestamp();
	RAISE INFO '>>Load duration for Table silver.crm_cust_info: %ms',EXTRACT(EPOCH FROM (end_time-start_time))*1000;
	--------------------------------------------------------------------------------------------------
	start_time := clock_timestamp();
	RAISE INFO '>>INSERTING DATA INTO silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	INSERT INTO silver.crm_sales_details(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity ,
		sls_price
	)
	select sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt not between 19000101 and 20260101 then NULL
			ELSE CAST(CAST(sls_order_dt as VARCHAR(50)) AS DATE) end sls_cust_id,
		CASE WHEN sls_ship_dt not between 19000101 and 20260101 then NULL
			ELSE CAST(CAST(sls_ship_dt as VARCHAR(50)) AS DATE) end sls_cust_id,
		CASE WHEN sls_due_dt not between 19000101 and 20260101 then NULL
			ELSE CAST(CAST(sls_due_dt as VARCHAR(50)) AS DATE) end sls_cust_id,
		CASE WHEN sls_sales<=0 or sls_sales IS NULL or sls_sales!=sls_quantity*ABS(sls_price) THEN sls_quantity*ABS(sls_price)
			ELSE sls_sales end sls_sales,
		sls_quantity,
		CASE WHEN sls_price<=0 or sls_price IS NULL THEN sls_sales/sls_quantity
			ELSE sls_price end sls_sales
	from bronze.crm_sales_details;
	end_time := clock_timestamp();
	RAISE INFO '>>Load duration for Table silver.crm_cust_info: %ms',EXTRACT(EPOCH FROM (end_time-start_time))*1000;
	--------------------------------------------------------------------------------------------------
	start_time := clock_timestamp();
	RAISE INFO '>>INSERTING DATA INTO silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	INSERT INTO silver.erp_cust_az12(
		cid,
		bdate,
		gen
	)
	SELECT CASE WHEN cid like 'NAS%' then SUBSTRING(cid,4,LENGTH(cid))
				ELSE cid end cid,
		CASE WHEN bdate>now() then NULL else bdate end bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
			ELSE 'n/a' end gen
	FROM bronze.erp_cust_az12;
	end_time := clock_timestamp();
	RAISE INFO '>>Load duration for Table silver.crm_cust_info: %ms',EXTRACT(EPOCH FROM (end_time-start_time))*1000;
	--------------------------------------------------------------------------------------------------
	start_time := clock_timestamp();
	RAISE INFO '>>INSERTING DATA INTO silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	INSERT INTO silver.erp_loc_a101(
		cid,
		cntry
	)
	select REPLACE(cid,'-','') cid,
		CASE WHEN TRIM(cntry) IN ('US','USA') then 'United States'
			WHEN TRIM(cntry)='DE' then 'Germany'
			WHEN TRIM(cntry)='' or TRIM(cntry) is null then 'n/a'
			ELSE TRIM(cntry) end cntry
	from bronze.erp_loc_a101;
	end_time := clock_timestamp();
	RAISE INFO '>>Load duration for Table silver.crm_cust_info: %ms',EXTRACT(EPOCH FROM (end_time-start_time))*1000;
	--------------------------------------------------------------------------------------------------
	start_time := clock_timestamp();
	RAISE INFO '>>INSERTING DATA INTO silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	INSERT INTO silver.erp_px_cat_g1v2(
		id,
		cat,
		subcat,
		maintenance
	)
	SELECT id,cat,subcat,maintenance from bronze.erp_px_cat_g1v2;
	end_time := clock_timestamp();
	RAISE INFO '>>Load duration for Table silver.crm_cust_info: %ms',EXTRACT(EPOCH FROM (end_time-start_time))*1000;

	batch_end_time := clock_timestamp();
	RAISE INFO '>>Load duration for Bronze Layer: %ms',EXTRACT(EPOCH FROM (batch_end_time-batch_start_time))*1000;

EXCEPTION
	WHEN others THEN
		RAISE NOTICE '---------------------------';
		RAISE NOTICE '❌ERROR OCCURED DURING LOADING SILVER LAYER';
		RAISE NOTICE 'SQL STATE: %, SQL MESSAGE: %',SQLSTATE,SQLERRM;
		RAISE NOTICE 'EXECUTING ROLLBACK';
		RAISE NOTICE '---------------------------';
END $$;

call silver.load_silver();