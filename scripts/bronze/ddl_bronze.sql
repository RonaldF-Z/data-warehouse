/*
=========================================================================
DDL SCRIPT: CREATE TABLES FOR BRONZE LAYER
=========================================================================
Script Purpose:
	This scripts creates tables for schema named "bronze", in case the table exists in bronze schema, the script also dropping the existing tables and recreate them.
*/

DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
	cst_id int,
	cst_key varchar(30),
	cst_firstname varchar(30),
	cst_lastname varchar(30),
	cst_marital_status varchar(30),
	cst_gndr varchar(30),
	cst_create_date date
);

DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
	prd_id int,
	prd_key varchar(50),
	prd_nm varchar(50),
	prd_cost int,
	prd_line varchar(50),
	prd_start_dt date,
	prd_end_dt date
);

DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num varchar(50),
	sls_prd_key varchar(50),
	sls_cust_id int ,
	sls_order_dt varchar(50),
	sls_ship_dt varchar(50),
	sls_due_dt varchar(50),
	sls_sales int,
	sls_quantity int,
	sls_price int
);

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
	cid varchar(59),
	bdate DATE,
	gen VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
	cid varchar(50),
	cntry varchar(50)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
	id varchar(50),
	cat varchar(50),
	subcat varchar(50),
	maintenance varchar(50)
);
