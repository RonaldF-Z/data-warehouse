/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

------------------------------------------------------------
--CHECK FOR TABLE silver.crm_cust_info
------------------------------------------------------------

--Check for duplicate or null in cst_id
--Expectation: No values returned
select cst_id,count(*) ctn_cst_id from silver.crm_cust_info
group by cst_id
having count(*)>1 or cst_id is null;

--Check for unwanted spaces
--Expectation: No values returned
select cst_key from silver.crm_cust_info
where cst_key!=trim(cst_key);

select cst_firstname from silver.crm_cust_info
where cst_firstname!=trim(cst_firstname);

select cst_lastname from silver.crm_cust_info
where cst_lastname!=trim(cst_lastname);

select cst_marital_status from silver.crm_cust_info
where cst_marital_status!=trim(cst_marital_status);

select cst_gndr from silver.crm_cust_info
where cst_gndr!=trim(cst_gndr);

--Data Standardization and Consistency
select distinct cst_marital_status from silver.crm_cust_info;

select distinct cst_gndr from silver.crm_cust_info;

------------------------------------------------------------
--CHECK FOR TABLE silver.crm_prd_info
------------------------------------------------------------

--Check for duplicate or null in prd_id
--Expectation: No values returned
select prd_id,count(*) ctn_prd_id from silver.crm_prd_info
group by prd_id
having count(*)>1 or prd_id is null;

--Check for unwanted spaces
--Expectation: No values returned
select prd_key from silver.crm_prd_info
where prd_key!=trim(prd_key);

select prd_nm from silver.crm_prd_info
where prd_nm!=trim(prd_nm);

select prd_line from silver.crm_prd_info
where prd_line!=trim(prd_line);

--Check for invalid numbers (not null or negative numbers)
select * from silver.crm_prd_info
where prd_cost<0 or prd_cost is null;

--Data Standardization and Consistency
select distinct prd_line from silver.crm_prd_info;

--Check for invalid dates
select * from silver.crm_prd_info
where prd_start_dt>prd_end_dt

------------------------------------------------------------
--CHECK FOR TABLE silver.crm_sales_details
------------------------------------------------------------

--Check for duplicate or null in sls_ord_num, in this table, sls_ord_num is not unique due to one customer can order many diferents products in the same order.
--Expectation: No values returned
select * from silver.crm_sales_details
where sls_ord_num is null

--Check for unwanted spaces
--Expectation: No values returned
select sls_prd_key from silver.crm_sales_details
where sls_prd_key!=trim(sls_prd_key);

--Check for prd_key if not exists in silver.prd_info
--Expectation: No values returned
select sls_prd_key from silver.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info);

--Check for cst_id if not exists in silver.cust_info
--Expectation: No values returned
select sls_cust_id from silver.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info);

--Check for invalid dates
select * from silver.crm_sales_details
where sls_order_dt not between 19000101 and 20260101;

select * from silver.crm_sales_details
where sls_ship_dt not between 19000101 and 20260101;

select * from silver.crm_sales_details
where sls_due_dt not between 19000101 and 20260101;

select * from silver.crm_sales_details
where sls_order_dt>sls_ship_dt or sls_order_dt>sls_due_dt;

--Check for invalid numbers
select sls_quantity from silver.crm_sales_details
where sls_quantity<=0 or sls_quantity is null;

select sls_price from silver.crm_sales_details
where sls_price<=0 or sls_price is null;

select * from silver.crm_sales_details
where sls_sales<=0 OR sls_price*sls_quantity!=sls_sales or sls_price is null;

------------------------------------------------------------
--CHECK FOR TABLE silver.erp_cust_az12
------------------------------------------------------------

--Check for duplicate or null in prd_id
--Expectation: No values returned
select cid,count(*) ctn from silver.erp_cust_az12
group by cid
having count(*)>1 or cid is null;

--Check for invalid bdate, in this table check just for future dates
select bdate from silver.erp_cust_az12
where bdate > now();

--Check for unwanted spaces and duplicates
select gen from silver.erp_cust_az12
where gen!=trim(gen);

select distinct gen from silver.erp_cust_az12

------------------------------------------------------------
--CHECK FOR TABLE silver.erp_loc_a101
------------------------------------------------------------

--Check for duplicates cid
select cid,count(*) cnt from silver.erp_loc_a101
group by cid
having count(*)>1;

--Data Standardization & consistency
select distinct cntry from silver.erp_loc_a101
order by cntry

------------------------------------------------------------
--CHECK FOR TABLE silver.erp_px_cat_g1v2
------------------------------------------------------------

--Check for duplicate or null in id 
select id,count(*) cnt from silver.erp_px_cat_g1v2
group by id
having count(*)>1 or id is null;

--Check for unwanted spaces
select cat from silver.erp_px_cat_g1v2
where cat!=trim(cat);

select subcat from silver.erp_px_cat_g1v2
where subcat!=trim(subcat);

select maintenance from silver.erp_px_cat_g1v2
where maintenance!=trim(maintenance);

--Check for invalid values
select distinct cat from silver.erp_px_cat_g1v2;

select distinct subcat from silver.erp_px_cat_g1v2;

select distinct maintenance from silver.erp_px_cat_g1v2;
