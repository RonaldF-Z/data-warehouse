------------------------------------------------------------------
-- gold.dim_customers
------------------------------------------------------------------
WITH CTE AS (
	select c1.cst_id,
		c1.cst_key,
		c1.cst_firstname,
		c1.cst_lastname,
		c1.cst_marital_status,
		c1.cst_gndr,
		c1.cst_create_date,
		c2.bdate,
		c3.cntry
	from silver.crm_cust_info c1
	left join silver.erp_cust_az12 c2 on c1.cst_key=c2.cid
	left join silver.erp_loc_a101 c3 on c1.cst_key=c3.cid
)
select cst_id,count(*) cnt from cte
group by cst_id
having count(*)>1 or cst_id is null

----------------------------------------------------------------
WITH CTE AS (
	select c1.cst_gndr,c2.gen, case when c1.cst_gndr=c2.gen then 1 else 0 end flag_gender
	from silver.crm_cust_info c1
	left join silver.erp_cust_az12 c2 on c1.cst_key=c2.cid
	left join silver.erp_loc_a101 c3 on c1.cst_key=c3.cid
)
select *, case when cst_gndr='n/a' then COALESCE(gen,'n/a') --cst_gndr is the MASTER for gender info
				else cst_gndr end fix_gndr
from cte
where flag_gender=0

--------------------------------------------------------------------
--CHECK FOR QUALITY DATA IN VIEW gold.dim_customers
--------------------------------------------------------------------
select distinct gender from gold.dim_customers



--------------------------------------------------------------------
--gold.dim_products
--------------------------------------------------------------------
with cte as (
	select 
		p.prd_id,
		p.cat_id,
		p.prd_key,
		p.prd_nm,
		p.prd_cost,
		p.prd_line,
		p.prd_start_dt,
		p.prd_end_dt,
		c.cat,
		c.subcat,
		c.maintenance
	from silver.crm_prd_info p
	left join silver.erp_px_cat_g1v2 c on p.cat_id=c.id
)
select prd_id, count(*) cnt from cte
group by prd_id
having count(*)>1 or prd_id is null;

--------------------------------------------------------------------
--gold.fact_sales
--------------------------------------------------------------------
with cte1 as (
	select p.product_key from gold.fact_sales s
	left join gold.dim_products p on s.product_key=p.product_key
)
select * from cte1 where product_key is null


with cte1 as (
	select c.customer_key from gold.fact_sales s
	left join gold.dim_products p on s.product_key=p.product_key
	left join gold.dim_customers c  on s.customer_key=c.customer_key
)
select * from cte1 where customer_key is null
