CREATE VIEW gold.dim_customers as
select row_number() over(order by c1.cst_id) customer_key,
	c1.cst_id customer_id,
	c1.cst_key customer_code,
	c1.cst_firstname first_name,
	c1.cst_lastname last_name,
	c2.bdate birthdate,
	c3.cntry country,
	case when c1.cst_gndr='n/a' then COALESCE(c2.gen,'n/a')
		else c1.cst_gndr end gender,
	c1.cst_marital_status marital_status,
	c1.cst_create_date create_date
from silver.crm_cust_info c1
left join silver.erp_cust_az12 c2 on c1.cst_key=c2.cid
left join silver.erp_loc_a101 c3 on c1.cst_key=c3.cid;
------------------------------------------------------------------------------

CREATE VIEW gold.dim_products as
select row_number() over(order by p.prd_start_dt,p.prd_id) product_key,
	p.prd_id product_id,
	p.prd_key product_code,
	p.prd_nm product_name,
	p.cat_id category_id,
	c.cat category_name,
	c.subcat subcategory_name,
	c.maintenance,
	p.prd_cost cost,
	p.prd_line product_line,
	p.prd_start_dt start_date
from silver.crm_prd_info p
left join silver.erp_px_cat_g1v2 c on p.cat_id=c.id
where p.prd_end_dt is null; --filtered out for current data
------------------------------------------------------------------------------

CREATE VIEW gold.fact_sales AS
select
	s.sls_ord_num order_code,
	p.product_key,
	c.customer_key,
	s.sls_order_dt order_date,
	s.sls_ship_dt shipping_date,
	s.sls_due_dt due_date,
	s.sls_sales sales_amount,
	s.sls_quantity quantity,
	s.sls_price price
from silver.crm_sales_details s
left join gold.dim_products p on s.sls_prd_key=p.product_code
left join gold.dim_customers c on s.sls_cust_id=c.customer_id;



