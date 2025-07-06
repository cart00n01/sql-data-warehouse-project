/*
===========================================================================
DDL Script : Create Gold Views
===========================================================================
Script Purpose;
	This script creates views for the Gold layer in the data warehoud.
	The Gold layer represents the final dimension and fact tables (Star Schema)

	Each view performs transformations and combines data from the Silver layer 
	to produce a lean, enriched, and business-ready dataset.

 Usage:
	-These views can be queried directly for analytics and reporting.
*/

---====================================================================
-- Create Dimension: gold.dim_customers
--=====================================================================

IF OBJECT_ID('gold.dim_customers','U') IS NOT NULL
	DROP TABLE gold.dim_customers;

GO

CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,--Surrogate key
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr --CRM is the master for gender info
		ELSE COALESCE(gen,'n/a') 
	END AS gender,
	ca.bdate [birthdate],
	ci.cst_create_date AS create_date
FROM Silver.crm_cust_info ci
LEFT JOIN Silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN Silver.erp_loc_a101 la
ON ci.cst_key = la.cid


---====================================================================
-- Create Dimension: gold.dim_products
--=====================================================================

IF OBJECT_ID('gold.dim_products','U') IS NOT NULL
	DROP TABLE gold.dim_products;

GO

CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt,pn.prd_key) AS product_key,--Surrogate key
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost cost,
	pn.prd_line [product_line],
	pn.prd_start_dt AS start_date
FROM Silver.crm_prd_info AS pn
LEFT JOIN Silver.erp_px_cat_g1v2 pc
ON		  pn.cat_id = pc.id 
WHERE pn.prd_end_dt IS NULL;--Filter out all historical data


---====================================================================
-- Create Dimension: gold.fact_sales
--=====================================================================

IF OBJECT_ID('gold.fact_sales','U') IS NOT NULL
	DROP TABLE gold.fact_sales;

GO

CREATE VIEW gold.fact_sales AS
SELECT 
	sd.sls_ord_num AS order_number,
	dp.product_key,
	dc.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM Silver.crm_sales_details sd
LEFT JOIN Gold.dim_customers dc
ON		  sd.sls_cust_id = dc.customer_id
LEFT JOIN Gold.dim_products dp
ON        sd.sls_prd_key = dp.product_number
