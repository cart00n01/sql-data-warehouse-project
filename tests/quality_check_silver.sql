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
-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================

--Check for Nulls or Duplicates in Primary Key
SELECT cst_id, COUNT(*) as Duplicates
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 or cst_id is NULL;

--Check for unwanted spaces in string values
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);-- gives the name with trailing or leading spaces

--Data Standardization & Consistency
--Check the consistency of values in low cardinality columns
SELECT DISTINCT(cst_gndr) FROM Silver.crm_cust_info;
SELECT DISTINCT(cst_marital_status) FROM Silver.crm_cust_info;


-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================

--Check for Nulls or Duplicates in Primary Key
SELECT prd_id , COUNT(*) AS Duplicates
FROM Silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 AND prd_id IS NULL;

--Check for unwanted spaces in string values
SELECT prd_nm
FROM Silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);-- gives the name with trailing or leading spaces


--Check for Negative Numbers and NULLS
SELECT prd_cost
FROM Silver.crm_prd_info
WHERE prd_cost < 1 OR prd_cost IS NULL;

--Data Standardization & Consistency
--Check the consistency of values in low cardinality columns
SELECT DISTINCT(prd_line) FROM Silver.crm_prd_info

--Check fro Invalid Date Orders
SELECT *
FROM Silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt; 

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================

--Check whether join key is missing or not 
SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity, 
sls_price
FROM Silver.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM Silver.crm_prd_info);

--Check for Invalid Date
--Check for whether the date is zero?
--Check for the outliers by validating the boundaries of the date range ( meaning date range higher
--than the actual today date )
SELECT 
NULLIF (sls_order_dt,0) AS sls_order_dt
FROM Silver.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101;

--Check for Invalid Order Date(Order Date must be less than the Shipping Date)
SELECT *
FROM Silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

--Check Data Consistency:Between Sales, Quantity and Price
-->> Sales = Quantity * Price
-->> Values must not be Negative , Zeros and Null

SELECT DISTINCT 
sls_sales , 
sls_quantity  , 
sls_price,
FROM Silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_quantity,sls_price,sls_sales;

-- ====================================================================
-- Checking 'Silver.erp_cust_az12'
-- ====================================================================

--Check for unmatching cid with cst_key(used for joining both table) in Silver.crm_cust_info
SELECT * FROM Silver.crm_cust_info;--Get col of the crm_cust_info

SELECT 
CASE
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	ELSE cid
END cid,
bdate,
gen 
FROM Silver.erp_cust_az12
WHERE CASE
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	ELSE cid
END  NOT IN (SELECT cst_key FROM Silver.crm_cust_info);

--Identify the out-of-range dates
SELECT 
bdate
FROM Silver.erp_cust_az12
WHERE bdate > GETDATE();

--Data Standardization & Normalization
--Check for the gender column
SELECT DISTINCT gen
FROM Silver.erp_cust_az12;


-- ====================================================================
-- Checking 'Silver.erp_loc_a101'
-- ====================================================================

--Check for unmatching cid with cst_key(used for joining both table) in Silver.crm_cust_info

SELECT 
cid,
CONCAT(SUBSTRING(cid,1,2),SUBSTRING(cid,4,LEN(cid))),
cntry
FROM Silver.erp_loc_a101
WHERE CONCAT(SUBSTRING(cid,1,2),SUBSTRING(cid,4,LEN(cid))) 
	NOT IN (SELECT cst_key FROM Silver.crm_cust_info);

---Data Standardization & Normalization
SELECT DISTINCT
cntry
FROM Silver.erp_loc_a101
ORDER BY cntry;


-- ====================================================================
-- Checking 'Silver.erp_px_cat_g1v2'
-- ====================================================================

--Check for unwanted spaces
SELECT *
FROM Silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

---Data Standardization & Normalization
SELECT DISTINCT
cat--Check for cat,subcat and maintenance
FROM Silver.erp_px_cat_g1v2;
