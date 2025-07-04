/*
*************************************************************************************
Stored Procedure : Load Silver Layer (Bronze -> Silver)
*************************************************************************************
Script Purpose:
	This stored procedure performs the ETL (Extract, Transform, Load ) process to populate
	the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		-Truncates Silver tables.
		-Inserts transformed and cleansed data from Bronze into Silver tables.
Parameters:
	None
	This stored procedures does not accept any parameters or return any values.

Usage Example:
	EXEC silver.load_silver;
*/

CREATE OR ALTER PROC silver.load_silver 
AS 
BEGIN
	DECLARE @load_start_time DATETIME , @load_end_time DATETIME , @batch_load_start_time DATETIME, @batch_load_end_time DATETIME;
	BEGIN TRY
		SET @batch_load_start_time = GETDATE();
		PRINT '=================================================';
		PRINT 'Loading Silver Layer';
		PRINT '=================================================';

		PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++';
		PRINT 'Loading CRM Tables';
		PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++';

		SET @load_start_time = GETDATE();
		PRINT '>>Truncating Table: Silver.crm_cust_info'; 
		TRUNCATE TABLE	Silver.crm_cust_info;
		PRINT '>>Inserting Data Into: Silver.crm_cust_info';  
		INSERT INTO Silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status
		,cst_gndr
		,cst_create_date)

		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_firstname) [cst_firstname],
		CASE
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'n/a'
		END AS cst_marital_status,--Normalize marital_status values to readable format
		CASE
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			ELSE 'n/a'
		END AS cst_gndr,--Normalize gender values to readable format
		cst_create_date
		FROM(
			SELECT *,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM Bronze.crm_cust_info
		) as t
		WHERE flag_last = 1 AND cst_id IS NOT NULL;--Select the most recent record per customer
		
		SET @load_end_time = GETDATE();
		PRINT '>>Load Duration : ' + CAST(DATEDIFF(second,@load_start_time,@load_end_time) AS NVARCHAR) + ' seconds';
		PRINT '...............';

		SET @load_start_time = GETDATE();
		PRINT '---------------------------------------------';
		PRINT '>>Truncating Table: Silver.crm_prd_info'; 
		TRUNCATE TABLE	Silver.crm_prd_info;
		PRINT '>>Inserting Data Into: Silver.crm_prd_info';

		INSERT INTO Silver.crm_prd_info(
			prd_id ,
			cat_id ,
			prd_key ,
			prd_nm ,
			prd_cost ,
			prd_line ,
			prd_start_dt ,
			prd_end_dt 
		)

		SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,--Replcaing NULL VALUE WITH ZERO
		CASE 
			UPPER(TRIM(prd_line)) 
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start_date AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_date) OVER (PARTITION BY prd_key ORDER BY prd_start_date)-1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info;

		SET @load_end_time = GETDATE();
		PRINT '>>Load Duration : ' + CAST(DATEDIFF(second,@load_start_time,@load_end_time) AS NVARCHAR) + ' seconds';
		PRINT '...............';

		SET @load_start_time = GETDATE();
		PRINT '---------------------------------------------';
		PRINT '>>Truncating Table: Silver.crm_sales_details'; 
		TRUNCATE TABLE	Silver.crm_sales_details;
		PRINT '>>Inserting Data Into: Silver.crm_sales_details';

		INSERT INTO Silver.crm_sales_details(
			sls_ord_num ,
			sls_prd_key ,
			sls_cust_id ,
			sls_order_dt ,
			sls_ship_dt ,
			sls_due_dt ,
			sls_sales ,
			sls_quantity ,
			sls_price
		)
		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE 
			WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE 
			WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE 
			WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE	
			WHEN  sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
			THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity, 
		CASE 
			WHEN sls_price IS NULL OR sls_price <= 0 
			THEN sls_sales / NULLIF(sls_quantity,0)--IF in future we get zero then changing to null
			ELSE sls_price
		END AS sls_price
		FROM Bronze.crm_sales_details;

		SET @load_end_time = GETDATE();
		PRINT '>>Load Duration : ' + CAST(DATEDIFF(second,@load_start_time,@load_end_time) AS NVARCHAR) + ' seconds';
		PRINT '...............';

		PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++';
		PRINT 'Loading ERP Tables';
		PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++';

		SET @load_start_time = GETDATE();
		PRINT '---------------------------------------------';
		PRINT '>>Truncating Table: Silver.erp_cust_az12'; 
		TRUNCATE TABLE	Silver.erp_cust_az12;
		PRINT '>>Inserting Data Into: Silver.erp_cust_az12';

		INSERT INTO Silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)

		SELECT 
		CASE
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			ELSE cid
		END cid,
		CASE 
			WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END bdate,
		CASE 
			WHEN UPPER(TRIM(gen))IN ('F' , 'FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen))IN ('M' , 'MALE') THEN 'Male'
			ELSE 'n/a'
		END gen
		FROM Bronze.erp_cust_az12;

		SET @load_end_time = GETDATE();
		PRINT 'Load Duration : ' + CAST(DATEDIFF(second,@load_start_time,@load_end_time) AS NVARCHAR) + ' seconds';
		PRINT '...............';

		SET @load_start_time = GETDATE();
		PRINT '---------------------------------------------';
		PRINT '>>Truncating Table: Silver.erp_loc_a101'; 
		TRUNCATE TABLE	Silver.erp_loc_a101;
		PRINT '>>Inserting Data Into: Silver.erp_loc_a101';

		INSERT INTO Silver.erp_loc_a101(
			cid,
			cntry
		)

		SELECT 
		REPLACE(cid,'-','') cid ,
		CASE	
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN  ('US','USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry) 
		END cntry
		FROM Bronze.erp_loc_a101;

		SET @load_end_time = GETDATE();
		PRINT 'Load Duration : ' + CAST(DATEDIFF(second,@load_start_time,@load_end_time) AS NVARCHAR) + ' seconds';
		PRINT '...............';

		SET @load_start_time = GETDATE();
		PRINT '---------------------------------------------';
		PRINT '>>Truncating Table: Silver.erp_px_cat_g1v2'; 
		TRUNCATE TABLE	Silver.erp_px_cat_g1v2;
		PRINT '>>Inserting Data Into: Silver.erp_px_cat_g1v2';

		INSERT INTO Silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		)

		SELECT 
		id,
		cat,
		subcat,
		maintenance
		FROM Bronze.erp_px_cat_g1v2;
		SET @load_end_time = GETDATE();
		PRINT 'Load Duration : ' + CAST(DATEDIFF(second,@load_start_time,@load_end_time) AS NVARCHAR) + ' seconds';
		PRINT '...............';


		SET @batch_load_end_time = GETDATE();
		PRINT '============================================';
		PRINT 'Loading Silver Layer is Completed.';
		PRINT '    >>>Total Load Duration of Silver Layer : ' + CAST(DATEDIFF(second,@batch_load_start_time,@batch_load_end_time) AS NVARCHAR) + ' seconds';
		PRINT '============================================';	
	END TRY
	BEGIN CATCH
		PRINT '--------------------------------------------';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'Error Message ' + ERROR_MESSAGE();
		PRINT 'Error Message ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '--------------------------------------------';
	END CATCH
END
