/*
=====================================================================================
Stored Procedure : Load Bronze Layer (Source -> Bronze)
=====================================================================================
Script Purpose:
	This stored procedure load data into the 'bronze' schema from external CSV files.
	It performs the following actions:
		-Truncates the bronze tables before loading data.
		-Uses the 'BULK INSERT' command to load data from csv files to bronze tables.
		-Also calculates the time taken to load each file to respective tables and the whole batch of the data as well.
Parameters:
	None
	This stored procedures does not accept any parameters or return any values.

Usage Example:
	EXEC bronze.load_bronze;
*/
USE DataWarehouse
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze 
AS
BEGIN
	DECLARE @load_start_time DATETIME , @load_end_time DATETIME , @batch_load_start_time DATETIME, @batch_load_end_time DATETIME;
	BEGIN TRY
		SET @batch_load_start_time = GETDATE();
		PRINT '=================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=================================================';

		PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++';
		PRINT 'Loading CRM Tables';
		PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++';

		SET @load_start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into : bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\Python\Data Engineer\Projects\SQL Data Warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW  = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @load_end_time = GETDATE();
		PRINT '>>Load Duration : ' + CAST(DATEDIFF(second,@load_start_time,@load_end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------';

		SET @load_start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into : bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\Python\Data Engineer\Projects\SQL Data Warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW  = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @load_end_time = GETDATE();
		PRINT 'Load Duration : ' + CAST(DATEDIFF(second,@load_start_time,@load_end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------';

		SET @load_start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into : bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\Python\Data Engineer\Projects\SQL Data Warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @load_end_time = GETDATE();
		PRINT 'Load Duration : ' + CAST(DATEDIFF(second,@load_start_time,@load_end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------';

		PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++';
		PRINT 'Loading ERP Tables';
		PRINT '+++++++++++++++++++++++++++++++++++++++++++++++++';
		
		SET @load_start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into : bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\Python\Data Engineer\Projects\SQL Data Warehouse\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @load_end_time = GETDATE();
		PRINT 'Load Duration : ' + CAST(DATEDIFF(second,@load_start_time,@load_end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------';

		SET @load_start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into : bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\Python\Data Engineer\Projects\SQL Data Warehouse\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @load_end_time = GETDATE();
		PRINT 'Load Duration : ' + CAST(DATEDIFF(second,@load_start_time,@load_end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------';

		SET @load_start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE [Bronze].[erp_px_cat_g1v2];

		PRINT '>> Inserting Data Into : bronze.erp_px_cat_g1v2';
		BULK INSERT [Bronze].[erp_px_cat_g1v2]
		FROM 'D:\Python\Data Engineer\Projects\SQL Data Warehouse\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @load_end_time = GETDATE();
		PRINT 'Load Duration : ' + CAST(DATEDIFF(second,@load_start_time,@load_end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------';

		SET @batch_load_end_time = GETDATE();
		PRINT '============================================';
		PRINT 'Loading Bronze Layer is Completed.';
		PRINT '    >>>Total Load Duration of Bronze Layer : ' + CAST(DATEDIFF(second,@batch_load_start_time,@batch_load_end_time) AS NVARCHAR) + ' seconds';
		PRINT '============================================';
	END TRY
	BEGIN CATCH 
		PRINT '--------------------------------------------';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message ' + ERROR_MESSAGE();
		PRINT 'Error Message ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '--------------------------------------------';
	END CATCH
END ;
