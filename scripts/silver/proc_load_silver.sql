/*
=========================================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=========================================================================================================
Script Purpose:
	This stored procedure performs the ETL (Extract, Transofrm, Load) process to
	populate the 'silver' schema tables from the 'bronze' schema.
  Actions Performed:
	- Truncates Silver table.
	- Inserts trransformed and cleansed data from Bronze into Silver tables.

Parameters:
	None.
	This stored procedure does not accept any paramters or return any values.

Usage Example:
	EXEC Silver.load_silver;
=========================================================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=============================================================';
		PRINT 'Loading Silver Layer';
		PRINT '=============================================================';

		PRINT '=============================================================';
		PRINT 'Loading CRM Tables';
		PRINT '=============================================================';

		-- Loading silver.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting data into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date
		)
		SELECT
				cst_id,
				cst_key,
				-- Remove unwanted spaces (Removes unnecessary spaces to ensure data consistency, and uniformity acreoss all records.)
				TRIM(cst_firstname) AS cst_firstname,
				TRIM(cst_lastname) AS cst_lastname,
				-- Data Normalization & Standardization (Maps coded values to meaningful, user-friendly descriptions)
				CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
					 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
					 -- Handling Missing Data (Fills in the blanks by adding a default value)
					 ELSE 'n/a'
				END cst_marital_status,
				CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
					 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
					 ELSE 'n/a'
				END cst_gndr,
				cst_create_date
		FROM 
				(
				-- Remove Duplicates (Ensure only one record per entity by identifying and retaining the most relevant row.)
				SELECT	*, 
						ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
				FROM	bronze.crm_cust_info
				WHERE	cst_id IS NOT NULL
				) t
		WHERE	flag_last = 1;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------';


		-- Loading silver.crm_prd_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting data into: silver.crm_prd_info';

		INSERT INTO silver.crm_prd_info(
				prd_id,
				cat_id,
				prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt
		)
		SELECT	prd_id,
				-- Derived column (Create new columns based on calculations or transactions of existing ones
				REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
				-- Derived column (Create new columns based on calculations or transactions of existing ones
				SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
				prd_nm,
				-- Handling missing information (Instead of NULL, we will replace with 0)
				ISNULL(prd_cost, 0) AS prd_cost,
				-- Data Normalization & Standardization (Maps coded values to meaningful, user-friendly descriptions)
				CASE UPPER(TRIM(prd_line))
					 WHEN 'M' THEN 'Mountain'
					 WHEN 'R' THEN 'Road'
					 WHEN 'S' THEN 'Other Sales'
					 WHEN 'T' THEN 'Touring'
					 -- Replace NULL with n/a
					 ELSE 'n/a'
				END AS prd_line,
				-- Data type casting, considered as data transformation 
				CAST(prd_start_dt AS DATE) AS prd_start_dt,
				-- Data Enrichment (Add new, relevant data to enhance the dataset for analysis)
				CAST(DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC)) AS DATE) AS prd_end_dt
		FROM	bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------';

		-- Loading silver.crm_sales_details
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting data into: silver.crm_sales_details';

		INSERT INTO silver.crm_sales_details(
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				sls_order_dt,
				sls_ship_dt,
				sls_due_dt,
				sls_sales,
				sls_quantity,
				sls_price
		)
		SELECT	sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				-- Handling Invalid Data
				CASE 
					 WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
					 -- Data type casting
					 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
				END AS sls_order_dt,
				CASE 
					 WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
					 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
				END AS sls_ship_dt,
				CASE 
					 WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
					 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
				END AS sls_due_dt,

				CASE -- Handling the Missing data, invalid data by deriving the column already existing one
					 WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
					 THEN sls_quantity * ABS(sls_price)
					 ELSE sls_sales
				END AS sls_sales, -- Recalclate sales if original value is missing or incorrect
				sls_quantity,
				CASE -- Handling invalid data by deriving it from specific calculation
					 WHEN sls_price IS NULL OR sls_price <= 0
					 THEN sls_sales / NULLIF(sls_quantity, 0)
					 ELSE sls_price -- Derive price if original value is invalid
				END AS sls_price
		FROM	bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------';

		-- Loading silver.erp_cust_az12
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting data into: silver.erp_cust_az12';

		INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
		SELECT	
				CASE -- Remove 'NAS' prefix if present
					 WHEN cid LIKE 'NAS%' THEN SUBSTRING(TRIM(cid), 4, LEN(cid))
					 ELSE cid
				END cid,
				CASE -- Set future birthdates to NULL
					 WHEN bdate > GETDATE() THEN NULL
					 ELSE bdate
				END AS bdate,
				CASE -- Normalize gender values and handle unknown cases
					 WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
					 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
					 ELSE 'n/a'
				END AS gen
		FROM	bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------';


		-- Loading silver.erp_loc_a101
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting data into: silver.erp_loc_a101';

		INSERT INTO silver.erp_loc_a101
		(cid, cntry)
		SELECT	
				-- Handle invalid values
				REPLACE(cid, '-','') AS cid1,
				CASE -- Normalize and Handle missing or blank country codes (replaced codes with friendly values)
					 WHEN TRIM(cntry) = 'DE' THEN 'Germany'
					 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
					 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
					 ELSE TRIM(cntry)
				END AS cntry
		FROM	bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------';


		-- Loading silver.erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>> Truncating table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting data into: silver.erp_px_cat_g1v2';

		INSERT INTO silver.erp_px_cat_g1v2
		(id, cat, subcat, maintenance)
		SELECT	id,
				cat,
				subcat,
				maintenance
		FROM	bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> --------------';

		SET @batch_end_time = GETDATE()
		PRINT '=============================================================';
		PRINT 'Loading Silver Layer is Completed';
		PRINT '		- Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=============================================================';

	END TRY

	BEGIN CATCH
		PRINT '=============================================================';
		PRINT 'ERROR OCCURRED DURING SILVER LAYER';
		PRINT 'Error Message ' + ERROR_MESSAGE();
		PRINT 'Error Number ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=============================================================';
	END CATCH

END
