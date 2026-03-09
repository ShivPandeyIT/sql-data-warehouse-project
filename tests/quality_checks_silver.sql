/*
=================================================================================
Quality Checks
=================================================================================
Script Purpose:
	This script performs various quality checks for data consistency, accuracy,
	and standardization across the 'silver' schemas. It includes checks for:
	- NUll or duplicate primary keys.
	- Unwanted spaces in sting fields.
	- Data standardization and consistency.
	- Invalid date ranges and orders.
	- Data consistency between related fields.

Usage notes:
	- Run these checks after data loading Silver Layer
	- Investigate and resolve any descrepancies found during the checks.
=================================================================================
*/

-- ==============================================================================
-- Checking 'silver.crm_cust_info
-- ==============================================================================

-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Result
SELECT		cst_id, 
			COUNT(*) 
FROM		silver.crm_cust_info
GROUP BY	cst_id
HAVING		COUNT(*) > 1
OR			cst_id IS NULL

-- Check for unwanted Spaces
-- Expectation: No Result
SELECT		cst_firstname
FROM		silver.crm_cust_info
WHERE		cst_firstname != TRIM(cst_firstname)

-- Check for unwanted Spaces
-- Expectation: No Result
SELECT		cst_lastname
FROM		silver.crm_cust_info
WHERE		cst_lastname != TRIM(cst_lastname)

-- Data Standardization & Consistency
SELECT	DISTINCT	cst_gndr
FROM		silver.crm_cust_info

SELECT * FROM silver.crm_cust_info



-- ==============================================================================
-- Checking 'silver.crm_prd_info
-- ==============================================================================

-- Quality Checks
-- Check For Nulls or Duplicates in Primary Key
-- Expectation: No Result
SELECT		prd_id,
			COUNT(*)
FROM		silver.crm_prd_info
GROUP BY	prd_id
HAVING		COUNT(*) > 1
OR			prd_id IS NULL

-- Check for unwanted Spaces
-- Expectation: No Results
SELECT		prd_nm
FROM		silver.crm_prd_info
WHERE		prd_nm != TRIM(prd_nm)

-- Check for NULLs or Negative Numbers
-- Expectation: No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for Invalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT	prd_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt,
		DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC)) AS prd_end_dt_test
FROM	DataWarehouse.bronze.crm_prd_info
WHERE	prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')
ORDER BY Prd_id


SELECT *
FROM silver.crm_prd_info

-- ==============================================================================
-- Checking 'silver.crm_sales_details
-- ==============================================================================

-- Check for Invalid Date Orders
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

SELECT		sls_sales,
			sls_quantity,
			sls_price
FROM		silver.crm_sales_details
WHERE		sls_sales != sls_quantity * sls_price
OR			sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR			sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY	sls_sales,
			sls_quantity,
			sls_price

SELECT * FROM silver.crm_sales_details


-- ==============================================================================
-- Checking 'silver.erp_cust_az12
-- ==============================================================================
SELECT	cid,
		bdate,
		gen
FROM	bronze.erp_cust_az12

SELECT * FROM silver.crm_cust_info

SELECT	cid,
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(TRIM(cid), 4, LEN(cid))
			 ELSE cid
		END cid,
		bdate,
		gen
FROM	bronze.erp_cust_az12

SELECT	cid,
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(TRIM(cid), 4, LEN(cid))
			 ELSE cid
		END cid,
		bdate,
		gen
FROM	bronze.erp_cust_az12
WHERE	cid IN (SELECT cst_key FROM silver.crm_cust_info)


SELECT	cid,
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(TRIM(cid), 4, LEN(cid))
			 ELSE cid
		END cid,
		bdate,
		gen
FROM	bronze.erp_cust_az12
WHERE	cid NOT IN (SELECT cst_key FROM silver.crm_cust_info)

-- No record found
SELECT	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(TRIM(cid), 4, LEN(cid))
			 ELSE cid
		END cid,
		bdate,
		gen
FROM	bronze.erp_cust_az12
WHERE	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(TRIM(cid), 4, LEN(cid))
		ELSE cid
		END NOT IN (SELECT cst_key FROM silver.crm_cust_info)


-- Identify Out-of-Range Dates

SELECT DISTINCT bdate
FROM	bronze.erp_cust_az12
WHERE	bdate < '1924-01-01' OR bdate > GETDATE()


-- Data Standardization & Consistency
SELECT DISTINCT 
		gen,
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			 ELSE 'n/a'
		END AS gen

FROM	bronze.erp_cust_az12

--------------------------------------------------------------
-- Check after loading into silver.erp_cust_az12

SELECT DISTINCT bdate
FROM	silver.erp_cust_az12
WHERE	bdate < '1924-01-01' OR bdate > GETDATE()


SELECT	DISTINCT
		gen
FROM	silver.erp_cust_az12

SELECT * FROM silver.erp_cust_az12


-- ==============================================================================
-- Checking 'silver.erp_loc_a101
-- ==============================================================================

SELECT	
		cid,
		REPLACE(cid, '-','') AS cid1,
		cntry
FROM	bronze.erp_loc_a101;

-- Check the ERD, the joined table primary/foreign key, it should be same, if not transform it
SELECT
		cst_key
FROM	silver.crm_cust_info


SELECT	
		cid,
		REPLACE(cid, '-','') AS cid1,
		cntry
FROM	bronze.erp_loc_a101
WHERE	REPLACE(cid, '-','') NOT IN 
(SELECT cst_key FROM	silver.crm_cust_info)

-- Data Standardization & Consistency
SELECT DISTINCT cntry 
FROM	bronze.erp_loc_a101
ORDER BY cntry


SELECT DISTINCT cntry AS old_cntry,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
			 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			 ELSE TRIM(cntry)
		END AS cntry
FROM	bronze.erp_loc_a101
ORDER BY cntry

-----------------------------------------------------------
-- Check the table after data gets loaded
-----------------------------------------------------------

SELECT DISTINCT cntry FROM silver.erp_loc_a101
SELECT * FROM silver.erp_loc_a101



-- ==============================================================================
-- Checking 'silver.erp_px_cat_g1v2
-- ==============================================================================


SELECT	id,
		cat,
		subcat,
		maintenance
FROM	bronze.erp_px_cat_g1v2

--SELECT * FROM silver.crm_prd_info

-- Check for unwanted Spaces
SELECT * 
FROM	bronze.erp_px_cat_g1v2
WHERE	cat != TRIM(cat) 
OR		subcat != TRIM(subcat)
OR		maintenance != TRIM(maintenance)

-- Data Standardization & Consistency
SELECT	DISTINCT
		cat
FROM	bronze.erp_px_cat_g1v2

SELECT	DISTINCT
		subcat
FROM	bronze.erp_px_cat_g1v2

SELECT	DISTINCT
		maintenance
FROM	bronze.erp_px_cat_g1v2

--------------------------------------------------
-- Check silver.erp_px_cat_g1v2 after loding the data

SELECT * FROM silver.erp_px_cat_g1v2
