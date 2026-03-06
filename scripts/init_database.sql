/*
	================================================================================================
	Create Database and Schemas
	================================================================================================
	Script Purpose:
		This script creates a new database named 'DataWarehouse' after checing if it already exists.
		If the database exists, it is dropped and recreated. Additionly, the script setup thress scehmas
		within the database: 'broze', 'silver', and 'gold'.

	WARNING:
		Running this script will drop the entire 'DataWarehouse' database if it exits.
		All data in the database will be permanently deleted. Proceed with caustion
		and ensure you have proper backups before running this script.
*/

USE Master;
GO

-- Drop and rereate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
