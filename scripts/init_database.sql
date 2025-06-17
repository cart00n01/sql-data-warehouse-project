/*
============================================
Create Database and Schemas
============================================
Script Purpose:
	This script creates a nwe database named 'DataWarehouse' after checking if it already exists.
	If the database exists, it is dropped and recreated.Additionally, the script sets up three schmas 
	within the database: 'bronze','silver' and 'gold'.

WARNING:
	Running this script will drop the entire 'DataWarehouse' database if it exists.
	All data in the database will be permanently deleted.Proceed with caution and 
	enure you have proper backups before running this script.
*/

USE master;
GO
--GO acts as a Batch separator

--Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;--Sets the database to SINGLE_USER mode so only one user can access it.
	DROP DATABASE DataWarehouse;--forces disconnect of all other users immediately, rolling back any open transactions to avoid delay or locking issues.
END;
GO

--Create database named DataWarehouse
CREATE DATABASE DataWarehouse;

USE DataWarehouse;

--Create Schemas
CREATE SCHEMA Bronze;
Go
CREATE SCHEMA Silver;
Go
CREATE SCHEMA Gold;
Go