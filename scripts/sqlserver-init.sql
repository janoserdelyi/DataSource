-- SQL Server initialization script
-- This script runs when the container starts for the first time

USE master;
GO

-- Create test database
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'testdb')
BEGIN
	CREATE DATABASE testdb;
END
GO

USE testdb;
GO

-- Create test user
IF NOT EXISTS (SELECT name FROM sys.server_principals WHERE name = 'testuser')
BEGIN
	CREATE LOGIN testuser WITH PASSWORD = 'TestPassword123!';
END
GO

IF NOT EXISTS (SELECT name FROM sys.database_principals WHERE name = 'testuser')
BEGIN
	CREATE USER testuser FOR LOGIN testuser;
	ALTER ROLE db_owner ADD MEMBER testuser;
END
GO
