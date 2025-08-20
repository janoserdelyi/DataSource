-- PostgreSQL initialization script
-- This script runs when the container starts for the first time

-- Create additional databases if needed
-- CREATE DATABASE additional_testdb;

-- Create schemas
-- CREATE SCHEMA IF NOT EXISTS test_schema;

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE testdb TO testuser;

-- You can add any additional setup here
-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
-- CREATE EXTENSION IF NOT EXISTS "hstore";
