-- MariaDB initialization script
-- This script runs when the container starts for the first time

-- Create additional databases if needed
-- CREATE DATABASE IF NOT EXISTS additional_testdb;

-- Grant additional permissions if needed
GRANT ALL PRIVILEGES ON testdb.* TO 'testuser'@'%';

-- You can add any additional setup here
-- CREATE TABLE IF NOT EXISTS test_table (
--     id INT AUTO_INCREMENT PRIMARY KEY,
--     name VARCHAR(255) NOT NULL
-- );

FLUSH PRIVILEGES;
