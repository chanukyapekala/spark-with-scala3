-- Initialize Hive Metastore Database

-- The database and user are created automatically by the postgres image
-- using POSTGRES_DB, POSTGRES_USER, and POSTGRES_PASSWORD environment variables

-- Grant all privileges
GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;

-- Additional schema setup if needed
-- Tables will be created automatically by Hive Metastore on first connection