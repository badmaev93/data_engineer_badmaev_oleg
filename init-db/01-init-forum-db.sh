#!/bin/bash
set -e

# Create forum user and farpost database
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER forum WITH PASSWORD 'forum';
    CREATE DATABASE farpost OWNER forum;
    GRANT ALL PRIVILEGES ON DATABASE farpost TO forum;
EOSQL

# Apply forum schema
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname farpost -f /schema.sql

# Grant privileges to forum user
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname farpost <<-EOSQL
    GRANT ALL ON ALL TABLES IN SCHEMA public TO forum;
    GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO forum;
    GRANT USAGE ON SCHEMA public TO forum;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO forum;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO forum;
EOSQL

echo "Forum database initialized successfully"
