#!/bin/bash
set -e

# Use environment variables for the forum database credentials, with defaults
FORUM_USER=${FORUM_DB_USER:-forum}
FORUM_PASSWORD=${FORUM_DB_PASSWORD:-forum}

# Create forum user and farpost database
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER $FORUM_USER WITH PASSWORD '$FORUM_PASSWORD';
    CREATE DATABASE farpost OWNER $FORUM_USER;
    GRANT ALL PRIVILEGES ON DATABASE farpost TO $FORUM_USER;
EOSQL

# Apply forum schema
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname farpost -f /schema.sql

# Grant privileges to forum user
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname farpost <<-EOSQL
    GRANT ALL ON ALL TABLES IN SCHEMA public TO $FORUM_USER;
    GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO $FORUM_USER;
    GRANT USAGE ON SCHEMA public TO $FORUM_USER;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO $FORUM_USER;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO $FORUM_USER;
EOSQL

echo "✅ Forum database initialized successfully"
