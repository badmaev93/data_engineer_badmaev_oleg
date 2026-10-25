#!/bin/bash
set -e

# Создаем пользователя forum и базу данных farpost
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER ${FORUM_DB_USER:-forum} WITH PASSWORD '${FORUM_DB_PASSWORD:-forum}';
    CREATE DATABASE farpost OWNER ${FORUM_DB_USER:-forum};
    GRANT ALL PRIVILEGES ON DATABASE farpost TO ${FORUM_DB_USER:-forum};
EOSQL

# Применяем схему forum
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname farpost -f /schema.sql

# Выдаем права пользователю forum
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname farpost <<-EOSQL
    GRANT ALL ON ALL TABLES IN SCHEMA public TO ${FORUM_DB_USER:-forum};
    GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO ${FORUM_DB_USER:-forum};
    GRANT USAGE ON SCHEMA public TO ${FORUM_DB_USER:-forum};
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO ${FORUM_DB_USER:-forum};
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO ${FORUM_DB_USER:-forum};
EOSQL

echo "База данных forum успешно инициализирована"
