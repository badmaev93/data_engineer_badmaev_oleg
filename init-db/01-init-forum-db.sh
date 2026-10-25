#!/bin/bash
set -e

if [ -z "$FORUM_DB_USER" ] || [ -z "$FORUM_DB_PASSWORD" ]; then
    echo "ОШИБКА: Переменные окружения FORUM_DB_USER и FORUM_DB_PASSWORD должны быть заданы."
    exit 1
fi

# Создаем пользователя forum и базу данных fp
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER "$FORUM_DB_USER" WITH PASSWORD '$FORUM_DB_PASSWORD';
    CREATE DATABASE fp OWNER "$FORUM_DB_USER";
    GRANT ALL PRIVILEGES ON DATABASE fp TO "$FORUM_DB_USER";
EOSQL

# Применяем схему forum
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname fp -f /schema.sql

# Выдаем права пользователю forum
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname fp <<-EOSQL
    GRANT ALL ON ALL TABLES IN SCHEMA public TO "$FORUM_DB_USER";
    GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO "$FORUM_DB_USER";
    GRANT USAGE ON SCHEMA public TO "$FORUM_DB_USER";
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO "$FORUM_DB_USER";
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO "$FORUM_DB_USER";
EOSQL

echo "База данных forum успешно инициализирована"
