#!/bin/bash
set -e

if [ -z "$FORUM_DB_USER" ] || [ -z "$FORUM_DB_PASSWORD" ]; then
    echo "ОШИБКА: Переменные окружения FORUM_DB_USER и FORUM_DB_PASSWORD должны быть заданы."
    exit 1
fi

# Создаем пользователя forum и базу данных fp
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -v forum_user="$FORUM_DB_USER" -v forum_password="$FORUM_DB_PASSWORD" <<-'EOSQL'
    CREATE USER :"forum_user" WITH PASSWORD :'forum_password';
    CREATE DATABASE fp OWNER :"forum_user";
    GRANT ALL PRIVILEGES ON DATABASE fp TO :"forum_user";
EOSQL

# Применяем схему forum
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname fp -f /schema.sql

# Выдаем права пользователю forum
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname fp -v forum_user="$FORUM_DB_USER" <<-'EOSQL'
    GRANT ALL ON ALL TABLES IN SCHEMA public TO :"forum_user";
    GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO :"forum_user";
    GRANT USAGE ON SCHEMA public TO :"forum_user";
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO :"forum_user";
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO :"forum_user";
EOSQL

echo "База данных forum успешно инициализирована"
