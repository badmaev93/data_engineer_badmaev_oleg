-- Типы для логирования (п. 1.3 ТЗ)
CREATE TYPE log_action_type AS ENUM (
    'first_visit',
    'register',
    'login',
    'logout',
    'create_topic',
    'view_topic',
    'delete_topic',
    'create_post'
);

CREATE TYPE server_response_status AS ENUM ('success', 'error');

-- Пользователи
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(40) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    joined_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Темы форума: создавать может только авторизованный пользователь (п. 1.1 ТЗ)
CREATE TABLE topics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    author_id UUID NOT NULL REFERENCES users(id),
    title VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Сообщения: писать может авторизованный пользователь или аноним (п. 1.2 ТЗ)
CREATE TABLE posts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic_id UUID NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    author_id UUID REFERENCES users(id) ON DELETE SET NULL,
    body TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Логи действий пользователей, партиционированные по месяцам (пп. 1.3, 1.4 ТЗ)
CREATE TABLE user_logs (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    session_or_ip VARCHAR(64),
    action_type log_action_type NOT NULL,
    description TEXT NOT NULL,
    target_object_id UUID,
    response_status server_response_status NOT NULL,
    response_code SMALLINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, created_at),
    CONSTRAINT chk_topic_requires_auth CHECK (
        NOT (action_type = 'create_topic' AND response_status = 'success' AND user_id IS NULL)
    )
) PARTITION BY RANGE (created_at);

CREATE TABLE user_logs_y2026m09 PARTITION OF user_logs
    FOR VALUES FROM ('2026-09-01 00:00:00+00') TO ('2026-10-01 00:00:00+00');

CREATE TABLE user_logs_y2026m10 PARTITION OF user_logs
    FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-11-01 00:00:00+00');

CREATE TABLE user_logs_y2026m11 PARTITION OF user_logs
    FOR VALUES FROM ('2026-11-01 00:00:00+00') TO ('2026-12-01 00:00:00+00');

CREATE TABLE user_logs_y2026m12 PARTITION OF user_logs
    FOR VALUES FROM ('2026-12-01 00:00:00+00') TO ('2027-01-01 00:00:00+00');

CREATE TABLE user_logs_default PARTITION OF user_logs DEFAULT;

-- Индексы контентных таблиц и таблицы логов (п. 1.5 ТЗ)
CREATE INDEX idx_topics_author ON topics (author_id, created_at DESC);
CREATE INDEX idx_posts_topic_created ON posts (topic_id, created_at ASC);
CREATE INDEX idx_posts_author ON posts (author_id, created_at DESC)
    WHERE author_id IS NOT NULL;

CREATE INDEX idx_user_logs_created_at_brin ON user_logs USING BRIN (created_at);
CREATE INDEX idx_user_logs_user_time ON user_logs (user_id, created_at DESC)
    WHERE user_id IS NOT NULL;
CREATE INDEX idx_user_logs_target_object ON user_logs (target_object_id, created_at DESC)
    WHERE target_object_id IS NOT NULL;
CREATE INDEX idx_user_logs_status_action ON user_logs (response_status, action_type, created_at DESC);
