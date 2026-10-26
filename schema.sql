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

CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(40) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    display_name VARCHAR(80),
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    joined_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE forums (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_id UUID REFERENCES forums(id) ON DELETE SET NULL,
    slug VARCHAR(100) NOT NULL UNIQUE,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    position INT NOT NULL DEFAULT 0
);

CREATE TABLE topics (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    forum_id UUID NOT NULL REFERENCES forums(id) ON DELETE CASCADE,
    author_id UUID NOT NULL REFERENCES users(id),
    slug VARCHAR(200) NOT NULL,
    title VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'open',
    is_pinned BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_post_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE posts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic_id UUID NOT NULL REFERENCES topics(id) ON DELETE CASCADE,
    author_id UUID REFERENCES users(id) ON DELETE SET NULL,
    author_guest_name VARCHAR(80),
    body TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

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

CREATE INDEX idx_topics_board_listing ON topics (forum_id, is_pinned DESC, last_post_at DESC);
CREATE INDEX idx_topics_author ON topics (author_id, created_at DESC);
CREATE INDEX idx_posts_topic_created ON posts (topic_id, created_at ASC) WHERE deleted_at IS NULL;
CREATE INDEX idx_posts_author ON posts (author_id, created_at DESC) WHERE author_id IS NOT NULL;

CREATE INDEX idx_user_logs_created_at_brin ON user_logs USING BRIN (created_at);
CREATE INDEX idx_user_logs_user_time ON user_logs (user_id, created_at DESC) WHERE user_id IS NOT NULL;
CREATE INDEX idx_user_logs_target_object ON user_logs (target_object_id, created_at DESC) WHERE target_object_id IS NOT NULL;
CREATE INDEX idx_user_logs_status_action ON user_logs (response_status, action_type, created_at DESC);