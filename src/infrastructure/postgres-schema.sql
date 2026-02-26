CREATE TABLE IF NOT EXISTS users (
    id BIGINT PRIMARY KEY,
    categories JSONB DEFAULT '[]'::jsonb,
    regions JSONB DEFAULT '[]'::jsonb,
    notification_settings JSONB DEFAULT '{"mode":"smart","interval":4,"limit":3,"silent":false,"last_sent":0,"last_source":"","source_history":[],"night_mode":true,"smart_history":[]}'::jsonb,
    onboarding_done INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS news (
    id BIGSERIAL PRIMARY KEY,
    hash TEXT UNIQUE,
    title_hash TEXT UNIQUE,
    title TEXT,
    body TEXT,
    image_url TEXT,
    video_url TEXT,
    source_name TEXT,
    link TEXT,
    category TEXT,
    region TEXT,
    published_at BIGINT,
    merged_sources JSONB DEFAULT '[]'::jsonb
);

CREATE TABLE IF NOT EXISTS seen_log (
    user_id BIGINT,
    news_id BIGINT,
    seen_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (user_id, news_id)
);

CREATE TABLE IF NOT EXISTS media_cache (
    url TEXT PRIMARY KEY,
    file_id TEXT,
    saved_at BIGINT
);

CREATE INDEX IF NOT EXISTS idx_news_category_pub ON news(category, published_at DESC);
CREATE INDEX IF NOT EXISTS idx_news_region_pub ON news(region, published_at DESC);
CREATE INDEX IF NOT EXISTS idx_seen_user ON seen_log(user_id);
CREATE INDEX IF NOT EXISTS idx_seen_user_news ON seen_log(user_id, news_id);
CREATE INDEX IF NOT EXISTS idx_users_mode ON users((notification_settings->>'mode'));
