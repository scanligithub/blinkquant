-- BlinkQuant 多自选股增量迁移
-- 适用于已经存在 users / watchlist / strategies 的生产数据库。

CREATE TABLE IF NOT EXISTS watchlists (
    id BIGSERIAL PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    is_default BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, name)
);
CREATE INDEX IF NOT EXISTS idx_watchlists_user ON watchlists (user_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_watchlists_one_default ON watchlists (user_id) WHERE is_default = TRUE;

CREATE TABLE IF NOT EXISTS watchlist_items (
    id BIGSERIAL PRIMARY KEY,
    watchlist_id BIGINT NOT NULL REFERENCES watchlists (id) ON DELETE CASCADE,
    code TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (watchlist_id, code)
);
CREATE INDEX IF NOT EXISTS idx_watchlist_items_list ON watchlist_items (watchlist_id);
CREATE INDEX IF NOT EXISTS idx_watchlist_items_code ON watchlist_items (code);

INSERT INTO watchlists (user_id, name, is_default)
SELECT DISTINCT old.user_id, '默认自选', TRUE
FROM watchlist old
ON CONFLICT (user_id, name) DO NOTHING;

INSERT INTO watchlist_items (watchlist_id, code, created_at)
SELECT w.id, old.code, old.created_at
FROM watchlist old
JOIN watchlists w ON w.user_id = old.user_id AND w.is_default = TRUE
ON CONFLICT (watchlist_id, code) DO NOTHING;
