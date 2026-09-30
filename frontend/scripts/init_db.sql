-- BlinkQuant 用户管理数据库初始化脚本
-- 在 Vercel Postgres 控制台或 psql 中执行一次即可

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'user',
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);
CREATE INDEX IF NOT EXISTS idx_users_status ON users (status);

-- 旧版单列表自选股表保留，用于向新模型迁移历史数据
CREATE TABLE IF NOT EXISTS watchlist (
    id BIGSERIAL PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    code TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, code)
);
CREATE INDEX IF NOT EXISTS idx_watchlist_user ON watchlist (user_id);

-- 多自选股列表：列表名称由用户自定义
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

-- 旧版单列表数据迁移到默认自选；可重复执行，不会重复插入。
INSERT INTO watchlists (user_id, name, is_default)
SELECT DISTINCT old.user_id, '默认自选', TRUE
FROM watchlist old
ON CONFLICT (user_id, name) DO NOTHING;

INSERT INTO watchlist_items (watchlist_id, code, created_at)
SELECT w.id, old.code, old.created_at
FROM watchlist old
JOIN watchlists w ON w.user_id = old.user_id AND w.is_default = TRUE
ON CONFLICT (watchlist_id, code) DO NOTHING;

CREATE TABLE IF NOT EXISTS strategies (
    id BIGSERIAL PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    formula TEXT NOT NULL,
    timeframe TEXT NOT NULL DEFAULT 'D',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_strategies_user ON strategies (user_id);

-- 选股策略版本：每次编辑保留一份不可变历史版本
CREATE TABLE IF NOT EXISTS strategy_versions (
    id BIGSERIAL PRIMARY KEY,
    strategy_id BIGINT NOT NULL REFERENCES strategies (id) ON DELETE CASCADE,
    version_no INTEGER NOT NULL,
    name TEXT NOT NULL,
    formula TEXT NOT NULL,
    timeframe TEXT NOT NULL DEFAULT 'D',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (strategy_id, version_no)
);
CREATE INDEX IF NOT EXISTS idx_strategy_versions_strategy ON strategy_versions (strategy_id, version_no DESC);

INSERT INTO strategy_versions (strategy_id, version_no, name, formula, timeframe, created_at)
SELECT id, 1, name, formula, timeframe, COALESCE(created_at, NOW())
FROM strategies s
WHERE NOT EXISTS (SELECT 1 FROM strategy_versions v WHERE v.strategy_id = s.id);
