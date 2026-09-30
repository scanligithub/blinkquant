-- BlinkQuant 选股策略版本表增量迁移
-- 当前 strategies 保存“策略身份 + 当前版本快照”，strategy_versions 保存每次编辑后的不可变版本。

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
