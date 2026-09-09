-- backend/migrations/002_add_generation.sql
-- 运行方式：在 Vercel Postgres → Data → Query 粘贴执行
-- 作用：为 cluster_nodes 和 task_queue 添加 generation 字段，用于乐观锁防止旧任务覆盖新状态

-- ============================================================
-- 1. cluster_nodes 添加 generation
-- ============================================================
ALTER TABLE cluster_nodes 
ADD COLUMN IF NOT EXISTS generation BIGINT NOT NULL DEFAULT 0;

-- 现有节点 generation 归零（已是默认值）
-- UPDATE cluster_nodes SET generation = 0 WHERE generation IS NULL;

-- ============================================================
-- 2. task_queue 添加 generation
-- ============================================================
ALTER TABLE task_queue 
ADD COLUMN IF NOT EXISTS generation BIGINT NOT NULL DEFAULT 0;

-- ============================================================
-- 3. 索引优化：按 generation 查询
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_cn_generation ON cluster_nodes (generation);
CREATE INDEX IF NOT EXISTS idx_tq_generation ON task_queue (generation);

-- ============================================================
-- 4. 触发器更新：generation 变更时自动 updated_at（复用现有触发器）
-- ============================================================
-- 现有 trg_cluster_nodes_updated 已覆盖 generation 字段更新