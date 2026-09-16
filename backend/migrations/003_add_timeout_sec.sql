-- backend/migrations/003_add_timeout_sec.sql
-- 运行方式：在 Vercel Postgres → Data → Query 粘贴执行

ALTER TABLE task_queue ADD COLUMN IF NOT EXISTS timeout_sec INTEGER;
