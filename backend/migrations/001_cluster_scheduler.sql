-- backend/migrations/001_cluster_scheduler.sql
-- 运行方式：psql $DATABASE_URL -f 001_cluster_scheduler.sql

-- ============================================================
-- 1. 节点注册表
-- ============================================================
CREATE TABLE IF NOT EXISTS cluster_nodes (
    node_id         VARCHAR(50) PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,
    endpoint        VARCHAR(200) NOT NULL,
    weight          INT DEFAULT 1,
    status          VARCHAR(20) NOT NULL DEFAULT 'idle',
    current_task_id BIGINT,
    task_type       VARCHAR(20),
    heartbeat_at    TIMESTAMPTZ,
    last_error      TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

-- 初始化 3 个 HF Space 节点
INSERT INTO cluster_nodes (node_id, name, endpoint, weight) VALUES
('node1', 'Node 1', 'https://scanli-blinkquant-node1.hf.space', 1),
('node2', 'Node 2', 'https://scanli-blinkquant-node2.hf.space', 1),
('node3', 'Node 3', 'https://scanli-blinkquant-node3.hf.space', 1)
ON CONFLICT (node_id) DO NOTHING;

-- ============================================================
-- 2. 任务队列表
-- ============================================================
CREATE TABLE IF NOT EXISTS task_queue (
    id              BIGSERIAL PRIMARY KEY,
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    task_type       VARCHAR(20) NOT NULL,
    payload         JSONB NOT NULL,
    priority        INT DEFAULT 0,
    status          VARCHAR(20) NOT NULL DEFAULT 'pending',
    assigned_node   VARCHAR(50),
    cluster_job_id  VARCHAR(100),
    result          JSONB,
    error           TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    queued_at       TIMESTAMPTZ,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    retry_count     INT DEFAULT 0,
    max_retries     INT DEFAULT 2,
    preempted_by    BIGINT
);

CREATE INDEX IF NOT EXISTS idx_tq_status_priority 
    ON task_queue (status, priority DESC, created_at);

CREATE INDEX IF NOT EXISTS idx_tq_user_status 
    ON task_queue (user_id, status);

CREATE INDEX IF NOT EXISTS idx_tq_assigned_status
    ON task_queue (assigned_node, status);

-- ============================================================
-- 3. 节点心跳记录
-- ============================================================
CREATE TABLE IF NOT EXISTS node_heartbeats (
    id            BIGSERIAL PRIMARY KEY,
    node_id       VARCHAR(50) REFERENCES cluster_nodes(node_id),
    status        VARCHAR(20),
    task_id       BIGINT,
    load          FLOAT,
    metrics       JSONB,
    reported_at   TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_nh_node_time 
    ON node_heartbeats (node_id, reported_at DESC);

-- ============================================================
-- 4. 触发器：自动维护 updated_at
-- ============================================================
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_cluster_nodes_updated ON cluster_nodes;
CREATE TRIGGER trg_cluster_nodes_updated
BEFORE UPDATE ON cluster_nodes
FOR EACH ROW EXECUTE PROCEDURE set_updated_at();

CREATE INDEX IF NOT EXISTS idx_cn_status
    ON cluster_nodes (status);