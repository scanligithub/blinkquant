PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS cluster_nodes (
    node_id         TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    endpoint        TEXT NOT NULL,
    weight          INTEGER DEFAULT 1,
    status          TEXT NOT NULL DEFAULT 'idle',
    current_task_id INTEGER,
    task_type       TEXT,
    heartbeat_at    TEXT,
    last_error      TEXT,
    generation      INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS task_queue (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         TEXT NOT NULL,
    task_type       TEXT NOT NULL,
    payload         TEXT NOT NULL,
    priority        INTEGER DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'pending',
    assigned_node   TEXT,
    cluster_job_id  TEXT,
    result          TEXT,
    result_summary  TEXT,          -- JSON: {total_return, max_drawdown, n_trades, ...}
    result_uri      TEXT,          -- by_user/{user_id}/task_{id}
    result_bytes    INTEGER,       -- 结果目录总字节
    error           TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    queued_at       TEXT,
    started_at      TEXT,
    finished_at     TEXT,
    retry_count     INTEGER DEFAULT 0,
    max_retries     INTEGER DEFAULT 2,
    preempted_by    INTEGER,
    generation      INTEGER NOT NULL DEFAULT 0,
    strategy_template_id INTEGER,
    strategy_template_name TEXT,
    strategy_template_updated_at TEXT,
    strategy_template_version INTEGER,
    source_task_id   INTEGER,
    timeout_sec     INTEGER,          -- 动态墙钟上限（秒），NULL 用全局默认
    progress_pct    REAL,             -- 0~100，回测进度百分比
    progress_json   TEXT              -- 完整 progress JSON
);

CREATE INDEX IF NOT EXISTS idx_tq_status_priority
    ON task_queue (status, priority DESC, created_at);
CREATE INDEX IF NOT EXISTS idx_tq_user_status
    ON task_queue (user_id, status);
CREATE INDEX IF NOT EXISTS idx_tq_assigned_status
    ON task_queue (assigned_node, status);
CREATE INDEX IF NOT EXISTS idx_tq_generation
    ON task_queue (generation);

CREATE TABLE IF NOT EXISTS artifacts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         TEXT NOT NULL,
    artifact_type   TEXT NOT NULL CHECK (artifact_type IN ('selection', 'backtest')),
    task_id         INTEGER NOT NULL UNIQUE,
    title           TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'ready',
    metadata        TEXT NOT NULL DEFAULT '{}',
    summary         TEXT,
    result_json     TEXT,
    result_uri      TEXT,
    result_bytes    INTEGER DEFAULT 0,
    created_at      TEXT DEFAULT (datetime('now')),
    finished_at     TEXT
);

CREATE INDEX IF NOT EXISTS idx_artifacts_user_type_created
    ON artifacts (user_id, artifact_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_artifacts_task
    ON artifacts (task_id);

CREATE TABLE IF NOT EXISTS backtest_strategy_templates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         TEXT NOT NULL,
    name            TEXT NOT NULL,
    description     TEXT,
    config          TEXT NOT NULL,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(user_id, name)
);
CREATE INDEX IF NOT EXISTS idx_bst_user_updated
    ON backtest_strategy_templates (user_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS backtest_strategy_versions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_template_id INTEGER NOT NULL,
    version_no      INTEGER NOT NULL,
    name            TEXT NOT NULL,
    description     TEXT,
    config          TEXT NOT NULL,
    created_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(strategy_template_id, version_no),
    FOREIGN KEY(strategy_template_id) REFERENCES backtest_strategy_templates(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_bsv_template_version
    ON backtest_strategy_versions (strategy_template_id, version_no DESC);

CREATE TABLE IF NOT EXISTS node_heartbeats (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id       TEXT,
    status        TEXT,
    task_id       INTEGER,
    load          REAL,
    metrics       TEXT,
    reported_at   TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_nh_node_time
    ON node_heartbeats (node_id, reported_at DESC);

INSERT OR IGNORE INTO cluster_nodes (node_id, name, endpoint, weight) VALUES
('node1', 'Node 1', 'https://scanli-blinkquant-node1.hf.space', 1),
('node2', 'Node 2', 'https://scanli-blinkquant-node2.hf.space', 1),
('node3', 'Node 3', 'https://scanli-blinkquant-node3.hf.space', 1);