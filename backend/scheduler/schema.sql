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
    error           TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    queued_at       TEXT,
    started_at      TEXT,
    finished_at     TEXT,
    retry_count     INTEGER DEFAULT 0,
    max_retries     INTEGER DEFAULT 2,
    preempted_by    INTEGER,
    generation      INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_tq_status_priority
    ON task_queue (status, priority DESC, created_at);
CREATE INDEX IF NOT EXISTS idx_tq_user_status
    ON task_queue (user_id, status);
CREATE INDEX IF NOT EXISTS idx_tq_assigned_status
    ON task_queue (assigned_node, status);
CREATE INDEX IF NOT EXISTS idx_tq_generation
    ON task_queue (generation);

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