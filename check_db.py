import os
import psycopg2

# Use environment variable or default
dsn = os.getenv("DATABASE_URL") or "postgresql://postgres:postgres@localhost:5432/postgres"

conn = psycopg2.connect(dsn)
cur = conn.cursor()

# Check tables
cur.execute("""
    SELECT table_name FROM information_schema.tables 
    WHERE table_schema='public' AND table_name IN ('task_queue', 'cluster_nodes')
""")
print("Tables:", cur.fetchall())

# Check cluster_nodes
cur.execute("SELECT node_id, status, heartbeat_at FROM cluster_nodes")
print("Nodes:", cur.fetchall())

# Check task_queue
cur.execute("SELECT id, task_type, status FROM task_queue ORDER BY created_at DESC LIMIT 5")
print("Recent tasks:", cur.fetchall())

cur.close()
conn.close()