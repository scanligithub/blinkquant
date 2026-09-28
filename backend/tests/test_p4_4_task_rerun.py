import asyncio
import json

from fastapi import HTTPException
from scheduler import db


def _cfg(max_positions=5):
    return {
        "strategy": {
            "universe": {"type": "all_a"},
            "entry": {"condition": "CLOSE > MA(CLOSE,20)", "trigger": "condition", "timeframe": "D"},
            "sizing": {"method": "top_n_equal_weight", "max_positions": max_positions},
            "rebalance": {"frequency": "weekly"},
            "mode": "target_portfolio",
        },
        "fee_policy": {"mode": "historical"},
        "benchmark": {"enabled": True, "type": "index", "index_id": "000300"},
        "min_listing_days": 30,
        "exclude_st": True,
    }


def _payload(cfg):
    return {
        "start_date": "2024-01-02",
        "end_signal_date": "2024-03-29",
        "initial_cash": 1000000,
        **cfg,
    }


def test_rerun_creates_independent_task_with_same_payload_and_template(tmp_path, monkeypatch):
    async def run():
        path = tmp_path / "scheduler.db"
        monkeypatch.setattr(db, "SCHEDULER_DB_PATH", str(path))
        await db.close_pool()
        await db.init_pool()
        try:
            from scheduler.routes import rerun_task

            cfg = _cfg(5)
            await db.execute(
                "INSERT INTO backtest_strategy_templates (user_id,name,config) VALUES ($1,$2,$3)",
                "u1", "tpl", json.dumps(cfg),
            )
            current_template = await db.fetchrow(
                "SELECT id,name,updated_at FROM backtest_strategy_templates WHERE id=1"
            )
            assert current_template is not None

            await db.execute(
                "INSERT INTO task_queue "
                "(user_id,task_type,payload,priority,status,strategy_template_id,"
                "strategy_template_name,strategy_template_updated_at) "
                "VALUES ($1,'backtest',$2,3,'done',1,'tpl','2026-09-28 05:00:00')",
                "u1", json.dumps(_payload(cfg)),
            )
            result = await rerun_task(1, user_id="u1", role="user")
            assert result["task_id"] == 2
            assert result["source_task_id"] == 1
            assert result["status"] == "pending"
            assert result["strategy_template_id"] == 1
            assert result["strategy_template_name"] == "tpl"
            assert result["strategy_template_updated_at"] == current_template["updated_at"]

            old = await db.fetchrow("SELECT payload,status,strategy_template_updated_at FROM task_queue WHERE id=1")
            new = await db.fetchrow(
                "SELECT payload,status,source_task_id,strategy_template_id,strategy_template_updated_at "
                "FROM task_queue WHERE id=2"
            )
            assert json.loads(new["payload"]) == json.loads(old["payload"])
            assert old["status"] == "done"
            assert old["strategy_template_updated_at"] == "2026-09-28 05:00:00"
            assert new["source_task_id"] == 1
            assert new["strategy_template_id"] == 1
            assert new["strategy_template_updated_at"] == current_template["updated_at"]
        finally:
            await db.close_pool()

    asyncio.run(run())


def test_rerun_changed_template_drops_binding_but_keeps_payload(tmp_path, monkeypatch):
    async def run():
        path = tmp_path / "scheduler.db"
        monkeypatch.setattr(db, "SCHEDULER_DB_PATH", str(path))
        await db.close_pool()
        await db.init_pool()
        try:
            from scheduler.routes import rerun_task

            old_cfg = _cfg(5)
            new_cfg = _cfg(6)
            await db.execute(
                "INSERT INTO backtest_strategy_templates (user_id,name,config) VALUES ($1,$2,$3)",
                "u1", "tpl", json.dumps(new_cfg),
            )
            await db.execute(
                "INSERT INTO task_queue "
                "(user_id,task_type,payload,priority,status,strategy_template_id,"
                "strategy_template_name,strategy_template_updated_at) "
                "VALUES ($1,'backtest',$2,0,'done',1,'tpl','2026-09-28 05:00:00')",
                "u1", json.dumps(_payload(old_cfg)),
            )
            result = await rerun_task(1, user_id="u1", role="user")
            assert result["source_task_id"] == 1
            assert result["strategy_template_id"] is None
            row = await db.fetchrow(
                "SELECT payload,strategy_template_id,source_task_id FROM task_queue WHERE id=2"
            )
            assert json.loads(row["payload"]) == _payload(old_cfg)
            assert row["strategy_template_id"] is None
            assert row["source_task_id"] == 1
        finally:
            await db.close_pool()

    asyncio.run(run())


def test_rerun_deleted_template_still_creates_task_without_binding(tmp_path, monkeypatch):
    async def run():
        path = tmp_path / "scheduler.db"
        monkeypatch.setattr(db, "SCHEDULER_DB_PATH", str(path))
        await db.close_pool()
        await db.init_pool()
        try:
            from scheduler.routes import rerun_task

            cfg = _cfg(5)
            await db.execute(
                "INSERT INTO task_queue "
                "(user_id,task_type,payload,status,strategy_template_id,"
                "strategy_template_name,strategy_template_updated_at) "
                "VALUES ($1,'backtest',$2,'done',99,'deleted','2026-09-28 05:00:00')",
                "u1", json.dumps(_payload(cfg)),
            )
            result = await rerun_task(1, user_id="u1", role="user")
            assert result["task_id"] == 2
            assert result["strategy_template_id"] is None
            assert result["source_task_id"] == 1
        finally:
            await db.close_pool()

    asyncio.run(run())


def test_rerun_rejects_nonterminal_cross_user_and_selection(tmp_path, monkeypatch):
    async def run():
        path = tmp_path / "scheduler.db"
        monkeypatch.setattr(db, "SCHEDULER_DB_PATH", str(path))
        await db.close_pool()
        await db.init_pool()
        try:
            from scheduler.routes import rerun_task

            cfg = _cfg()
            payload = json.dumps(_payload(cfg))
            await db.execute(
                "INSERT INTO task_queue (user_id,task_type,payload,status) VALUES "
                "('u1','backtest',$1,'running')", payload,
            )
            await db.execute(
                "INSERT INTO task_queue (user_id,task_type,payload,status) VALUES "
                "('u1','selection',$1,'done')", payload,
            )
            await db.execute(
                "INSERT INTO task_queue (user_id,task_type,payload,status) VALUES "
                "('u2','backtest',$1,'done')", payload,
            )

            try:
                await rerun_task(1, user_id="u1", role="user")
            except HTTPException as exc:
                assert exc.status_code == 409
            else:
                raise AssertionError("running task must not rerun")

            try:
                await rerun_task(2, user_id="u1", role="user")
            except HTTPException as exc:
                assert exc.status_code == 400
            else:
                raise AssertionError("selection task must not rerun")

            try:
                await rerun_task(3, user_id="u1", role="user")
            except HTTPException as exc:
                assert exc.status_code == 403
            else:
                raise AssertionError("cross-user task must be rejected")
        finally:
            await db.close_pool()

    asyncio.run(run())


def test_existing_scheduler_db_migrates_source_task_id(tmp_path, monkeypatch):
    async def run():
        path = tmp_path / "scheduler.db"
        import aiosqlite
        async with aiosqlite.connect(path) as conn:
            await conn.execute("""
                CREATE TABLE task_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    priority INTEGER DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'pending'
                )
            """)
            await conn.commit()

        monkeypatch.setattr(db, "SCHEDULER_DB_PATH", str(path))
        await db.close_pool()
        await db.init_pool()
        try:
            cols = await db.fetch("PRAGMA table_info(task_queue)")
            names = {row["name"] for row in cols}
            assert "source_task_id" in names
            indexes = await db.fetch("PRAGMA index_list(task_queue)")
            assert any(row["name"] == "idx_tq_source_task" for row in indexes)
        finally:
            await db.close_pool()

    asyncio.run(run())
