from __future__ import annotations

import json
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException

from .db import acquire, execute, fetch, fetchrow
from .config import INTERNAL_TOKEN

router = APIRouter(prefix="/internal/user-assets", tags=["user-assets"])

async def verify_internal_token(authorization: Optional[str] = Header(None)) -> None:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing Bearer token")
    if authorization[7:] != INTERNAL_TOKEN:
        raise HTTPException(401, "Invalid internal token")

def require_user_id(user_id: Optional[str]) -> str:
    uid = str(user_id or "").strip()
    if not uid:
        raise HTTPException(401, "user_id required")
    return uid

def validate_timeframe(value: object) -> str:
    timeframe = str(value or "D").strip()
    if timeframe not in ("D", "W", "M"):
        raise HTTPException(400, "Invalid timeframe")
    return timeframe

def clean_source(value: object) -> dict:
    if not isinstance(value, dict):
        return {}
    return {
        "source_backtest_strategy_id": value.get("strategy_id"),
        "source_backtest_strategy_version": value.get("version_no"),
        "source_backtest_strategy_name": value.get("name"),
        "source_backtest_strategy_trigger": value.get("trigger"),
    }

async def ensure_default_watchlist(user_id: str) -> dict:
    row = await fetchrow(
        "SELECT id,user_id,name,is_default,created_at,updated_at FROM watchlists "
        "WHERE user_id=? AND is_default=1 ORDER BY created_at ASC,id ASC LIMIT 1", user_id)
    if row:
        return row
    await execute("INSERT OR IGNORE INTO watchlists(user_id,name,is_default) VALUES(?,?,1)", user_id, "默认自选")
    row = await fetchrow(
        "SELECT id,user_id,name,is_default,created_at,updated_at FROM watchlists "
        "WHERE user_id=? AND is_default=1 ORDER BY created_at ASC,id ASC LIMIT 1", user_id)
    if not row:
        raise HTTPException(500, "无法创建默认自选列表")
    return row

@router.get("/strategies", dependencies=[Depends(verify_internal_token)])
async def list_strategies(user_id: Optional[str] = None) -> dict:
    uid = require_user_id(user_id)
    rows = await fetch(
        "SELECT s.id,s.name,s.formula,s.timeframe,s.created_at,s.updated_at,"
        "s.source_backtest_strategy_id,s.source_backtest_strategy_version,s.source_backtest_strategy_name,"
        "s.source_backtest_strategy_trigger,s.source_backtest_artifact_id,s.source_backtest_artifact_title,"
        "COALESCE((SELECT MAX(v.version_no) FROM strategy_versions v WHERE v.strategy_id=s.id),1) AS version_no "
        "FROM strategies s WHERE s.user_id=? ORDER BY s.updated_at DESC,s.id DESC", uid)
    return {"strategies": rows}

@router.post("/strategies", dependencies=[Depends(verify_internal_token)])
async def create_strategy(body: dict) -> dict:
    uid = require_user_id(body.get("user_id"))
    name, formula = str(body.get("name") or "").strip(), str(body.get("formula") or "").strip()
    if not name or not formula: raise HTTPException(400, "名称和公式必填")
    timeframe = validate_timeframe(body.get("timeframe"))
    source = clean_source(body.get("source_backtest"))
    source_id = body.get("source_backtest_strategy_id", source.get("source_backtest_strategy_id"))
    source_version = body.get("source_backtest_strategy_version", source.get("source_backtest_strategy_version"))
    source_name = body.get("source_backtest_strategy_name", source.get("source_backtest_strategy_name"))
    source_trigger = body.get("source_backtest_strategy_trigger", source.get("source_backtest_strategy_trigger"))
    source_artifact_id, source_artifact_title = body.get("source_backtest_artifact_id"), body.get("source_backtest_artifact_title")
    async with acquire() as conn:
        try:
            row = await conn.fetchrow(
                "INSERT INTO strategies(user_id,name,formula,timeframe,source_backtest_strategy_id,"
                "source_backtest_strategy_version,source_backtest_strategy_name,source_backtest_strategy_trigger,"
                "source_backtest_artifact_id,source_backtest_artifact_title) VALUES(?,?,?,?,?,?,?,?,?,?) "
                "RETURNING id,name,formula,timeframe,created_at,updated_at,source_backtest_strategy_id,"
                "source_backtest_strategy_version,source_backtest_strategy_name,source_backtest_strategy_trigger,"
                "source_backtest_artifact_id,source_backtest_artifact_title",
                uid,name,formula,timeframe,source_id,source_version,source_name,source_trigger,source_artifact_id,source_artifact_title)
            await conn.execute(
                "INSERT INTO strategy_versions(strategy_id,version_no,name,formula,timeframe,"
                "source_backtest_strategy_id,source_backtest_strategy_version,source_backtest_strategy_name,"
                "source_backtest_strategy_trigger,source_backtest_artifact_id,source_backtest_artifact_title) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)", row["id"],1,row["name"],row["formula"],row["timeframe"],
                source_id,source_version,source_name,source_trigger,source_artifact_id,source_artifact_title)
        except Exception as exc:
            if "UNIQUE constraint failed: strategies.user_id, strategies.name" in str(exc):
                raise HTTPException(409, "已存在同名策略")
            raise
    return {"strategy": {**row,"version_no":1}}

@router.get("/strategies/{strategy_id}", dependencies=[Depends(verify_internal_token)])
async def get_strategy(strategy_id: int, user_id: Optional[str] = None) -> dict:
    uid = require_user_id(user_id)
    row = await fetchrow(
        "SELECT s.id,s.name,s.formula,s.timeframe,s.created_at,s.updated_at,"
        "s.source_backtest_strategy_id,s.source_backtest_strategy_version,s.source_backtest_strategy_name,"
        "s.source_backtest_strategy_trigger,s.source_backtest_artifact_id,s.source_backtest_artifact_title,"
        "COALESCE((SELECT MAX(v.version_no) FROM strategy_versions v WHERE v.strategy_id=s.id),1) AS version_no "
        "FROM strategies s WHERE s.id=? AND s.user_id=? LIMIT 1", strategy_id,uid)
    if not row: raise HTTPException(404, "策略不存在")
    return {"strategy":row}

@router.put("/strategies/{strategy_id}", dependencies=[Depends(verify_internal_token)])
async def update_strategy(strategy_id: int, body: dict) -> dict:
    uid = require_user_id(body.get("user_id"))
    current = await fetchrow("SELECT * FROM strategies WHERE id=? AND user_id=? LIMIT 1",strategy_id,uid)
    if not current: raise HTTPException(404,"策略不存在")
    name = str(body["name"]).strip() if "name" in body else current["name"]
    formula = str(body["formula"]).strip() if "formula" in body else current["formula"]
    timeframe = validate_timeframe(body.get("timeframe",current["timeframe"]))
    if not name or not formula: raise HTTPException(400,"名称和公式不能为空")
    async with acquire() as conn:
        try:
            updated = await conn.fetchrow(
                "UPDATE strategies SET name=?,formula=?,timeframe=?,updated_at=datetime('now') "
                "WHERE id=? AND user_id=? RETURNING id,name,formula,timeframe,created_at,updated_at",
                name,formula,timeframe,strategy_id,uid)
            next_version=int(await conn.fetchval("SELECT COALESCE(MAX(version_no),0)+1 FROM strategy_versions WHERE strategy_id=?",strategy_id))
            await conn.execute(
                "INSERT INTO strategy_versions(strategy_id,version_no,name,formula,timeframe,"
                "source_backtest_strategy_id,source_backtest_strategy_version,source_backtest_strategy_name,"
                "source_backtest_strategy_trigger,source_backtest_artifact_id,source_backtest_artifact_title) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",strategy_id,next_version,name,formula,timeframe,
                current["source_backtest_strategy_id"],current["source_backtest_strategy_version"],
                current["source_backtest_strategy_name"],current["source_backtest_strategy_trigger"],
                current["source_backtest_artifact_id"],current["source_backtest_artifact_title"])
        except Exception as exc:
            if "UNIQUE constraint failed: strategies.user_id, strategies.name" in str(exc):
                raise HTTPException(409,"已存在同名策略")
            raise
    return {"strategy":{**updated,"version_no":next_version}}

@router.delete("/strategies/{strategy_id}", dependencies=[Depends(verify_internal_token)])
async def delete_strategy(strategy_id: int, user_id: Optional[str] = None) -> dict:
    uid = require_user_id(user_id)
    result = await execute("DELETE FROM strategies WHERE id=? AND user_id=?",strategy_id,uid)
    if result.startswith("0 "): raise HTTPException(404,"策略不存在")
    return {"success":True}

@router.get("/strategies/{strategy_id}/versions", dependencies=[Depends(verify_internal_token)])
async def list_strategy_versions(strategy_id: int, user_id: Optional[str] = None) -> dict:
    uid=require_user_id(user_id)
    if not await fetchrow("SELECT id FROM strategies WHERE id=? AND user_id=?",strategy_id,uid):
        raise HTTPException(404,"策略不存在")
    rows=await fetch(
        "SELECT id,strategy_id,version_no,name,formula,timeframe,"
        "source_backtest_strategy_id,source_backtest_strategy_version,source_backtest_strategy_name,"
        "source_backtest_strategy_trigger,source_backtest_artifact_id,source_backtest_artifact_title,created_at "
        "FROM strategy_versions WHERE strategy_id=? ORDER BY version_no DESC",strategy_id)
    return {"versions":rows}

@router.get("/strategies/export", dependencies=[Depends(verify_internal_token)])
async def export_selection_strategies(user_id: Optional[str] = None) -> dict:
    uid=require_user_id(user_id)
    rows=await fetch("SELECT * FROM strategies WHERE user_id=? ORDER BY id ASC",uid)
    result=[]
    for s in rows:
        versions=await fetch(
            "SELECT version_no,name,formula,timeframe,source_backtest_strategy_id,source_backtest_strategy_version,"
            "source_backtest_strategy_name,source_backtest_strategy_trigger,created_at FROM strategy_versions "
            "WHERE strategy_id=? ORDER BY version_no ASC",s["id"])
        result.append({
            "name":s["name"],"formula":s["formula"],"timeframe":s["timeframe"],"created_at":s["created_at"],"updated_at":s["updated_at"],
            "source_backtest":({"strategy_id":s["source_backtest_strategy_id"],"version_no":s["source_backtest_strategy_version"],
                "name":s["source_backtest_strategy_name"],"trigger":s["source_backtest_strategy_trigger"]}
                if s["source_backtest_strategy_id"] is not None else None),
            "versions":[{"version_no":v["version_no"],"name":v["name"],"formula":v["formula"],"timeframe":v["timeframe"],
                "created_at":v["created_at"],
                "source_backtest":({"strategy_id":v["source_backtest_strategy_id"],"version_no":v["source_backtest_strategy_version"],
                    "name":v["source_backtest_strategy_name"],"trigger":v["source_backtest_strategy_trigger"]}
                    if v["source_backtest_strategy_id"] is not None else None)} for v in versions],
        })
    return {"format":"blinkquant-selection-strategies-v1","exported_at":__import__("datetime").datetime.utcnow().isoformat()+"Z","strategies":result}

@router.post("/strategies/import", dependencies=[Depends(verify_internal_token)])
async def import_selection_strategies(body: dict) -> dict:
    uid=require_user_id(body.get("user_id"))
    items=body.get("strategies") if isinstance(body.get("strategies"),list) else []
    if not items: raise HTTPException(400,"没有可导入的策略")
    if len(items)>100: raise HTTPException(400,"单次最多导入 100 个选股策略")
    imported,skipped=0,[]
    for item in items:
        name=str(item.get("name") or "").strip(); formula=str(item.get("formula") or "").strip(); timeframe=str(item.get("timeframe") or "D").strip()
        if not name or not formula or timeframe not in ("D","W","M"): skipped.append(name or "(未命名)"); continue
        raw=item.get("versions") if isinstance(item.get("versions"),list) else []
        versions=[]; seen=set()
        for v in raw[:100]:
            try: vn=int(v.get("version_no"))
            except Exception: continue
            if vn<=0 or vn in seen: continue
            vn_name=str(v.get("name") or name).strip(); vn_formula=str(v.get("formula") or formula).strip(); vn_tf=str(v.get("timeframe") or timeframe).strip()
            if vn_name and vn_formula and vn_tf in ("D","W","M"): versions.append((vn,vn_name,vn_formula,vn_tf)); seen.add(vn)
        if not versions: versions=[(1,name,formula,timeframe)]
        try:
            async with acquire() as conn:
                if await conn.fetchrow("SELECT id FROM strategies WHERE user_id=? AND name=?",uid,name): skipped.append(name); continue
                row=await conn.fetchrow("INSERT INTO strategies(user_id,name,formula,timeframe) VALUES(?,?,?,?) RETURNING id",uid,name,formula,timeframe)
                for vn,vn_name,vn_formula,vn_tf in sorted(versions):
                    await conn.execute("INSERT INTO strategy_versions(strategy_id,version_no,name,formula,timeframe) VALUES(?,?,?,?,?)",row["id"],vn,vn_name,vn_formula,vn_tf)
            imported+=1
        except Exception: skipped.append(name)
    return {"imported":imported,"skipped":skipped}

@router.get("/watchlists", dependencies=[Depends(verify_internal_token)])
async def list_watchlists(user_id: Optional[str] = None, code: str = "") -> dict:
    uid=require_user_id(user_id); await ensure_default_watchlist(uid)
    rows=await fetch(
        "SELECT w.id,w.name,w.is_default,w.created_at,w.updated_at,COUNT(wi.code) AS item_count,"
        "CASE WHEN ?='' THEN 0 ELSE EXISTS(SELECT 1 FROM watchlist_items wx WHERE wx.watchlist_id=w.id AND wx.code=?) END AS contains "
        "FROM watchlists w LEFT JOIN watchlist_items wi ON wi.watchlist_id=w.id WHERE w.user_id=? "
        "GROUP BY w.id ORDER BY w.is_default DESC,w.created_at ASC,w.id ASC",code,code,uid)
    return {"watchlists":[{**r,"is_default":bool(r["is_default"]),"item_count":int(r["item_count"] or 0),"contains":bool(r["contains"])} for r in rows]}

@router.post("/watchlists", dependencies=[Depends(verify_internal_token)])
async def create_watchlist(body: dict) -> dict:
    uid=require_user_id(body.get("user_id")); name=str(body.get("name") or "").strip()
    if not name: raise HTTPException(400,"自选股列表名称不能为空")
    if len(name)>40: raise HTTPException(400,"自选股列表名称过长")
    try: row=await fetchrow("INSERT INTO watchlists(user_id,name,is_default) VALUES(?,?,0) RETURNING id,name,is_default,created_at,updated_at",uid,name)
    except Exception as exc:
        if "UNIQUE constraint failed: watchlists.user_id, watchlists.name" in str(exc): raise HTTPException(409,"已存在同名的自选股列表")
        raise
    return {"watchlist":{**row,"is_default":bool(row["is_default"]),"item_count":0}}

@router.get("/watchlists/{watchlist_id}", dependencies=[Depends(verify_internal_token)])
async def get_watchlist(watchlist_id:int,user_id:Optional[str]=None)->dict:
    uid=require_user_id(user_id)
    row=await fetchrow("SELECT id,user_id,name,is_default,created_at,updated_at FROM watchlists WHERE id=? AND user_id=?",watchlist_id,uid)
    if not row: raise HTTPException(404,"自选股列表不存在")
    items=await fetch("SELECT code,created_at FROM watchlist_items WHERE watchlist_id=? ORDER BY created_at ASC,id ASC",watchlist_id)
    return {"watchlist":{**row,"is_default":bool(row["is_default"]),"item_count":len(items),"codes":[str(x["code"]) for x in items]}}

@router.patch("/watchlists/{watchlist_id}", dependencies=[Depends(verify_internal_token)])
async def update_watchlist(watchlist_id:int,body:dict)->dict:
    uid=require_user_id(body.get("user_id"))
    if not await fetchrow("SELECT id FROM watchlists WHERE id=? AND user_id=?",watchlist_id,uid): raise HTTPException(404,"自选股列表不存在")
    name=str(body.get("name") or "").strip()
    if not name or len(name)>40: raise HTTPException(400,"无效的自选股列表名称")
    try: updated=await fetchrow("UPDATE watchlists SET name=?,updated_at=datetime('now') WHERE id=? AND user_id=? RETURNING id,name,is_default,created_at,updated_at",name,watchlist_id,uid)
    except Exception as exc:
        if "UNIQUE constraint failed: watchlists.user_id, watchlists.name" in str(exc): raise HTTPException(409,"已存在同名的自选股列表")
        raise
    return {"watchlist":{**updated,"is_default":bool(updated["is_default"])}}

@router.delete("/watchlists/{watchlist_id}", dependencies=[Depends(verify_internal_token)])
async def delete_watchlist(watchlist_id:int,user_id:Optional[str]=None)->dict:
    uid=require_user_id(user_id); row=await fetchrow("SELECT id,is_default FROM watchlists WHERE id=? AND user_id=?",watchlist_id,uid)
    if not row: raise HTTPException(404,"自选股列表不存在")
    if row["is_default"]: raise HTTPException(400,"默认自选列表不可删除")
    result=await execute("DELETE FROM watchlists WHERE id=? AND user_id=?",watchlist_id,uid)
    if result.startswith("0 "): raise HTTPException(404,"自选股列表不存在")
    return {"success":True}

@router.get("/watchlist", dependencies=[Depends(verify_internal_token)])
async def legacy_watchlist_get(user_id:Optional[str]=None,listId:Optional[int]=None)->dict:
    uid=require_user_id(user_id); list_id=int(listId) if listId else int((await ensure_default_watchlist(uid))["id"])
    return await get_watchlist(list_id,uid)

@router.post("/watchlist/items", dependencies=[Depends(verify_internal_token)])
async def add_watchlist_items(body:dict)->dict:
    uid=require_user_id(body.get("user_id")); list_id=int(body.get("listId")) if body.get("listId") else int((await ensure_default_watchlist(uid))["id"])
    if not await fetchrow("SELECT id FROM watchlists WHERE id=? AND user_id=?",list_id,uid): raise HTTPException(404,"自选股列表不存在")
    raw=body.get("codes") if isinstance(body.get("codes"),list) else [body.get("code")]
    codes=list(dict.fromkeys(str(x or "").strip() for x in raw if str(x or "").strip()))
    if not codes: raise HTTPException(400,"缺少股票代码")
    if len(codes)>5000: raise HTTPException(400,"一次最多加入 5000 只股票")
    added=0
    async with acquire() as conn:
        for code in codes:
            cur=await conn.execute("INSERT OR IGNORE INTO watchlist_items(watchlist_id,code) VALUES(?,?)",list_id,code)
            added += max(0, cur.rowcount)
        await conn.execute("UPDATE watchlists SET updated_at=datetime('now') WHERE id=? AND user_id=?",list_id,uid)
    return {"success":True,"listId":list_id,"requested_count":len(codes),"added_count":added}

@router.delete("/watchlist/items", dependencies=[Depends(verify_internal_token)])
async def delete_watchlist_items(user_id:Optional[str]=None,listId:Optional[int]=None,code:Optional[str]=None,codes:Optional[str]=None)->dict:
    uid=require_user_id(user_id); list_id=int(listId) if listId else int((await ensure_default_watchlist(uid))["id"])
    if not await fetchrow("SELECT id FROM watchlists WHERE id=? AND user_id=?",list_id,uid): raise HTTPException(404,"自选股列表不存在")
    values=list(dict.fromkeys([x.strip() for x in (codes or "").split(",") if x.strip()])) if codes else ([str(code).strip()] if code else [])
    if not values: raise HTTPException(400,"缺少股票代码")
    if len(values)>5000: raise HTTPException(400,"一次最多移除 5000 只股票")
    removed=0
    async with acquire() as conn:
        for value in values:
            cur=await conn.execute("DELETE FROM watchlist_items WHERE watchlist_id=? AND code=?",list_id,value); removed+=cur.rowcount
        if removed: await conn.execute("UPDATE watchlists SET updated_at=datetime('now') WHERE id=? AND user_id=?",list_id,uid)
    return {"success":True,"listId":list_id,"requested_count":len(values),"removed_count":removed}

@router.get("/watchlists/export", dependencies=[Depends(verify_internal_token)])
async def export_watchlists(user_id:Optional[str]=None)->dict:
    uid=require_user_id(user_id); await ensure_default_watchlist(uid)
    rows=await fetch("SELECT w.id,w.name,w.is_default,w.created_at,w.updated_at,wi.code FROM watchlists w LEFT JOIN watchlist_items wi ON wi.watchlist_id=w.id WHERE w.user_id=? ORDER BY w.is_default DESC,w.created_at ASC,wi.created_at ASC,wi.code ASC",uid)
    grouped={}
    for r in rows:
        item=grouped.setdefault(int(r["id"]),{"name":r["name"],"is_default":bool(r["is_default"]),"created_at":r["created_at"],"updated_at":r["updated_at"],"codes":[]})
        if r["code"] is not None: item["codes"].append(str(r["code"]))
    return {"format":"blinkquant-watchlists-v1","exported_at":__import__("datetime").datetime.utcnow().isoformat()+"Z","watchlists":list(grouped.values())}

@router.post("/watchlists/import", dependencies=[Depends(verify_internal_token)])
async def import_watchlists(body:dict)->dict:
    uid=require_user_id(body.get("user_id")); items=body.get("watchlists") if isinstance(body.get("watchlists"),list) else []
    if not items: raise HTTPException(400,"没有可导入的自选股列表")
    if len(items)>100: raise HTTPException(400,"单次最多导入 100 个自选股列表")
    imported,skipped=0,[]
    for item in items:
        name=str(item.get("name") or "").strip()
        if not name or len(name)>40: skipped.append(name or "(未命名)"); continue
        codes=list(dict.fromkeys(str(x or "").strip() for x in (item.get("codes") or []) if str(x or "").strip()))
        if len(codes)>5000: skipped.append(name); continue
        try:
            async with acquire() as conn:
                existing=await conn.fetchrow("SELECT id FROM watchlists WHERE user_id=? AND name=?",uid,name)
                if existing: skipped.append(name); continue
                new=await conn.fetchrow("INSERT INTO watchlists(user_id,name,is_default) VALUES(?,?,0) RETURNING id",uid,name)
                for code in codes: await conn.execute("INSERT OR IGNORE INTO watchlist_items(watchlist_id,code) VALUES(?,?)",new["id"],code)
            imported+=1
        except Exception: skipped.append(name)
    return {"imported":imported,"skipped":skipped}

@router.post("/migrate-legacy", dependencies=[Depends(verify_internal_token)])
async def migrate_legacy_assets(body: dict) -> dict:
    strategies = body.get("strategies") if isinstance(body.get("strategies"), list) else []
    watchlists = body.get("watchlists") if isinstance(body.get("watchlists"), list) else []
    migrated_strategies = skipped_strategies = migrated_watchlists = merged_watchlist_codes = 0
    for item in strategies:
        uid=str(item.get("user_id") or "").strip(); name=str(item.get("name") or "").strip()
        formula=str(item.get("formula") or "").strip(); timeframe=str(item.get("timeframe") or "D").strip()
        if not uid or not name or not formula or timeframe not in ("D","W","M"): skipped_strategies+=1; continue
        try:
            async with acquire() as conn:
                if await conn.fetchrow("SELECT id FROM strategies WHERE user_id=? AND name=?",uid,name): skipped_strategies+=1; continue
                row=await conn.fetchrow(
                    "INSERT INTO strategies(user_id,name,formula,timeframe,source_backtest_strategy_id,source_backtest_strategy_version,"
                    "source_backtest_strategy_name,source_backtest_strategy_trigger,source_backtest_artifact_id,source_backtest_artifact_title,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,COALESCE(?,datetime('now')),COALESCE(?,datetime('now'))) RETURNING id",
                    uid,name,formula,timeframe,item.get("source_backtest_strategy_id"),item.get("source_backtest_strategy_version"),
                    item.get("source_backtest_strategy_name"),item.get("source_backtest_strategy_trigger"),item.get("source_backtest_artifact_id"),
                    item.get("source_backtest_artifact_title"),item.get("created_at"),item.get("updated_at"))
                versions=item.get("versions") if isinstance(item.get("versions"),list) else []
                clean=[]; seen=set()
                for v in versions[:100]:
                    try: vn=int(v.get("version_no"))
                    except Exception: continue
                    if vn<=0 or vn in seen: continue
                    vn_name=str(v.get("name") or name).strip(); vn_formula=str(v.get("formula") or formula).strip(); vn_tf=str(v.get("timeframe") or timeframe).strip()
                    if not vn_name or not vn_formula or vn_tf not in ("D","W","M"): continue
                    source=v.get("source_backtest") if isinstance(v.get("source_backtest"),dict) else {}
                    clean.append((vn,vn_name,vn_formula,vn_tf,source,v.get("created_at"))); seen.add(vn)
                if not clean: clean=[(1,name,formula,timeframe,{},item.get("created_at"))]
                for vn,vn_name,vn_formula,vn_tf,source,created_at in sorted(clean):
                    await conn.execute("INSERT INTO strategy_versions(strategy_id,version_no,name,formula,timeframe,source_backtest_strategy_id,source_backtest_strategy_version,source_backtest_strategy_name,source_backtest_strategy_trigger,created_at) VALUES(?,?,?,?,?,?,?,?,?,COALESCE(?,datetime('now')))",row["id"],vn,vn_name,vn_formula,vn_tf,source.get("strategy_id"),source.get("version_no"),source.get("name"),source.get("trigger"),created_at)
            migrated_strategies+=1
        except Exception: skipped_strategies+=1
    for item in watchlists:
        uid=str(item.get("user_id") or "").strip(); name=str(item.get("name") or "").strip()
        if not uid or not name or len(name)>40: continue
        codes=list(dict.fromkeys(str(x or "").strip() for x in (item.get("codes") or []) if str(x or "").strip()))
        if len(codes)>5000: continue
        try:
            async with acquire() as conn:
                existing=await conn.fetchrow("SELECT id FROM watchlists WHERE user_id=? AND name=?",uid,name)
                if existing: list_id=existing["id"]
                elif item.get("is_default"):
                    existing_default=await conn.fetchrow("SELECT id FROM watchlists WHERE user_id=? AND is_default=1 LIMIT 1",uid)
                    if existing_default: list_id=existing_default["id"]
                    else:
                        created=await conn.fetchrow("INSERT INTO watchlists(user_id,name,is_default,created_at,updated_at) VALUES(?,?,1,COALESCE(?,datetime('now')),COALESCE(?,datetime('now'))) RETURNING id",uid,name,item.get("created_at"),item.get("updated_at")); list_id=created["id"]; migrated_watchlists+=1
                else:
                    created=await conn.fetchrow("INSERT INTO watchlists(user_id,name,is_default,created_at,updated_at) VALUES(?,?,0,COALESCE(?,datetime('now')),COALESCE(?,datetime('now'))) RETURNING id",uid,name,item.get("created_at"),item.get("updated_at")); list_id=created["id"]; migrated_watchlists+=1
                for code in codes:
                    cur=await conn.execute("INSERT OR IGNORE INTO watchlist_items(watchlist_id,code) VALUES(?,?)",list_id,code); merged_watchlist_codes+=cur.rowcount
        except Exception: continue
    return {"migrated_strategies":migrated_strategies,"skipped_strategies":skipped_strategies,"migrated_watchlists":migrated_watchlists,"merged_watchlist_codes":merged_watchlist_codes}


@router.post("/migrate-legacy", dependencies=[Depends(verify_internal_token)])
async def migrate_legacy_assets(body: dict) -> dict:
    strategies = body.get("strategies") if isinstance(body.get("strategies"), list) else []
    watchlists = body.get("watchlists") if isinstance(body.get("watchlists"), list) else []
    migrated_strategies = skipped_strategies = migrated_watchlists = merged_watchlist_codes = 0

    for item in strategies:
        uid = str(item.get("user_id") or "").strip()
        name = str(item.get("name") or "").strip()
        formula = str(item.get("formula") or "").strip()
        timeframe = str(item.get("timeframe") or "D").strip()
        if not uid or not name or not formula or timeframe not in ("D", "W", "M"):
            skipped_strategies += 1
            continue
        try:
            async with acquire() as conn:
                if await conn.fetchrow("SELECT id FROM strategies WHERE user_id=? AND name=?", uid, name):
                    skipped_strategies += 1
                    continue
                row = await conn.fetchrow(
                    "INSERT INTO strategies(user_id,name,formula,timeframe,source_backtest_strategy_id,"
                    "source_backtest_strategy_version,source_backtest_strategy_name,source_backtest_strategy_trigger,"
                    "source_backtest_artifact_id,source_backtest_artifact_title,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,COALESCE(?,datetime('now')),COALESCE(?,datetime('now'))) RETURNING id",
                    uid, name, formula, timeframe,
                    item.get("source_backtest_strategy_id"), item.get("source_backtest_strategy_version"),
                    item.get("source_backtest_strategy_name"), item.get("source_backtest_strategy_trigger"),
                    item.get("source_backtest_artifact_id"), item.get("source_backtest_artifact_title"),
                    item.get("created_at"), item.get("updated_at"),
                )
                versions = item.get("versions") if isinstance(item.get("versions"), list) else []
                clean, seen = [], set()
                for v in versions[:100]:
                    try: vn = int(v.get("version_no"))
                    except Exception: continue
                    if vn <= 0 or vn in seen: continue
                    vn_name = str(v.get("name") or name).strip()
                    vn_formula = str(v.get("formula") or formula).strip()
                    vn_tf = str(v.get("timeframe") or timeframe).strip()
                    if not vn_name or not vn_formula or vn_tf not in ("D", "W", "M"): continue
                    src = v.get("source_backtest") if isinstance(v.get("source_backtest"), dict) else {}
                    clean.append((vn, vn_name, vn_formula, vn_tf, src, v.get("created_at")))
                    seen.add(vn)
                if not clean:
                    clean = [(1, name, formula, timeframe, {}, item.get("created_at"))]
                for vn, vn_name, vn_formula, vn_tf, src, created_at in sorted(clean):
                    await conn.execute(
                        "INSERT INTO strategy_versions(strategy_id,version_no,name,formula,timeframe,"
                        "source_backtest_strategy_id,source_backtest_strategy_version,source_backtest_strategy_name,"
                        "source_backtest_strategy_trigger,created_at) VALUES(?,?,?,?,?,?,?,?,?,COALESCE(?,datetime('now')))",
                        row["id"], vn, vn_name, vn_formula, vn_tf,
                        src.get("strategy_id"), src.get("version_no"), src.get("name"), src.get("trigger"),
                        created_at,
                    )
            migrated_strategies += 1
        except Exception:
            skipped_strategies += 1

    for item in watchlists:
        uid = str(item.get("user_id") or "").strip()
        name = str(item.get("name") or "").strip()
        if not uid or not name or len(name) > 40: continue
        codes = list(dict.fromkeys(str(x or "").strip() for x in (item.get("codes") or []) if str(x or "").strip()))
        if len(codes) > 5000: continue
        try:
            async with acquire() as conn:
                existing = await conn.fetchrow("SELECT id FROM watchlists WHERE user_id=? AND name=?", uid, name)
                if existing:
                    list_id = existing["id"]
                elif item.get("is_default"):
                    default = await conn.fetchrow("SELECT id FROM watchlists WHERE user_id=? AND is_default=1 LIMIT 1", uid)
                    if default:
                        list_id = default["id"]
                    else:
                        row = await conn.fetchrow(
                            "INSERT INTO watchlists(user_id,name,is_default,created_at,updated_at) "
                            "VALUES(?,?,1,COALESCE(?,datetime('now')),COALESCE(?,datetime('now'))) RETURNING id",
                            uid, name, item.get("created_at"), item.get("updated_at"),
                        )
                        list_id = row["id"]
                        migrated_watchlists += 1
                else:
                    row = await conn.fetchrow(
                        "INSERT INTO watchlists(user_id,name,is_default,created_at,updated_at) "
                        "VALUES(?,?,0,COALESCE(?,datetime('now')),COALESCE(?,datetime('now'))) RETURNING id",
                        uid, name, item.get("created_at"), item.get("updated_at"),
                    )
                    list_id = row["id"]
                    migrated_watchlists += 1
                for code in codes:
                    cur = await conn.execute(
                        "INSERT OR IGNORE INTO watchlist_items(watchlist_id,code) VALUES(?,?)", list_id, code
                    )
                    merged_watchlist_codes += max(0, cur.rowcount)
        except Exception:
            continue

    return {
        "migrated_strategies": migrated_strategies,
        "skipped_strategies": skipped_strategies,
        "migrated_watchlists": migrated_watchlists,
        "merged_watchlist_codes": merged_watchlist_codes,
    }
