from fastapi import APIRouter, HTTPException, BackgroundTasks
from fastapi.responses import Response # New import
from pydantic import BaseModel
import polars as pl
import os
import re
import psutil
import psycopg2
from pypinyin import pinyin, Style
from core.data_manager import data_manager
from core.engine import selection_engine
import logging
import io # New import

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1")

# 正则用于提取公式中的指标
METRIC_REGEX = re.compile(r'(MA|EMA|STD|ROC)\s*\(\s*(CLOSE|OPEN|HIGH|LOW|VOL|AMOUNT)\s*,\s*(\d+)\s*\)', re.IGNORECASE)

class SelectionRequest(BaseModel):
    formula: str
    timeframe: str = "D"

def report_metrics_usage(formula: str):
    """
    后台任务：上报指标计数
    策略：全周期统一 Key (如 MA_CLOSE_20)，不带后缀
    """
    if not data_manager.postgres_url: return
    
    matches = METRIC_REGEX.findall(formula)
    if not matches: return

    try:
        conn = psycopg2.connect(data_manager.postgres_url)
        cur = conn.cursor()
        for func, field, param in matches:
            # 统一 Key 格式: MA_CLOSE_20
            metric_key = f"{func.upper()}_{field.upper()}_{param}"
            
            # UPSERT
            cur.execute("""
                INSERT INTO metrics_stats (metric_key, usage_count, last_used)
                VALUES (%s, 1, CURRENT_TIMESTAMP)
                ON CONFLICT (metric_key) 
                DO UPDATE SET usage_count = metrics_stats.usage_count + 1, last_used = CURRENT_TIMESTAMP;
            """, (metric_key,))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"DB Report Error: {e}")

@router.post("/select")
async def select_stocks(req: SelectionRequest, background_tasks: BackgroundTasks):
    if data_manager.df_daily is None:
        raise HTTPException(status_code=503, detail="Nodes are loading data...")
    
    results = selection_engine.execute_selector(req.formula, req.timeframe, background_tasks)
    
    if isinstance(results, dict) and "error" in results:
        raise HTTPException(status_code=400, detail=results["error"])
    
    # 上报热度 (不再需要传 timeframe)
    background_tasks.add_task(report_metrics_usage, req.formula)
    
    return {"node": os.getenv("NODE_INDEX"), "count": len(results), "results": results}

@router.get("/kline")
def get_kline(code: str, timeframe: str = "D"):
    df = data_manager.df_daily
    if timeframe == "W": df = data_manager.df_weekly
    elif timeframe == "M": df = data_manager.df_monthly

    if df is None: raise HTTPException(status_code=503, detail="Data not ready")
    
    # 过滤并排序
    stock_df = df.filter(pl.col("code") == code).sort("date")

    # 仅选择 K 线图所需的核心列
    stock_df = stock_df.select(["date", "code", "open", "high", "low", "close", "volume", "amount", "turn", "pctChg", "peTTM", "pbMRQ", "isST", "adjustFactor", "net_amount", "main_net", "super_net", "large_net", "medium_net", "small_net"])



    if len(stock_df) == 0:
        raise HTTPException(status_code=404, detail="Stock not found")

    # 将 Polars DataFrame 写入内存中的 Parquet 文件，并使用 ZSTD 压缩
    buffer = io.BytesIO()
    stock_df.write_parquet(buffer, compression="zstd")
    buffer.seek(0) # 将文件指针移到开头
    

    # 以二进制响应的形式返回 Parquet 数据
    return Response(content=buffer.getvalue(), media_type="application/octet-stream")

def _get_pinyin_initials(text: str) -> str:
    """获取中文文本的拼音首字母，并转换为小写"""
    if not text:
        return ""
    
    # 检查是否包含中文字符
    if not any('\u4e00' <= char <= '\u9fff' for char in text):
        return text.lower() # 如果没有中文，直接返回小写

    # full模式返回所有拼音，然后取首字母并拼接
    pinyin_list = pinyin(text, style=Style.FIRST_LETTER)
    initials = ''.join([item[0] for item in pinyin_list])
    # 只保留字母字符，移除所有非字母字符（如空格、括号、数字等）
    return ''.join(c for c in initials.lower() if c.isalpha())

@router.get("/search")
def search_stocks(q: str):
    if not q:
        return []

    q_lower = q.lower()
    q_pinyin_initials = _get_pinyin_initials(q)
    logger.info(f"Search query: {q}, q_lower: {q_lower}, q_pinyin_initials: {q_pinyin_initials}")

    results = []
    
    for code, name in data_manager.code_to_name.items():
        name_lower = name.lower()
        name_pinyin_initials = _get_pinyin_initials(name)
        logger.debug(f"Checking stock: code={code}, name={name}, name_lower={name_lower}, name_pinyin_initials={name_pinyin_initials}")

        if (q_lower in code.lower() or
            q_lower in name_lower or
            q_pinyin_initials and q_pinyin_initials in name_pinyin_initials):
            results.append({"code": code, "name": name})
        if len(results) >= 10: # Limit to 10 results
            break
            
    return results

@router.get("/stock-list")
def get_stock_list():
    """返回所有股票代码与名称的映射，仅用于前端缓存"""
    # 调试：打印前10条数据
    sample = list(data_manager.code_to_name.items())[:10]
    logger.info(f"Stock list sample: {sample}")
    
    # 过滤掉空名称的股票
    filtered = [{"code": code, "name": name}
                for code, name in data_manager.code_to_name.items()
                if name and name.strip()]
    
    logger.info(f"Total stocks: {len(data_manager.code_to_name)}, Filtered: {len(filtered)}")
    return filtered

@router.get("/status")
def get_node_status():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    
    # 获取系统级统计
    vm = psutil.virtual_memory()
    du = psutil.disk_usage('/')

    return {
        "node": os.getenv("NODE_INDEX"),
        "status": "healthy" if data_manager.df_daily is not None else "loading",
        
        # 进程内存
        "process_memory_gb": round(mem_info.rss / (1024**3), 2),
        
        # 系统内存状态
        "system_memory_total_gb": round(vm.total / (1024**3), 2),
        "system_memory_free_gb": round(vm.available / (1024**3), 2), # available 比 free 更准确反映可用内存
        
        # 磁盘状态
        "disk_total_gb": round(du.total / (1024**3), 2),
        "disk_free_gb": round(du.free / (1024**3), 2),
        
        # 数据量
        "rows_daily": len(data_manager.df_daily) if data_manager.df_daily is not None else 0
    }

@router.get("/health")
def health_check():
    return {"status": "healthy" if data_manager.df_daily is not None else "loading"}
