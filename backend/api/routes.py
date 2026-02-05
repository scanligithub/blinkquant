from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
import polars as pl
import os
import psutil
from core.data_manager import data_manager
from core.engine import selection_engine

router = APIRouter(prefix="/api/v1")

class SelectionRequest(BaseModel):
    formula: str
    timeframe: str = "D"

@router.post("/select")
async def select_stocks(req: SelectionRequest, background_tasks: BackgroundTasks):
    """
    选股接口：计算完成后立即返回，将 JIT 挂载任务丢入后台
    """
    if data_manager.df_daily is None:
        raise HTTPException(status_code=503, detail="System initializing...")
    
    # 执行选股，并将后台任务句柄传入
    results = selection_engine.execute_selector(req.formula, req.timeframe, background_tasks)
    
    if isinstance(results, dict) and "error" in results:
        raise HTTPException(status_code=400, detail=results["error"])
        
    return {
        "node": os.getenv("NODE_INDEX"),
        "count": len(results),
        "results": results
    }

@router.get("/status")
def get_node_status():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info().rss / (1024 ** 3)
    return {
        "node": os.getenv("NODE_INDEX"),
        "status": "healthy" if data_manager.df_daily is not None else "loading",
        "memory_used_gb": round(mem_info, 2),
        "data_counts": {
            "daily": len(data_manager.df_daily) if data_manager.df_daily is not None else 0
        },
        "cached_indicators": len(data_manager.column_metadata)
    }

# ... 其他接口(kline, health, peek)保持不变 ...
@router.get("/kline")
async def get_kline(code: str, timeframe: str = "D"):
    """
    K线数据接口：返回个股完整历史数据
    """
    # 1. 路由至正确的数据集
    if timeframe == "W":
        df = data_manager.df_weekly
    elif timeframe == "M":
        df = data_manager.df_monthly
    else:
        df = data_manager.df_daily

    if df is None:
        raise HTTPException(status_code=503, detail="Data not loaded")

    # 2. 提取并排序 (Polars 高效过滤)
    # 我们将日期转换为字符串以适配 JSON 传输
    try:
        stock_data = (
            df.filter(pl.col(AShareDataSchema.CODE) == code)
            .sort(AShareDataSchema.DATE)
            .with_columns(
                pl.col(AShareDataSchema.DATE).dt.strftime("%Y-%m-%d")
            )
        )

        if len(stock_data) == 0:
            raise HTTPException(status_code=404, detail=f"Stock {code} not found on Node {os.getenv('NODE_INDEX')}")

        # 3. 转换为字典列表
        return {
            "code": code,
            "timeframe": timeframe,
            "data": stock_data.to_dicts()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/health")
def health_check():
    """基础健康检查"""
    if data_manager.df_daily is not None:
        return {"status": "healthy"}
    return {"status": "loading"}

@router.get("/peek")
def peek_data():
    """窥探内存中前 10 条数据，确认 code 格式"""
    if data_manager.df_daily is None:
        return {"error": "data not loaded"}
    
    # 采样前 5 行数据看一眼
    sample = data_manager.df_daily.head(5).to_dicts()
    return {
        "sample_data": sample,
        "total_rows": len(data_manager.df_daily)
    }


