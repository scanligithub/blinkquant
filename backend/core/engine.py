import polars as pl
import logging
from .data_manager import data_manager
from .security import blink_parser

logger = logging.getLogger(__name__)

class SelectionEngine:
    def execute_selector(self, formula: str, timeframe: str, background_tasks):
        df = data_manager.df_daily if timeframe == 'D' else data_manager.df_weekly
        if df is None: return {"error": "Data loading..."}
        
        lf = df.lazy()
        # 防御性关联板块行情
        if data_manager.df_mapping is not None and data_manager.df_sector_daily is not None:
            s_df = data_manager.df_sector_daily
            sector_exprs = [pl.col("date"), pl.col("code").alias("sector_code"), pl.col("close").alias("s_close")]
            if "pctChg" in s_df.columns: 
                sector_exprs.append(pl.col("pctChg").alias("s_pctChg"))
            
            s_lazy = s_df.lazy().select(sector_exprs)
            lf = (lf.join(data_manager.df_mapping.lazy(), on="code", how="left")
                    .join(s_lazy, on=["date", "sector_code"], how="left"))

        try:
            expr = blink_parser.parse_expression(formula)
            lf = lf.with_columns(expr.alias("_signal"))
            last_date = df.select(pl.col("date").max()).item()
            
            result_df = (lf.filter(pl.col("date") == last_date)
                         .filter(pl.col("_signal").fill_null(False) == True)
                         .select("code")
                         .collect())
            return result_df["code"].to_list()
        except Exception as e:
            return {"error": str(e)}

selection_engine = SelectionEngine()
