import pyarrow as pa
import polars as pl

class AShareDataSchema:
    # === 字段常量 ===
    DATE = 'date'
    CODE = 'code'
    
    # 1. 个股日线 (Stock Kline)
    OPEN = 'open'
    HIGH = 'high'
    LOW = 'low'
    CLOSE = 'close'
    VOLUME = 'volume'
    AMOUNT = 'amount'
    TURN = 'turn'
    PCT_CHG = 'pctChg'
    PE_TTM = 'peTTM'
    PB_MRQ = 'pbMRQ'
    ADJ_FACTOR = 'adjustFactor'
    IS_ST = 'isST'

    # 3. 板块行情 (Sector Kline)
    NAME = 'name'
    TYPE = 'type'

    @staticmethod
    def get_stock_kline_schema():
        # Polars Schema (用于 scan_parquet 时的类型强制优化)
        return {
            AShareDataSchema.DATE: pl.String,
            AShareDataSchema.CODE: pl.String,
            AShareDataSchema.OPEN: pl.Float32,
            AShareDataSchema.HIGH: pl.Float32,
            AShareDataSchema.LOW: pl.Float32,
            AShareDataSchema.CLOSE: pl.Float32,
            AShareDataSchema.VOLUME: pl.Float64,
            AShareDataSchema.AMOUNT: pl.Float64,
            AShareDataSchema.TURN: pl.Float32,
            AShareDataSchema.PCT_CHG: pl.Float32,
            AShareDataSchema.PE_TTM: pl.Float32,
            AShareDataSchema.PB_MRQ: pl.Float32,
            AShareDataSchema.ADJ_FACTOR: pl.Float32,
            AShareDataSchema.IS_ST: pl.Int8
        }
