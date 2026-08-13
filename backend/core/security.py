import ast
import re
import polars as pl
from typing import Any
from .data_manager import data_manager
from .indicator_registry import INDICATORS, FIELDS

def _require_whitelist_field(node: ast.AST) -> str:
    """参数必须是白名单字段名的 ast.Name。返回大写字段名。"""
    if not isinstance(node, ast.Name):
        raise ValueError("Field argument must be a field name")
    name = node.id.upper()
    if name not in FIELDS:
        raise ValueError(f"Unknown field {name}")
    return name

def _require_positive_int(node: ast.AST) -> int:
    """参数必须是正整数常量。"""
    if not isinstance(node, ast.Constant) or isinstance(node.value, bool) or not isinstance(node.value, int):
        raise ValueError("Window argument must be an integer constant")
    if node.value <= 0:
        raise ValueError("Window argument must be positive")
    return node.value

class BlinkParser:
    def __init__(self):
        # 基础算子映射
        self.operators = {
            ast.Add: lambda l, r: l + r, ast.Sub: lambda l, r: l - r,
            ast.Mult: lambda l, r: l * r, ast.Div: lambda l, r: l / r,
            ast.Gt: lambda l, r: l > r, ast.Lt: lambda l, r: l < r,
            ast.GtE: lambda l, r: l >= r, ast.LtE: lambda l, r: l <= r,
            ast.Eq: lambda l, r: l == r, ast.BitAnd: lambda l, r: l & r,
            ast.BitOr: lambda l, r: l | r, ast.And: lambda l, r: l & r,
            ast.Or: lambda l, r: l | r,
        }
        # 字段映射
        self.fields = {
            'CLOSE': pl.col('close'), 'OPEN': pl.col('open'),
            'HIGH': pl.col('high'), 'LOW': pl.col('low'),
            'VOL': pl.col('volume'), 'AMOUNT': pl.col('amount'),
            'PCT_CHG': pl.col('pctChg'), 'S_CLOSE': pl.col('s_close'),
            # ★ 新增：基本面/事件因子（数值型）
            'PE_TTM': pl.col('peTTM'),
            'PB_MRQ': pl.col('pbMRQ'),
            'FORECAST_YOY': pl.col('forecast_yoy'),
            'IS_FORECAST_GOOD': pl.col('is_forecast_good'),
            'IS_FORECAST_BAD': pl.col('is_forecast_bad'),
            # ★ 新增：股本市值因子（数值型）
            'TOTAL_SHARES': pl.col('total_shares'),
            'FLOAT_SHARES': pl.col('float_shares'),
            'TOTAL_MV': pl.col('total_mv'),
            'FLOAT_MV': pl.col('float_mv'),
            'TURN': pl.col('turn'),
        }
        # 当前解析上下文
        self.current_df = None

    def parse_expression(self, expr_str: str, timeframe: str = 'D') -> pl.Expr:
        """解析入口：根据 timeframe 设置当前数据上下文"""
        if timeframe == 'W': self.current_df = data_manager.df_weekly
        elif timeframe == 'M': self.current_df = data_manager.df_monthly
        else: self.current_df = data_manager.df_daily
        
        # 兼容性替换（含大写逻辑词归一化，兼容 LLM 输出；\b 词边界容忍多空格/开头位置）
        clean_expr = re.sub(r'\b(AND|OR|NOT)\b', lambda m: m.group(1).lower(), expr_str.strip())
        clean_expr = clean_expr.replace('&&', '&').replace('||', '|')
        tree = ast.parse(clean_expr, mode='eval')
        return self._visit(tree.body)

    def _visit(self, node: Any) -> Any:
        if isinstance(node, ast.Constant): return node.value
        
        elif isinstance(node, ast.Name):
            name = node.id.upper()
            # 1. 如果该名称已经是内存中的列（如 MA_CLOSE_20），直接引用
            if self.current_df is not None and name in self.current_df.columns:
                return pl.col(name)
            # 2. 否则查找基础字段映射
            return self.fields.get(name, pl.col(name.lower()))

        elif isinstance(node, ast.BinOp):
            return self.operators[type(node.op)](self._visit(node.left), self._visit(node.right))

        elif isinstance(node, ast.Compare):
            left = self._visit(node.left)
            res = self.operators[type(node.ops[0])](left, self._visit(node.comparators[0]))
            return res

        elif isinstance(node, ast.BoolOp):
            values = [self._visit(v) for v in node.values]
            res = values[0]
            for v in values[1:]: res = self.operators[type(node.op)](res, v)
            return res

        elif isinstance(node, ast.Call):
            # 非 Name 函数名（如 foo.bar(1)）统一走 ValueError
            if not isinstance(node.func, ast.Name):
                raise ValueError("Function call target must be a name")
            func = node.func.id.upper()
            if func not in INDICATORS or not INDICATORS[func].get("window"):
                raise ValueError(f"Unknown function {func}")
            if len(node.args) != 2 or node.keywords:
                raise ValueError(f"Function {func} expects exactly 2 positional args")
            field_name = _require_whitelist_field(node.args[0])
            n = _require_positive_int(node.args[1])
            # ★ 快路径必须保留：命中 Hot-JIT 挂载列则直接返回列引用（提速来源，勿删）
            pure_key = f"{func}_{field_name}_{n}"
            if self.current_df is not None and pure_key in self.current_df.columns:
                return pl.col(pure_key)
            # 慢路径：实时向量化计算（首算后 engine 会挂载，下次即命中快路径）
            return INDICATORS[func]["func"](self.fields[field_name], n)

        raise ValueError(f"Syntax not allowed: {type(node)}")

blink_parser = BlinkParser()
