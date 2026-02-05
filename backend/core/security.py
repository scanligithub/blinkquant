import ast
import polars as pl
from typing import Any
from .data_manager import data_manager

class BlinkParser:
    def __init__(self):
        # ... (算子定义保持不变) ...
        self.operators = {
            ast.Add: lambda l, r: l + r, ast.Sub: lambda l, r: l - r,
            ast.Mult: lambda l, r: l * r, ast.Div: lambda l, r: l / r,
            ast.Gt: lambda l, r: l > r, ast.Lt: lambda l, r: l < r,
            ast.GtE: lambda l, r: l >= r, ast.LtE: lambda l, r: l <= r,
            ast.Eq: lambda l, r: l == r, ast.BitAnd: lambda l, r: l & r,
            ast.BitOr: lambda l, r: l | r, ast.And: lambda l, r: l & r,
            ast.Or: lambda l, r: l | r,
        }
        self.fields = {
            'CLOSE': pl.col('close'), 'OPEN': pl.col('open'),
            'HIGH': pl.col('high'), 'LOW': pl.col('low'),
            'VOL': pl.col('volume'), 'AMOUNT': pl.col('amount'),
            'PCT_CHG': pl.col('pctChg'), 'S_CLOSE': pl.col('s_close'),
        }
        # 新增：当前解析上下文
        self.current_timeframe = 'D'
        self.current_df = None

    def parse_expression(self, expr_str: str, timeframe: str = 'D') -> pl.Expr:
        """解析入口，传入当前周期"""
        self.current_timeframe = timeframe
        # 根据周期确定当前使用的数据表，用于判断缓存是否存在
        if timeframe == 'W': self.current_df = data_manager.df_weekly
        elif timeframe == 'M': self.current_df = data_manager.df_monthly
        else: self.current_df = data_manager.df_daily
        
        tree = ast.parse(expr_str.strip().replace('&&','&').replace('||','|'), mode='eval')
        return self._visit(tree.body)

    def _visit(self, node: Any) -> Any:
        if isinstance(node, ast.Constant): return node.value
        elif isinstance(node, ast.Name):
            name = node.id.upper()
            # 优先检查带后缀的缓存列
            suffix = {'W': '_W', 'M': '_M'}.get(self.current_timeframe, '')
            suffixed_name = f"{name}{suffix}"
            
            if self.current_df is not None:
                if suffixed_name in self.current_df.columns: return pl.col(suffixed_name)
                if name in self.current_df.columns: return pl.col(name)

            return self.fields.get(name, pl.col(name.lower()))

        # ... (BinOp, Compare, BoolOp 保持不变) ...
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
            func = node.func.id.upper()
            if func in ['MA', 'EMA', 'STD', 'ROC'] and len(node.args) == 2:
                if isinstance(node.args[0], ast.Name) and isinstance(node.args[1], ast.Constant):
                    arg_field = node.args[0].id.upper()
                    arg_param = node.args[1].value
                    
                    # 关键逻辑：构造带后缀的缓存键
                    suffix = {'W': '_W', 'M': '_M'}.get(self.current_timeframe, '')
                    cache_key = f"{func}_{arg_field}_{arg_param}{suffix}"
                    
                    # 如果该带后缀的列在当前 DF 中存在，直接返回
                    if self.current_df is not None and cache_key in self.current_df.columns:
                        return pl.col(cache_key)

            args = [self._visit(arg) for arg in node.args]
            # 如果没命中缓存，回退到原始计算
            if func == 'MA': return args[0].rolling_mean(window_size=int(args[1])).over("code")
            if func == 'EMA': return args[0].ewm_mean(span=int(args[1]), adjust=False).over("code")
            if func == 'STD': return args[0].rolling_std(window_size=int(args[1])).over("code")
            if func == 'REF': return args[0].shift(int(args[1])).over("code")
            if func == 'ROC': return ((args[0] / args[0].shift(int(args[1])).over("code")) - 1) * 100
            raise ValueError(f"Unknown function {func}")

        raise ValueError(f"Syntax not allowed: {type(node)}")

blink_parser = BlinkParser()
