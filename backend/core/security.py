import ast
import polars as pl
from typing import Any
from .data_manager import data_manager

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
        }
        # 当前解析上下文
        self.current_df = None

    def parse_expression(self, expr_str: str, timeframe: str = 'D') -> pl.Expr:
        """解析入口：根据 timeframe 设置当前数据上下文"""
        if timeframe == 'W': self.current_df = data_manager.df_weekly
        elif timeframe == 'M': self.current_df = data_manager.df_monthly
        else: self.current_df = data_manager.df_daily
        
        # 兼容性替换
        clean_expr = expr_str.strip().replace('&&','&').replace('||','|')
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
            func = node.func.id.upper()
            if func in ['MA', 'EMA', 'STD', 'ROC'] and len(node.args) == 2:
                if isinstance(node.args[0], ast.Name) and isinstance(node.args[1], ast.Constant):
                    arg_field = node.args[0].id.upper()
                    arg_param = node.args[1].value
                    
                    # 生成统一列名：MA_CLOSE_20 (无后缀)
                    pure_key = f"{func}_{arg_field}_{arg_param}"
                    
                    # 检查当前上下文的 DF 中是否有该列
                    if self.current_df is not None and pure_key in self.current_df.columns:
                        return pl.col(pure_key)

            # 回退：实时向量化计算
            args = [self._visit(arg) for arg in node.args]
            if func == 'MA': return args[0].rolling_mean(window_size=int(args[1])).over("code")
            if func == 'EMA': return args[0].ewm_mean(span=int(args[1]), adjust=False).over("code")
            if func == 'STD': return args[0].rolling_std(window_size=int(args[1])).over("code")
            if func == 'REF': return args[0].shift(int(args[1])).over("code")
            if func == 'ROC': return ((args[0] / args[0].shift(int(args[1])).over("code")) - 1) * 100
            
            raise ValueError(f"Unknown function {func}")

        raise ValueError(f"Syntax not allowed: {type(node)}")

blink_parser = BlinkParser()
