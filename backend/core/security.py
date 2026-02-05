import ast
import polars as pl
from typing import Any
from .data_manager import data_manager

class BlinkParser:
    def __init__(self):
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

    def parse_expression(self, expr_str: str) -> pl.Expr:
        tree = ast.parse(expr_str.strip().replace('&&','&').replace('||','|'), mode='eval')
        return self._visit(tree.body)

    def _visit(self, node: Any) -> Any:
        if isinstance(node, ast.Constant): return node.value
        elif isinstance(node, ast.Name):
            name = node.id.upper()
            # 增加检查：如果该 Name 本身就是一个已经预计算好的 Key (如 MA_CLOSE_20)
            if data_manager.df_daily is not None and name in data_manager.df_daily.columns:
                return pl.col(name)
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
                # 尝试命中进化/预计算列
                if isinstance(node.args[0], ast.Name) and isinstance(node.args[1], ast.Constant):
                    cache_key = f"{func}_{node.args[0].id.upper()}_{node.args[1].value}"
                    if data_manager.df_daily is not None and cache_key in data_manager.df_daily.columns:
                        return pl.col(cache_key)

            args = [self._visit(arg) for arg in node.args]
            if func == 'MA': return args[0].rolling_mean(window_size=int(args[1])).over("code")
            if func == 'EMA': return args[0].ewm_mean(span=int(args[1]), adjust=False).over("code")
            if func == 'STD': return args[0].rolling_std(window_size=int(args[1])).over("code")
            if func == 'REF': return args[0].shift(int(args[1])).over("code")
            if func == 'ROC': return ((args[0] / args[0].shift(int(args[1])).over("code")) - 1) * 100
            raise ValueError(f"Unknown function {func}")

blink_parser = BlinkParser()
