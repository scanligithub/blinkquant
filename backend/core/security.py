import ast
import polars as pl
from typing import Any

class BlinkParser:
    def __init__(self):
        self.operators = {
            ast.Add: lambda l, r: l + r,
            ast.Sub: lambda l, r: l - r,
            ast.Mult: lambda l, r: l * r,
            ast.Div: lambda l, r: l / r,
            ast.Gt: lambda l, r: l > r,
            ast.Lt: lambda l, r: l < r,
            ast.GtE: lambda l, r: l >= r,
            ast.LtE: lambda l, r: l <= r,
            ast.Eq: lambda l, r: l == r,
            ast.BitAnd: lambda l, r: l & r,
            ast.BitOr: lambda l, r: l | r,
        }

        # 个股字段
        self.fields = {
            'CLOSE': pl.col('close'),
            'OPEN': pl.col('open'),
            'HIGH': pl.col('high'),
            'LOW': pl.col('low'),
            'VOL': pl.col('volume'),
            'PCT_CHG': pl.col('pctChg'),
            # 板块对应字段 (前缀 S_)
            'S_CLOSE': pl.col('s_close'),
            'S_OPEN': pl.col('s_open'),
            'S_HIGH': pl.col('s_high'),
            'S_LOW': pl.col('s_low'),
            'S_PCT_CHG': pl.col('s_pctChg'),
        }

    def parse_expression(self, expr_str: str) -> pl.Expr:
        try:
            tree = ast.parse(expr_str, mode='eval')
            return self._visit(tree.body)
        except Exception as e:
            raise ValueError(f"AST Error: {str(e)}")

    def _visit(self, node: Any) -> Any:
        if isinstance(node, ast.Constant): return node.value
        elif isinstance(node, ast.Name):
            name = node.id.upper()
            if name in self.fields: return self.fields[name]
            raise ValueError(f"Unknown field: {name}")
        elif isinstance(node, ast.Compare):
            left = self._visit(node.left)
            combined = None
            for op, right_node in zip(node.ops, node.comparators):
                right = self._visit(right_node)
                res = self.operators[type(op)](left, right)
                combined = res if combined is None else combined & res
            return combined
        elif isinstance(node, ast.BinOp):
            return self.operators[type(node.op)](self._visit(node.left), self._visit(node.right))
        elif isinstance(node, ast.Call):
            func_name = node.func.id.upper()
            args = [self._visit(arg) for arg in node.args]
            if func_name == 'MA': return args[0].rolling_mean(window_size=int(args[1]))
            if func_name == 'REF': return args[0].shift(int(args[1]))
            if func_name == 'STD': return args[0].rolling_std(window_size=int(args[1]))
            raise ValueError(f"Unknown func: {func_name}")
        raise ValueError("Unsupported Syntax")

blink_parser = BlinkParser()
