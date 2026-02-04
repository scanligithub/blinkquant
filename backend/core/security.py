import ast
import polars as pl
from typing import Any

class BlinkParser:
    """
    BlinkQuant AST 解析器
    将字符串公式转换为 Polars 表达式 (pl.Expr)
    """
    def __init__(self):
        # 允许的运算符映射
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

        # 允许的字段映射 (不区分大小写)
        self.fields = {
            'CLOSE': pl.col('close'),
            'OPEN': pl.col('open'),
            'HIGH': pl.col('high'),
            'LOW': pl.col('low'),
            'VOL': pl.col('volume'),
            'AMOUNT': pl.col('amount'),
            'PCT_CHG': pl.col('pctChg'),
            'TURN': pl.col('turn'),
        }

    def parse_expression(self, expr_str: str) -> pl.Expr:
        """入口函数"""
        try:
            tree = ast.parse(expr_str, mode='eval')
            return self._visit(tree.body)
        except Exception as e:
            raise ValueError(f"Formula Error: {str(e)}")

    def _visit(self, node: Any) -> Any:
        # 处理数字 (如 20)
        if isinstance(node, ast.Constant):
            return node.value

        # 处理变量 (如 CLOSE)
        elif isinstance(node, ast.Name):
            name = node.id.upper()
            if name in self.fields:
                return self.fields[name]
            raise ValueError(f"Unsupported field: {name}")

        # 处理二元运算 (如 CLOSE > 20)
        elif isinstance(node, ast.Compare):
            left = self._visit(node.left)
            combined = None
            for op, right_node in zip(node.ops, node.comparators):
                right = self._visit(right_node)
                op_func = self.operators.get(type(op))
                if not op_func:
                    raise ValueError(f"Unsupported operator: {type(op)}")
                res = op_func(left, right)
                combined = res if combined is None else combined & res
            return combined

        # 处理数学运算 (如 CLOSE + OPEN)
        elif isinstance(node, ast.BinOp):
            left = self._visit(node.left)
            right = self._visit(node.right)
            op_func = self.operators.get(type(node.op))
            if not op_func:
                raise ValueError(f"Unsupported binary op: {type(node.op)}")
            return op_func(left, right)

        # 处理函数调用 (如 MA(CLOSE, 20))
        elif isinstance(node, ast.Call):
            func_name = node.func.id.upper()
            args = [self._visit(arg) for arg in node.args]

            if func_name == 'MA':
                # 参数: MA(col, window)
                return args[0].rolling_mean(window_size=int(args[1]))
            elif func_name == 'EMA':
                return args[0].ewm_mean(span=int(args[1]), adjust=False)
            elif func_name == 'REF':
                # 参数: REF(col, offset)
                return args[0].shift(int(args[1]))
            elif func_name == 'STD':
                return args[0].rolling_std(window_size=int(args[1]))
            elif func_name == 'ABS':
                return args[0].abs()
            
            raise ValueError(f"Unsupported function: {func_name}")

        raise ValueError(f"Unsupported syntax node: {type(node)}")

# 单例
blink_parser = BlinkParser()
