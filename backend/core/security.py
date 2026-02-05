import ast
import polars as pl
from typing import Any
from .data_manager import data_manager

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
            ast.And: lambda l, r: l & r,
            ast.Or: lambda l, r: l | r,
        }

        self.fields = {
            'CLOSE': pl.col('close'),
            'OPEN': pl.col('open'),
            'HIGH': pl.col('high'),
            'LOW': pl.col('low'),
            'VOL': pl.col('volume'),
            'AMOUNT': pl.col('amount'),
            'PCT_CHG': pl.col('pctChg'),
            'TURN': pl.col('turn'),
            'S_CLOSE': pl.col('s_close'),
            'S_PCT_CHG': pl.col('s_pctChg'),
        }

    def parse_expression(self, expr_str: str) -> pl.Expr:
        try:
            tree = ast.parse(expr_str.strip(), mode='eval')
            return self._visit(tree.body)
        except Exception as e:
            raise ValueError(f"Formula Error: {str(e)}")

    def _visit(self, node: Any) -> Any:
        if isinstance(node, ast.Constant):
            return node.value

        elif isinstance(node, ast.Name):
            name = node.id.upper()
            if name in self.fields:
                return self.fields[name]
            raise ValueError(f"Unknown variable: {name}")

        elif isinstance(node, ast.BinOp):
            left, right = self._visit(node.left), self._visit(node.right)
            return self.operators[type(node.op)](left, right)

        elif isinstance(node, ast.Compare):
            left = self._visit(node.left)
            combined = None
            for op, right_node in zip(node.ops, node.comparators):
                right = self._visit(right_node)
                res = self.operators[type(op)](left, right)
                combined = res if combined is None else combined & res
                left = right
            return combined

        elif isinstance(node, ast.BoolOp):
            values = [self._visit(v) for v in node.values]
            res = values[0]
            for v in values[1:]:
                res = self.operators[type(node.op)](res, v)
            return res

        elif isinstance(node, ast.Call):
            func_name = node.func.id.upper()
            
            # --- Step 5 核心：智能路由重定向 ---
            # 尝试构建标准缓存键，如 MA_CLOSE_20
            if func_name in ['MA', 'EMA', 'STD', 'ROC'] and len(node.args) == 2:
                arg_field = ""
                # 提取第一个参数名，如 CLOSE
                if isinstance(node.args[0], ast.Name):
                    arg_field = node.args[0].id.upper()
                # 提取第二个参数值，如 20
                if isinstance(node.args[1], ast.Constant):
                    arg_param = node.args[1].value
                    
                    cache_key = f"{func_name}_{arg_field}_{arg_param}"
                    
                    # 检查内存列中是否已有该预计算结果
                    if data_manager.df_daily is not None and cache_key in data_manager.df_daily.columns:
                        # 命中：直接返回列引用 (O(1) 复杂度)
                        return pl.col(cache_key)

            # --- Fallback: 未命中缓存，执行原始向量化计算 ---
            args = [self._visit(arg) for arg in node.args]
            
            if func_name == 'MA':
                return args[0].rolling_mean(window_size=int(args[1])).over("code")
            elif func_name == 'EMA':
                return args[0].ewm_mean(span=int(args[1]), adjust=False).over("code")
            elif func_name == 'REF':
                return args[0].shift(int(args[1])).over("code")
            elif func_name == 'STD':
                return args[0].rolling_std(window_size=int(args[1])).over("code")
            elif func_name == 'ABS':
                return args[0].abs()

            raise ValueError(f"Unsupported function: {func_name}")

        raise ValueError(f"Syntax not allowed: {type(node)}")

blink_parser = BlinkParser()
