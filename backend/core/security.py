import ast
import polars as pl
from typing import Any
from .data_manager import data_manager

class BlinkParser:
    def __init__(self):
        # 基础算子映射
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
            ast.NotEq: lambda l, r: l != r,
            ast.BitAnd: lambda l, r: l & r,
            ast.BitOr: lambda l, r: l | r,
            ast.And: lambda l, r: l & r,
            ast.Or: lambda l, r: l | r,
        }

        # 字段映射 (映射到 Parquet 列名)
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
        if not expr_str or not expr_str.strip():
            raise ValueError("Empty formula")
        try:
            # 清洗公式，将 && 替换为 &，|| 替换为 | (方便用户输入)
            clean_expr = expr_str.replace('&&', '&').replace('||', '|')
            tree = ast.parse(clean_expr.strip(), mode='eval')
            return self._visit(tree.body)
        except Exception as e:
            raise ValueError(f"Formula Error: {str(e)}")

    def _visit(self, node: Any) -> Any:
        # 1. 处理常量 (数字)
        if isinstance(node, ast.Constant):
            return node.value

        # 2. 处理变量 (CLOSE, VOL 等)
        elif isinstance(node, ast.Name):
            name = node.id.upper()
            if name in self.fields:
                return self.fields[name]
            raise ValueError(f"Unknown variable: {name}")

        # 3. 处理二元运算 (+, -, *, /)
        elif isinstance(node, ast.BinOp):
            left, right = self._visit(node.left), self._visit(node.right)
            return self.operators[type(node.op)](left, right)

        # 4. 处理比较运算 (>, <, ==)
        elif isinstance(node, ast.Compare):
            left = self._visit(node.left)
            combined = None
            for op, right_node in zip(node.ops, node.comparators):
                right = self._visit(right_node)
                res = self.operators[type(op)](left, right)
                combined = res if combined is None else combined & res
                left = right
            return combined

        # 5. 处理布尔运算 (AND, OR)
        elif isinstance(node, ast.BoolOp):
            values = [self._visit(v) for v in node.values]
            res = values[0]
            for v in values[1:]:
                res = self.operators[type(node.op)](res, v)
            return res

        # 6. 处理函数调用 (MA, EMA, STD 等)
        elif isinstance(node, ast.Call):
            func_name = node.func.id.upper()
            
            # --- JIT 缓存命中逻辑 ---
            if func_name in ['MA', 'EMA', 'STD', 'ROC'] and len(node.args) == 2:
                # 尝试匹配预计算列名，例如 MA_CLOSE_250
                if isinstance(node.args[0], ast.Name) and isinstance(node.args[1], ast.Constant):
                    arg_field = node.args[0].id.upper()
                    arg_param = node.args[1].value
                    cache_key = f"{func_name}_{arg_field}_{arg_param}"
                    
                    # 如果数据管理器中已经有这一列，直接使用列名 (极速)
                    if data_manager.df_daily is not None and cache_key in data_manager.df_daily.columns:
                        return pl.col(cache_key)

            # --- 向量化计算回退逻辑 ---
            args = [self._visit(arg) for arg in node.args]
            
            if func_name == 'MA':
                # rolling_mean 必须配 .over("code") 否则会跨股票计算
                return args[0].rolling_mean(window_size=int(args[1])).over("code")
            elif func_name == 'EMA':
                return args[0].ewm_mean(span=int(args[1]), adjust=False).over("code")
            elif func_name == 'STD':
                return args[0].rolling_std(window_size=int(args[1])).over("code")
            elif func_name == 'REF':
                return args[0].shift(int(args[1])).over("code")
            elif func_name == 'ABS':
                return args[0].abs()
            elif func_name == 'ROC':
                # (Close / Ref(Close, n) - 1) * 100
                return ((args[0] / args[0].shift(int(args[1])).over("code")) - 1) * 100

            raise ValueError(f"Unsupported function: {func_name}")

        raise ValueError(f"Syntax not allowed: {type(node)}")

blink_parser = BlinkParser()
