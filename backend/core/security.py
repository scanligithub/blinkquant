import ast
import polars as pl
from typing import Any

class BlinkParser:
    """
    BlinkQuant AST 安全解析引擎
    将用户输入的字符串公式安全地转换为 Polars 表达式链
    """
    def __init__(self):
        # 1. 允许的运算符映射 (运算/比较/逻辑)
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

        # 2. 允许的字段映射 (个股与板块)
        self.fields = {
            # 个股字段
            'CLOSE': pl.col('close'),
            'OPEN': pl.col('open'),
            'HIGH': pl.col('high'),
            'LOW': pl.col('low'),
            'VOL': pl.col('volume'),
            'AMOUNT': pl.col('amount'),
            'PCT_CHG': pl.col('pctChg'),
            'TURN': pl.col('turn'),
            # 板块/指数对照字段 (前缀 S_)
            'S_CLOSE': pl.col('s_close'),
            'S_OPEN': pl.col('s_open'),
            'S_HIGH': pl.col('s_high'),
            'S_LOW': pl.col('s_low'),
            'S_PCT_CHG': pl.col('s_pctChg'),
        }

    def parse_expression(self, expr_str: str) -> pl.Expr:
        """解析入口"""
        try:
            # 去除首尾空格并清理潜在的非法字符
            expr_str = expr_str.strip()
            if not expr_str:
                raise ValueError("Formula cannot be empty")
            
            tree = ast.parse(expr_str, mode='eval')
            return self._visit(tree.body)
        except Exception as e:
            # 统一异常处理，防止抛出系统级错误
            raise ValueError(f"Invalid Formula Syntax: {str(e)}")

    def _visit(self, node: Any) -> Any:
        """递归遍历 AST 节点"""
        
        # 3. 处理常数 (数字)
        if isinstance(node, ast.Constant):
            return node.value

        # 4. 处理变量 (CLOSE, MA 等)
        elif isinstance(node, ast.Name):
            name = node.id.upper()
            if name in self.fields:
                return self.fields[name]
            raise ValueError(f"Unknown variable: {name}")

        # 5. 处理二元数学运算 (CLOSE + OPEN)
        elif isinstance(node, ast.BinOp):
            left = self._visit(node.left)
            right = self._visit(node.right)
            op_type = type(node.op)
            if op_type in self.operators:
                return self.operators[op_type](left, right)
            raise ValueError(f"Unsupported operator: {op_type}")

        # 6. 处理比较运算 (CLOSE > 20)
        elif isinstance(node, ast.Compare):
            left = self._visit(node.left)
            combined = None
            for op, right_node in zip(node.ops, node.comparators):
                right = self._visit(right_node)
                op_type = type(op)
                if op_type in self.operators:
                    res = self.operators[op_type](left, right)
                    combined = res if combined is None else combined & res
                    left = right # 支持链式比较如 10 < CLOSE < 20
                else:
                    raise ValueError(f"Unsupported comparison: {op_type}")
            return combined

        # 7. 处理逻辑运算 ( (C > 20) and (V > 100) )
        elif isinstance(node, ast.BoolOp):
            values = [self._visit(v) for v in node.values]
            op_type = type(node.op)
            if op_type in self.operators:
                res = values[0]
                for v in values[1:]:
                    res = self.operators[op_type](res, v)
                return res
            raise ValueError(f"Unsupported logic op: {op_type}")

        # 8. 处理函数调用 (MA, REF, STD, ABS)
        elif isinstance(node, ast.Call):
            func_name = node.func.id.upper()
            args = [self._visit(arg) for arg in node.args]

            # 均线: MA(CLOSE, 20)
            if func_name == 'MA':
                # 必须 .over("code") 确保在分布式节点内不同股票计算相互隔离
                return args[0].rolling_mean(window_size=int(args[1])).over("code")
            
            # 引用: REF(CLOSE, 1) 获取昨收
            elif func_name == 'REF':
                return args[0].shift(int(args[1])).over("code")
            
            # 标准差: STD(CLOSE, 20)
            elif func_name == 'STD':
                return args[0].rolling_std(window_size=int(args[1])).over("code")
            
            # 绝对值: ABS(PCT_CHG)
            elif func_name == 'ABS':
                return args[0].abs()

            raise ValueError(f"Unsupported function: {func_name}")

        raise ValueError(f"Syntax not allowed: {type(node)}")

# 单例模式供外部调用
blink_parser = BlinkParser()
