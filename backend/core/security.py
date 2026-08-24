import ast
import re
import polars as pl
from typing import Any
from .data_manager import data_manager
from .indicator_registry import INDICATORS, FIELDS, WINDOW_NAMES

def _limit_up_pct_expr():
    """按 code 前缀计算每行涨停幅度：科创(688/689)/创业(30*) → 20，北交所(bj.*) → 30，其余(沪深主板) → 10。

    2026-07-06 起主板 ST 亦 10%（与普通股一致），故仅按代码判板别即可。
    注意：这只是理论限幅，不是实际涨停价（实际涨停价经 0.01 元修约，当日涨幅可能低于限幅）。
    收盘封板/触板请用 IS_LIMIT_UP / IS_TOUCH_LIMIT_UP（由 data_manager 按未复权价预计算，
    含主板 ST 历史 5% 规则）。
    """
    return pl.when(
        pl.col("code").str.starts_with("sh.688")
        | pl.col("code").str.starts_with("sh.689")
        | pl.col("code").str.starts_with("sz.30")
    ).then(pl.lit(20.0)).when(
        pl.col("code").str.starts_with("bj.")
    ).then(pl.lit(30.0)).otherwise(pl.lit(10.0))

def _require_whitelist_field(node: ast.AST) -> str:
    """参数必须是白名单字段名的 ast.Name。返回大写字段名。"""
    if not isinstance(node, ast.Name):
        raise ValueError("Field argument must be a field name")
    name = node.id.upper()
    if name not in FIELDS:
        raise ValueError(f"Unknown field {name}")
    return name

WINDOW_MAX = 500

ARITH_MAX_OPS = 3

def _split_arith_top_level(text: str) -> list:
    """按 + - * / 在括号外拆分；e/E 指数记号（5e9、1e-3）的 -/+ 不算操作符。与前端 splitArithTopLevel 语义一致。"""
    parts = []
    depth = 0
    cur = ''
    for i, ch in enumerate(text):
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        if depth == 0 and ch in '+-*/':
            if (ch in '+-') and i > 0 and text[i - 1] in 'eE':
                cur += ch
                continue
            parts.append(cur.strip())
            cur = ''
        else:
            cur += ch
    if cur.strip():
        parts.append(cur.strip())
    return [p for p in parts if p]


def _is_outer_paren_balanced(t: str) -> bool:
    """首括号是否配对并闭合于末位。"""
    if not t.startswith('('):
        return False
    depth = 0
    for i, ch in enumerate(t):
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        if depth == 0:
            return i == len(t) - 1
    return False


def _strip_outer_parens(text: str) -> str:
    t = text.strip()
    while t.startswith('(') and _is_outer_paren_balanced(t):
        t = t[1:-1].strip()
    return t


def _require_positive_int(node: ast.AST) -> int:
    """参数必须是正整数常量，且 1 ≤ n ≤ 500。"""
    if not isinstance(node, ast.Constant) or isinstance(node.value, bool) or not isinstance(node.value, int):
        raise ValueError("Window argument must be an integer constant")
    if node.value <= 0:
        raise ValueError("Window argument must be positive")
    if node.value > WINDOW_MAX:
        raise ValueError(f"Window argument must be at most {WINDOW_MAX}")
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
            'LIMIT_UP_PCT': _limit_up_pct_expr(),
            # 涨停/跌停标志（data_manager 预计算的布尔列，仅日线）
            'IS_LIMIT_UP': pl.col('is_limit_up'),
            'IS_TOUCH_LIMIT_UP': pl.col('is_touch_limit_up'),
            'IS_LIMIT_DOWN': pl.col('is_limit_down'),
            'IS_TOUCH_LIMIT_DOWN': pl.col('is_touch_limit_down'),
            'PREV_CLOSE': pl.col('prev_close'),
        }
        # 当前解析上下文
        self.current_df = None
        self.current_source = None

    def parse_expression(self, expr_str: str, timeframe: str = 'D') -> pl.Expr:
        """解析入口：根据 timeframe 设置当前数据上下文"""
        if timeframe == 'W': self.current_df = data_manager.df_weekly
        elif timeframe == 'M': self.current_df = data_manager.df_monthly
        else: self.current_df = data_manager.df_daily
        
        # 兼容性替换（含大写逻辑词归一化，兼容 LLM 输出；\b 词边界容忍多空格/开头位置）
        clean_expr = re.sub(r'\b(AND|OR|NOT)\b', lambda m: m.group(1).lower(), expr_str.strip())
        clean_expr = clean_expr.replace('&&', '&').replace('||', '|')
        self.current_source = clean_expr
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

        elif isinstance(node, ast.UnaryOp):
            val = self._visit(node.operand)
            if isinstance(val, bool):
                raise ValueError("Unary operator not allowed on boolean operand")
            if type(node.op) is ast.USub:
                return -val
            if type(node.op) is ast.UAdd:
                return +val
            raise ValueError(f"Unary operator not allowed: {type(node.op).__name__}")

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
            if not isinstance(node.func, ast.Name):
                raise ValueError("Function call target must be a name")
            func = node.func.id.upper()
            entry = INDICATORS.get(func)
            if entry is None:
                raise ValueError(f"Unknown function {func}")
            sig = entry["signature"]
            if len(node.args) != len(sig) or node.keywords:
                raise ValueError(f"Function {func} expects {len(sig)} positional args")
            args = [self._visit_arg(a, s, func) for a, s in zip(node.args, sig)]
            if entry.get("window"):
                field_name, n = args
                pure_key = f"{func}_{field_name}_{n}"
                if self.current_df is not None and pure_key in self.current_df.columns:
                    return pl.col(pure_key)
                return entry["func"](self.fields[field_name], n)
            return entry["func"](*args)

        raise ValueError(f"Syntax not allowed: {type(node)}")

    def _visit_arg(self, node: Any, kind: str, func: str) -> Any:
        """按签名声明的形态校验并求值单个参数。"""
        if kind == "field":
            return _require_whitelist_field(node)
        if kind == "pos_int":
            return _require_positive_int(node)
        if kind == "series":
            return self._require_series(node, func)
        if kind == "cond":
            return self._require_cond(node, func)
        raise ValueError(f"Unknown signature kind {kind}")

    def _require_series(self, node: Any, func: str) -> Any:
        """series = 白名单字段 或 签名不含 cond 形态的算子调用（含窗口/非窗口） 或 +-*/ 算术表达式。"""
        if isinstance(node, ast.Name):
            name = _require_whitelist_field(node)
            return self.fields[name]
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id.upper() in INDICATORS
                and "cond" not in INDICATORS[node.func.id.upper()]["signature"]):
            return self._visit(node)
        if isinstance(node, ast.BinOp) and type(node.op) in (ast.Add, ast.Sub, ast.Mult, ast.Div):
            self._require_arith(node, func)
            return self._visit(node)
        raise ValueError(f"Function {func} arg must be a field, single-value indicator call, or arithmetic expression")

    def _top_level_ops(self, node) -> int:
        """按源码文本统计顶层算术运算符数（括号内不计入）。get_source_segment 失败时退化为整树计数兜底。"""
        seg = ast.get_source_segment(self.current_source, node) if isinstance(self.current_source, str) else None
        if seg is not None:
            t = _strip_outer_parens(seg)
            parts = _split_arith_top_level(t)
            return max(0, len(parts) - 1)
        return self._arith_ops_tree(node)

    def _arith_ops_tree(self, node) -> int:
        """兜底：整棵 BinOp 子树运算符总数。"""
        if not isinstance(node, ast.BinOp) or type(node.op) not in (ast.Add, ast.Sub, ast.Mult, ast.Div):
            return 0
        return 1 + self._arith_ops_tree(node.left) + self._arith_ops_tree(node.right)

    def _require_arith(self, node: Any, func: str) -> Any:
        """算术表达式结构校验：操作数 = 数值常量 / series / 更浅的算术；顶层运算符数 ≤ ARITH_MAX_OPS。"""
        if self._top_level_ops(node) > ARITH_MAX_OPS:
            raise ValueError(f"Function {func} arithmetic too many top-level operators (max {ARITH_MAX_OPS})")
        for child in (node.left, node.right):
            if isinstance(child, ast.Constant):
                if isinstance(child.value, bool):
                    raise ValueError(f"Function {func} arithmetic operand must be number or series")
                continue
            if isinstance(child, ast.BinOp) and type(child.op) in (ast.Add, ast.Sub, ast.Mult, ast.Div):
                self._require_arith(child, func)
            else:
                self._require_series(child, func)

    def _require_cond(self, node: Any, func: str) -> Any:
        """cond = Compare(> >= < <=) 或 BoolOp(AND/OR)。先结构白名单校验，再委托 _visit。"""
        self._validate_cond_structure(node, func, depth=0)
        return self._visit(node)

    def _validate_cond_structure(self, node: Any, func: str, depth: int) -> None:
        if depth > 2:
            raise ValueError(f"Function {func} cond nesting too deep")
        if isinstance(node, ast.Compare):
            if len(node.ops) != 1 or type(node.ops[0]) not in (ast.Gt, ast.GtE, ast.Lt, ast.LtE):
                raise ValueError(f"Function {func} cond must use > >= < <=")
            if (isinstance(node.left, ast.Constant)
                    and isinstance(node.comparators[0], ast.Constant)):
                raise ValueError(f"Function {func} cond needs at least one series operand")
            self._require_series_operand(node.left, func)
            self._require_series_operand(node.comparators[0], func)
            return
        if isinstance(node, ast.BoolOp) and type(node.op) in (ast.And, ast.Or):
            for v in node.values:
                self._validate_cond_structure(v, func, depth + 1)
            return
        raise ValueError(f"Function {func} cond must be a comparison or AND/OR expression")

    def _require_series_operand(self, node: Any, func: str) -> None:
        """cond 的操作数：series（字段/窗口调用）或数值常量（支持一元正负号，如 -0.05、+10）。"""
        if isinstance(node, ast.UnaryOp) and type(node.op) in (ast.USub, ast.UAdd):
            node = node.operand
        if isinstance(node, ast.Constant):
            if isinstance(node.value, bool):
                raise ValueError(f"Function {func} cond operand must be number or series")
            return
        self._require_series(node, func)

blink_parser = BlinkParser()
