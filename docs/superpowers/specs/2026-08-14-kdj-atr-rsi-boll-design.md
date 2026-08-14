# KDJ / ATR / RSI / BOLL 指标扩展 设计文档

日期：2026-08-14

## 背景

BlinkQuant 选股 DSL 现有 15 个算子（MA/EMA/STD/ROC/REF/HHV/LLV/SUM + CROSS_UP/DOWN/MAX/MIN/ABS/COUNT/BARSLAST），dispatch 签名驱动递归校验（security.py）。A 股量化常用的 KDJ / ATR / RSI / BOLL 尚未覆盖，用户需求是补全这些复合/多字段指标。

## 非目标（YAGNI）

- 不做 KDJ 三值全部暴露（J 值不用，K/D 足够表达常见选股：KDJ 金叉 = K 上穿 D）
- 不做 MACD（其 DIF 已可由 `EMA(CLOSE,12) − EMA(CLOSE,26)` 表达，无需新算子）
- 不做 BOLL 中轨独立算子（= MA(CLOSE,n)，已有）
- 不上 BOLL 双参数 `pos_int` 之外的形态（k 倍数用整数即可）
- 不参与 Hot-JIT（非 window 慢路径，与 CROSS/COUNT 一致）

## 架构决策：单值拆分 + 非 window 慢路径

**核心约束**：DSL 每个表达式必须是单个 `pl.Expr` 列。KDJ/BOLL 天然输出多值，因此**每个输出拆成独立算子**（各自返回单列），由现有 `CROSS_UP`/`MAX`/比较运算组合表达完整策略。

**形态选择**：全部走**非 window 分支**（`entry["func"](*args)`，security.py 已支持任意签名组合，无需改 security.py / 前端校验）。`signature` 用现有四种形态（`series`/`pos_int`）。

**字段选择**：
- ATR / KDJ 需要 H/L/C 三列 → **不接收 series 参数**，函数内部固定引用 `pl.col("high"/"low"/"close")`，签名只用 `pos_int`
- RSI / BOLL 只依赖单序列 → **接收 series 参数**（支持嵌套窗口调用如 `RSI(MA(CLOSE,20), 6)`）

## 新算子

| 算子 | 签名 | 中文说明 | 实现 |
|---|---|---|---|
| `ATR(10)` | `["pos_int"]` | 真实波幅 | TR=max(H−L, \|H−昨C\|, \|L−昨C\|)；ATR=TR 的 n 期 rolling_mean |
| `RSI(CLOSE, 6)` | `["series","pos_int"]` | 相对强弱（简化版） | delta=diff；avg_gain=clip(delta,0) 的 n 期均值；avg_loss=clip(−delta,0) 的 n 期均值；RSI=100·avg_gain/(avg_gain+avg_loss) |
| `BOLL_UPPER(CLOSE, 20, 2)` | `["series","pos_int","pos_int"]` | 布林上轨 | MA + k×STD（同窗口） |
| `BOLL_LOWER(CLOSE, 20, 2)` | `["series","pos_int","pos_int"]` | 布林下轨 | MA − k×STD（同窗口） |
| `KDJ_K(9, 3)` | `["pos_int","pos_int"]` | KDJ 随机指标 K 值（简化版） | RSV=(C−LLV(L,n))/(HHV(H,n)−LLV(L,n))×100；K=RSV 的 m 期均值 |
| `KDJ_D(9, 3)` | `["pos_int","pos_int"]` | KDJ 随机指标 D 值（简化版） | D=K 的 m 期均值 |

**原则性简化**（在 DESCRIPTIONS 中注明「简化版」）：
- KDJ 平滑用 rolling_mean（非通达信 SMA(X,N,M) ewm），文档标注
- RSI 平滑用 rolling_mean（非 Wilder 平滑），文档标注

## 组合表达示例

- KDJ 金叉：`CROSS_UP(KDJ_K(9,3), KDJ_D(9,3))`
- 放量突破布林上轨：`CLOSE > BOLL_UPPER(CLOSE,20,2) AND VOL > 1.5 * MA(VOL,5)`
- 波动缩窄（布林收口）：`BOLL_UPPER(CLOSE,20,2) − BOLL_LOWER(CLOSE,20,2) < 0.5`
- ATR 波动过滤：`ATR(14) < 0.8`

（这些加入 `EXAMPLE_QUERIES` 供 LLM 提示词参考。）

## 数据流

1. `INDICATORS` 新增 6 条目（非 window，func 为 lambda，见「新算子」）
2. `DESCRIPTIONS` 新增 6 中文说明；`EXAMPLE_QUERIES` 新增组合示例
3. `security.py:_require_series` 放宽（见下「series 放行规则」）
4. `nl_meta()` 自动带出（注册表驱动）：indicators/signatures/descriptions/example_queries
5. 前端 `selectNL.ts:isSeriesExpr` 与 `validateFormula` 同步放宽（见下）
6. 前端测试 `select-nl.test.mjs` 的 META 复制版同步（indicators/signatures/descriptions/example_queries）
7. Engine Hot-JIT 不变：新算子非 window，不进 `WINDOW_NAMES`/`INDICATOR_MAP`，走慢路径

## 关键约束：series 放行规则（必须改 前后端安全校验）

`CROSS_UP(KDJ_K(9,3), KDJ_D(9,3))` / `CROSS_UP(RSI(CLOSE,6), RSI(CLOSE,24))` / `COUNT(RSI(CLOSE,6) > 70, 5)` 均需新算子可作为 series/cond 操作数。现状：

- **后端** `security.py:_require_series`（L133-141）只放行 `WINDOW_NAMES` 调用（8 个 window 算子）或白名单字段
- **前端** `selectNL.ts:isSeriesExpr`（L182-190）硬编码 `sig.length===2 && sig[0]==='field' && sig[1]==='pos_int'`，且括号正则 `[^()]*` 不允许系列参数嵌套

**统一规则（前后端一致）**：series 位置（CROSS_UP/MAX/MIN/ABS 参数、cond 操作数）接受「白名单字段 或 签名不含 `cond` 形态的任意算子调用」。

```
后端 _require_series:
  原  if func in WINDOW_NAMES: return self._visit(node)
  改  if func in INDICATORS and "cond" not in INDICATORS[func]["signature"]:
          return self._visit(node)

前端 isSeriesExpr:
  原  sig = signatures?.[name]; 只接受 [field,pos_int] 且参数无括号
  改  提取 NAME( args ) → 查 signatures[name] → 若存在且不含 "cond" → 递归 validateCallArgs(args)
```

**安全论证**：
- 递归深度由公式长度上限（500 字符）+ AST 白名单限制自然封顶，前后端一致
- `MA(MA(CLOSE,2),2)` 仍拒：MA 首参是 `field` 形态，`_require_whitelist_field` 要求 ast.Name，Call 被拒（不变）
- `COUNT(COUNT(CLOSE>10,2)>1,3)` 仍拒：COUNT 签名含 `cond`，不在此放宽范围（不变）
- 新放行的反例 `MAX(MAX(CLOSE,OPEN),OPEN)` 两层在前后端**一致放行**（现无测试；新增正向测试固化该行为，避免前后端漂移）
- 新算子顶层调用不受影响（`_visit` 非 window 分支直接 `entry["func"](*args)`，签名驱动 `_visit_arg` 校验，本就支持任意形态）

## 测试策略

### 后端

`backend/tests/test_registry.py` 追加（可放独立测试方法）：
- 6 算子存在、signature 正确（[pos_int] / [series,pos_int] / [series,pos_int,pos_int] / [pos_int,pos_int]）、非 window、func 可调用
- nl_meta 派生覆盖 6 新算子 + signatures

`backend/tests/test_security.py` 追加 `TestKDJATRRSIBoll`：
- `ATR(3)` 在含 high/low/close 的测试 df 求值，断言与手算 TR 的 rolling_mean 对齐（前 2 行 None）
- `RSI(CLOSE, 3)` 断言贴 hand-check 值（100·avg_gain/(avg_gain+avg_loss)）
- `BOLL_UPPER(CLOSE, 3, 2)` / `BOLL_LOWER(CLOSE, 3, 2)` 断言 = MA±2·STD
- `KDJ_K(9,3)` / `KDJ_D(9,3)` 在 ≥10 行 df 断言非 None 行数与手算一致（注意 KDJ_D 再套一层，需足够行数）
- `CROSS_UP(KDJ_K(9,3), KDJ_D(9,3))` **通过**（关键回归：series 放行后金叉可表达）
- `CROSS_UP(RSI(CLOSE,6), RSI(CLOSE,24))` 通过（series 首参嵌套）
- `COUNT(RSI(CLOSE,6) > 70, 5)` 通过（cond 操作数放行非 window series）
- `MAX(MAX(CLOSE,OPEN), OPEN)` **通过**（新放行的两层一致行为，固化防漂移）
- `CROSS_UP(MA(MA(CLOSE,2),2), OPEN)` 仍拒绝（field 首参白名单不变）
- `COUNT(COUNT(CLOSE>10,2)>1,3)` 仍拒绝（cond 签名不在此放宽范围）
- `ATR(0)` / `ATR(501)` 拒绝；`BOLL_UPPER(CLOSE,20,501)` 拒绝（pos_int 校验自动生效）

### 前端

`frontend/tests/select-nl.test.mjs`：
- META 增 `ATR/BOLL_UPPER/BOLL_LOWER/KDJ_K/KDJ_D/RSI` 到 indicators + signatures + descriptions + example_queries
- `CROSS_UP(KDJ_K(9,3), KDJ_D(9,3))` 通过（isSeriesExpr 递归放行后）
- `CROSS_UP(RSI(CLOSE,6), RSI(CLOSE,24))` 通过
- `MAX(MAX(CLOSE,OPEN), OPEN)` 通过（与后端一致）
- `CROSS_UP(MA(MA(CLOSE,2),2), OPEN)` 仍拒绝
- `COUNT(COUNT(CLOSE>10,2)>1,3)` 仍拒绝
- `BOLL_UPPER(CLOSE, 20, 501)` 拒绝（pos_int 上限生效）
- `buildSystemPrompt` 断言包含新算子说明（如 `KDJ_K` / `布林`)）
- node --test 全绿 + `npx tsc --noEmit` 无错误

## 验收标准

1. `cd backend && python tests/test_registry.py && python tests/test_security.py` 全绿
2. `cd frontend && node --test tests/select-nl.test.mjs` 全绿；`npx tsc --noEmit -p tsconfig.json` 无错误
3. 冒烟：`CROSS_UP(KDJ_K(9,3), KDJ_D(9,3))` 后端可解析求值；前端 validateFormula 通过
4. 部署后 `nl-meta` 返回 21 个算子（15+6）