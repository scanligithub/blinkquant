# 涨停/跌停按板别确定翻译设计

> 日期：2026-08-19
> 状态：已与用户确认设计方向（后端派生字段 LIMIT_UP_PCT 实时算、仅涨停/跌停双向、分析阶段中文固定语）

## 1. 背景与问题

自然语言选股中，口语「涨停」「跌停」是最常见需求之一，但当前系统**没有任何涨停语义处理**：

- 字段白名单只有 `PCT_CHG`（当日涨跌幅，百分比），无板别信息；
- 公式 DSL 是**逐股行内求值**（如 `PCT_CHG >= 10`），无法按代码前缀在公式内分支；
- LLM 若收到「涨停的股票」，大概率翻译成 `PCT_CHG >= 10`，会：
  - 漏选创业板（`sz.30*`，20%）、科创板（`sh.688*`，20%）、北交所（`bj.*`，30%）的涨停股；
  - 把创业/科创 10%-20% 的普通上涨误判为未涨停（需阈值正确才不漏不误）。

### A股涨停幅度标准（2026-07-06 新规后）

| 市场/板别 | 代码前缀 | 涨停幅度 |
|-----------|----------|---------|
| 沪主板 | `sh.600/601/603/605` | ±10% |
| 深主板 | `sz.000/001/002/003` | ±10% |
| 科创板 | `sh.688/689` | ±20% |
| 创业板 | `sz.300/301` | ±20% |
| 北交所 | `bj.43/83/87/88/92` | ±30% |

- 2026-07-06 起沪深主板 ST/*ST 涨跌幅由 5% 调整为 10%，与主板普通股一致；科创/创业/北交 ST 本就与同板一致 → **ST 无独立限幅，板别固定值即可**。
- 新股例外（主板首日 44%、注册制前 5 日无限制、北交首日无限制）**不做处理**（YAGNI，经用户确认仅按代码判板别固定值）。

## 2. 需求

1. 口语「涨停」→ 按板别正确翻译；「跌停」→ 对称处理。
2. 支持「涨停或跌停」这类 OR 组合。
3. 不改公式校验器、不改变 SELECT 语义。
4. 覆盖测试与指南文档同步更新。

## 3. 设计

### 3.1 核心思路

新增**派生字段 `LIMIT_UP_PCT`**（每只股票的涨停幅度，百分比）：

- 公式 `PCT_CHG >= LIMIT_UP_PCT` 即涨停、`PCT_CHG <= 0 - LIMIT_UP_PCT` 即跌停；
- 求值时依当行 `code` 前缀实时算出该股限幅，天然按板别正确；
- 新字段进白名单后，前端公式校验器（`validateFormula`）无需修改：`LIMIT_UP_PCT` 是合法 field token，`0 - LIMIT_UP_PCT` 是合法算术式。

### 3.2 后端改动

#### `backend/core/indicator_registry.py`
- `FIELDS` 追加 `"LIMIT_UP_PCT"`（在 `TURN` 之后）。
- `UNITS` 追加 `"LIMIT_UP_PCT": "百分比(%)"`。

#### `backend/core/security.py`
`blink_parser.fields` 新增映射，由 `code` 前缀实时计算：

```python
'LIMIT_UP_PCT': pl.when(
    pl.col("code").str.starts_with("sh.688")
    | pl.col("code").str.starts_with("sz.30")
).then(pl.lit(20.0)).when(
    pl.col("code").str.starts_with("bj.")
).then(pl.lit(30.0)).otherwise(pl.lit(10.0))
```

> 说明：`sh.688*/sz.30*` 一律 20%，`bj.*` 一律 30%，其余（沪深主板 `sh.60*/sz.00*`）一律 10%。`code` 列在 df_daily/df_weekly/df_monthly 均存在（都是 `code` 分组）。

行为验证：`test_fields_match_registry` 自动锁住 `FIELDS` 键集与 `parser.fields` 键集一致，无需额外同步测试（改 FIELDS 不加 parser 映射会红）。

#### 后端新增测试 `backend/tests/test_security.py`
- 验证 `LIMIT_UP_PCT` 派生表达式：
  - 构造含 `sh.600000`（→10）、`sh.688001`（→20）、`sz.300001`（→20）、`sz.000001`（→10）、`bj.830001`（→30）的 dataframe；
  - `PCT_CHG >= LIMIT_UP_PCT` 过滤结果分别：主板股 `pctChg=10` 通过、`pctChg=9.5` 拒绝；科创/创业股 `pctChg=15` 不通过（阈值 20）、`pctChg=20` 通过；北交股 `pctChg=30` 通过。
  - `PCT_CHG <= 0 - LIMIT_UP_PCT` 对称验证 1 例。

### 3.3 前端提示词改动（`frontend/src/lib/selectNL.ts`）

1. `buildSystemPrompt` 易错模式追加一条：
   ```
   '12) 涨停/跌停：涨停 = PCT_CHG >= LIMIT_UP_PCT；跌停 = PCT_CHG <= 0 - LIMIT_UP_PCT。'
   '   禁止写死 10/20/30（各板限幅不同，必须用 LIMIT_UP_PCT 字段）。'
   ```

2. `buildHardConstraintSuffix`：analysis 文本含「涨停|跌停|封板|一字板」时追加硬约束：`本需求含涨停/跌停语义，必须使用 PCT_CHG 与 LIMIT_UP_PCT 比较（涨停 PCT_CHG >= LIMIT_UP_PCT；跌停 PCT_CHG <= 0 - LIMIT_UP_PCT），禁止写死 10/20/30 数值。`

3. `buildAnalyzePrompt` 分析要求第 5 条歧义术语追加：
   - 「涨停」→「当日收盘涨幅达到或超过该股涨停幅度」
   - 「跌停」→「当日收盘跌幅达到或超过该股跌停幅度」

### 3.4 前端确定性改写（`frontend/src/lib/selectNL.ts`）

新增 `trySafeLimitUpDownRewrite(formula, analysis)`：

- 当 `analysis` 文本含「涨停/跌停/封板/一字板」，而公式写死了数值幅度（如 `PCT_CHG >= 10`）时确定性改写：
  - 涨停形态 → `PCT_CHG >= LIMIT_UP_PCT`
  - 跌停形态 → `PCT_CHG <= 0 - LIMIT_UP_PCT`
- 匹配形态（大小写不敏感、容忍空白）：
  - `PCT_CHG >= N` / `PCT_CHG > N`（N ∈ 10/20/30）→ `PCT_CHG >= LIMIT_UP_PCT`
  - `PCT_CHG <= -N` / `PCT_CHG < -N` 或 `0 - PCT_CHG >= N` 等 → `PCT_CHG <= 0 - LIMIT_UP_PCT`
- **守则**：仅当 analysis 含涨停/跌停关键词且公式整体形态精确匹配才改写，否则返回 null（不误改「涨幅大于 5%」这类普通查询）。

#### 接入点 `frontend/src/app/api/select-nl/route.ts`
改写链顺序（零 token，成功即跳过 repair）：
```
trySafeBollRefRewrite → trySafeAbsAbsRewrite → trySafeNumericCrossRewrite → trySafeLimitUpDownRewrite
```

### 3.5 覆盖测试与文档

#### `frontend/scripts/nl-coverage.mjs`
- `FIELD_GEN` 追加 `LIMIT_UP_PCT: { q: '涨停的股票', sub: ['LIMIT_UP_PCT'] }`（注意不要与 IND_GEN 冲突；token 提取按 `LIMIT_UP_PCT` 匹配）。
- 覆盖矩阵预期：字段 19/19。

#### `frontend/tests/select-nl.test.mjs`（仓库惯例：同步函数副本 + 新增测试）
- `META.fields` 追加 `'LIMIT_UP_PCT'`（与后端一致）。
- 同步复制 `trySafeLimitUpDownRewrite` + `buildHardConstraintSuffix` 最新实现。
- guard 断言 `export function trySafeLimitUpDownRewrite` 存在。
- 新增测试：
  - 涨停确定性改写：analysis 含「涨停」+ `PCT_CHG >= 10` → `PCT_CHG >= LIMIT_UP_PCT`；`PCT_CHG >= 20`/`>= 30` 同样改写；
  - 跌停确定性改写：`PCT_CHG <= -10` → `PCT_CHG <= 0 - LIMIT_UP_PCT`；
  - 不误改：analysis 不含涨停关键词 + `PCT_CHG >= 5` → null；含「涨停」但公式是 `PCT_CHG >= 5`（非 10/20/30）→ null；
  - 校验器接受新字段：`validateFormula` 对 `PCT_CHG >= LIMIT_UP_PCT` 与 `PCT_CHG <= 0 - LIMIT_UP_PCT` 均 ok；
  - `buildHardConstraintSuffix` 含「涨停」analysis 追加硬约束行。

#### `docs/NL-STOCK-SELECT-GUIDE.md`
- 字段表追加 `LIMIT_UP_PCT | 涨停幅度 | 百分比(%) | 涨停的股票`；
- 新增「涨停/跌停」说明小节（各板限幅表 + 公式写法）；
- 易混淆表更新（含「你以为的写法」行）。

## 4. 验收标准

1. 后端：`python -m pytest backend/tests` 全绿（含新增 LIMIT_UP_PCT 派生测试、`test_fields_match_registry`）。
2. 前端：`cd frontend && node --test tests/**/*.mjs` 全绿（含新增改写/校验/硬约束测试）；TSC 通过。
3. 线上：nl-meta 返回 19 字段含 `LIMIT_UP_PCT`；跑批含「涨停的股票」用例 PASS 且公式为 `PCT_CHG >= LIMIT_UP_PCT`。
4. 覆盖矩阵：字段 19/19、算子 47/47。

## 5. 明确不做（YAGNI）

- 新股例外（首日 44%、注册制前 5 日无限制、北交首日无限制）。
- ST 特殊处理（2026-07-06 新规已取消独立限幅）。
- 「接近涨停」「涨幅大于 9%」等派生幅度口语。
- 实时算 LIMIT_UP_PCT 而非建列（确认实时算方案）。