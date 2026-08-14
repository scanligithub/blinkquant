# KDJ / ATR / RSI / BOLL 指标扩展 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 BlinkQuant 选股 DSL 新增 ATR / RSI / BOLL_UPPER / BOLL_LOWER / KDJ_K / KDJ_D 六个算子，并放宽前后端 series 位置校验，使 KDJ 金叉、布林突破等 A 股常见策略可表达。

**Architecture:** 新算子全部走「非 window 慢路径」（`entry["func"](*args)`），注册在 `backend/core/indicator_registry.py` 的 `INDICATORS` 字典，`nl_meta()`/前端路由自动派生。安全层两处放宽：后端 `security.py:_require_series`（原只放行 `WINDOW_NAMES`）、前端 `selectNL.ts:isSeriesExpr`（原硬编码 `[field,pos_int]`）。KDJ/ATR 固定使用 H/L/C 三列（签名只有 `pos_int`），RSI/BOLL 接收 `series` 参数。

**Tech Stack:** Python 3 / Polars 1.39（backend，unittest）、TypeScript 5（frontend，`node --test` + `npx tsc --noEmit`）。

**测试命令：**
- 后端：`cd backend && python tests/test_registry.py` 与 `python tests/test_security.py`
- 前端：`cd frontend && node --test tests/select-nl.test.mjs`；类型检查 `npx tsc --noEmit`

**参考值（已用 polars 1.39 实测，测试断言以此为准）：**

6 行 df（date=[1..6] idx，close=[10.0,10.5,10.3,11.0,11.8,12.1], open=[9.9,10.4,10.6,10.2,11.2,11.9], high=[10.2,10.8,10.7,11.2,12.0,12.3], low=[9.8,10.2,10.1,10.8,11.4,11.7]）：
- `ATR(2)`=[None, 0.6, 0.7, 0.75, 0.95, 0.8]
- `RSI(CLOSE,2)`=[None, None, 71.4286, 77.7778, 100, 100]
- BOLL: MA(2)=[None,10.25,10.4,10.65,11.4,11.95]；STD(2,ddof=1)=[None,0.3536,0.1414,0.4950,0.5657,0.2121]；`BOLL_UPPER(CLOSE,2,2)`[2]=10.6828、`BOLL_LOWER(CLOSE,2,2)`[2]=10.1172

12 行 df（close=[10.0,10.5,10.3,11.0,11.8,12.1,11.5,11.0,12.0,13.0,13.5,14.0]，同前 high/low 系列）：
- `KDJ_K(2,2)`=[None,None,49.2857,55.1948,82.5758,80.5556,58.1197,31.7308,50.7353,77.1242,73.5043,69.2308]
- `KDJ_D(2,2)`=[None,None,None,52.2403,68.8853,81.5657,69.3376,44.9252,41.2330,63.9297,75.3142,71.3675]

参考 spec：`docs/superpowers/specs/2026-08-14-kdj-atr-rsi-boll-design.md`

**已知缺口（最终评审记录，本期不实施）：**
1. RSI 与 KDJ 在「连续相同报价」（停牌等）时 `gain+loss==0` / `high_max-low_min==0` 会产生 `nan` 而非 null（复用 code review 实测结论：backend/core/indicator_registry.py:41,:70）。选股时 `nan < 30` / `CROSS_UP(KDJ_K, KDJ_D)` 均判 False，被静默剔除。后续可加分母守卫 `fill_nan(None)`。属计划认可的简化，不阻塞本期。
2. 前端文案滞后（Task 6 code review 记录）：`selectNL.ts:171`（测试复制版 :148）报错信息仍写「必须是字段或窗口指标调用」，现应含非窗口单值算子，宜改「字段或单值指标调用」；`selectNL.ts:237` 提示词仍写「不支持更深嵌套」，与递归放行后的实际能力不符（fail-closed 方向，LLM 少生成而非越权，非安全问题）。均为措辞层面，不阻塞本期。

---

### Task 1: 后端测试先红——`test_registry.py` 断言新算子存在

**Files:**
- Modify: `backend/tests/test_registry.py`
- Test: `backend/tests/test_registry.py`

- [x] **Step 1: 更新 `test_builtin_indicators_present` 期望 21 个算子**

将 `backend/tests/test_registry.py:18-21` 改为：

```python
    def test_builtin_indicators_present(self):
        self.assertEqual(sorted(INDICATORS.keys()), [
            "ABS", "ATR", "BARSLAST", "BOLL_LOWER", "BOLL_UPPER",
            "COUNT", "CROSS_DOWN", "CROSS_UP", "EMA", "HHV", "KDJ_D",
            "KDJ_K", "LLV", "MA", "MAX", "MIN", "REF", "ROC", "RSI",
            "STD", "SUM",
        ])
```

- [x] **Step 2: 新增 `test_new_indicators_signatures`**

在 `TestRegistry` 类内（`test_signatures_match_expected` 方法后）追加：

```python
    def test_new_indicators_signatures(self):
        expect = {
            "ATR": ["pos_int"],
            "RSI": ["series", "pos_int"],
            "BOLL_UPPER": ["series", "pos_int", "pos_int"],
            "BOLL_LOWER": ["series", "pos_int", "pos_int"],
            "KDJ_K": ["pos_int", "pos_int"],
            "KDJ_D": ["pos_int", "pos_int"],
        }
        got = {name: entry["signature"] for name, entry in INDICATORS.items() if name in expect}
        self.assertEqual(got, expect)
        for name in expect:
            self.assertFalse(INDICATORS[name]["window"], f"{name} must be window=False")
            self.assertIn("func", INDICATORS[name])
```

- [x] **Step 3: 运行后端测试确认红**

Run: `python tests/test_registry.py`
Expected: FAIL——`test_builtin_indicators_present` 断言 21 个失败（现在只有 15），`test_new_indicators_signatures` 获空 dict 报 AssertionError。

- [x] **Step 4: 提交（只提交测试，确认红色）**

```bash
git add backend/tests/test_registry.py
git commit -m "test: expect ATR/RSI/BOLL/KDJ indicators in registry"
```

---

### Task 2: 后端实现——注册表新增 6 算子 + 描述 + 示例

**Files:**
- Modify: `backend/core/indicator_registry.py`
- Test: `backend/tests/test_registry.py`

- [x] **Step 1: 添加模块级辅助函数**

在 `indicator_registry.py` 的 `barslast` 函数后（L34 之后）添加：

```python
def _kdj_rsv(n: int):
    """KDJ 中间量 RSV：RSV=(C-LLV(L,n))/(HHV(H,n)-LLV(L,n))*100（固定用 H/L/C 列）"""
    low_min = pl.col("low").rolling_min(window_size=n).over("code")
    high_max = pl.col("high").rolling_max(window_size=n).over("code")
    return (pl.col("close") - low_min) / (high_max - low_min) * 100
```

- [x] **Step 2: 在 `INDICATORS` 字典末尾（`BARSLAST` 条目后，L54-55 之间）追加 6 条目**

```python
    # ---- 单值复合指标（非 window，慢路径实时计算）----
    "ATR": {"func": lambda n: pl.max_horizontal(
            pl.col("high") - pl.col("low"),
            (pl.col("high") - pl.col("close").shift(1)).abs(),
            (pl.col("low") - pl.col("close").shift(1)).abs(),
        ).rolling_mean(window_size=n).over("code"),
        "window": False, "signature": ["pos_int"]},
    "RSI": {"func": lambda c, n: (lambda gain, loss: 100 * gain / (gain + loss))(
            c.diff().over("code").clip(lower_bound=0).rolling_mean(window_size=n).over("code"),
            (-c.diff().over("code")).clip(lower_bound=0).rolling_mean(window_size=n).over("code")),
        "window": False, "signature": ["series", "pos_int"]},
    "BOLL_UPPER": {"func": lambda c, n, k: c.rolling_mean(window_size=n).over("code")
            + k * c.rolling_std(window_size=n).over("code"),
        "window": False, "signature": ["series", "pos_int", "pos_int"]},
    "BOLL_LOWER": {"func": lambda c, n, k: c.rolling_mean(window_size=n).over("code")
            - k * c.rolling_std(window_size=n).over("code"),
        "window": False, "signature": ["series", "pos_int", "pos_int"]},
    "KDJ_K": {"func": lambda n, m: _kdj_rsv(n).rolling_mean(window_size=m).over("code"),
        "window": False, "signature": ["pos_int", "pos_int"]},
    "KDJ_D": {"func": lambda n, m: _kdj_rsv(n).rolling_mean(window_size=m).over("code")
            .rolling_mean(window_size=m).over("code"),
        "window": False, "signature": ["pos_int", "pos_int"]},
```

- [x] **Step 3: 更新 `DESCRIPTIONS`（L75-82 追加 6 条）**

```python
    "ATR": "N日真实波幅均值（最高最低与昨收的最大差距，简化版）", "RSI": "N日相对强弱（涨跌幅均值比，简化版）",
    "BOLL_UPPER": "布林上轨（N日均价 + K倍N日标准差）", "BOLL_LOWER": "布林下轨（N日均价 - K倍N日标准差）",
    "KDJ_K": "KDJ随机指标K值（固定用HIGH/LOW/CLOSE，简化版）", "KDJ_D": "KDJ随机指标D值（固定用HIGH/LOW/CLOSE，简化版）",
```

- [x] **Step 4: 更新 `EXAMPLE_QUERIES`（追加 2 条）**

```python
    "CROSS_UP(KDJ_K(9, 3), KDJ_D(9, 3))",
    "CLOSE > BOLL_UPPER(CLOSE, 20, 2)",
```

- [x] **Step 5: 运行后端测试确认绿**

Run: `python tests/test_registry.py`
Expected: ALL PASS

注意：既有 `test_signatures_match_expected`（test_registry.py:70-81）比较全量 `INDICATORS` 签名 dict 对 15 条 expect——Step 2 加 6 算子后它必然失败。**必须同步更新**该 expect dict，追加 6 条：

```python
    def test_signatures_match_expected(self):
        expect = {
            "MA": ["field", "pos_int"], "EMA": ["field", "pos_int"],
            "STD": ["field", "pos_int"], "ROC": ["field", "pos_int"],
            "REF": ["field", "pos_int"], "HHV": ["field", "pos_int"],
            "LLV": ["field", "pos_int"], "SUM": ["field", "pos_int"],
            "CROSS_UP": ["series", "series"], "CROSS_DOWN": ["series", "series"],
            "MAX": ["series", "series"], "MIN": ["series", "series"],
            "ABS": ["series"], "COUNT": ["cond", "pos_int"], "BARSLAST": ["cond"],
            "ATR": ["pos_int"], "RSI": ["series", "pos_int"],
            "BOLL_UPPER": ["series", "pos_int", "pos_int"], "BOLL_LOWER": ["series", "pos_int", "pos_int"],
            "KDJ_K": ["pos_int", "pos_int"], "KDJ_D": ["pos_int", "pos_int"],
        }
        got = {name: entry["signature"] for name, entry in INDICATORS.items()}
        self.assertEqual(got, expect)
```

（`test_new_indicators_signatures` 作为聚焦的新算子断言保留——其 window/func 检查与既有测试有少许冗余，属接受的权衡。）

- [x] **Step 6: 提交**

```bash
git add backend/core/indicator_registry.py backend/tests/test_registry.py
git commit -m "feat: add ATR/RSI/BOLL/KDJ indicators to registry"
```

---

### Task 3: 后端实现——`_require_series` 放宽非 window 单值算子

**Files:**
- Modify: `backend/core/security.py`
- Test: `backend/tests/test_security.py`

- [x] **Step 1: 修改 `_require_series`**

将 `backend/core/security.py:133-141` 替换为：

```python
    def _require_series(self, node: Any, func: str) -> Any:
        """series = 白名单字段 或 签名不含 cond 形态的任意算子调用（含窗口与非窗口单值算子）。"""
        if isinstance(node, ast.Name):
            name = _require_whitelist_field(node)
            return self.fields[name]
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id.upper() in INDICATORS
                and "cond" not in INDICATORS[node.func.id.upper()]["signature"]):
            return self._visit(node)
        raise ValueError(f"Function {func} arg must be a field or single-value indicator call")
```

注意：`INDICATORS` 已在 L6 import；无需新 import。

- [x] **Step 2: 运行既有后端测试确认不回归**

Run: `python tests/test_security.py`
Expected: ALL PASS（`test_series_second_level_nesting_rejected` 用 `MA(MA(...))`——MA 首参 field 形态照样拒绝；`test_count_cond_nested_count_rejected` 用 COUNT——cond 形态照样拒绝）

- [x] **Step 3: 提交**

```bash
git add backend/core/security.py
git commit -m "feat: relax _require_series to accept single-value non-window indicators"
```

---

### Task 4: 后端测试——`TestKDJATRRSIBoll` 求值与边界

**Files:**
- Modify: `backend/tests/test_security.py`
- Test: `backend/tests/test_security.py`

- [x] **Step 1: 添加测试类**（在 `TestSignatureRecursion` 之后、`if __name__` 块之前，约 L242 前）

```python
class TestKDJATRRSIBoll(unittest.TestCase):
    def setUp(self):
        self.df6 = pl.DataFrame({
            "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-06"],
            "code": ["sh.600000"] * 6,
            "close": [10.0, 10.5, 10.3, 11.0, 11.8, 12.1],
            "open": [9.9, 10.4, 10.6, 10.2, 11.2, 11.9],
            "high": [10.2, 10.8, 10.7, 11.2, 12.0, 12.3],
            "low": [9.8, 10.2, 10.1, 10.8, 11.4, 11.7],
        })
        self.df12 = pl.DataFrame({
            "date": ["2024-0%d-01" % i for i in range(1, 10)] + ["2024-10-01", "2024-11-01", "2024-12-01"],
            "code": ["sh.600000"] * 12,
            "close": [10.0, 10.5, 10.3, 11.0, 11.8, 12.1, 11.5, 11.0, 12.0, 13.0, 13.5, 14.0],
            "open": [9.9, 10.4, 10.6, 10.2, 11.2, 11.9, 11.7, 11.3, 11.8, 12.5, 13.2, 13.8],
            "high": [10.2, 10.8, 10.7, 11.2, 12.0, 12.3, 11.9, 11.4, 12.4, 13.4, 13.9, 14.4],
            "low": [9.8, 10.2, 10.1, 10.8, 11.4, 11.7, 11.0, 10.7, 11.6, 12.6, 13.1, 13.6],
        })

    def eval_df(self, expr, df):
        blink_parser.current_df = df
        return blink_parser.parse_expression(expr, "D")

    def values(self, expr, df):
        return df.with_columns(expr.alias("v")).select("v").to_series().to_list()

    def test_atr(self):
        got = self.values(self.eval_df("ATR(2)", self.df6), self.df6)
        self.assertEqual(len(got), 6)
        self.assertIsNone(got[0])
        self.assertAlmostEqual(got[1], 0.6, places=3)
        self.assertAlmostEqual(got[2], 0.7, places=3)
        self.assertAlmostEqual(got[3], 0.75, places=3)
        self.assertAlmostEqual(got[4], 0.95, places=3)
        self.assertAlmostEqual(got[5], 0.8, places=3)

    def test_rsi(self):
        got = self.values(self.eval_df("RSI(CLOSE, 2)", self.df6), self.df6)
        self.assertEqual(len(got), 6)
        self.assertIsNone(got[0])
        self.assertIsNone(got[1])
        self.assertAlmostEqual(got[2], 71.4286, places=3)
        self.assertAlmostEqual(got[3], 77.7778, places=3)
        self.assertAlmostEqual(got[4], 100, places=3)
        self.assertAlmostEqual(got[5], 100, places=3)

    def test_boll_band(self):
        up = self.values(self.eval_df("BOLL_UPPER(CLOSE, 2, 2)", self.df6), self.df6)
        low = self.values(self.eval_df("BOLL_LOWER(CLOSE, 2, 2)", self.df6), self.df6)
        # MA(2)=10.4, STD=0.1414 → upper=10.6828, lower=10.1172
        self.assertIsNone(up[0])
        self.assertIsNone(low[0])
        self.assertAlmostEqual(up[2], 10.6828, places=3)
        self.assertAlmostEqual(low[2], 10.1172, places=3)

    def test_kdj_k_d(self):
        k = self.values(self.eval_df("KDJ_K(2, 2)", self.df12), self.df12)
        d = self.values(self.eval_df("KDJ_D(2, 2)", self.df12), self.df12)
        self.assertIsNone(k[0])
        self.assertIsNone(k[1])
        self.assertAlmostEqual(k[2], 49.2857, places=3)
        self.assertAlmostEqual(k[11], 69.2308, places=3)
        self.assertIsNone(d[0])
        self.assertIsNone(d[2])
        self.assertAlmostEqual(d[3], 52.2403, places=3)
        self.assertAlmostEqual(d[11], 71.3675, places=3)

    def test_kdj_golden_cross_parses(self):
        expr = self.eval_df("CROSS_UP(KDJ_K(2, 2), KDJ_D(2, 2))", self.df12)
        self.assertIsNotNone(expr)
        rows = self.values(expr, self.df12)
        self.assertEqual(len(rows), 12)

    def test_rsi_cross_nested(self):
        expr = self.eval_df("CROSS_UP(RSI(CLOSE, 2), RSI(CLOSE, 4))", self.df6)
        self.assertIsNotNone(expr)

    def test_count_rsi_cond(self):
        got = self.values(self.eval_df("COUNT(RSI(CLOSE, 2) > 70, 2)", self.df6), self.df6)
        # RSI(2)=[None,None,71.43,77.78,100,100]; >70 掩码 [N,N,T,T,T,T]; rolling_sum(2)=[N,N,N,2,2,2]
        self.assertEqual(got, [None, None, None, 2, 2, 2])

    def test_max_two_level_allowed(self):
        expr = self.eval_df("MAX(MAX(CLOSE, OPEN), OPEN)", self.df6)
        got = self.values(expr, self.df6)
        self.assertEqual(got, [10.0, 10.5, 10.6, 11.0, 11.8, 12.1])

    def test_deep_nesting_still_rejected(self):
        with self.assertRaises(ValueError):
            self.eval_df("CROSS_UP(MA(MA(CLOSE, 2), 2), OPEN)", self.df6)
        with self.assertRaises(ValueError):
            self.eval_df("COUNT(COUNT(CLOSE > 10, 2) > 1, 3)", self.df6)

    def test_pos_int_bounds(self):
        with self.assertRaises(ValueError):
            self.eval_df("ATR(0)", self.df6)
        with self.assertRaises(ValueError):
            self.eval_df("ATR(501)", self.df6)
        with self.assertRaises(ValueError):
            self.eval_df("BOLL_UPPER(CLOSE, 20, 501)", self.df6)
```

- [x] **Step 2: 运行测试确认通过**

Run: `python tests/test_security.py`
Expected: ALL PASS（既有 34 项 + 新增 10 项 = 44 项）

- [x] **Step 3: 提交**

```bash
git add backend/tests/test_security.py
git commit -m "test: KDJ/ATR/RSI/BOLL evaluation and series relaxation bounds"
```

---

### Task 5: 前端测试先红——META 含新算子

**Files:**
- Modify: `frontend/tests/select-nl.test.mjs`
- Test: `frontend/tests/select-nl.test.mjs`

- [x] **Step 1: 更新测试 META（L6-14）**

修改 `frontend/tests/select-nl.test.mjs` 的 `META`：
- `indicators` 数组改为：`['ABS', 'ATR', 'BARSLAST', 'BOLL_LOWER', 'BOLL_UPPER', 'COUNT', 'CROSS_DOWN', 'CROSS_UP', 'EMA', 'HHV', 'KDJ_D', 'KDJ_K', 'LLV', 'MA', 'MAX', 'MIN', 'REF', 'ROC', 'RSI', 'STD', 'SUM']`
- `signatures` 追加：`ATR: ['pos_int'], RSI: ['series', 'pos_int'], BOLL_UPPER: ['series', 'pos_int', 'pos_int'], BOLL_LOWER: ['series', 'pos_int', 'pos_int'], KDJ_K: ['pos_int', 'pos_int'], KDJ_D: ['pos_int', 'pos_int']`
- `descriptions` 追加：`ATR: 'N日真实波幅均值（最高最低与昨收的最大差距，简化版）', RSI: 'N日相对强弱（涨跌幅均值比，简化版）', BOLL_UPPER: '布林上轨（N日均价 + K倍N日标准差）', BOLL_LOWER: '布林下轨（N日均价 - K倍N日标准差）', KDJ_K: 'KDJ随机指标K值（固定用HIGH/LOW/CLOSE，简化版）', KDJ_D: 'KDJ随机指标D值（固定用HIGH/LOW/CLOSE，简化版）'`
- `example_queries` 追加：`'CROSS_UP(KDJ_K(9, 3), KDJ_D(9, 3))'`、`'CLOSE > BOLL_UPPER(CLOSE, 20, 2)'`

- [x] **Step 2: 新增测试用例（红）**

在 `validateFormula: 窗口超上限拒绝` 测试后追加：

```javascript
test('validateFormula: KDJ 金叉通过', () => {
  const r = validateFormula(META, 'CROSS_UP(KDJ_K(9, 3), KDJ_D(9, 3))');
  assert.equal(r.ok, true);
});

test('validateFormula: BOLL 突破通过', () => {
  assert.equal(validateFormula(META, 'CLOSE > BOLL_UPPER(CLOSE, 20, 2)').ok, true);
});

test('validateFormula: ATR 顶层通过', () => {
  assert.equal(validateFormula(META, 'ATR(14) < 0.8').ok, true);
});

test('validateFormula: RSI 交叉通过', () => {
  assert.equal(validateFormula(META, 'CROSS_UP(RSI(CLOSE, 6), RSI(CLOSE, 24))').ok, true);
});

test('validateFormula: BOLL 窗口超上限拒绝', () => {
  const r = validateFormula(META, 'BOLL_UPPER(CLOSE, 20, 501)');
  assert.equal(r.ok, false);
});

test('validateFormula: 两层嵌套一致放行', () => {
  const r = validateFormula(META, 'CROSS_UP(MAX(MAX(CLOSE, OPEN), OPEN), MAX(CLOSE, OPEN))');
  assert.equal(r.ok, true);
});
```

- [x] **Step 3: 运行前端测试确认红**

Run: `cd frontend && node --test tests/select-nl.test.mjs`
Expected: FAIL——`isSeriesExpr`（测试复制版与实现同源）当前硬编码 `sig[0]==='field' && sig[1]==='pos_int'`，`KDJ_K(9, 3)`/`RSI(CLOSE, 6)`/`BOLL_UPPER(CLOSE, 20, 2)` 作为 series 嵌套全被拒。顶层 `ATR(14) < 0.8` 与 `BOLL_UPPER(...)>...` 会被 callRegex 正常校验（签名含 `pos_int`）；失败集中在 CROSS_UP 的嵌套参数校验。

- [x] **Step 4: 提交（只提交测试，确认红色）**

```bash
git add frontend/tests/select-nl.test.mjs
git commit -m "test: frontend META and cases for ATR/RSI/BOLL/KDJ"
```

---

### Task 6: 前端实现——`isSeriesExpr` 递归放行

**Files:**
- Modify: `frontend/src/lib/selectNL.ts`
- Test: `frontend/tests/select-nl.test.mjs`

- [x] **Step 1: 修改 `isSeriesExpr`**

将 `frontend/src/lib/selectNL.ts:182-190` 的 `isSeriesExpr` 替换为递归版本（配合既有 `matchParen`/`splitTopLevel`，`splitTopLevel` 已支持括号深度计数——栈顶同理）：

```typescript
function isSeriesExpr(meta: NLMeta, tok: string): boolean {
  if (meta.fields.includes(tok)) return true;
  const mm = /^([A-Z_][A-Z0-9_]*)\s*\(/.exec(tok);
  if (!mm) return false;
  const sig = meta.signatures?.[mm[1]];
  if (!sig || sig.includes('cond')) return false;
  const openIdx = mm.index + mm[0].length - 1;
  const closeIdx = matchParen(tok, openIdx);
  if (closeIdx !== tok.length - 1) return false; // 括号尾部有残留
  const argStr = tok.slice(openIdx + 1, closeIdx);
  const args = splitTopLevel(argStr, ',').map((s) => s.trim());
  return validateCallArgs(meta, sig, args, mm[1]).ok === true;
}
```

注意：这是**递归**实现，`validateCallArgs` 对 `series` 形态参数再调 `isSeriesExpr`（如 `MAX(MAX(CLOSE, OPEN), OPEN)` 的参数 `MAX(CLOSE, OPEN)` 继续展开）。与原实现的差异：
- 原硬编码 `sig[0]==='field' && sig[1]==='pos_int'` → 只放行窗口指标
- 新逻辑放行「签名不含 cond 的任意非窗口单值算子」（`ATR`/`RSI`/`BOLL_*`/`KDJ_*` 等），与后端 `_require_series` 规则一致
- `MA(MA(...))` 仍拒：校验 `MA` 首参时 `validateCallArgs` 对 `field` 形态要求 `MA(CLOSE, 2)` 参数本身是字段名，参数 `MA(CLOSE, 2)` 不在 `meta.fields` → 拒
- 递归深度由 `MAX_FORMULA_LENGTH=500` 封顶，与后端 AST `_visit` 递归天然一致

- [x] **Step 2: 同步修改测试复制版 `isSeriesExpr`（frontend/tests/select-nl.test.mjs L159-190）**

将测试中 `function isSeriesExpr` 替换为与 Step 1 完全相同的实现（同样使用测试文件里已有的 `matchParen`/`splitTopLevel`）。

- [x] **Step 3: 运行前端测试确认绿**

Run: `cd frontend && node --test tests/select-nl.test.mjs`
Expected: ALL PASS（既有 ~30 项 + 新增 5 项 + 一致性用例；`二层嵌套拒绝`（`MA(MA(...))`）与 `COUNT 条件嵌套 COUNT 拒绝`（`MA(MA)` 因 field 校验、`COUNT` 因签名含 `cond`）仍通过；新增 `MAX(MAX(CLOSE, OPEN), OPEN)` 通过，与后端一致）

- [x] **Step 4: 类型检查**

Run: `cd frontend && npx tsc --noEmit`
Expected: 无错误

- [x] **Step 5: 提交**

```bash
git add frontend/src/lib/selectNL.ts frontend/tests/select-nl.test.mjs
git commit -m "feat: recursive isSeriesExpr accepts single-value non-window indicators"
```

---

### Task 7: 全量回归 + 冒烟

**Files:**
- Test: 全部

- [x] **Step 1: 后端全量**（确认 registry + security 一起绿）

Run: `cd backend && python tests/test_registry.py && python tests/test_security.py`
Expected: 两个文件 ALL PASS

- [x] **Step 2: 前端全量 + 类型检查**

Run: `cd frontend && node --test tests/select-nl.test.mjs && npx tsc --noEmit`
Expected: ALL PASS，无类型错误

- [x] **Step 3: 冒烟（手动，确保提示词含新算子说明）**

Run: `cd frontend && node -e "import('./tests/select-nl.test.mjs').catch(()=>{})"` 后，人工确认 `frontend/src/lib/selectNL.ts` 的 `buildSystemPrompt` 会在部署时从 nl-meta 拼入 `KDJ_K(pos_int, pos_int): KDJ随机指标K值（固定用HIGH/LOW/CLOSE，简化版）` 等描述（由注册表自动派生，无需代码改动）。

- [x] **Step 4: 提交测试更新例（示例 queries 同步至前后端一致）**

若 Step 1-2 全绿，无额外代码改动，跳过提交（已在 Task 2/5/6 分别提交）。否则修复并提交。

### 验收对照

- [x] spec「验收标准 1」：backend 两个测试文件全绿
- [x] spec「验收标准 2」：前端测试 + tsc 全绿
- [x] spec「验收标准 3」：`CROSS_UP(KDJ_K(9,3), KDJ_D(9,3))` 后端可解析、前端 validateFormula 通过
- [x] spec「验收标准 4」：`nl_meta()` 返回 21 个算子（由 Task 2 的 `test_new_indicators_signatures` + `test_nl_meta_shape` 锁住）
---

