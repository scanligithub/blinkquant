# 指标注册表补齐 23 个算子 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 23 个常规量化平台指标（DMI 三兄弟、OBV、CCI、WR、MFI、SAR、Aroon 双子、TRIX、BBI、VWAP、BIAS、KDJ_J、BOLL_MID、PPO、DEMA、TEMA、UO、VR、PSY、CR）注册进 BlinkQuant 选股 DSL 注册表，使算子数 24→47，前后端数据驱动同步并线上满覆盖验证。

**Architecture:** 唯一事实来源为 `backend/core/indicator_registry.py` 的 `INDICATORS` 字典；后端 `security.py` 解析器按 `signature` 校验调用并按 `func` 求值（window:False 走慢路径）。前端 `selectNL.ts` 通过 `nl-meta` 拉取 meta 动态拼接提示词与校验公式；`frontend/tests/select-nl.test.mjs` 内嵌 META/函数副本须同步，`frontend/scripts/nl-coverage.mjs` 生成覆盖用例。SAR 因迭代语义用 `pl.struct(...).map_batches(...).over("code")` 实现（已验证 polars 1.39 可行）。

**Tech Stack:** Python 3 + polars 1.39、unittest；Node 18 + node:test、TypeScript、Vercel。测试命令：后端 `python -m unittest discover -s tests`（工作目录 backend，**无 pytest**）；前端 `node --test tests`（工作目录 frontend）；类型检查 `npx -p typescript@5.3.3 tsc --noEmit --noResolve --skipLibCheck --jsx preserve --esModuleInterop`（在 frontend 下用 `npx --yes` 直装）。

---

## 文件结构

| 文件 | 职责 | 动作 |
|---|---|---|
| `backend/core/indicator_registry.py` | 23 个新算子 func/signature/description/example | Modify |
| `backend/tests/test_registry.py` | 存在性/签名/数学定义/partition 测试 | Modify |
| `frontend/tests/select-nl.test.mjs` | META 副本、buildSystemPrompt/repair 副本、新校验用例 | Modify |
| `frontend/scripts/nl-coverage.mjs` | IND_GEN 新增 23 条生成器 | Modify |
| `frontend/src/lib/selectNL.ts` | 易错模式第 9 条 + buildRepairSystemSuffix 追加规则 | Modify |

---

## Task 1: 后端 RED — 注册表存在性/签名期望更新

**Files:**
- Modify: `backend/tests/test_registry.py`

- [x] **Step 1: 更新 `test_builtin_indicators_present` 期望 47 项全列表**

将 `backend/tests/test_registry.py:17-23` 的期望列表替换为：

```python
    def test_builtin_indicators_present(self):
        self.assertEqual(sorted(INDICATORS.keys()), [
            "ABS", "AROON_DOWN", "AROON_UP", "ATR", "BARSLAST", "BBI",
            "BIAS", "BOLL_LOWER", "BOLL_MID", "BOLL_UPPER", "CCI",
            "COUNT", "CR", "CROSS_DOWN", "CROSS_UP", "DEMA", "DMI_ADX",
            "DMI_MDI", "DMI_PDI", "EMA", "HHV", "KDJ_D", "KDJ_J",
            "KDJ_K", "LLV", "MA", "MACD_DEA", "MACD_DIF", "MACD_HIST",
            "MAX", "MFI", "MIN", "OBV", "PPO", "PSY", "REF", "ROC",
            "RSI", "SAR", "STD", "SUM", "TEMA", "TRIX", "UO", "VR",
            "VWAP", "WR",
        ])
```

- [x] **Step 2: 更新 `test_signatures_match_expected` 期望全签名**

将 `backend/tests/test_registry.py:72-88` 的 `expect` 字典整体替换为：

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
            "MACD_DIF": ["pos_int", "pos_int"], "MACD_DEA": ["pos_int", "pos_int", "pos_int"],
            "MACD_HIST": ["pos_int", "pos_int", "pos_int"],
            "DMI_PDI": ["pos_int"], "DMI_MDI": ["pos_int"], "DMI_ADX": ["pos_int"],
            "OBV": [], "CCI": ["pos_int"], "WR": ["pos_int"], "MFI": ["pos_int"],
            "SAR": [], "AROON_UP": ["pos_int"], "AROON_DOWN": ["pos_int"],
            "TRIX": ["pos_int"], "BBI": [], "VWAP": ["pos_int"], "BIAS": ["pos_int"],
            "KDJ_J": ["pos_int", "pos_int"], "BOLL_MID": ["series", "pos_int"],
            "PPO": ["pos_int", "pos_int"], "DEMA": ["series", "pos_int"],
            "TEMA": ["series", "pos_int"], "UO": [], "VR": ["pos_int"],
            "PSY": ["pos_int"], "CR": ["pos_int"],
        }
        got = {name: entry["signature"] for name, entry in INDICATORS.items()}
        self.assertEqual(got, expect)
```

- [x] **Step 3: 新增数学定义/partition 测试**（追加到文件末尾、`if __name__` 之前）

```python
    def test_obv_matches_definition(self):
        # OBV = 累计带符号量：收涨 +vol、收跌 -vol、平 0
        closes = [10.0, 11.0, 9.0, 9.0, 12.0]
        vols = [100.0, 200.0, 300.0, 400.0, 500.0]
        df = pl.DataFrame({
            "code": ["sh.600000"] * 5,
            "close": closes,
            "volume": vols,
        })
        obv = INDICATORS["OBV"]["func"]()
        got = df.with_columns(obv.alias("o")).select("o").to_series().to_list()
        # 首行无前值 → 0；随后 +200、-300、+0、+500 累积 → [0, 200, -100, -100, 400]
        expected = [0.0, 200.0, -100.0, -100.0, 400.0]
        for g, e in zip(got, expected):
            self.assertAlmostEqual(g, e, places=6)

    def test_obv_partitions_by_code(self):
        # 两代码混排，常数序列第二支 OBV 应恒为 0（不跨 code 串算）
        df = pl.DataFrame({
            "code": ["sh.600000"] * 4 + ["sz.000001"] * 4,
            "close": [10.0, 11.0, 12.0, 13.0] + [100.0, 100.0, 100.0, 100.0],
            "volume": [1.0, 2.0, 3.0, 4.0] + [10.0, 20.0, 30.0, 40.0],
        })
        obv = INDICATORS["OBV"]["func"]()
        got = df.with_columns(obv.alias("o")).select(["code", "o"]).sort(["code"])
        rows = list(got.iter_rows())
        sz = [v for c, v in rows if c == "sz.000001"]
        self.assertEqual(len(sz), 4)
        for v in sz:
            self.assertAlmostEqual(v, 0.0, places=6)

    def test_vwap_definition(self):
        # VWAP(2) = SUM(C*VOL,2)/SUM(VOL,2)
        df = pl.DataFrame({
            "code": ["sh.600000"] * 4,
            "close": [10.0, 12.0, 14.0, 16.0],
            "volume": [100.0, 100.0, 200.0, 100.0],
        })
        vwap = INDICATORS["VWAP"]["func"](2)
        got = df.with_columns(vwap.alias("v")).select("v").to_series().to_list()
        # 第2行: (10*100+12*100)/200=11.0; 第3行: (12*100+14*200)/300=13.333...
        self.assertAlmostEqual(got[1], 11.0, places=6)
        self.assertAlmostEqual(got[2], 40.0 / 3.0, places=6)

    def test_wr_definition(self):
        # WR(2) = (HHV(H,2)-C)/(HHV(H,2)-LLV(L,2))*100
        df = pl.DataFrame({
            "code": ["sh.600000"] * 3,
            "high": [11.0, 12.0, 13.0],
            "low": [9.0, 10.0, 8.0],
            "close": [10.5, 11.5, 9.0],
        })
        wr = INDICATORS["WR"]["func"](2)
        got = df.with_columns(wr.alias("w")).select("w").to_series().to_list()
        # 第3行: HHV=13, LLV=8, C=9 → (13-9)/(13-8)*100 = 80.0
        self.assertAlmostEqual(got[2], 80.0, places=6)

    def test_sar_iterates_partitioned(self):
        # SAR 迭代不跨 code 串算：第二支全部低点在首日，SAR 应恒为该 low
        df = pl.DataFrame({
            "code": ["sh.600000"] * 4 + ["sz.000001"] * 4,
            "open": [10.0, 10.5, 11.0, 11.5] + [50.0, 51.0, 52.0, 53.0],
            "high": [10.5, 11.0, 11.5, 12.0] + [52.0, 53.0, 54.0, 55.0],
            "low": [9.5, 10.0, 10.5, 11.0] + [49.0, 50.0, 51.0, 52.0],
            "close": [10.0, 10.5, 11.0, 11.5] + [50.0, 51.0, 52.0, 53.0],
        })
        sar = INDICATORS["SAR"]["func"]()
        got = df.with_columns(sar.alias("s")).select(["code", "s"]).sort(["code"])
        rows = list(got.iter_rows())
        sz = [v for c, v in rows if c == "sz.000001"]
        # 第二支单调上行，SAR 首值=low[0]=49.0，且全程不越过当前 low（迭代稳定）
        self.assertEqual(len(sz), 4)
        # 精确值：独立参照实现实测 [49, 50, 50, 50.08]；若跨 code 泄漏应为 [10.1, 12.6, 15.9, 19.7]
        self.assertEqual([round(v, 2) for v in sz], [49.0, 50.0, 50.0, 50.08])

    def test_dmi_pdi_positive_trend(self):
        # 单调上行趋势下 DMI_PDI 应显著大于 DMI_MDI（PDI>MDI）
        df = pl.DataFrame({
            "code": ["sh.600000"] * 20,
            "high": [10.0 + i * 0.5 for i in range(20)],
            "low": [9.5 + i * 0.5 for i in range(20)],
            "close": [9.8 + i * 0.5 for i in range(20)],
        })
        pdi = INDICATORS["DMI_PDI"]["func"](5)
        mdi = INDICATORS["DMI_MDI"]["func"](5)
        got = df.with_columns(pdi.alias("p"), mdi.alias("m")).select(["p", "m"])
        last = got.tail(1).row(0)
        self.assertGreater(last[0], last[1])
        self.assertGreaterEqual(last[0], 0.0)

    def test_aroon_up_high_at_window(self):
        # 窗口内今天创新高时 AROON_UP 应为 100；max 距今天 1 根时应为 80（可判别恒 100 假实现）
        df = pl.DataFrame({
            "code": ["sh.600000"] * 6,
            "high": [10.0, 11.0, 12.0, 13.0, 13.0, 12.0],
            "low": [9.0, 10.0, 11.0, 12.0, 12.0, 11.0],
        })
        up = INDICATORS["AROON_UP"]["func"](5)
        got = df.with_columns(up.alias("u")).select("u").to_series().to_list()
        self.assertAlmostEqual(got[4], 100.0, places=6)
        self.assertAlmostEqual(got[5], 80.0, places=6)
```

- [x] **Step 4: 运行确认 RED（存在性/签名两类失败）**

Run: `python -m unittest discover -s tests -k test_builtin_indicators_present`（backend 工作目录）
Expected: FAIL（实际 24 项，期望 47 项）。签名测试同理 FAIL。

> 已提交 `43df005`（RED 测试）+ `550cce9`（评审后收紧 SAR/AROON 精确断言、修正 BIAS 签名）。当前全量：`Ran 20 tests … failures=2, errors=7`（全为预期 RED）。

---

## Task 2: 后端 GREEN — 注册表实现 23 个算子

**Files:**
- Modify: `backend/core/indicator_registry.py`

- [x] **Step 1: 新增模块级 helper 函数**（放在 `_macd_hist` 之后、`INDICATORS` 之前）

```python
def _wilder(col, n: int):
    """Wilder 平滑：SMMA_t = ((n-1)*SMMA_{t-1} + val_t)/n ≡ ewm(alpha=1/n)"""
    return col.ewm_mean(alpha=1 / n, adjust=False).over("code")


def _dmi_tr():
    high, low, close = pl.col("high"), pl.col("low"), pl.col("close")
    prev_c = close.shift(1).over("code")
    return pl.max_horizontal(high - low, (high - prev_c).abs(), (low - prev_c).abs())


def _dmi_dm_plus():
    high, low = pl.col("high"), pl.col("low")
    prev_h, prev_l = high.shift(1).over("code"), low.shift(1).over("code")
    dm = high - prev_h
    return pl.when((dm > 0) & (dm > prev_l - low)).then(dm).otherwise(0.0)


def _dmi_dm_minus():
    high, low = pl.col("high"), pl.col("low")
    prev_h, prev_l = high.shift(1).over("code"), low.shift(1).over("code")
    dm = prev_l - low
    return pl.when((dm > 0) & (dm > high - prev_h)).then(dm).otherwise(0.0)


def _dmi_di(sign: str, n: int):
    tr_s = _wilder(_dmi_tr(), n)
    dm_s = _wilder(_dmi_dm_plus() if sign == "p" else _dmi_dm_minus(), n)
    return 100.0 * dm_s / tr_s


def _dmi_adx(n: int):
    pdi, mdi = _dmi_di("p", n), _dmi_di("m", n)
    dx = 100.0 * (pdi - mdi).abs() / (pdi + mdi)
    return _wilder(dx, n)


def _obv():
    close, vol = pl.col("close"), pl.col("volume")
    prev_c = close.shift(1).over("code")
    signed = pl.when(close > prev_c).then(vol).when(close < prev_c).then(-vol).otherwise(0.0)
    return signed.cum_sum().over("code")


def _cci(n: int):
    tp = (pl.col("high") + pl.col("low") + pl.col("close")) / 3.0
    ma_tp = tp.rolling_mean(window_size=n).over("code")
    md = (tp - ma_tp).abs().rolling_mean(window_size=n).over("code")
    return (tp - ma_tp) / (0.015 * md)


def _wr(n: int):
    high_max = pl.col("high").rolling_max(window_size=n).over("code")
    low_min = pl.col("low").rolling_min(window_size=n).over("code")
    return (high_max - pl.col("close")) / (high_max - low_min) * 100.0


def _mfi(n: int):
    tp = (pl.col("high") + pl.col("low") + pl.col("close")) / 3.0
    mf = tp * pl.col("volume")
    prev_tp = tp.shift(1).over("code")
    pos = pl.when(tp > prev_tp).then(mf).otherwise(0.0).rolling_sum(window_size=n).over("code")
    neg = pl.when(tp < prev_tp).then(mf).otherwise(0.0).rolling_sum(window_size=n).over("code")
    return 100.0 - 100.0 / (1.0 + pos / neg)


def _sar_from_hloc(s):
    """SAR 迭代（afStep=0.02, afMax=0.2）。s 为单 code 组的 struct Series，按时间序。"""
    h = s.struct.field("high").to_list()
    l = s.struct.field("low").to_list()
    o = s.struct.field("open").to_list()
    c = s.struct.field("close").to_list()
    n = len(h)
    out = [0.0] * n
    if n == 0:
        return pl.Series("sar", out, dtype=pl.Float64)
    sar, ep, af, is_up = 0.0, h[0], 0.02, True
    for i in range(n):
        if i < 2:
            out[i] = l[i]
            if i == 1:
                sar, ep = l[0], h[0]
                is_up = c[1] > o[1]
                if not is_up:
                    sar, ep = h[0], l[0]
            continue
        prev_sar = out[i - 1]
        new_sar = prev_sar + af * (ep - prev_sar)
        if is_up:
            if l[i] < new_sar:
                new_sar = ep
            if h[i] > ep:
                ep = h[i]
                af = min(af + 0.02, 0.2)
            if l[i] < out[i - 2]:
                is_up, ep, af = False, l[i], 0.02
                new_sar = out[i - 1]
        else:
            if h[i] > new_sar:
                new_sar = ep
            if l[i] < ep:
                ep = l[i]
                af = min(af + 0.02, 0.2)
            if h[i] > out[i - 2]:
                is_up, ep, af = True, h[i], 0.02
                new_sar = out[i - 1]
        out[i] = new_sar
    return pl.Series("sar", out, dtype=pl.Float64)


def _sar():
    return pl.struct(["high", "low", "open", "close"]).map_batches(_sar_from_hloc).over("code")


def _aroon_up(n: int):
    cond = pl.col("high") == pl.col("high").rolling_max(window_size=n).over("code")
    return 100.0 * (n - barslast(cond)) / n


def _aroon_down(n: int):
    cond = pl.col("low") == pl.col("low").rolling_min(window_size=n).over("code")
    return 100.0 * (n - barslast(cond)) / n


def _trix(n: int):
    e3 = _ema(_ema(_ema(pl.col("close"), n), n), n)
    prev = e3.shift(1).over("code")
    return (e3 - prev) / prev * 100.0


def _bbi():
    c = pl.col("close")
    return (c.rolling_mean(window_size=3).over("code")
            + c.rolling_mean(window_size=6).over("code")
            + c.rolling_mean(window_size=12).over("code")
            + c.rolling_mean(window_size=24).over("code")) / 4.0


def _vwap(n: int):
    return ((pl.col("close") * pl.col("volume")).rolling_sum(window_size=n).over("code")
            / pl.col("volume").rolling_sum(window_size=n).over("code"))


def _bias(c, n: int):
    ma = c.rolling_mean(window_size=n).over("code")
    return (c - ma) / ma * 100.0


def _kdj_j(n: int, m: int):
    k = _kdj_rsv(n).rolling_mean(window_size=m).over("code")
    d = k.rolling_mean(window_size=m).over("code")
    return 3.0 * k - 2.0 * d


def _boll_mid(c, n: int):
    return c.rolling_mean(window_size=n).over("code")


def _ppo(f: int, s: int):
    ef, es = _ema(pl.col("close"), f), _ema(pl.col("close"), s)
    return (ef - es) / es * 100.0


def _dema(c, n: int):
    e = _ema(c, n)
    return 2.0 * e - _ema(e, n)


def _tema(c, n: int):
    e1, e2 = _ema(c, n), _ema(_ema(c, n), n)
    return 3.0 * e1 - 3.0 * e2 + _ema(e2, n)


def _uo():
    prev_c = pl.col("close").shift(1).over("code")
    bp = pl.col("close") - pl.min_horizontal(pl.col("low"), prev_c)
    tr = pl.max_horizontal(pl.col("high"), prev_c) - pl.min_horizontal(pl.col("low"), prev_c)
    avg7 = bp.rolling_sum(window_size=7).over("code") / tr.rolling_sum(window_size=7).over("code")
    avg14 = bp.rolling_sum(window_size=14).over("code") / tr.rolling_sum(window_size=14).over("code")
    avg28 = bp.rolling_sum(window_size=28).over("code") / tr.rolling_sum(window_size=28).over("code")
    return 100.0 * (4.0 * avg7 + 2.0 * avg14 + avg28) / 7.0


def _vr(n: int):
    prev_c = pl.col("close").shift(1).over("code")
    vol = pl.col("volume")
    up = pl.when(pl.col("close") > prev_c).then(vol).otherwise(0.0).rolling_sum(window_size=n).over("code")
    dn = pl.when(pl.col("close") < prev_c).then(vol).otherwise(0.0).rolling_sum(window_size=n).over("code")
    fl = pl.when(pl.col("close") == prev_c).then(vol).otherwise(0.0).rolling_sum(window_size=n).over("code")
    return (up + 0.5 * fl) / (dn + 0.5 * fl) * 100.0


def _psy(n: int):
    cond = pl.col("close") > pl.col("close").shift(1).over("code")
    return cond.cast(pl.Int32).rolling_sum(window_size=n).over("code") / n * 100.0


def _cr(n: int):
    mid = (pl.col("high") + pl.col("low") + pl.col("close")) / 3.0
    prev_mid = mid.shift(1).over("code")
    pm = pl.when(pl.col("high") - prev_mid > 0).then(pl.col("high") - prev_mid).otherwise(0.0)
    pn = pl.when(prev_mid - pl.col("low") > 0).then(prev_mid - pl.col("low")).otherwise(0.0)
    return pm.rolling_sum(window_size=n).over("code") / pn.rolling_sum(window_size=n).over("code") * 100.0
```

- [x] **Step 2: 在 `INDICATORS` 字典 `MACD_HIST` 条目后追加 23 条**

```python
    # ---- 常规量化平台指标补齐（慢路径实时计算）----
    "DMI_PDI": {"func": lambda n: _dmi_di("p", n), "window": False, "signature": ["pos_int"]},
    "DMI_MDI": {"func": lambda n: _dmi_di("m", n), "window": False, "signature": ["pos_int"]},
    "DMI_ADX": {"func": _dmi_adx, "window": False, "signature": ["pos_int"]},
    "OBV": {"func": _obv, "window": False, "signature": []},
    "CCI": {"func": _cci, "window": False, "signature": ["pos_int"]},
    "WR": {"func": _wr, "window": False, "signature": ["pos_int"]},
    "MFI": {"func": _mfi, "window": False, "signature": ["pos_int"]},
    "SAR": {"func": _sar, "window": False, "signature": []},
    "AROON_UP": {"func": _aroon_up, "window": False, "signature": ["pos_int"]},
    "AROON_DOWN": {"func": _aroon_down, "window": False, "signature": ["pos_int"]},
    "TRIX": {"func": _trix, "window": False, "signature": ["pos_int"]},
    "BBI": {"func": _bbi, "window": False, "signature": []},
    "VWAP": {"func": _vwap, "window": False, "signature": ["pos_int"]},
    "BIAS": {"func": lambda n: _bias(pl.col("close"), n), "window": False, "signature": ["pos_int"]},
    "KDJ_J": {"func": _kdj_j, "window": False, "signature": ["pos_int", "pos_int"]},
    "BOLL_MID": {"func": _boll_mid, "window": False, "signature": ["series", "pos_int"]},
    "PPO": {"func": _ppo, "window": False, "signature": ["pos_int", "pos_int"]},
    "DEMA": {"func": _dema, "window": False, "signature": ["series", "pos_int"]},
    "TEMA": {"func": _tema, "window": False, "signature": ["series", "pos_int"]},
    "UO": {"func": _uo, "window": False, "signature": []},
    "VR": {"func": _vr, "window": False, "signature": ["pos_int"]},
    "PSY": {"func": _psy, "window": False, "signature": ["pos_int"]},
    "CR": {"func": _cr, "window": False, "signature": ["pos_int"]},
```

- [x] **Step 3: `DESCRIPTIONS` 追加 23 条中文说明**

在 `DESCRIPTIONS` 字典 `MACD_HIST` 键后追加：

```python
    "DMI_PDI": "+DI上升趋向指标（N日，固定用HIGH/LOW/CLOSE）",
    "DMI_MDI": "-DI下降趋向指标（N日，固定用HIGH/LOW/CLOSE）",
    "DMI_ADX": "ADX趋向平均线（N日，固定用HIGH/LOW/CLOSE）",
    "OBV": "能量潮（累计量：收涨+量/收跌-量，固定用CLOSE/VOL）",
    "CCI": "顺势指标CCI（N日，固定用HIGH/LOW/CLOSE）",
    "WR": "威廉指标WR（N日，固定用HIGH/LOW/CLOSE，>80超买/<20超卖）",
    "MFI": "资金流量指数MFI（N日，固定用HIGH/LOW/CLOSE/VOL）",
    "SAR": "抛物线停损SAR（固定0.02/0.2，固定用HIGH/LOW/CLOSE）",
    "AROON_UP": "阿隆上升（N日新高比例，固定用HIGH）",
    "AROON_DOWN": "阿隆下降（N日新低比例，固定用LOW）",
    "TRIX": "三重指数均线变动率（N日，固定用CLOSE）",
    "BBI": "多空指标（3/6/12/24日均线均值，固定用CLOSE）",
    "VWAP": "N日量价均价（SUM(C*VOL,n)/SUM(VOL,n)）",
    "BIAS": "N日乖离率（(C-MA(C,n))/MA(C,n)*100）",
    "KDJ_J": "KDJ随机指标J值（3K-2D，固定用HIGH/LOW/CLOSE）",
    "BOLL_MID": "布林带中轨（N日均价）",
    "PPO": "价格振荡百分比（(EMA(C,f)-EMA(C,s))/EMA(C,s)*100）",
    "DEMA": "双重指数均线（2*EMA-EMA(EMA)）",
    "TEMA": "三重指数均线（3*EMA-3*EMA(EMA)+EMA(EMA(EMA))）",
    "UO": "终极摆动指标（固定7/14/28窗口，固定用HIGH/LOW/CLOSE）",
    "VR": "N日量比（(上涨量+0.5平盘量)/(下跌量+0.5平盘量)*100）",
    "PSY": "N日心理线（上涨天数占比*100）",
    "CR": "N日能量指标（上涨中间价动量/下跌中间价动量*100）",
```

- [x] **Step 4: `EXAMPLE_QUERIES` 追加 5 条代表例**

在 `EXAMPLE_QUERIES` 列表 `MACD_HIST` 示例后追加：

```python
    "CROSS_UP(DMI_PDI(14), DMI_MDI(14))",
    "WR(14) > 80",
    "CCI(14) > 100",
    "MFI(14) < 20",
    "CLOSE > SAR()",
```

- [x] **Step 5: 运行测试确认 GREEN**

Run: `python -m unittest discover -s tests -v`（backend 工作目录）
Expected: 全部 PASS（13 个 registry 测试 + 6 个新增数学测试）。

> 评审驱动的修复：DMI_ADX NaN 传播（0/0 + polars NaN>0 陷阱）、AROON 单调趋势全 null/旧锚点外溢衰减、SAR/AROON 空值容错；追加 ADX/AROON Down/AROON 空值回归测试。当前 `Ran 98 tests ... OK`。

- [x] **Step 6: Commit**

```bash
git add backend/core/indicator_registry.py backend/tests/test_registry.py
git commit -m "feat(backend): 注册表补齐 23 个常规量化平台指标"
```

> 实际提交：`c95710c`（实现）+ `05c34ce`/`5806e0c`/`52337db`（评审修复）+ `54bcd96`（回归测试）。

---

## Task 3: 前端 RED — 新增校验测试（META 引用新算子）

**Files:**
- Modify: `frontend/tests/select-nl.test.mjs`

- [x] **Step 1: 在测试文件末尾追加新用例**（置于 guard 测试之前、coverage 测试之后均可；建议追加到文件最后 `} )` 之后新 test 块）

```javascript
// ---- 新增 23 算子：公式校验 + 提示词联动 ----
test('new-indicators: 零参算子 OBV()/SAR()/BBI()/UO() 语法通过', () => {
  for (const f of ['OBV() > 0', 'CLOSE > SAR()', 'BBI() < CLOSE', 'UO() > 50']) {
    const r = validateFormula(META, f);
    assert.deepEqual(r, { ok: true }, f);
  }
});

test('new-indicators: 单值算子比较通过', () => {
  const ok = [
    'DMI_PDI(14) > DMI_MDI(14)',
    'DMI_ADX(14) > 25',
    'WR(14) > 80',
    'CCI(14) > 100',
    'MFI(14) < 20',
    'AROON_UP(14) > 80',
    'AROON_DOWN(14) < 20',
    'TRIX(12) > 0',
    'VWAP(20) < CLOSE',
    'BIAS(20) > 5',
    'KDJ_J(9, 3) > 100',
    'PPO(12, 26) > 0',
    'VR(14) > 150',
    'PSY(12) > 60',
    'CR(20) > 100',
  ];
  for (const f of ok) {
    const r = validateFormula(META, f);
    assert.deepEqual(r, { ok: true }, f);
  }
});

test('new-indicators: series 参数算子通过', () => {
  const ok = [
    'CLOSE > BOLL_MID(CLOSE, 20)',
    'CLOSE > DEMA(CLOSE, 20)',
    'CLOSE > TEMA(CLOSE, 20)',
  ];
  for (const f of ok) {
    const r = validateFormula(META, f);
    assert.deepEqual(r, { ok: true }, f);
  }
});

test('new-indicators: 参数个数错误拒绝', () => {
  for (const f of ['OBV(20) > 0', 'SAR(14) < CLOSE', 'DMI_PDI() > 10', 'CCI(14, 20) > 0', 'WR(14, 5) > 50']) {
    const r = validateFormula(META, f);
    assert.equal(r.ok, false, f);
  }
});

test('new-indicators: 金叉组合通过', () => {
  const f = 'CROSS_UP(DMI_PDI(14), DMI_MDI(14))';
  const r = validateFormula(META, f);
  assert.deepEqual(r, { ok: true }, f);
});

test('new-indicators: buildSystemPrompt 含新算子说明与零参写法', () => {
  const p = buildSystemPrompt(META);
  assert.match(p, /DMI_PDI/);
  assert.match(p, /WR\(/);
  assert.match(p, /SAR\(\)/);
  assert.match(p, /OBV\(\)/);
  assert.match(p, /BOLL_MID/);
});
```

- [x] **Step 2: 运行确认 RED**

Run: `node --test tests/select-nl.test.mjs`（frontend 工作目录）
Expected: 新用例 FAIL（`OBV` 未注册 / `SAR` 未注册 等校验失败——因为 META 副本还没有新算子）。

> 评审修复：补齐 `AROON_DOWN` 覆盖（`new-indicators: 单值算子比较通过` 与计划同步）。当前 `100 pass / 5 fail`（5 个 `new-indicators:*` 因 META 未同步而红）。

---

## Task 4: 前端 GREEN — META 副本 / IND_GEN / selectNL.ts 提示词同步

**Files:**
- Modify: `frontend/tests/select-nl.test.mjs`
- Modify: `frontend/scripts/nl-coverage.mjs`
- Modify: `frontend/src/lib/selectNL.ts`

- [x] **Step 1: 更新测试文件 `META.indicators`（第 18 行）**

替换为（sorted 全 47 项，与 Task 1 后端列表一致）：

```javascript
  indicators: ['ABS', 'AROON_DOWN', 'AROON_UP', 'ATR', 'BARSLAST', 'BBI', 'BIAS', 'BOLL_LOWER', 'BOLL_MID', 'BOLL_UPPER', 'CCI', 'COUNT', 'CR', 'CROSS_DOWN', 'CROSS_UP', 'DEMA', 'DMI_ADX', 'DMI_MDI', 'DMI_PDI', 'EMA', 'HHV', 'KDJ_D', 'KDJ_J', 'KDJ_K', 'LLV', 'MA', 'MACD_DEA', 'MACD_DIF', 'MACD_HIST', 'MAX', 'MFI', 'MIN', 'OBV', 'PPO', 'PSY', 'REF', 'ROC', 'RSI', 'SAR', 'STD', 'SUM', 'TEMA', 'TRIX', 'UO', 'VR', 'VWAP', 'WR'],
```

- [x] **Step 2: 更新测试文件 `META.signatures`（第 22 行）**

在 `MACD_HIST: ['pos_int', 'pos_int', 'pos_int']` 后追加：

```javascript
, DMI_PDI: ['pos_int'], DMI_MDI: ['pos_int'], DMI_ADX: ['pos_int'], OBV: [], CCI: ['pos_int'], WR: ['pos_int'], MFI: ['pos_int'], SAR: [], AROON_UP: ['pos_int'], AROON_DOWN: ['pos_int'], TRIX: ['pos_int'], BBI: [], VWAP: ['pos_int'], BIAS: ['pos_int'], KDJ_J: ['pos_int', 'pos_int'], BOLL_MID: ['series', 'pos_int'], PPO: ['pos_int', 'pos_int'], DEMA: ['series', 'pos_int'], TEMA: ['series', 'pos_int'], UO: [], VR: ['pos_int'], PSY: ['pos_int'], CR: ['pos_int']
```

（注意：该行已以 `}` 结尾，需把 ` }` 前的内容扩展；保持同一对象字面量语法正确。）

- [x] **Step 3: 更新测试文件 `META.descriptions`（第 23 行）**

在 `MACD_HIST: '...'` 后追加 23 条（中文说明与 Task 2 Step 3 逐字节一致）：

```javascript
, DMI_PDI: '+DI上升趋向指标（N日，固定用HIGH/LOW/CLOSE）', DMI_MDI: '-DI下降趋向指标（N日，固定用HIGH/LOW/CLOSE）', DMI_ADX: 'ADX趋向平均线（N日，固定用HIGH/LOW/CLOSE）', OBV: '能量潮（累计量：收涨+量/收跌-量，固定用CLOSE/VOL）', CCI: '顺势指标CCI（N日，固定用HIGH/LOW/CLOSE）', WR: '威廉指标WR（N日，固定用HIGH/LOW/CLOSE，>80超买/<20超卖）', MFI: '资金流量指数MFI（N日，固定用HIGH/LOW/CLOSE/VOL）', SAR: '抛物线停损SAR（固定0.02/0.2，固定用HIGH/LOW/CLOSE）', AROON_UP: '阿隆上升（N日新高比例，固定用HIGH）', AROON_DOWN: '阿隆下降（N日新低比例，固定用LOW）', TRIX: '三重指数均线变动率（N日，固定用CLOSE）', BBI: '多空指标（3/6/12/24日均线均值，固定用CLOSE）', VWAP: 'N日量价均价（SUM(C*VOL,n)/SUM(VOL,n)）', BIAS: 'N日乖离率（(C-MA(C,n))/MA(C,n)*100）', KDJ_J: 'KDJ随机指标J值（3K-2D，固定用HIGH/LOW/CLOSE）', BOLL_MID: '布林带中轨（N日均价）', PPO: '价格振荡百分比（(EMA(C,f)-EMA(C,s))/EMA(C,s)*100）', DEMA: '双重指数均线（2*EMA-EMA(EMA)）', TEMA: '三重指数均线（3*EMA-3*EMA(EMA)+EMA(EMA(EMA))）', UO: '终极摆动指标（固定7/14/28窗口，固定用HIGH/LOW/CLOSE）', VR: 'N日量比（(上涨量+0.5平盘量)/(下跌量+0.5平盘量)*100）', PSY: 'N日心理线（上涨天数占比*100）', CR: 'N日能量指标（上涨中间价动量/下跌中间价动量*100）'
```

- [x] **Step 4: 更新测试文件 `META.example_queries`（第 21 行）**

在 `'CROSS_UP(MACD_DIF(12, 26), MACD_DEA(12, 26, 9))'` 后追加：

```javascript
, 'CROSS_UP(DMI_PDI(14), DMI_MDI(14))', 'WR(14) > 80', 'CCI(14) > 100', 'MFI(14) < 20', 'CLOSE > SAR()'
```

- [x] **Step 5: `nl-coverage.mjs` 的 `IND_GEN` 追加 23 条**（在 `WR` 之前按字母序插入，或直接加在对象内任意位置）

```javascript
  AROON_DOWN: { q: '阿隆下降大于50的股票', sub: ['AROON_DOWN'] },
  AROON_UP: { q: '阿隆上升大于80的股票', sub: ['AROON_UP'] },
  BBI: { q: '收盘价站上多空指标的股票', sub: ['BBI'] },
  BIAS: { q: '20日乖离率大于5%的股票', sub: ['BIAS'] },
  BOLL_MID: { q: '收盘价高于布林中轨的股票', sub: ['BOLL_MID'] },
  CCI: { q: '14日CCI突破100的股票', sub: ['CCI'] },
  CR: { q: '20日能量指标大于100的股票', sub: ['CR'] },
  DEMA: { q: '收盘价站上20日双重指数均线的股票', sub: ['DEMA'] },
  DMI_ADX: { q: '14日ADX大于25的股票', sub: ['DMI_ADX'] },
  DMI_MDI: { q: '14日-DI小于20的股票', sub: ['DMI_MDI'] },
  DMI_PDI: { q: '14日+DI大于-DI的股票', sub: ['DMI_PDI'] },
  KDJ_J: { q: 'KDJ的J值大于100的股票', sub: ['KDJ_J'] },
  MFI: { q: '14日资金流量指数小于20的股票', sub: ['MFI'] },
  OBV: { q: '能量潮OBV大于0的股票', sub: ['OBV'] },
  PPO: { q: '价格振荡PPO大于0的股票', sub: ['PPO'] },
  PSY: { q: '12日心理线大于60的股票', sub: ['PSY'] },
  SAR: { q: '收盘价站上抛物线SAR的股票', sub: ['SAR'] },
  TEMA: { q: '收盘价站上20日三重指数均线的股票', sub: ['TEMA'] },
  TRIX: { q: '12日TRIX大于0的股票', sub: ['TRIX'] },
  UO: { q: '终极摆动指标大于50的股票', sub: ['UO'] },
  VR: { q: '14日量比大于150的股票', sub: ['VR'] },
  VWAP: { q: '收盘价低于20日量价均价的股票', sub: ['VWAP'] },
  WR: { q: '14日威廉指标大于80的股票', sub: ['WR'] },
```

- [x] **Step 6: `selectNL.ts` 易错模式追加第 9 条**（在 `buildSystemPrompt` 的 `8) MACD 金叉/死叉...` 行后追加）

```typescript
    '9) DMI/ADX 金叉：用 CROSS_UP(DMI_PDI(N), DMI_MDI(N))；ADX 强弱用 DMI_ADX(N) > 25 直接比较。',
    '10) 零参算子必须写括号：OBV()、BBI()、SAR()、UO()，禁止写裸名 OBV/BBI/SAR/UO。',
    '11) CCI 突破用 CCI(N) > 100；WR 超买 WR(N) > 80、超卖 WR(N) < 20；MFI 用 MFI(N) < 20。',
```

- [x] **Step 7: `buildRepairSystemSuffix` 追加 2 条规则**（在 `selectNL.ts` 该函数规则数组末尾 `'- 若上次把 CROSS_UP...'` 后追加）

```typescript
    '- 若上次写了裸名 OBV/BBI/SAR/UO 或漏了括号，改为带括号调用（OBV()/BBI()/SAR()/UO()）；',
    '- 若上次把 CROSS_UP/CROSS_DOWN 用于 DMI，参数应为 DMI_PDI(N)/DMI_MDI(N)；',
```

- [x] **Step 8: 同步 `select-nl.test.mjs` 内嵌的 `buildSystemPrompt` / `buildRepairSystemSuffix` 副本**

在测试文件中找到复制的 `buildSystemPrompt`（含 `易错模式` 文本，约第 452 行）与复制的 `buildRepairSystemSuffix`（约第 989-1026 行区域），执行与 Step 6/Step 7 完全相同的文本追加（第 9/10/11 条 + 2 条 repair 规则）。测试副本必须与 `src/lib/selectNL.ts` 文本一致。

- [x] **Step 9: 运行确认 GREEN**

Run: `node --test tests/**/*.mjs`（frontend 工作目录）
Expected: 全量 PASS（含新 6 个用例，select-nl 总数 ~105）。

> 当前 `select-nl.test.mjs: 105 pass`；`tests/**/*.mjs: 171 pass`。

- [ ] **Step 10: Commit** (handled by implementer)

---

## Task 5: 全量验证 + TSC

- [x] **Step 1: 后端全量**

Run: `python -m unittest discover -s tests`（backend 工作目录）
Expected: 全部 PASS（registry 13 + 新增 6 + 其余后端测试）。

> 实测：`Ran 98 tests ... OK`

- [x] **Step 2: 前端 9 套件全量**

Run: `node --test tests/**/*.mjs`（frontend 工作目录）
Expected: 全部 PASS。

> 实测：`171 pass / 0 fail`

- [x] **Step 3: TSC 类型检查**

Run: `npx --yes -p typescript@5.3.3 tsc --noEmit --noResolve --skipLibCheck --jsx preserve --esModuleInterop src/lib/selectNL.ts`
Expected: 无错误输出。

> 实测：无错误（退出码 0）

- [x] **Step 4: 提交**

```bash
git add frontend/tests/select-nl.test.mjs frontend/scripts/nl-coverage.mjs frontend/src/lib/selectNL.ts
git commit -m "feat(frontend): 同步 23 新算子 META/覆盖生成器/提示词易错模式"
```

> 已提交 `9a065da`；docs 计划更新 `21f9bb1`。

---

## Task 6: 部署 + 线上满覆盖验证

- [ ] **Step 1: 推送 main 触发后端 3 节点 + 前端部署**

```bash
git push origin main
```

- [ ] **Step 2: 等部署（约 90-120 秒）后拉取 nl-meta 确认算子数**

Run: `node -e "fetch('https://blinkquant.de5.net/api/nl-meta').then(r=>r.json()).then(j=>console.log(j.indicators.length, j.indicators.join(',')))"`
Expected: `47` 且包含 `DMI_PDI`、`SAR`、`WR`、`BOLL_MID` 等。

- [ ] **Step 3: 线上全量覆盖验证（限流豁免模式）**

Run: `node scripts/nl-test.mjs "https://blinkquant.de5.net" "1@1.com" "22222222"`（frontend 工作目录；满跑约 4-10 分钟，用大 timeout 或重定向到日志文件）
Expected: 覆盖矩阵 **算子 47/47、字段 18/18**，无缺失。若有算子翻译失败，用 systematic-debugging 定位（多为 Nemotron 未命中注册名/签名错位，需在提示词易错模式补一条）。

---

## Self-Review

- **Spec coverage**：23 算子全部在 Task 2 实现；后端测试 Task 1；前端 META/IND_GEN/prompt Task 3-4；部署验证 Task 6。SAR 零参、OBV()/BBI()/UO() 零参、VWAP(n) rolling 语义均与设计文档一致。易错模式第 9/10/11 条覆盖 DMI 金叉、零参括号、CCI/WR/MFI 阈值。
- **Placeholder scan**：所有步骤含完整代码与预期输出；无 TBD/TODO。
- **Type consistency**：后端 helper 命名（`_dmi_di("p"|"m")`、`_kdj_j`、`_boll_mid`）与 INDICATORS 条目 lambda 一致；前端 META 签名与后端 `test_signatures_match_expected` 完全对齐；`IND_GEN` 键名与 META.indicators 一致。
