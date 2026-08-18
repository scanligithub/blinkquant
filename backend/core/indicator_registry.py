"""指标注册表：BlinkQuant 选股 DSL 的单一事实来源。

约定：注册函数签名由每条目的 "signature" 字段声明，参数形态取值：
- field   = 白名单字段的 ast.Name
- pos_int = 正整数常量（1 ≤ n ≤ 500）
- series  = field 或 一层窗口函数调用（如 MA(CLOSE,20)）
- cond    = 布尔表达式（Compare > >= < <= 或 AND/OR 组合）

"window": True 的条目签名恒为 [field, pos_int]，参与 Hot-JIT 挂载/统计；
其余为慢路径实时计算。开发者新增指标只需在此字典加一项。
"""

import polars as pl


def cross_up(a, b):
    prev_a, prev_b = a.shift(1).over("code"), b.shift(1).over("code")
    return (a > b) & (prev_a <= prev_b)


def cross_down(a, b):
    prev_a, prev_b = a.shift(1).over("code"), b.shift(1).over("code")
    return (a < b) & (prev_a >= prev_b)


def count(cond, n):
    return cond.cast(pl.Int32).rolling_sum(window_size=n).over("code")


def barslast(cond):
    row = pl.int_range(pl.len()).over("code")
    anchor = pl.when(cond).then(row).otherwise(None)
    filled = anchor.forward_fill().over("code")
    return (row - filled).cast(pl.Int32)


def _kdj_rsv(n: int):
    """KDJ 中间量 RSV：RSV=(C-LLV(L,n))/(HHV(H,n)-LLV(L,n))*100（固定用 H/L/C 列）"""
    low_min = pl.col("low").rolling_min(window_size=n).over("code")
    high_max = pl.col("high").rolling_max(window_size=n).over("code")
    return (pl.col("close") - low_min) / (high_max - low_min) * 100


def _ema(col, n: int):
    return col.ewm_mean(span=n, adjust=False).over("code")


def _macd_dif(fast: int, slow: int):
    """DIF = EMA(CLOSE, fast) - EMA(CLOSE, slow)（固定用 CLOSE 列）"""
    return _ema(pl.col("close"), fast) - _ema(pl.col("close"), slow)


def _macd_dea(fast: int, slow: int, signal: int):
    """DEA = EMA(DIF, signal)"""
    return _ema(_macd_dif(fast, slow), signal)


def _macd_hist(fast: int, slow: int, signal: int):
    """MACD 柱 = 2 * (DIF - DEA)"""
    dif = _macd_dif(fast, slow)
    return (dif - _macd_dea(fast, slow, signal)) * 2


def _wilder(col, n: int):
    """Wilder 平滑：SMMA_t = ((n-1)*SMMA_{t-1} + val_t)/n ≡ ewm(alpha=1/n)"""
    return col.ewm_mean(alpha=1 / n, adjust=False).over("code")


def _dmi_tr():
    """DMI 真实波幅 TR：max(H-L, |H-前收|, |L-前收|)（固定用 HIGH/LOW/CLOSE）"""
    high, low, close = pl.col("high"), pl.col("low"), pl.col("close")
    prev_c = close.shift(1).over("code")
    return pl.max_horizontal(high - low, (high - prev_c).abs(), (low - prev_c).abs())


def _dmi_dm_plus():
    """+DM 上升动向：今高>前高 且 今高-前高>前低-今低"""
    high, low = pl.col("high"), pl.col("low")
    prev_h, prev_l = high.shift(1).over("code"), low.shift(1).over("code")
    dm = high - prev_h
    return pl.when((dm > 0) & (dm > prev_l - low)).then(dm).otherwise(0.0)


def _dmi_dm_minus():
    """-DM 下降动向：前低>今低 且 前低-今低>今高-前高"""
    high, low = pl.col("high"), pl.col("low")
    prev_h, prev_l = high.shift(1).over("code"), low.shift(1).over("code")
    dm = prev_l - low
    return pl.when((dm > 0) & (dm > high - prev_h)).then(dm).otherwise(0.0)


def _dmi_di(sign: str, n: int):
    """±DI：100 × Wilder平滑(DM) / Wilder平滑(TR)（sign='p'/'m'）"""
    tr_s = _wilder(_dmi_tr(), n)
    dm_s = _wilder(_dmi_dm_plus() if sign == "p" else _dmi_dm_minus(), n)
    return 100.0 * dm_s / tr_s


def _dmi_adx(n: int):
    """ADX：Wilder平滑(100×|PDI-MDI|/(PDI+MDI))，分母为 0 时置 0 防 NaN"""
    pdi, mdi = _dmi_di("p", n), _dmi_di("m", n)
    dx = pl.when((pdi + mdi) > 0).then(100.0 * (pdi - mdi).abs() / (pdi + mdi)).otherwise(0.0)
    return _wilder(dx, n)


def _obv():
    """能量潮：收涨累计+VOL、收跌累计-VOL、平收0（固定用 CLOSE/VOL）"""
    close, vol = pl.col("close"), pl.col("volume")
    prev_c = close.shift(1).over("code")
    signed = pl.when(close > prev_c).then(vol).when(close < prev_c).then(-vol).otherwise(0.0)
    return signed.cum_sum().over("code")


def _cci(n: int):
    """CCI 顺势指标：(TP-MA(TP,n))/(0.015×MD(TP,n))（固定用HIGH/LOW/CLOSE）"""
    tp = (pl.col("high") + pl.col("low") + pl.col("close")) / 3.0
    ma_tp = tp.rolling_mean(window_size=n).over("code")
    md = (tp - ma_tp).abs().rolling_mean(window_size=n).over("code")
    return (tp - ma_tp) / (0.015 * md)


def _wr(n: int):
    """威廉指标 WR：(HHV(H,n)-C)/(HHV(H,n)-LLV(L,n))×100（固定用HIGH/LOW/CLOSE）"""
    high_max = pl.col("high").rolling_max(window_size=n).over("code")
    low_min = pl.col("low").rolling_min(window_size=n).over("code")
    return (high_max - pl.col("close")) / (high_max - low_min) * 100.0


def _mfi(n: int):
    """资金流量 MFI：100-100/(1+正流量/负流量)（固定用HIGH/LOW/CLOSE/VOL）"""
    tp = (pl.col("high") + pl.col("low") + pl.col("close")) / 3.0
    mf = tp * pl.col("volume")
    prev_tp = tp.shift(1).over("code")
    pos = pl.when(tp > prev_tp).then(mf).otherwise(0.0).rolling_sum(window_size=n).over("code")
    neg = pl.when(tp < prev_tp).then(mf).otherwise(0.0).rolling_sum(window_size=n).over("code")
    return 100.0 - 100.0 / (1.0 + pos / neg)


def _sar_from_hloc(s):
    """SAR 迭代（afStep=0.02, afMax=0.2）。s 为单 code 组的 struct Series，按时间序；含 None（停牌日）行沿用前值。"""
    high = s.struct.field("high").to_list()
    low = s.struct.field("low").to_list()
    open = s.struct.field("open").to_list()
    close = s.struct.field("close").to_list()
    n = len(high)
    out = [0.0] * n
    if n == 0:
        return pl.Series("sar", out, dtype=pl.Float64)
    ep, af, is_up = high[0], 0.02, True
    for i in range(n):
        if None in (high[i], low[i], open[i], close[i]):
            out[i] = out[i - 1] if i else 0.0
            continue
        if i < 2:
            out[i] = low[i]
            if i == 1:
                ep = high[0] if high[0] is not None else high[1]
                is_up = close[1] > open[1]
                if not is_up:
                    ep = low[0] if low[0] is not None else low[1]
            continue
        prev_sar = out[i - 1]
        if ep is None:
            ep = high[i] if is_up else low[i]
        new_sar = prev_sar + af * (ep - prev_sar)
        if is_up:
            if low[i] < new_sar:
                new_sar = ep
            if high[i] > ep:
                ep = high[i]
                af = min(af + 0.02, 0.2)
            if low[i] < out[i - 2]:
                is_up, ep, af = False, low[i], 0.02
                new_sar = out[i - 1]
        else:
            if high[i] > new_sar:
                new_sar = ep
            if low[i] < ep:
                ep = low[i]
                af = min(af + 0.02, 0.2)
            if high[i] > out[i - 2]:
                is_up, ep, af = True, high[i], 0.02
                new_sar = out[i - 1]
        out[i] = new_sar
    return pl.Series("sar", out, dtype=pl.Float64)


def _sar():
    """抛物线停损 SAR：map_batches 逐 code 组迭代（固定0.02/0.2，固定用H/L/O/C）"""
    return pl.struct(["high", "low", "open", "close"]).map_batches(_sar_from_hloc).over("code")


def _aroon_from_hl(s, n: int, up: bool):
    """单 code 组的 Aroon：窗口内最后一次极值距今天数 → 100*(n-bars_since)/n。

    bars_since = (n-1) - j（j 为窗口内极值 0 基位置，最末一个匹配）；前 n-1 行不足窗口返回 None。
    """
    ext = s.struct.field("high" if up else "low").to_list()
    k = len(ext)
    out = [None] * k
    for i in range(k):
        if i < n - 1:
            continue
        w = ext[i - n + 1:i + 1]
        m = max(w) if up else min(w)
        for j in range(n - 1, -1, -1):
            if w[j] == m:
                out[i] = 100.0 * (n - (n - 1 - j)) / n
                break
    return pl.Series("a", out, dtype=pl.Float64)


def _aroon_up(n: int):
    """阿隆上升：100×(N-BARSLAST(H==HHV(H,N)))/N，窗口内最后一次新高（固定用HIGH）"""
    return pl.struct(["high"]).map_batches(lambda s: _aroon_from_hl(s, n, True)).over("code")


def _aroon_down(n: int):
    """阿隆下降：100×(N-BARSLAST(L==LLV(L,N)))/N，窗口内最后一次新低（固定用LOW）"""
    return pl.struct(["low"]).map_batches(lambda s: _aroon_from_hl(s, n, False)).over("code")


def _trix(n: int):
    """TRIX：EMA³(CLOSE) 的逐期变动率 ×100（固定用CLOSE）"""
    e3 = _ema(_ema(_ema(pl.col("close"), n), n), n)
    prev = e3.shift(1).over("code")
    return (e3 - prev) / prev * 100.0


def _bbi():
    """多空指标：(MA3+MA6+MA12+MA24)/4（固定用CLOSE）"""
    c = pl.col("close")
    return (c.rolling_mean(window_size=3).over("code")
            + c.rolling_mean(window_size=6).over("code")
            + c.rolling_mean(window_size=12).over("code")
            + c.rolling_mean(window_size=24).over("code")) / 4.0


def _vwap(n: int):
    """N日量价均价：SUM(C×VOL,n)/SUM(VOL,n)"""
    return ((pl.col("close") * pl.col("volume")).rolling_sum(window_size=n).over("code")
            / pl.col("volume").rolling_sum(window_size=n).over("code"))


def _bias(c, n: int):
    """乖离率：(C-MA(C,n))/MA(C,n)×100"""
    ma = c.rolling_mean(window_size=n).over("code")
    return (c - ma) / ma * 100.0


def _kdj_j(n: int, m: int):
    """KDJ J 值：3K-2D，K=D=RSV 的 m 期均值"""
    k = _kdj_rsv(n).rolling_mean(window_size=m).over("code")
    d = k.rolling_mean(window_size=m).over("code")
    return 3.0 * k - 2.0 * d


def _boll_mid(c, n: int):
    """布林带中轨：N日简单均值"""
    return c.rolling_mean(window_size=n).over("code")


def _ppo(f: int, s: int):
    """PPO：100×(EMA(C,f)-EMA(C,s))/EMA(C,s)"""
    ef, es = _ema(pl.col("close"), f), _ema(pl.col("close"), s)
    return (ef - es) / es * 100.0


def _dema(c, n: int):
    """双重指数均线：2×EMA(C,n)-EMA(EMA(C,n),n)"""
    e = _ema(c, n)
    return 2.0 * e - _ema(e, n)


def _tema(c, n: int):
    """三重指数均线：3×EMA-3×EMA²+EMA³"""
    e1, e2 = _ema(c, n), _ema(_ema(c, n), n)
    return 3.0 * e1 - 3.0 * e2 + _ema(e2, n)


def _uo():
    """终极摆动指标：100×(4·BP7+2·BP14+BP28)/7（固定7/14/28窗口）"""
    prev_c = pl.col("close").shift(1).over("code")
    bp = pl.col("close") - pl.min_horizontal(pl.col("low"), prev_c)
    tr = pl.max_horizontal(pl.col("high"), prev_c) - pl.min_horizontal(pl.col("low"), prev_c)
    avg7 = bp.rolling_sum(window_size=7).over("code") / tr.rolling_sum(window_size=7).over("code")
    avg14 = bp.rolling_sum(window_size=14).over("code") / tr.rolling_sum(window_size=14).over("code")
    avg28 = bp.rolling_sum(window_size=28).over("code") / tr.rolling_sum(window_size=28).over("code")
    return 100.0 * (4.0 * avg7 + 2.0 * avg14 + avg28) / 7.0


def _vr(n: int):
    """量比：100×(上量+0.5平量)/(下量+0.5平量)"""
    prev_c = pl.col("close").shift(1).over("code")
    vol = pl.col("volume")
    up = pl.when(pl.col("close") > prev_c).then(vol).otherwise(0.0).rolling_sum(window_size=n).over("code")
    dn = pl.when(pl.col("close") < prev_c).then(vol).otherwise(0.0).rolling_sum(window_size=n).over("code")
    fl = pl.when(pl.col("close") == prev_c).then(vol).otherwise(0.0).rolling_sum(window_size=n).over("code")
    return (up + 0.5 * fl) / (dn + 0.5 * fl) * 100.0


def _psy(n: int):
    """心理线：N日上涨天数占比×100"""
    cond = pl.col("close") > pl.col("close").shift(1).over("code")
    return cond.cast(pl.Int32).rolling_sum(window_size=n).over("code") / n * 100.0


def _cr(n: int):
    """能量指标：N日上涨/下跌中间价动量比×100"""
    mid = (pl.col("high") + pl.col("low") + pl.col("close")) / 3.0
    prev_mid = mid.shift(1).over("code")
    pm = pl.when(pl.col("high") - prev_mid > 0).then(pl.col("high") - prev_mid).otherwise(0.0)
    pn = pl.when(prev_mid - pl.col("low") > 0).then(prev_mid - pl.col("low")).otherwise(0.0)
    return pm.rolling_sum(window_size=n).over("code") / pn.rolling_sum(window_size=n).over("code") * 100.0


INDICATORS = {
    # ---- window 型（签名 [field, pos_int]，Hot-JIT 挂载）----
    "MA":  {"func": lambda c, n: c.rolling_mean(window_size=n).over("code"),            "window": True, "signature": ["field", "pos_int"]},
    "EMA": {"func": lambda c, n: c.ewm_mean(span=n, adjust=False).over("code"),          "window": True, "signature": ["field", "pos_int"]},
    "STD": {"func": lambda c, n: c.rolling_std(window_size=n).over("code"),             "window": True, "signature": ["field", "pos_int"]},
    "ROC": {"func": lambda c, n: ((c / c.shift(n).over("code")) - 1) * 100, "window": True, "signature": ["field", "pos_int"]},
    "REF": {"func": lambda c, n: c.shift(n).over("code"),                               "window": True, "signature": ["field", "pos_int"]},
    "HHV": {"func": lambda c, n: c.rolling_max(window_size=n).over("code"),             "window": True, "signature": ["field", "pos_int"]},
    "LLV": {"func": lambda c, n: c.rolling_min(window_size=n).over("code"),             "window": True, "signature": ["field", "pos_int"]},
    "SUM": {"func": lambda c, n: c.rolling_sum(window_size=n).over("code"),             "window": True, "signature": ["field", "pos_int"]},
    # ---- 非 window 型（慢路径实时计算）----
    "CROSS_UP":   {"func": cross_up,   "window": False, "signature": ["series", "series"]},
    "CROSS_DOWN": {"func": cross_down, "window": False, "signature": ["series", "series"]},
    "MAX": {"func": lambda a, b: pl.max_horizontal(a, b), "window": False, "signature": ["series", "series"]},
    "MIN": {"func": lambda a, b: pl.min_horizontal(a, b), "window": False, "signature": ["series", "series"]},
    "ABS": {"func": lambda x: x.abs(), "window": False, "signature": ["series"]},
    "COUNT":    {"func": count,    "window": False, "signature": ["cond", "pos_int"]},
    "BARSLAST": {"func": barslast, "window": False, "signature": ["cond"]},
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
    # ---- MACD 三分量（固定用 CLOSE，慢路径实时计算）----
    "MACD_DIF": {"func": lambda fast, slow: _macd_dif(fast, slow),
        "window": False, "signature": ["pos_int", "pos_int"]},
    "MACD_DEA": {"func": lambda fast, slow, signal: _macd_dea(fast, slow, signal),
        "window": False, "signature": ["pos_int", "pos_int", "pos_int"]},
    "MACD_HIST": {"func": lambda fast, slow, signal: _macd_hist(fast, slow, signal),
        "window": False, "signature": ["pos_int", "pos_int", "pos_int"]},
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
}

# 字段白名单：必须与 security.py 现有 fields 键集逐项一致（防 drift）
FIELDS = [
    "CLOSE", "OPEN", "HIGH", "LOW", "VOL", "AMOUNT", "PCT_CHG", "S_CLOSE",
    "PE_TTM", "PB_MRQ", "FORECAST_YOY", "IS_FORECAST_GOOD", "IS_FORECAST_BAD",
    "TOTAL_SHARES", "FLOAT_SHARES", "TOTAL_MV", "FLOAT_MV", "TURN",
]

# 单位标注：用于 LLM 提示词与前端展示（覆盖全部白名单字段）
UNITS = {
    "TOTAL_MV": "元", "FLOAT_MV": "元", "TOTAL_SHARES": "股",
    "FLOAT_SHARES": "股", "AMOUNT": "元", "VOL": "股",
    "CLOSE": "元", "OPEN": "元", "HIGH": "元", "LOW": "元",
    "PE_TTM": "无量纲(倍)", "PB_MRQ": "无量纲(倍)", "TURN": "百分比(%)",
    "FORECAST_YOY": "百分比(%)", "PCT_CHG": "百分比(%)", "S_CLOSE": "指数点位",
    "IS_FORECAST_GOOD": "布尔标记(0/1)", "IS_FORECAST_BAD": "布尔标记(0/1)",
}

# 算子中文说明：用于 LLM 提示词（value 会拼入 buildSystemPrompt）
DESCRIPTIONS = {
    "MA": "N日简单移动平均", "EMA": "N日指数移动平均", "STD": "N日标准差",
    "ROC": "N日变动率(%)", "REF": "N日前值", "HHV": "N周期内最高值",
    "LLV": "N周期内最低值", "SUM": "N周期内求和",
    "CROSS_UP": "上穿（今日A>B且昨日A<=B）", "CROSS_DOWN": "下穿（今日A<B且昨日A>=B）",
    "MAX": "取两序列较大值", "MIN": "取两序列较小值", "ABS": "绝对值",
    "COUNT": "N周期内条件成立次数", "BARSLAST": "距上次条件成立周期数",
    "ATR": "N日真实波幅均值（最高最低与昨收的最大差距，简化版）", "RSI": "N日相对强弱（涨跌幅均值比，简化版）",
    "BOLL_UPPER": "布林上轨（N日均价 + K倍N日标准差）", "BOLL_LOWER": "布林下轨（N日均价 - K倍N日标准差）",
    "KDJ_K": "KDJ随机指标K值（固定用HIGH/LOW/CLOSE，简化版）", "KDJ_D": "KDJ随机指标D值（固定用HIGH/LOW/CLOSE，简化版）",
    "MACD_DIF": "MACD快慢线差（EMA(CLOSE,fast) - EMA(CLOSE,slow)，固定用CLOSE）",
    "MACD_DEA": "MACD信号线（DIF的signal期EMA，固定用CLOSE）",
    "MACD_HIST": "MACD柱（2 × (DIF - DEA)，固定用CLOSE）",
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
}

EXAMPLE_QUERIES = [
    "CLOSE > MA(CLOSE, 20)",
    "PE_TTM < 20 AND TOTAL_MV > 1e10",
    "CROSS_UP(MA(CLOSE, 20), MA(CLOSE, 60))",
    "SUM(AMOUNT, 5) > 5e9",
    "CROSS_UP(KDJ_K(9, 3), KDJ_D(9, 3))",
    "CLOSE > BOLL_UPPER(CLOSE, 20, 2)",
    "CROSS_UP(MACD_DIF(12, 26), MACD_DEA(12, 26, 9))",
    "CROSS_UP(DMI_PDI(14), DMI_MDI(14))",
    "WR(14) > 80",
    "CCI(14) > 100",
    "MFI(14) < 20",
    "CLOSE > SAR()",
]

TIMEFRAMES = ["D", "W", "M"]

# window 型纯函数子集（供 Hot-JIT 与动态正则）
INDICATOR_FUNCS = {name: entry["func"] for name, entry in INDICATORS.items() if entry.get("window")}
WINDOW_NAMES = sorted(INDICATOR_FUNCS.keys())
INDICATOR_NAMES = sorted(INDICATORS.keys())


def nl_meta() -> dict:
    """nl-meta 接口数据（注册表驱动的单一事实来源）"""
    return {
        "fields": FIELDS,
        "indicators": INDICATOR_NAMES,
        "timeframes": TIMEFRAMES,
        "units": UNITS,
        "example_queries": EXAMPLE_QUERIES,
        "signatures": {name: entry["signature"] for name, entry in INDICATORS.items()},
        "descriptions": DESCRIPTIONS,
    }
