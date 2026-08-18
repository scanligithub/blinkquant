# 注册表补齐 23 个常见量化指标 — 设计文档

日期：2026-08-18

## 背景与目标

当前 DSL 注册表单算子 24 个（`backend/core/indicator_registry.py` 的 `INDICATORS` 字典）。对照常规量化平台（TALib/DMI 通/聚宽/掘金）主流指标集，补齐第一梯队、第二梯队与国内特色三组共 **23 个新算子**，使注册表达到 47 个算子。用户决策：只按常规量化平台判断（不以前端是否实现为准）。

### 用户已确认的决策
1. DMI 拆分为 `DMI_PDI` / `DMI_MDI` / `DMI_ADX` 三个独立算子（风格对齐 MACD_DIF/DEA/HIST、KDJ_K/KDJ_D）。
2. `SAR()` 采用零参、固定标准参数（afStep=0.02, afMax=0.2），不扩展 float 签名。
3. 零参口径：`OBV()` / `BBI()` 零参；`VWAP(n)` 为 N 日 rolling 量价均价 `SUM(C*VOL,n)/SUM(VOL,n)`。
4. 补齐全量：第一梯队 + 第二梯队 + 国内特色一次全做（方案 A）。

## 新增算子清单（24 → 47，全部 window:False 慢路径）

### 第一梯队（趋势/量能/超买超卖）
| 算子 | 签名 | 固定用列 | 描述 |
|---|---|---|---|
| DMI_PDI(n) | [pos_int] | H/L/C | +DI 上升趋向指标 |
| DMI_MDI(n) | [pos_int] | H/L/C | -DI 下降趋向指标 |
| DMI_ADX(n) | [pos_int] | H/L/C | ADX 趋向平均线 |
| OBV() | [] | C/VOL | 能量潮（累计量） |
| CCI(n) | [pos_int] | H/L/C | 顺势指标 |
| WR(n) | [pos_int] | H/L/C | 威廉指标 |
| MFI(n) | [pos_int] | H/L/C/VOL | 资金流量指数 |
| SAR() | [] | H/L/O/C | 抛物线停损（0.02/0.2） |
| AROON_UP(n) | [pos_int] | H | 阿隆上升 |
| AROON_DOWN(n) | [pos_int] | L | 阿隆下降 |

### 第二梯队（均线族/趋势强度）
| 算子 | 签名 | 说明 |
|---|---|---|
| TRIX(n) | [pos_int] | EMA³ 变动率 100*(EMA3−prev)/prev |
| BBI() | [] | (MA3+MA6+MA12+MA24)/4 |
| VWAP(n) | [pos_int] | N 日量价均价 SUM(C*VOL,n)/SUM(VOL,n) |
| BIAS(n) | [pos_int] | (C−MA(C,n))/MA(C,n)*100 乖离率 |
| KDJ_J(n,m) | [pos_int,pos_int] | 3K−2D（复用 _kdj_rsv） |
| BOLL_MID(series,n) | [series,pos_int] | 布林带中轨（=MA，但独立注册） |
| PPO(f,s) | [pos_int,pos_int] | (EMA(C,f)−EMA(C,s))/EMA(C,s)*100 |
| DEMA(series,n) | [series,pos_int] | 2·EMA−EMA(EMA) 双重指数均线 |
| TEMA(series,n) | [series,pos_int] | 3·EMA−3·EMA(EMA)+EMA(EMA(EMA)) 三重指数均线 |
| UO() | [] | 终极摆动指标，固定 7/14/28 窗口 |

### 国内特色
| 算子 | 签名 | 说明 |
|---|---|---|
| VR(n) | [pos_int] | 量比 (AVS+0.5·BVS)/(CVS+0.5·BVS)×100 |
| PSY(n) | [pos_int] | 心理线：N 日上涨天数占比×100 |
| CR(n) | [pos_int] | 能量指标：SUM(max(0,H−refMid))/SUM(max(0,refMid−L)) |

## 设计要点

### 执行模型约束
`INDICATORS[entry]["func"]` 返回 `pl.Expr`，由 `security.py:176` `_visit` 调用后在 `engine.py:88` `with_columns` 内求值，所有窗口运算必须 `.over("code")` 保证按股票分组不串算。`window:False` 走慢路径（实时计算，不参与 Hot-JIT/INDICATOR_MAP）。

### 关键技术实现
- **Wilder 平滑**：`_wilder(col,n)=col.ewm_mean(alpha=1/n, adjust=False)`。DMI 的 ±DM、TR、DX 都用它平滑，给出标准的 DI/ADX。
- **DMI**：TR=max(H−L,|H−prevC|,|L−prevC|)；+DM=H−prevH（当 H−prevH>0 且 H−prevH>prevL−L，否则 0）；−DM 对称；+DI=100·_wilder(+DM,n)/_wilder(TR,n)；ADX=100·_wilder(|+DI−−DI|/(+DI+−DI), n)。`window:False`，签名全 [pos_int]。
- **SAR（迭代/递归）**：不能纯表达式实现，采用 `pl.struct(["high","low","open","close"]).map_batches(sar_python_func).over("code")`。已验证 polars 1.39 map_batches 在 `.over("code")` 分组内按 code 边界正确切分，返回 f64 Series。移植前端 `SAR.ts` 算法（afStep=0.02, afMax=0.2）。
- **Aroon**：复用 `barslast` 手法：`n - barslast(high == rolling_max(high,n))`，比例乘 100。AROON_DOWN 用 `low == rolling_min(low,n)`。
- **OBV**：`sign(close.diff()) * vol` 或 when/otherwise，`cum_sum().over("code")`。
- **KDJ_J**：复用 `_kdj_rsv(n)` 得 RSV → K=rollmean(M) → D=rollmean(M) → J=3K−2D。
- **BOLL_MID**：`c.rolling_mean(n).over("code")`，即 MA 独立注册。

### 签名形态（security.py `_visit_arg` / 前端 validateCallArgs 已支持）
- `field`：白名单字段名
- `pos_int`：1..500 正整数
- `series`：field 或函数调用或算术
- `[]` 零参：`SAR()`/`OBV()`/`BBI()`/`UO()` 需要验证前端 validateCallArgs 支持空参数（args.length===sig.length 且 sig=[]），后端 `_visit` len(node.args)==0 时 func() 调用。**前端 callRegex / tokenRegex 需确认空参不报「未识别标识符」。**

### LLM 提示词联动
`buildSystemPrompt` / `buildRepairSystemSuffix` / `buildAnalyzePrompt` 会自动从 `meta.descriptions`/`meta.signatures`/`meta.example_queries` 拼接算子清单，无需手工维护函数级字符串，但需要：
- `DESCRIPTIONS` 新增 23 条（中文说明，拼进 prompt）
- `EXAMPLE_QUERIES` 新增代表查询（如 DMI 金叉 CROSS_UP(DMI_PDI(14),DMI_MDI(14))）
- 易错模式新增条目：DMI/ADX 用 CROSS_UP(DMI_PDI,DMI_MDI)、ADX>25 直接比较；零参算子必须写括号 `OBV()`；不可把 window 函数 MA 参数传二级调用。

## 测试策略（TDD）

### 后端 `backend/tests/test_registry.py`
- 更新 `test_builtin_indicators_present` 期望全列表 47 项（sorted）
- 更新 `test_signatures_match_expected` 期望全部签名
- 新增每个新算子的数学定义测试（对照手工计算/已知结果），核心是 partition 不串算测试 + SAR/OBV/DMI 定义对照表
- `test_all_entries_have_signature` / `test_nl_meta_shape` 等保持通过（自动遍历，无需改）

### 前端 `frontend/tests/select-nl.test.mjs`
- META 副本同步：`indicators` +23、`signatures` 全量、`descriptions` 全量、`example_queries` 追加
- guard 测试保持（防漂移，验证 src/lib/selectNL.ts 与副本一致）
- 新增用例：零参 `OBV()` 通过、`DMI_PDI(14)>DMI_MDI(14)` 通过、`BBI()` 语法、`VWAP(20)`、`BOLL_MID(CLOSE,20)`、`KDJ_J(9,3)`、非法参数个数拒绝
- 前端 select-nl 全量（99→~143）+ 9 套件全过 + TSC

### 覆盖生成器 `frontend/scripts/nl-coverage.mjs`
- `IND_GEN` 新增 23 条（每条含口语查询 + sub 指向算子），FIELD_GEN 不变
- `nl-test.mjs` 线上用例数量随 meta 增长自动扩展，覆盖矩阵应满 47/47

## 验证
1. 后端：`python -m unittest discover -s tests`（backend 全量）
2. 前端 select-nl 全量 + `node --test tests` 全 9 套件 + TSC
3. 部署 Vercel 后：`node scripts/nl-test.mjs "https://blinkquant.de5.net" "1@1.com" "22222222"` 全量线上用例，期望 **47/47 算子 + 18/18 字段满覆盖**
4. 提交：git add/commit/push，等待部署完成后跑覆盖

## 范围外
- 不改 engine Hot-JIT / data_manager / FIELDS / UNITS（字段集不变）
- 不新增 float 签名类型（SAR 用零参规避）
- 不改前端 K 线图（用户明确：只补注册表，跟随常规量化平台）