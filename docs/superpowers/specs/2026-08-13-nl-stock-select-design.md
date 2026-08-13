# 自然语言选股 + 声明式指标注册表 设计文档

版本: v1.0 | 日期: 2026-08-13
状态: 待评审

---

## 背景与目标

BlinkQuant 现有选股链路：用户在「策略公式」框输入公式（如 `CLOSE > MA(CLOSE, 20)`）→ 前端 `/api/select`（Vercel Edge）Promise.all 并发 3 个 HF 节点 → 后端执行引擎取 `code.hash() % total_nodes` 分片数据 → 返回股票代码列表。

两大诉求：

1. **自然语言选股**：用户用中文描述（如「市盈率低于 20 且总市值大于 100 亿的股票」），由大模型翻译成现有公式 DSL，复用现有选股管道。
2. **指标可扩展 + 安全**：当前新增技术指标需要同时改 `security.py`（Call 分支）、`data_manager.py`（INDICATOR_MAP）、`engine.py`（正则）三处。需要一种既能维持 AST 白名单安全模型、又允许开发者一行注册新指标的方案。

## 关键洞察

- 白名单安全的本质是**输入形态校验**（只允许 `Name | Constant | 算子 | 注册表函数调用` 四类节点），「有哪些字段/有哪些函数」是**数据**而非逻辑。把「合法集合」从解析器实现中分离为声明式注册表，即可扩展且不破坏安全。
- 后端 Hot-JIT（`engine.py:_prepare_hot_jit`）首次遇到 `MA(CLOSE,20)` 时把 `MA_CLOSE_20` 列挂载到日/周/月三表并永久驻留内存，之后同指标命中快路径。LLM 输出必须命中 AST 快路径（规范函数名 + 白名单字段）才能享受预计算加速。
- 跨语言单一事实来源：字段/指标知识住在后端 Python 注册表，前端 TS 需通过只读接口获取，避免两套列表手工同步的 drift。

## 架构决策

| 决策点 | 结论 |
|---|---|
| LLM 调用位置 | Vercel 前端 API（新增 `/api/select-nl`），后端节点保持主链路不变 |
| LLM 提供商 | 可配置（`LLM_ENDPOINT`/`LLM_API_KEY`/`LLM_MODEL` 环境变量），先用 mock 验证管道 |
| 交互形态 | AI 弹窗 + 公式预览（可编辑）→ 确认后走现有运行选股 |
| LLM 输出格式 | 结构化 JSON `{formula, timeframe, explanation}` |
| 失败处理 | 直接报错（LLM 失败/非法公式显式提示，不静默降级） |
| 调用限流 | 每用户内存计数（3 次/分钟，20 次/天），超限 429 |
| 字段知识来源 | 后端 `/api/v1/nl-meta` 动态生成，前端拉取缓存 |
| 指标注册方式 | 开发者声明式注册表（方案 A，Python lambda 注册） |

---

## 后端设计

### 1. 新增 `backend/core/indicator_registry.py`

单一事实来源，唯一需要人工扩展的地方。

```python
# 约定签名：f(column: pl.col, n: int) -> pl.Expr
# 第一个参数永远是白名单字段列，第二个永远是正整数窗口常量

INDICATORS = {
    "MA":  { "func": lambda c, n: c.rolling_mean(window_size=n),  "window": True },
    "EMA": { "func": lambda c, n: c.ewm_mean(span=n, adjust=False), "window": True },
    "STD": { "func": lambda c, n: c.rolling_std(window_size=n),      "window": True },
    "ROC": { "func": lambda c, n: ((c / c.shift(n).over("code")) - 1) * 100, "window": True },
    "REF": { "func": lambda c, n: c.shift(n).over("code"),             "window": True },
}

# 字段白名单（与 security.py 现有 fields 逐项对齐，禁止多余/缺失）
FIELDS = [
    "CLOSE", "OPEN", "HIGH", "LOW", "VOL", "AMOUNT", "PCT_CHG", "S_CLOSE",
    "PE_TTM", "PB_MRQ", "FORECAST_YOY", "IS_FORECAST_GOOD", "IS_FORECAST_BAD",
    "TOTAL_SHARES", "FLOAT_SHARES", "TOTAL_MV", "FLOAT_MV", "TURN",
]

# 单位标注（用于 LLM 提示词与前端展示）
UNITS = {
    "TOTAL_MV": "元", "FLOAT_MV": "元", "TOTAL_SHARES": "股",
    "FLOAT_SHARES": "股", "AMOUNT": "元", "VOL": "股",
    "PE_TTM": "无量纲(倍)", "PB_MRQ": "无量纲(倍)", "TURN": "百分比(%)",
    "FORECAST_YOY": "百分比(%)", "PCT_CHG": "百分比(%)", "S_CLOSE": "指数点位",
    # 资金流字段由 /api/v1/kline 提供（万元），但选股 DSL 暂无独立资金流字段
}
```

### 2. 改 `backend/core/security.py`: Call 分支查注册表

`_visit(ast.Call)` 从 5 个 `if` 分支重构为统一校验路径：

```python
elif isinstance(node, ast.Call):
    func = node.func.id.upper()
    entry = INDICATORS.get(func)
    if entry is None or not entry.get("window") or len(node.args) != 2:
        raise ValueError(f"Unknown function {func}")
    # 参数形态强制校验
    field_name = _require_whitelist_field(node.args[0])  # ast.Name 且 ∈ FIELDS
    n = _require_positive_int(node.args[1])              # ast.Constant 且 > 0
    # ★ 快路径必须保留：命中 Hot-JIT 挂载列则直接返回列引用（提速来源，勿删）
    pure_key = f"{func}_{field_name}_{n}"
    if self.current_df is not None and pure_key in self.current_df.columns:
        return pl.col(pure_key)
    # 慢路径：实时向量化计算（首算后 engine 会挂载，下次即命中快路径）
    return entry["func"](pl.col(field_name.lower()), n)
```

`_require_whitelist_field` / `_require_positive_int` 抽成模块级纯函数，供单测直接覆盖。

**行为兼容性要求**：
- 现有合法公式（`MA/EMA/STD/ROC/REF` 两参形式）结果不变；`REF` 在注册表中以 `"func"` 方式保留
- **快路径语义与现状一致**（对照 `security.py:87`）：`pure_key` 已挂载 → 返回 `pl.col(pure_key)`；未挂载 → 实时计算。这是「先使用者播种、后使用者白拣」Hot-JIT 收益的直接实现层，重构时禁止丢失
- `_require_whitelist_field` 只校验字段 ∈ `FIELDS`（字段是第一参数），与 `_visit(ast.Name)` 的「列名已挂载即引用」是两件事，互不影响

### 3. 改 `backend/core/data_manager.py`: INDICATOR_MAP 由注册表派生

删除 `self.INDICATOR_MAP` 手写 dict，改为：

```python
from .indicator_registry import INDICATORS
self.INDICATOR_MAP = {name: entry["func"] for name, entry in INDICATORS.items()}
```

### 4. 改 `backend/core/engine.py`: 正则动态生成

`metric_pattern` 由 `INDICATORS.keys()` 生成，替换硬编码：

```python
from .indicator_registry import INDICATORS
_funcs = "|".join(sorted(INDICATORS.keys()))
self.metric_pattern = re.compile(
    fr'({_funcs})\s*\(\s*(CLOSE|OPEN|HIGH|LOW|VOL|AMOUNT)\s*,\s*(\d+)\s*\)',
    re.IGNORECASE)
```

注意 `data_manager.py` 的 `INDICATOR_MAP` / engine 的 `metric_pattern` 均只应为第二参数为窗口的指标（`window: True`），注册表结构已按此区分。

### 5. 加 `backend/api/routes.py`: GET /api/v1/nl-meta

只读、公开（无需登录），返回注册表驱动的元数据：

```json
GET /api/v1/nl-meta
{
  "fields": ["CLOSE", "...", "S_CLOSE"],
  "indicators": ["MA", "EMA", "STD", "ROC", "REF"],
  "timeframes": ["D", "W", "M"],
  "units": { "TOTAL_MV": "元", "TURN": "百分比(%)", "..." : "..." },
  "example_queries": ["CLOSE > MA(CLOSE, 20)", "PE_TTM < 20 AND TOTAL_MV > 1e10"]
}
```

`example_queries` 为固定示例（供 LLM few-shot），硬编码在 routes 或注册表侧。

---

## 前端设计

### 6. 新增 `frontend/src/app/api/select-nl/route.ts` (Edge)

流程：`requireAuth` → 每用户限流（内存 Map<userId,{timestamps}>，3 次/分钟、20 次/天）→ 拉取 `/api/v1/nl-meta`（`Promise.any×3 节点`，24h 内存缓存）→ 构建提示词 → 调 LLM（配置化 endpoint/key/model，`AbortSignal.timeout`）→ JSON 解析 → 强校验 → 返回 `{formula, timeframe, explanation}`。

- 无 `LLM_ENDPOINT` 环境变量 → 返回 503「AI 选股未配置」，弹窗禁用，公式框不受影响
- 每轮尽量做一次重试（LLM 非 JSON 输出时）；仍失败 → 报错
- JSON 解析容忍 ```` ```json ```` 代码围栏剥离

### 7. 新增 `frontend/src/lib/selectNL.ts`（纯函数）

- `buildPrompt(nlMeta, userQuery)`：把 fields/units/indicators/timeframes + 单位换算规则（`亿→1e8`、`万→1e4`）+ few-shot 组装成系统提示词
- `parseSelectNLText(raw)`：剥离围栏 → JSON.parse → 归一化
- `validateFormula(fields, indicators)`：强校验公式，全部基于 `nl-meta` 数据驱动而不是硬编码数组
  - 字段断言：正则枚举白名单大写标识符
  - 算子断言：`MA|EMA|STD|ROC|REF` 且参数形态 `(<字段>, <正整数>)` 与注册表一致
  - `timeframe ∈ {D,W,M}`；公式长度上限（如 500）
- 校验不通过返回 `{ ok:false, reason }`，附详细原因供弹窗展示

### 8. 新增 `frontend/src/components/AISelectModal.tsx`

- 固定遮罩 + 居中弹窗（与保存策略弹窗风格一致）
- 上部：自然语言输入框 + 发送按钮（loading 态：LLM 请求中）
- 中部：公式预览（可编辑 `textarea`）+ LLM 解释文本 + 周期选择（复用 D/W/M）
- 底部：取消 / 运行选股（confirm → 调用父组件 `onRun(formula, timeframe)`，复用现有 `handleSelect` 路径）
- 错误信息区：LLM 不可用 / 翻译非法 / 限流 429，均显示在弹窗内
- 移动端适配：弹窗 `max-w-lg w-full` 自然适配，无额外覆盖层

### 9. 改 `frontend/src/app/page.tsx`

- 公式区「运行选股」旁新增「AI 选股」按钮
- 新增 `showAISelect` 状态；`onRun` 回调里 `setFormula(formula); setTimeframe(timeframe)` 后调用 `handleSelect()`
- 「AI 选股」按钮默认可用；若请求 `select-nl` 返回未配置（503），弹窗内显示配置提示并禁用发送，按钮保持可用（由首次弹窗承载，避免多余探测请求）

---

## 环境变量（Vercel）

| 变量 | 必填 | 说明 |
|---|---|---|
| `LLM_ENDPOINT` | 是 | LLM API URL（OpenAI 兼容接口） |
| `LLM_API_KEY` | 是 | 密钥 |
| `LLM_MODEL` | 是 | 模型名 |
| `LLM_TIMEOUT_MS` | 否 | 默认 15000 ms |

新增 /api/v1/nl-meta 无需额外变量（读注册表）。

---

## 测试

### 后端
- `backend/tests/test_indicator_registry.py`（或并入现有逻辑单测风格，以仓库实际布局为准）：
  - 未知函数拒绝
  - 非白名单字段拒绝
  - 负窗口 / 非整数窗口拒绝
  - 两参) 之外参数数量拒绝
  - `MA_CLOSE_20` Hot-JIT 挂载后 `_visit(ast.Name)` 快路径命中
  - `FIELDS` 与 `security.py` 现 fields 键集一致（防 drift）
- 回归：现有合法公式在重构后结果不变

### 前端（`frontend/tests/select-nl.test.mjs`，node --test 风格与现有测试一致）
- `buildPrompt` 输出含 nl-meta 字段/单位
- `parseSelectNLText` 幂等（纯文本、围栏包裹、非法 JSON → 明确报错）
- `validateFormula` 通过/拒绝用例（合法公式、未知函数、未知字段、负窗口、错 timeframe）
- 单位换算规则（亿→1e8、万→1e4）在提示词中正确注入
- 限流逻辑（少于/等于/超过阈值）

---

## 部署与验收

1. 后端改动推 main 触发 3 节点部署（`deploy_backend.yml` 路径 `backend/**`）
2. Vercel env 配 `LLM_ENDPOINT`/`LLM_API_KEY`/`LLM_MODEL`（未配则 AI 入口禁用）
3. 手动验收：
   - 正常翻译：`市盈率低于20且总市值大于100亿` → 公式 `PE_TTM < 20 AND TOTAL_MV > 10000000000`（或 `1e10`）→ 预览可编辑 → 运行
   - 非法输入：中文无法翻译时给出错误提示
   - LLM 故障 / 限流：显式错误提示
   - 移动端弹窗可用
   - 指标扩展验收：注册表加一行 `DUMMY` 指标 → 后端重新部署 → nl-meta 出现 → select-nl 提示词/校验随之更新

## 扩展指南（新增指标）

「开发者 + 一行」：
1. `indicator_registry.py` 的 `INDICATORS` 加一项（函数签名 `(column, n) -> pl.Expr`）
2. 可选：若用到新字段，同步 `FIELDS` 与 `security.py` fields 与 kline target_cols
3. 单测覆盖新指标的 Call 形态
4. 推 main 部署 → nl-meta / AST / Hot-JIT 三处自动派生

**禁止**：注册表加入无 `window` 语义的单参数指标而绕过参数校验器；在 `_require_whitelist_field` 之外放开任意字段。

## 非目标（YAGNI）

- 不做 Agentic 多轮/函数调用检索式翻译（方案 B）
- 不做候选公式多选（方案 C）
- 不做前端硬编码字段常量（双重维护）
- 不做面向最终用户的指标配置 UI（方案 B）