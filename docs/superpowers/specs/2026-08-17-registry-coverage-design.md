# 注册表全覆盖测试设计

日期: 2026-08-17
状态: 已批准

## 背景

`frontend/scripts/nl-test.mjs` 现有 23 条用例（字段 9/18、算子 13/21），对 NLP 选股的横向覆盖不足。
本设计将覆盖提升到「注册表全覆盖」：运行前拉取 `nl-meta`，自动生成覆盖全部字段与算子的用例并执行，输出覆盖矩阵。

## 目标

- 每个 `nl-meta.fields` 字段至少一条用例，断言公式含该字段 token。
- 每个 `nl-meta.indicators` 算子至少一条用例，断言公式含该算子 token。
- 运行结束打印覆盖矩阵（字段 N/18、算子 N/21、未覆盖项）。
- 新增注册表项但无生成器时，脚本标记该字段/算子「未覆盖」。

## 非目标

- 不做签名形态验证（仅 token 存在性断言）。
- 不改变现有 23 条手工用例。
- 不修改后端/引擎。

## 方案

### 1. 用例生成（描述驱动，半自动）

在 `nl-test.mjs` 新增两张生成器表，以 `nl-meta.fields` / `nl-meta.indicators` 为输入：

```js
const FIELD_GEN = {
  CLOSE: { q: '收盘价大于10元的股票', sub: ['CLOSE'] },
  VOL:   { q: '成交量大于100万股的股票', sub: ['VOL'] },
  // ...
};
const IND_GEN = {
  ABS:    { q: '收盘价距离均线的绝对偏差大于2', sub: ['ABS'] },
  ATR:    { q: '14日真实波幅均值大于3', sub: ['ATR'] },
  BOLL_LOWER: { q: '收盘价跌破布林下轨的股票', sub: ['BOLL_LOWER'] },
  EMA:    { q: '5日指数均线高于20日均线的股票', sub: ['EMA'] },
  ROC:    { q: '5日变动率大于5%的股票', sub: ['ROC'] },
  STD:    { q: '20日标准差大于2的股票', sub: ['STD'] },
  SUM:    { q: '5日成交额之和大于100亿的股票', sub: ['SUM'] },
  BARSLAST: { q: '距上次突破20日均线不超过3天的股票', sub: ['BARSLAST'] },
  // ...
};
```

- 生成器查询是人工给每个算子写的最佳口语（描述决定语义口径，查询我写），不机械拼描述。
- 已有手工用例先跑，生成的用例追加执行。
- 注册表项缺生成器 → 计为未覆盖，在矩阵中列出。

### 2. 覆盖矩阵

执行结束后打印：

```
--- 覆盖矩阵 ---
字段: 18/18 (CLOSE ✓ VOL ✓ …)
算子: 21/21 (MA ✓ ABS ✓ …)
未覆盖: 无
```

矩阵基于「已执行用例断言子串」反推（从每个成功用例的 formula 提取 token），与注册表比对。

### 3. 断言

沿用现有 `sub`/`sub_any` token 存在性断言。

### 4. 已知语义待确认字段

以下字段口语罕见，生成器里标注 `note`：
`S_CLOSE`、`IS_FORECAST_GOOD`、`IS_FORECAST_BAD`、`FORECAST_YOY`、`TOTAL_SHARES`、`FLOAT_SHARES`、`OPEN`。

## 验收

- `node scripts/nl-test.mjs <url> <email> <pass>` 通过且矩阵显示 18/18、21/21。
- 新增注册表项（无生成器）时矩阵出现未覆盖项。

## 测试

- 生成器与矩阵逻辑用纯函数封装，`frontend/tests/select-nl.test.mjs` 镜像复制 + 单测。
- 全量回归：现有用例保持通过。
