# K线图财务指标显示 — 设计文档

日期: 2026-08-11
状态: 已评审

## 目标

在个股 K 线图上展示财务指标：市盈率 (PE TTM)、总市值、流通市值、成交额、成交量、换手率。

## 现状

后端 `/api/v1/kline` 已在 Parquet 中返回全部所需字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| `peTTM` | float32 | 滚动市盈率 TTM |
| `total_mv` | float64 | 总市值 (元) |
| `float_mv` | float64 | 流通市值 (元) |
| `amount` | float64 | 成交额 (元) |
| `turn` | float32 | 换手率 (%) |
| `volume` | float64 | 成交量 (股) |

但前端在 `parseParquetRecords`、`applyAdjust`、`resampleData` 三处丢弃了除 OHLCV + `main_net` 外的所有字段。

后端零改动，纯前端实现。

## 数据链路贯穿

### 1. `frontend/src/utils/parquet.ts` — `parseParquetRecords`
新增透传字段：`amount`、`turn`、`peTTM`、`total_mv`、`float_mv`。

### 2. `frontend/src/utils/applyAdjust.ts` — `applyAdjust`
新增字段不随复权变化（成交额/换手率/PE/市值是真实值），在返回对象中原样保留。

### 3. `frontend/src/app/page.tsx` — `resampleData`（周/月线聚合）
- `amount`、`volume` 求和
- `turn`、`peTTM`、`total_mv`、`float_mv` 取该周期最后一根

## 顶部信息栏 (page.tsx 股票名旁)

仅个股 (`kind === 'stock'`) 显示最新一根的快照，跟随周/月线切换自动更新：

- 市盈率 PE(TTM)
- 总市值
- 流通市值
- 成交额
- 换手率

数值用亿/万单位格式化（复用 `formatMoney` 思路）。字段为 null/undefined 时显示 `--`。

## 悬浮框增强 (KLineChart.tsx tooltip)

在现有工具条（开盘/收盘/成交量/涨幅）追加每根 K 线对应值：

- 成交额 (formatMoney)
- 换手率
- 市盈率 PE(TTM)

板块视图这些字段为 null 时自动隐藏。

## 测试

沿用 `node --test` + 复制实现模式 (tests/*.test.mjs)：

- `tests/parquet.test.mjs`：新字段透传
- `tests/apply-adjust.test.mjs`：新增字段保真
- `tests/resample.test.mjs`（新增）：聚合逻辑（求和 / 取末根）

## 范围外

- 后端接口无改动
- 板块 K 线无财务字段，不做展示
- 不新增副图指标