# 设计文档：K 线图显示股票所属板块（可点击跳转板块 K 线）

## 背景
- 后端已在内存中加载板块数据：`df_mapping`（1-to-1，行业优先、概念兜底）、`df_sector_list`（板块元数据）、`df_sector_daily/weekly/monthly`（板块 K 线）。
- 现有 `/api/v1/stock-list` 只返回 `{code, name}`，前端缓存 24h。
- 需求：在 K 线图标题栏显示当前股票所属的**全部板块**（行业 + 概念 + 地域），点击标签可切换到该板块自己的 K 线，并提供「返回个股」按钮回到之前的股票。

## 数据流
| 环节 | 说明 |
|------|------|
| 板块标签数据 | 后端内存 `self.stock_sectors = {股票代码: [(sector_code, sector_name, type), ...]}`，按 `code+sector_code` 去重，来自 `sector_constituents` 关联 `sector_list` |
| 标签接口 | `GET /api/v1/stock-sectors?code=` → `{code, sectors:[{code,name,type}]}`，按需懒加载 |
| 板块 K 线 | `GET /api/v1/sector-kline?code=&timeframe=D` → Parquet 二进制（复用 `/kline` 响应模式） |
| 前端链路 | Edge 代理 `Promise.any`×3 节点 → `viewStock` 拉标签 → 标题栏渲染 chips → 点击调用 `viewSector` |

## 后端实现思路
1. **构建全量映射**：在 `_build_sector_mapping` 中，于 `mapped` 构建后、行业/概念过滤**之前**，取 `unique(subset=["code","sector_code"])` 的 `[code, sector_code, sector_name, type]`，聚合为 dict 存入 `self.stock_sectors`。代码规范化复用 `_normalize_code_expr`。
   - 内存占用小（~几十万行字符串 dict），不影响分片键、1-to-1 约束、前复权公式。
2. **`GET /stock-sectors`**：直接查 `self.stock_sectors.get(code, [])`，无板块时返回空数组。
3. **`GET /sector-kline`**：按 timeframe 从 `df_sector_daily` / `df_sector_weekly` / `df_sector_monthly`（用 `getattr` 兜底 None）过滤排序，列取 `date, code, open, high, low, close, volume, amount, name, type`（存在才取），写入内存 Parquet（ZSTD）返回。

## 前端实现思路
1. **Edge 代理**：
   - `frontend/src/app/api/stock-sectors/route.ts`：JSON，`requireAuth`，`Promise.any`×3 节点，仿 `stock-list`。
   - `frontend/src/app/api/sector-kline/route.ts`：octet-stream，仿 `kline`。
2. **解析工具**：新增 `frontend/src/utils/parquet.ts` 纯函数 `parseParquetRecords(records)`（Date→epoch 秒、`main_net`/`adjustFactor` 缺失兜底），`viewStock` / `viewSector` 复用。
3. **状态改造**（`page.tsx`）：
   - `selectedStock` 增加 `kind: 'stock' | 'sector'`；新增 `sectorDataCache`（板块原始日线）、`sectors`（当前股票标签）、`lastStockRef`（返回目标）。
   - `viewStock` 选股后懒请求 `/api/stock-sectors`，失败静默。
   - 标题栏股票名下渲染板块 chips（按 type 着色：行业 / 概念 / 地域），点击 → 记录 `lastStockRef` → `viewSector(code, name)`。
   - 板块视图：隐藏「加自选」「复权」按钮，显示板块名 + type 徽章 + 「返回个股」按钮（用 `resampleData(adjustedDaily, chartTimeframe)` 恢复，`dailyDataCache` 未被板块数据覆盖）。
   - 周期按钮按 `kind` 分支重采样：股票用 `adjustedDaily`，板块用 `sectorDataCache`。

## 边界处理
- `stock-sectors` 请求失败 → 静默忽略，不显示标签。
- `sector-kline` 404 → alert（同现有 kline 模式）。
- 股票无任何板块 → 标签区为空。
- `lastStockRef` 为空（如深链）→ 隐藏返回按钮。
- `sector_kline` 各分表未构建（`getattr` 返回 None）→ 503「Data not ready」。

## 测试计划
- 后端：`python -m py_compile` 改动文件；逻辑以手动/接口验证为主（沿用项目无 pytest 基础设施的现状）。
- 前端：新增 `frontend/tests/parquet.test.mjs`（复制实现规避 TS import，沿用 `apply-adjust.test.mjs` 模式），覆盖 Date 转换、main_net/adjustFactor 兜底；`node --test frontend/tests/*.mjs`。
- 类型检查：`npx tsc --noEmit`。
- 手动：选股 → 见行业/概念/地域标签 → 点击进板块 K 线 → 返回个股 → 切换周期/复权正常。

## 影响范围
- 后端：`backend/core/data_manager.py`、`backend/api/routes.py`。
- 前端：`frontend/src/app/page.tsx`、新增 2 个 route + 1 个 util + 1 个测试。
- `KLineChart.tsx` 零改动。

## 里程碑
1. 设计文档 + 实施计划并提交。
2. 后端映射与接口实现，`py_compile` 通过。
3. 前端代理路由 + 解析工具 + 单测。
4. `page.tsx` UI 集成。
5. `node --test`、`tsc --noEmit`、手动验证。
6. 提交 PR（无 CI，人工检查）。
