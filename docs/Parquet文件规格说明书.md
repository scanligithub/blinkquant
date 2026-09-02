# A股量化基础数据库成果文件规格说明书
**Document Specification: A-Share Quantitative Base Dataset Output Specification**
---
## 1. 概述 (Overview)
本规格说明书定义了 `stockA` 项目由 GitHub Actions 自动化流水线每日/全量构建生成的量化数据集成果文件规范。 数据集旨在为 Qlib、DuckDB、Polars 及深度学习（GNN/Transformer）量化回测系统提供**高吞吐、零未来函数（Look-ahead Bias Free）、极致压缩（Parquet ZSTD）**的基本面、行情与**全量自愈板块图谱**数据支持。

### 1.1 上游板块自愈数据源说明
针对东方财富等主流数据源历史板块 K 线存在"断流"、"成分股漂移"及"历史缺口"等问题，本工程集成了上游开源自动化自愈引擎 [em-sector-auto-healer](https://github.com/scanligithub/em-sector-auto-healer) 的产物 `Full_Sector_Klines.zip`。 通过 `scripts/import_healed_sectors.py` 脚本，解包并解析其 `metadata/` 目录下的三大维度板块定义（`industries.json`、`concepts.json`、`regions.json`）与成分股映射文件（`components.json`），配合全历史 `90.BK*.json` K 线文件进行强类型对齐与清洗，融合输出为标准 Parquet 格式。
* **存储格式**：Apache Parquet（采用 ZSTD 极致列式压缩）
* **分区策略**：核心时序数据按**年份（`YYYY`）**物理切片；静态/元数据独立落盘。
* **编码标准**：UTF-8
* **复权说明**：个股行情价格列（`open`, `high`, `low`, `close`）为**不复权原始价格**；同步提供后复权因子 `adjustFactor` 供下游自由计算前/后复权。

---
## 2. 数据产物目录结构 (Directory Structure)
产物统一落盘于仓库 `output/` 目录下：
```text
output/
├── 📄 元数据与数据字典 (Metadata)
│   ├── stock_list.parquet           # 全市场 A 股个股主表
│   ├── sector_list.parquet         # 全量自愈板块（行业/概念/地域）元数据主表
│   └── index_list.parquet          # 37 只核心大盘与风格指数主表
│   ├── 📈 交易日行情与因子宽表 (Daily Market & Factors)
│   ├── stock_kline_{YYYY}.parquet  # A 股个股日线全维度宽表（含估值、产业链与事件因子）
│   ├── stock_money_flow_{YYYY}.parquet # A 股个股主力资金流向日线
│   ├── sector_kline_{YYYY}.parquet     # 融合自愈引擎的全历史板块 K 线宽表
│   ├── sector_constituents_{YYYY}.parquet # 自愈板块最新成分股关系映射表
│   └── index_kline_{YYYY}.parquet     # 37 只大盘与行业风格指数日线
│   ├── ❄️ 基本面与事件源数据表 (Raw Fundamental & Events)
│   ├── all_stocks_f10_raw.parquet      # 东财 37 字段全量季度财务历史主表
│   ├── all_stocks_mainbus_raw.parquet  # 雪球全量历史主营业务与产品构成明细
│   └── event_earnings_forecast.parquet # Baostock 业绩预告与同比增速事件库
│   └── 📊 质检与审计报告 (Quality Control Reports)
│       ├── qc_report.json             # 机器可读 JSON 深度质检报告
│       ├── qc_summary.md              # 综合数据质量与自愈审计报告
│       ├── f10_audit_summary.md       # 财报披露时滞与覆盖度深度审计
│       ├── mainbus_audit_summary.md   # 产业链产品分类与集中度审计
│       └── event_audit_summary.md     # 业绩预告事件库质量诊断报告
```

---
## 3. 核心文件 Schema 与字段规格说明
### 3.1 A 股个股日线全维度宽表 (`stock_kline_{YYYY}.parquet`)
* **描述**：包含全市场个股每日行情、股本市值、通过 ASOF JOIN 无未来函数注入的 TTM 估值因子、雪球产品暴露度及业绩预告事件因子。
* **排序**：`code` 升序, `date` 升序

| 字段名 (Field) | 数据类型 (Type) | 物理单位/格式 | 说明与计算逻辑 (Description) |
| :--- | :--- | :--- | :--- |
| `date` | Utf8 (String) | YYYY-MM-DD | 交易日期 |
| `code` | Utf8 (String) | sh.600000 / sz.000001 | 带市场前缀的标准股票代码 |
| `open` | Float32 | 元 | 当日开盘价（未复权） |
| `high` | Float32 | 元 | 当日最高价（未复权） |
| `low` | Float32 | 元 | 当日最低价（未复权） |
| `close` | Float32 | 元 | 当日收盘价（未复权） |
| `amount` | Float64 | 元 | 当日成交额 |
| `volume` | Float64 | 手 (100股) | 当日成交量 |
| `turn` | Float32 | % | 当日换手率 (`volume * 10000 / float_shares`) |
| `pctChg` | Float32 | % | 当日涨跌幅 (`(close - prev_close) / prev_close * 100`) |
| `peTTM` | Float32 | 无维度 | 滚动市盈率（结合 ASOF 财报公告日与总市值动态计算，亏损或数据缺失置为 `0.0`） |
| `pbMRQ` | Float32 | 无维度 | 市净率 (`close / BPS`，基于最新已披露季报 BPS 计算，缺失置为 `0.0`） |
| `adjustFactor` | Float32 | 无维度 | 累乘后复权因子（起始为 1.0，根据 GBBQ 除权息事件正序累乘） |
| `isST` | Int8 | 0 / 1 | 当日是否为 ST / *ST 股票 |
| `total_shares` | Float64 | 股 | 当日总股本（根据 GBBQ 股本变迁动态对齐） |
| `float_shares` | Float64 | 股 | 当日流通股本 |
| `total_mv` | Float64 | 元 | 当日总市值 (`close * total_shares`) |
| `float_mv` | Float64 | 元 | 当日流通市值 (`close * float_shares`) |
| `product_ratios` | Utf8 (String) | 文本 | **无未来函数产业链敞口**（基于 ASOF JOIN 对齐的最新季报产品明细，格式：`产品A:80.5\|产品B:15.2`） |
| `forecast_type` | Utf8 (String) | 预增/略增/首亏等 | **业绩预告事件分类**（基于 ASOF JOIN 对齐当日最新公告，无事件填 `无`） |
| `forecast_yoy` | Float64 | % | 业绩预告归母净利润同比增速中轴 (`(yoy_min + yoy_max) / 2`) |
| `is_forecast_good`| Int32 | 0 / 1 | 业绩正面预告 One-Hot 标记 (预增、略增、扭亏) |
| `is_forecast_bad` | Int32 | 0 / 1 | 业绩负面预告 One-Hot 标记 (预减、略减、首亏、增亏) |

---
### 3.2 A 股主力资金流向日线 (`stock_money_flow_{YYYY}.parquet`)
* **描述**：新浪财经全市场资金分档净流入数据。
* **排序**：`code` 升序, `date` 升序

| 字段名 (Field) | 数据类型 (Type) | 物理单位 | 说明 (Description) |
| :--- | :--- | :--- | :--- |
| `date` | Utf8 | YYYY-MM-DD | 交易日期 |
| `code` | Utf8 | sh.600000 | 股票代码 |
| `net_amount` | Float32 | 万元 | 净流入总额 |
| `main_net` | Float32 | 万元 | 主力净流入总额 (`super_net + large_net`) |
| `super_net` | Float32 | 万元 | 超大单净流入 |
| `large_net` | Float32 | 万元 | 大单净流入 |
| `medium_net` | Float32 | 万元 | 中单净流入 |
| `small_net` | Float32 | 万元 | 小单净流入 |

---
### 3.3 上游板块自愈源数据 JSON 规范 (`Full_Sector_Klines.zip/metadata/`)
项目在编译合成板块 Parquet 产物之前，首先需要解析上游自愈引擎导出的 4 个核心 JSON 配置文件：
#### 3.3.1 三大板块维度元数据 (`industries.json` / `concepts.json` / `regions.json`)
* **路径**：`Full_Sector_Klines.zip` 内的 `metadata/industries.json` (行业)、`concepts.json` (概念)、`regions.json` (地域)
* **结构**：JSON 数组，包含对应维度下的全量板块定义。
```json
[
  {
    "sid": "90.BK1043",
    "name": "半导体"
  },
  {
    "sid": "90.BK0427",
    "name": "白酒"
  }
]
```

| 键名 (Key) | 数据类型 | 范例 | 解析与映射规则 (Processing Rules) |
| :--- | :--- | :--- | :--- |
| `sid` | String | `"90.BK1043"` | 官方板块带前缀唯一标识。解析时截取 `.` 之后的内容生成纯编码 `BK1043` |
| `name` | String | `"半导体"` | 板块官方中文名称 |
| *(来源文件)* | 文件名 | `industries.json` | 来源文件名决定板块最终分类：<br>• `industries.json` → **行业板块**<br>• `concepts.json` → **概念板块**<br>• `regions.json` → **地域板块** |

#### 3.3.2 板块成分股映射元数据 (`components.json`)
* **路径**：`Full_Sector_Klines.zip` 内的 `metadata/components.json`
* **结构**：JSON 数组，包含全量板块与个股的最新对应关系。
```json
[
  {
    "sector_id": "90.BK1043",
    "stock_id": "SH600000"
  },
  {
    "sector_id": "90.BK1043",
    "stock_id": "SZ000001"
  }
]
```

| 键名 (Key) | 数据类型 | 范例 | 解析与映射规则 (Processing Rules) |
| :--- | :--- | :--- | :--- |
| `sector_id` | String | `"90.BK1043"` | 截取后 6 位/`.` 之后内容，提取为 `sector_code` (`BK1043`) |
| `stock_id` | String | `"SH600000"` | 截取末尾 6 位数字，提取为纯数字股票代码 `stock_code` (`600000`) |

---
### 3.4 板块与指数最终成果产物规格
#### 3.4.1 板块元数据主表 (`sector_list.parquet`)
* **描述**：合并 `industries.json`、`concepts.json` 与 `regions.json` 解析结果后生成的全量板块字典。
* **排序**：`type` 升序, `code` 升序

| 字段名 (Field) | 数据类型 | 范例 | 说明 (Description) |
| :--- | :--- | :--- | :--- |
| `code` | Utf8 | `BK1043` | 官方板块唯一编码（已去除 `90.` 前缀） |
| `name` | Utf8 | `半导体` | 板块中文官方名称 |
| `type` | Utf8 | `行业板块` / `概念板块` / `地域板块` | 板块归属官方维度分类 |

#### 3.4.2 自愈板块日线 K 线表 (`sector_kline_{YYYY}.parquet`)
* **描述**：基于自愈包中全量 `90.BK*.json` 文件转化而成，包含历史补全与修复后的板块日线数据，完全兼容 `stockA` 历史 Schema。
* **排序**：`code` 升序, `date` 升序

| 字段名 (Field) | 数据类型 | 物理单位 | 说明与清洗逻辑 (Description) |
| :--- | :--- | :--- | :--- |
| `date` | Utf8 | YYYY-MM-DD | 交易日期 |
| `code` | Utf8 | `BK1043` | 板块代码 |
| `name` | Utf8 | `半导体` | 板块中文名称 |
| `type` | Utf8 | `行业板块` | 板块官方维度分类（行业/概念/地域） |
| `open` | Float32 | 点数 | 当日开盘指数 |
| `high` | Float32 | 点数 | 当日最高指数 |
| `low` | Float32 | 点数 | 当日最低指数 |
| `close` | Float32 | 点数 | 当日收盘指数 |
| `volume` | Float64 | 股 | 板块总成交量 |
| `amount` | Float64 | 元 | 板块总成交额 |
| `amplitude` | Utf8 | % | 振幅（保留原始字符串形式，确保格式向下兼容） |

#### 3.4.3 自愈板块成分股映射表 (`sector_constituents_{YYYY}.parquet`)
* **描述**：解析自愈包 `metadata/components.json` 得到的最新板块与个股映射关系。
* **排序**：`sector_code` 升序, `stock_code` 升序

| 字段名 (Field) | 数据类型 | 范例 | 说明 (Description) |
| :--- | :--- | :--- | :--- |
| `sector_code` | Utf8 | `BK1043` | 板块代码（去前缀） |
| `stock_code` | Utf8 | `600000` | 成分股 6 位纯数字代码（去 `SH/SZ` 前缀） |
| `sector_name` | Utf8 | `半导体` | 板块中文名称 |
| `date` | Utf8 | YYYY-MM-DD | 映射关系的建立/同步日期（当前运行日期） |

#### 3.4.4 核心指数元数据与 K 线 (`index_list.parquet` / `index_kline_{YYYY}.parquet`)
* **描述**：覆盖上证指数（sh.000001）、深证成指、创业板指、科创50、北证50、沪深300、中证500、中证1000、国证2000 等 37 只稳健核心大盘与风格指数。
* **`index_kline_{YYYY}.parquet` 字段**：`date`, `code`, `open`, `high`, `low`, `close`, `volume`, `amount`, `pctChg`

---
### 3.5 东财全量 37 列财务季度历史表 (`all_stocks_f10_raw.parquet`)
* **描述**：东方财富 DataCenter 抽取的所有 A 股历史全量财报，追溯至 1995 年至今。
* **关键特征**：同时包含 `report_date`（报告期）与 `notice_date`（物理公告披露日），是消除量化回测"未来函数"的关键基石。

| 字段名 (Field) | 数据类型 | 说明 (Description) |
| :--- | :--- | :--- |
| `code` | Utf8 | 纯数字股票代码 (如 `600519`) |
| `name` | Utf8 | 股票名称 |
| `report_date` | Utf8 | 财报报告期截止日 (如 `2023-12-31`) |
| `notice_date` | Utf8 | **真实物理公告披露日 (如 `2024-04-03`)** |
| `basic_eps` | Float64 | 基本每股收益 (元) |
| `deduct_basic_eps` | Float64 | 扣除非经常性损益后每股收益 (元) |
| `total_operate_income`| Float64 | 营业总收入 (元) |
| `parent_netprofit` | Float64 | 归属于母公司所有者的净利润 (元) |
| `weightavg_roe` | Float64 | 加权平均净资产收益率 (%) |
| `ystz` | Float64 | 营业总收入同比增长率 (%) |
| `sjltz` | Float64 | 归母净利润同比增长率 (%) |
| `bps` | Float64 | 每股净资产 (元) |
| `mgjyxjje` | Float64 | 每股经营活动产生的现金流量净额 (元) |
| `xsmll` | Float64 | 销售毛利率 (%) |
| `assigndscrpt` | Utf8 | 分红送配方案描述 (如 `10派308元(含税)`) |
| `board_name` | Utf8 | 所属板块名称 |
| *(其余 21 个字段)* | *(各类型)* | 包含 `publishname`, `zxgxl`, `org_code`, `eitime` 等全量元数据 |

---
### 3.6 雪球主营业务与产品明细表 (`all_stocks_mainbus_raw.parquet`)
* **描述**：雪球网抓取的全市场个股历史主营构成与产品级收入占比。

| 字段名 (Field) | 数据类型 | 说明 (Description) |
| :--- | :--- | :--- |
| `code` | Utf8 | 纯数字股票代码 |
| `name` | Utf8 | 股票简称 |
| `report_date` | Utf8 | 报告期 (`YYYY-MM-DD`) |
| `report_name` | Utf8 | 报告期名称 (如 `2023年报`) |
| `item_type` | Int32 | 明细粒度：**`1` = 按行业分类；`2` = 按产品分类** |
| `item_name` | Utf8 | 业务/产品/行业明细名称 (如 `液冷服务器`) |
| `income` | Float64 | 该项主营业务收入 (元) |
| `income_ratio` | Float64 | 该项收入占主营业务总收入比例 (%) |
| `gross_margin` | Float64 | 该项业务毛利率 (%) |

---
### 3.7 业绩预告事件表 (`event_earnings_forecast.parquet`)
* **描述**：Baostock 采集的上市公司业绩预告发布事件库，追溯至 2005 年。

| 字段名 (Field) | 数据类型 | 说明 (Description) |
| :--- | :--- | :--- |
| `code` | Utf8 | 纯数字股票代码 |
| `notice_date` | Utf8 | 业绩预告物理公告日 |
| `report_date` | Utf8 | 所预测的财务报告期 |
| `forecast_type` | Utf8 | 预告类型：`预增` / `略增` / `扭亏` / `预减` / `略减` / `首亏` / `增亏` / `续亏` |
| `forecast_yoy_mid`| Float64 | 预计归母净利润同比增长率中轴 (%) |
| `summary` | Utf8 | 业绩预告摘要文本 |

---
## 4. 核心衍生因子的代数计算与对齐标准
### 4.1 TTM 滚动归母净利润 (`ttm_net_profit`)
为了在任意交易日提取无未来函数的 `peTTM`，系统按照以下代数公式对季报归母净利润进行向量化时序平滑：
1. **第 4 季度 (年报 FY)**：
$$\text{TTM} = \text{ParentNetProfit}_{\text{Q4}}$$
2. **第 1~3 季度 (Q1/H1/Q3)**：
* **标准差分公式**（上年同期及 Q4 健全）：
$$\text{TTM} = \text{CumProfit}_{\text{Current}} + \text{FYProfit}_{\text{PrevYear}} - \text{CumProfit}_{\text{PrevYearSameQuarter}}$$
* **线性估算兜底公式**（当历史数据缺失）：
$$\text{TTM} = \text{CumProfit}_{\text{Current}} \times \frac{4}{\text{QuarterNum}}$$

### 4.2 无未来函数 ASOF 对齐规则
时序宽表中的基本面/事件特征**严禁以 `report_date` 直接对齐**。所有估值与基本面指标，在 DuckDB 中必须执行以下严格时间约束：
$$\text{TradeDate} \ge \text{NoticeDate}$$
即：在上市公司正式发布公告（`NoticeDate`）的**当日及后续交易日**，该笔财务数据与事件特征才对量化策略可见；在公告日之前，策略将继续沿用上一期已发布的财务数据。

### 4.3 板块数据自愈与导入清洗规范 (`import_healed_sectors.py`)
1. **编码标准化**：自愈包原始 ID 形如 `90.BK1043` 或 `SH600000`，解析时统一剔除 `90.` 或 `SH/SZ` 前缀，转化为标准 6 位字符串代码（如 `BK1043` 与 `600000`）。
2. **三维字典建立**：读取 `metadata/industries.json`（行业）、`concepts.json`（概念）、`regions.json`（地域），建立 `code -> type` 与 `code -> name` 映射哈希表；若单个板块未在集中找到定义，默认归类为 `概念板块`。
3. **架构去重**：通过 Polars `unique(subset=["date", "code"], keep="last")` 强制保证板块日线数据的全局唯一性与时序严格递增。

---
## 5. 数据质量与物理完整性指标 (Data QC Metrics)
所有生成的 Parquet 产物均需通过 `QualityControl` 模块的以下硬性门禁：
1. **零死锁与零空表**：个股、板块及指数行情表行数 $> 0$，文件物理大小 $> 0$。
2. **逻辑合理性**：
    * $\text{High} \ge \text{Low} > 0$
    * $\text{Volume} \ge 0$
3. **复权因子强校验（DuckDB 向量化熔断机制）**：
    * **物理边界**：`adjustFactor` 不得为 `<=0`、`NaN` 或 `Inf`。
    * **单调性**：正序 `adjustFactor` 必须满足单调非递减（`adjustFactor >= LAG(adjustFactor) - 0.0001`）。
    * **暴跳熔断**：单日 `adjustFactor` 跳变倍数必须在 $(0.2, 5.0)$ 闭区间内。
    * **除权收益一致性**：在除权日，根据复权因子计算的复权收益率与真实 `pctChg` 的偏差率不得超过 $1\%$。
4. **类型约束**：万亿级市值（`total_mv`）与大股本（`total_shares`）必须强制使用 `Float64` 双精度浮点数存储，防止数值溢出。
