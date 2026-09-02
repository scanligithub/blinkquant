# BlinkQuant 数据字典

版本: v1.0 | 更新: 2026-08-04

> 对应 Hugging Face Dataset: scanli/stocka-data
> 存储格式: Parquet + ZSTD 压缩，按年分片 (YYYY)
> 复权标准: 前复权 (QFQ)，adjustFactor 字段保留原始值

---

## stock_kline_{YYYY}.parquet

个股日线全维度数据 (前复权)

| 字段 | 类型 | 说明 |
|------|------|------|
| date | string | 交易日期 YYYY-MM-DD |
| code | string | 股票代码 (sh.600000) |
| open | float32 | 前复权开盘价 |
| high | float32 | 前复权最高价 |
| low | float32 | 前复权最低价 |
| close | float32 | 前复权收盘价 |
| volume | float64 | 成交量 (股) |
| amount | float64 | 成交额 (元) |
| turn | float32 | 换手率 (%) |
| pctChg | float32 | 涨跌幅 (%) 基于前复权价 |
| peTTM | float32 | 滚动市盈率 TTM |
| pbMRQ | float32 | 市净率 MRQ |
| adjustFactor | float32 | 复权因子 |
| isST | int8 | 是否 ST (0/1) |
| total_shares | float64 | 总股本 (股) |
| float_shares | float64 | 流通股本 (股) |
| total_mv | float64 | 总市值 (元) |
| float_mv | float64 | 流通市值 (元) |
| net_amount | float32 | 主力净流入 (万元) |
| main_net | float32 | 大单净流入 (万元) |
| super_net | float32 | 超大单净流入 (万元) |
| large_net | float32 | 大单净流入 (万元) |
| medium_net | float32 | 中单净流入 (万元) |
| small_net | float32 | 小单净流入 (万元) |

---

## stock_money_flow_{YYYY}.parquet

个股资金流向详细

| 字段 | 类型 | 说明 |
|------|------|------|
| date | string | 交易日期 |
| code | string | 股票代码 |
| net_amount | float32 | 主力净流入 (万元) |
| main_net | float32 | 大单净流入 (万元) |
| super_net | float32 | 超大单净流入 (万元) |
| large_net | float32 | 大单净流入 (万元) |
| medium_net | float32 | 中单净流入 (万元) |
| small_net | float32 | 小单净流入 (万元) |

---

## sector_kline_{YYYY}.parquet

板块 K 线 (行业/概念/地域)

| 字段 | 类型 | 说明 |
|------|------|------|
| date | string | 交易日期 |
| code | string | 板块代码 (如 BK1043) |
| name | string | 板块名称 |
| type | string | 类型: 行业/概念/地域 |
| open | float32 | 开盘价 |
| high | float32 | 最高价 |
| low | float32 | 最低价 |
| close | float32 | 收盘价 |
| volume | float64 | 成交量 |
| amount | float64 | 成交额 |
| amplitude | string | 振幅 (保留字符串原样) |

---

## sector_constituents_{YYYY}.parquet

板块成分股映射 (1-to-N)

| 字段 | 类型 | 说明 |
|------|------|------|
| date | string | 快照日期 |
| sector_code | string | 板块代码 |
| stock_code | string | 股票代码 (纯数字) |
| sector_name | string | 板块名称 |

---

## index_kline_{YYYY}.parquet

37 核心指数 + 行业指数

| 字段 | 类型 | 说明 |
|------|------|------|
| date | string | 交易日期 |
| code | string | 指数代码 (sh.000001) |
| open | float32 | 开盘价 |
| high | float32 | 最高价 |
| low | float32 | 最低价 |
| close | float32 | 收盘价 |
| volume | float64 | 成交量 |
| amount | float64 | 成交额 |

---

## all_stocks_f10_raw.parquet

东方财富 F10 原始财务 (37 字段)

| 字段 | 类型 | 说明 |
|------|------|------|
| code | string | 股票代码 |
| name | string | 股票名称 |
| report_date | string | 报告期 |
| notice_date | string | 公告日期 |
| basic_eps | float64 | 基本每股收益 |
| deduct_basic_eps | float64 | 扣非每股收益 |
| total_operate_income | float64 | 营业总收入 |
| parent_netprofit | float64 | 归母净利润 |
| weightavg_roe | float64 | 加权 ROE |
| bps | float64 | 每股净资产 |
| ttm_net_profit | float64 | TTM 净利润 (合并时计算) |

---

## all_stocks_mainbus_raw.parquet

主营业务构成 (雪球)

| 字段 | 类型 | 说明 |
|------|------|------|
| code | string | 股票代码 |
| name | string | 股票名称 |
| report_date | string | 报告期 |
| item_type | int8 | 1=行业 2=产品 |
| item_name | string | 行业/产品名称 |
| income | float64 | 收入 (元) |
| income_ratio | float64 | 收入占比 (%) |
| gross_margin | float64 | 毛利率 (%) |

---

## event_earnings_forecast.parquet

业绩预告事件

| 字段 | 类型 | 说明 |
|------|------|------|
| code | string | 股票代码 |
| notice_date | string | 预告日期 |
| report_date | string | 报告期 |
| forecast_type | string | 预增/预减/扭亏/首亏/续亏/略增/略减 |
| forecast_yoy_mid | float64 | 同比增减中值 (%) |
| summary | string | 预告摘要 |

---

## stock_list.parquet

全市场股票列表

| 字段 | 类型 | 说明 |
|------|------|------|
| code | string | 标准代码 (sh./sz./bj.) |
| name | string | 股票名称 |
| tradeStatus | string | 交易状态 |

---

## sector_list.parquet

板块目录

| 字段 | 类型 | 说明 |
|------|------|------|
| code | string | 板块代码 |
| name | string | 板块名称 |
| type | string | 行业/概念/地域 |

---

## index_list.parquet

37 核心指数列表

| 字段 | 类型 | 说明 |
|------|------|------|
| code | string | 指数代码 |
| name | string | 指数名称 |
