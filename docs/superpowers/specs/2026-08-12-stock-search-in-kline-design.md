# 将搜索股票编辑框移入 K 线图视图

## 背景与目的

当前主页面中，确定 K 线图显示哪只股票有两种方法：

1. 策略公式 → 运行选股 → 选股结果列表 → 点选列表中的一支股票
2. 在顶部 section 的「搜索股票」编辑框输入股票名称/代码/拼音首字母，从下拉菜单选中

需求：将方法 2 的搜索股票编辑框从顶部 section 移到 K 线图视图（图表卡片头部工具栏），使其贴近图表工作流。

## 现状

- `src/app/page.tsx` 为单页工作台，共 894 行
- 顶部 section（530-596 行）含两个块：「搜索股票」「策略公式」
- 搜索逻辑（286-326 行）内联在 page.tsx：debounce 300ms、拼音首字母/代码/名称打分排序、取前 10
- 搜索结果下拉浮层在输入框正下方（562-571 行）
- 图表卡片 header（646-839 行）由 `{selectedStock && (...)}` 包裹，仅在选中股票后渲染，内含股票信息、板块标签、复权、自选、全屏、周期切换工具栏
- 图表区未选中时显示「选择股票查看图表」占位

## 需求明细

1. 将「搜索股票」编辑框从顶部 section 移入 K 线卡片头部工具栏
2. 顶部 section 只保留「策略公式 + 运行选股 + 保存策略」
3. 搜索框在图表头部**始终常驻**（无论是否已选中股票）
4. 搜索结果下拉浮层仍显示在输入框正下方
5. 移动端（<768px）工具栏内搜索框折叠为 🔍 图标按钮，点击弹出覆盖层搜索（`fixed inset-0`，顶部输入框 + 结果列表 + 关闭按钮），关闭后恢复
6. 方法 1（选股结果列表点选）及所有图表/工具栏功能保持不变

## 设计

### 布局变更

```
顶部 section
├─ 策略公式 + 运行选股 + 保存策略（保留）

K线卡片
├─ header（常驻，不再依赖 selectedStock）
│  ├─ 搜索框（含下方结果浮层）——md+ 内联，移动端显示 🔍 图标按钮
│  └─ selectedStock 存在时 → 股票信息 + 板块标签 + 指标 + 工具栏（不变）
│  └─ selectedStock 为空时 → header 仅搜索框
└─ 图表区（未选中时显示原「选择股票查看图表」占位）
```

### 代码组织

新增 `src/components/StockSearch.tsx`：

- 封装搜索框、结果下拉浮层、移动端覆盖层
- 将 debounce 搜索打分逻辑从 page.tsx（286-326 行）移入组件内部
- 内部管理 `searchQuery / searchResults / searchLoading`
- Props：
  - `stockList: Array<{ code: string; name: string }>`
  - `onSelect: (code: string) => void`
- 依赖 `getPinyinInitials`（`src/utils/pinyin`）、`cleanSearchInput`（`src/utils/cleanInput`），从 page.tsx 引入路径不变

page.tsx 变更：

- 顶部 section 删除「搜索股票」块
- 图表 header 由 `{selectedStock && (...)}` 改为常驻渲染，搜索框放最左侧；选中信息/工具栏仅 `selectedStock` 存在时渲染
- `viewStock` 作为 `onSelect` 传入
- 删除 page.tsx 内原搜索相关状态与逻辑（`searchQuery/searchResults/searchLoading`、286-326 行搜索 effect）

### 移动端覆盖层

- 触发：工具栏 🔍 图标按钮（`md:hidden`）
- 结构：`fixed inset-0 z-50 bg-black/40` → 顶部白色面板，含输入框 + 结果列表 + 关闭按钮
- 交互：输入框自动聚焦；点选结果调用 `onSelect` 并关闭；关闭按钮/点击遮罩关闭

### 不变项

- 选股结果列表、Watchlist、板块跳转、复权、全屏、周期切换、图表渲染
- 全屏时搜索框随 header 一起全屏显示（保留搜索能力）

## 验证

- `npm run lint` / `npm run typecheck`（以 frontend 目录现有脚本为准）
- 手动验证：
  - 桌面端：顶部无搜索框；图表 header 有搜索框；输入名称/代码/拼音首字母下拉正常；点选加载图表
  - 移动端：🔍 图标 → 覆盖层搜索 → 选股关闭并加载
  - 未选中股票时 header 显示搜索框，图表区显示占位
  - 全屏模式下搜索框仍可用
