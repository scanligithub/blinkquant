# BlinkQuant 全局视觉升级设计文档

**日期：** 2026-08-11
**范围：** 公共层（Tailwind 配置 / globals.css / layout.tsx）
**方案：** A. 经典终端升级

## 1. 目标

- 建立可复用的设计 token（颜色 / 圆角 / 阴影 / 字号 / 间距 / 字体）
- 支持浅色 / 深色双主题，跟随系统偏好 + 手动切换（持久化到 localStorage）
- 保留 A 股习惯（红涨绿跌），主色统一为电蓝
- 优化全局排版：Inter（UI）+ JetBrains Mono（数据）
- 收紧圆角与阴影语言，从"卡片 SaaS"转为"金融终端"
- 只动公共层：Tailwind 配置 / globals.css / layout.tsx。零页面逻辑改动

## 2. 非目标（本次不做）

- 不重写任何页面或组件的视觉
- 不引入组件库（shadcn / Radix 等）
- 不调整 KLineChart、Watchlist、StrategyList 内部布局
- 不修改后端、API、数据库

## 3. 设计 Token

### 3.1 颜色（CSS 变量 + Tailwind theme.extend）

`:root`（浅色）：

```css
:root {
  --bg:        248 250 252;   /* slate-50  */
  --panel:     255 255 255;   /* white     */
  --panel-2:   241 245 249;   /* slate-100 */
  --line:      226 232 240;   /* slate-200 */
  --line-2:    203 213 225;   /* slate-300 */
  --ink:        15  23  42;   /* slate-900 */
  --ink-2:      51  65  85;   /* slate-700 */
  --muted:     100 116 139;   /* slate-500 */
  --brand:      37  99 235;   /* blue-600  */
  --brand-2:    29  78 216;   /* blue-700  */
  --brand-3:    96 165 250;   /* blue-400  */
  --up:        220  38  38;   /* red-600   */
  --up-2:      239  68  68;   /* red-500   */
  --down:       22 163  74;   /* green-600 */
  --down-2:     34 197  94;   /* green-500 */
  --warn:      217 119   6;   /* amber-600 */
  --danger:    220  38  38;   /* red-600   */
}
```

`.dark`（深色）：

```css
.dark {
  --bg:         11  18  32;   /* #0b1220 蓝黑 */
  --panel:      17  26  46;   /* #111a2e     */
  --panel-2:    26  37  60;   /* #1a253c     */
  --line:       31  42  68;   /* #1f2a44     */
  --line-2:     51  65 102;   /* #334166     */
  --ink:       230 237 247;   /* #e6edf7     */
  --ink-2:     203 213 225;   /* #cbd5e1     */
  --muted:     148 163 184;   /* #94a3b8     */
  --brand:      96 165 250;   /* blue-400    */
  --brand-2:    59 130 246;   /* blue-500    */
  --brand-3:   147 197 253;   /* blue-300    */
  --up:        239  68  68;   /* red-500     */
  --up-2:      248 113 113;   /* red-400     */
  --down:       34 197  94;   /* green-500   */
  --down-2:    74 222 128;   /* green-400   */
  --warn:      251 191  36;   /* amber-400   */
  --danger:    248 113 113;   /* red-400     */
}
```

### 3.2 Tailwind 配置

```js
const cssVar = (name) => `rgb(var(--${name}) / <alpha-value>)`;
module.exports = {
  content: [
    "./src/app/**/*.{js,ts,jsx,tsx,mdx}",
    "./src/components/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        bg: cssVar('bg'),
        panel: cssVar('panel'),
        'panel-2': cssVar('panel-2'),
        line: cssVar('line'),
        'line-2': cssVar('line-2'),
        ink: cssVar('ink'),
        'ink-2': cssVar('ink-2'),
        muted: cssVar('muted'),
        brand: cssVar('brand'),
        'brand-2': cssVar('brand-2'),
        'brand-3': cssVar('brand-3'),
        up: cssVar('up'),
        'up-2': cssVar('up-2'),
        down: cssVar('down'),
        'down-2': cssVar('down-2'),
        warn: cssVar('warn'),
        danger: cssVar('danger'),
      },
      borderRadius: {
        DEFAULT: '0.5rem',
        card: '0.625rem',
        chip: '0.375rem',
      },
      boxShadow: {
        panel: '0 1px 0 0 rgb(var(--line) / 1), 0 1px 2px 0 rgb(0 0 0 / 0.04)',
        pop: '0 8px 24px -8px rgb(0 0 0 / 0.18), 0 2px 4px -2px rgb(0 0 0 / 0.06)',
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'ui-monospace', 'SFMono-Regular', 'monospace'],
      },
      fontSize: {
        '2xs': ['0.6875rem', { lineHeight: '1rem' }],
      },
    },
  },
  plugins: [],
};
```

### 3.3 圆角 / 阴影

- 卡片：`rounded-card`（10px），`shadow-panel`
- 弹窗：`rounded-card`，`shadow-pop`
- 按钮 / 输入：`rounded-chip`（6px）
- Pill / Tag：`rounded-full`（保留）

## 4. 主题切换机制

### 4.1 layout.tsx（启动脚本 + 字体）

```tsx
import type { Metadata } from 'next'
import { Inter, JetBrains_Mono } from 'next/font/google'
import './globals.css'
import { IdleTimeoutProvider } from '@/components/IdleTimeoutProvider'

const inter = Inter({ subsets: ['latin'], display: 'swap', variable: '--font-inter' })
const mono = JetBrains_Mono({ subsets: ['latin'], display: 'swap', variable: '--font-mono' })

export const metadata: Metadata = {
  title: 'BlinkQuant · 分布式量化交易',
  description: 'Distributed Quant System',
}

const themeScript = `
(function(){try{
  var s=localStorage.getItem('bq-theme');
  var m=window.matchMedia('(prefers-color-scheme: dark)').matches;
  var t=s||(m?'dark':'light');
  document.documentElement.classList.add(t);
  document.documentElement.setAttribute('data-theme',t);
}catch(e){}})();
`

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="zh-CN" className={`${inter.variable} ${mono.variable}`}>
      <head><script dangerouslySetInnerHTML={{ __html: themeScript }} /></head>
      <body className="font-sans bg-bg text-ink antialiased">
        <IdleTimeoutProvider>{children}</IdleTimeoutProvider>
      </body>
    </html>
  )
}
```

### 4.2 切换 API（新增 `frontend/src/lib/theme.ts`）

```ts
export type Theme = 'light' | 'dark';

export function setTheme(t: Theme) {
  document.documentElement.classList.remove('light', 'dark');
  document.documentElement.classList.add(t);
  document.documentElement.setAttribute('data-theme', t);
  localStorage.setItem('bq-theme', t);
}

export function getTheme(): Theme {
  if (typeof document === 'undefined') return 'light';
  return document.documentElement.classList.contains('dark') ? 'dark' : 'light';
}

export function toggleTheme() {
  setTheme(getTheme() === 'light' ? 'dark' : 'light');
}
```

## 5. globals.css

```css
@tailwind base;
@tailwind components;
@tailwind utilities;

:root {
  --bg:        248 250 252;
  --panel:     255 255 255;
  --panel-2:   241 245 249;
  --line:      226 232 240;
  --line-2:    203 213 225;
  --ink:        15  23  42;
  --ink-2:      51  65  85;
  --muted:     100 116 139;
  --brand:      37  99 235;
  --brand-2:    29  78 216;
  --brand-3:    96 165 250;
  --up:        220  38  38;
  --up-2:      239  68  68;
  --down:       22 163  74;
  --down-2:     34 197  94;
  --warn:      217 119   6;
  --danger:    220  38  38;
}

.dark {
  --bg:         11  18  32;
  --panel:      17  26  46;
  --panel-2:    26  37  60;
  --line:       31  42  68;
  --line-2:     51  65 102;
  --ink:       230 237 247;
  --ink-2:     203 213 225;
  --muted:     148 163 184;
  --brand:      96 165 250;
  --brand-2:    59 130 246;
  --brand-3:   147 197 253;
  --up:        239  68  68;
  --up-2:      248 113 113;
  --down:       34 197  94;
  --down-2:    74 222 128;
  --warn:      251 191  36;
  --danger:    248 113 113;
}

html { color-scheme: light dark; }

body {
  @apply bg-bg text-ink;
  font-feature-settings: "tnum" 1, "ss01" 1;
}

::selection {
  background: rgb(var(--brand) / 0.25);
  color: rgb(var(--ink));
}

:focus-visible {
  outline: 2px solid rgb(var(--brand) / 0.7);
  outline-offset: 2px;
  border-radius: 6px;
}

.custom-scrollbar {
  scrollbar-width: thin;
  scrollbar-color: rgb(var(--line-2)) transparent;
}
.custom-scrollbar::-webkit-scrollbar { width: 6px; height: 6px; }
.custom-scrollbar::-webkit-scrollbar-track { background: transparent; }
.custom-scrollbar::-webkit-scrollbar-thumb {
  background: rgb(var(--line-2));
  border-radius: 3px;
}
.custom-scrollbar::-webkit-scrollbar-thumb:hover { background: rgb(var(--muted)); }

.font-mono, code, kbd, samp { font-variant-numeric: tabular-nums; }

.text-up   { color: rgb(var(--up)); }
.text-down { color: rgb(var(--down)); }
.bg-up     { background: rgb(var(--up)); }
.bg-down   { background: rgb(var(--down)); }

a { color: rgb(var(--brand)); }
a:hover { color: rgb(var(--brand-2)); }
```

## 6. 测试验证

### 6.1 构建验证

```bash
cd frontend && npm run build
```

### 6.2 现有测试

```bash
cd frontend && node --test tests/apply-adjust.test.mjs tests/auth.test.mjs tests/export.test.mjs tests/idle-timeout.test.mjs tests/invite.test.mjs tests/parquet.test.mjs tests/resample.test.mjs
```

### 6.3 视觉冒烟

- 刷新 `/`、`/login`、`/register`、`/admin`，首屏不闪烁主题
- 切换系统主题，页面跟随
- 浅色 / 深色下，选区色一致
- 任意输入框 Tab 后焦点环可见
- K 线图、Watchlist、StrategyList 视觉无回归

## 7. 风险与回滚

| 风险 | 影响 | 缓解 |
|---|---|---|
| `darkMode: 'class'` 与现有 `bg-slate-50` 冲突 | 极少（utility 不随主题变） | 公共层只新增，不替换 |
| `next/font/google` 构建期下载失败 | 国内偶发 | 提供 displayName 兜底 system-ui |
| 主题切换脚本 FOUC | 闪烁一瞬 | 脚本在 `<head>` 内同步执行 |
| 现有 `bg-slate-50` 字面值被覆盖 | body 颜色被改 token | 显式设 `bg-bg`；测试覆盖 |

回滚：3 个核心文件 + 1 个新增文件，可在任意 commit 一次性 revert。

## 8. 实施步骤

| # | 任务 | 文件 |
|---|---|---|
| 1 | 改写 tailwind.config.js | `frontend/tailwind.config.js` |
| 2 | 改写 globals.css | `frontend/src/app/globals.css` |
| 3 | 改写 layout.tsx | `frontend/src/app/layout.tsx` |
| 4 | 新增 theme.ts | `frontend/src/lib/theme.ts` |
| 5 | npm run build 验证 | — |
| 6 | 跑现有测试 | — |
| 7 | 视觉冒烟 | — |

## 9. 后续可扩展（本次不做）

- 把 `bg-slate-50` / `bg-white` / `text-slate-900` 等散点 utility 替换为 token
- 在 Header 加主题切换按钮（用 `lib/theme.ts`）
- KLineChart 接主题色（红绿随主题微调）
- 引入 `prefers-reduced-motion` 平滑过渡
