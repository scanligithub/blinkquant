# BlinkQuant 全局视觉升级 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 建立设计 token 体系（CSS 变量 + Tailwind theme），支持浅/深双主题，引入 Inter + JetBrains Mono 字体，收紧圆角/阴影，零页面改动。

**Architecture:** 在 `tailwind.config.js` 里注册 CSS 变量为 Tailwind 色值；`globals.css` 顶部定义浅/深两套 CSS 变量；`layout.tsx` 通过 `next/font/google` 引入字体并注入主题启动脚本；`theme.ts` 提供运行时切换 API。

**Tech Stack:** Next.js 14, Tailwind CSS 3, next/font/google, CSS custom properties

---

## 文件结构

| 路径 | 操作 | 职责 |
|---|---|---|
| `frontend/tailwind.config.js` | 改写 | 暴露 CSS 变量为 Tailwind token；darkMode: 'class'；borderRadius / boxShadow / fontFamily / fontSize |
| `frontend/src/app/globals.css` | 改写 | 定义浅/深 CSS 变量；全局排版；滚动条；选区；焦点环；涨跌语义；链接默认色 |
| `frontend/src/app/layout.tsx` | 改写 | 引入 Inter + JetBrains Mono；注入主题启动脚本；body 切到 token 颜色；lang 改 zh-CN |
| `frontend/src/lib/theme.ts` | 新增 | setTheme / getTheme / toggleTheme API |

---

### Task 1: 改写 tailwind.config.js

**Files:**
- Modify: `frontend/tailwind.config.js`

- [ ] **Step 1: 完整替换 tailwind.config.js**

```js
/** @type {import('tailwindcss').Config} */
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
        sans: ['var(--font-inter)', 'Inter', 'system-ui', 'sans-serif'],
        mono: ['var(--font-mono)', 'JetBrains Mono', 'ui-monospace', 'SFMono-Regular', 'monospace'],
      },
      fontSize: {
        '2xs': ['0.6875rem', { lineHeight: '1rem' }],
      },
    },
  },
  plugins: [],
};
```

- [ ] **Step 2: 验证配置无语法错误**

Run: `cd frontend && node -e "require('./tailwind.config.js')"`
Expected: 无输出（无报错即成功）

- [ ] **Step 3: Commit**

```bash
cd frontend && git add tailwind.config.js && git commit -m "style: add design tokens via CSS variables in tailwind config"
```

---

### Task 2: 改写 globals.css

**Files:**
- Modify: `frontend/src/app/globals.css`

- [ ] **Step 1: 完整替换 globals.css**

```css
@tailwind base;
@tailwind components;
@tailwind utilities;

/* === 浅色 Token === */
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

/* === 深色 Token === */
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

/* === 全局 === */
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

/* === 滚动条 === */
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

/* === 数据等宽数字 === */
.font-mono, code, kbd, samp { font-variant-numeric: tabular-nums; }

/* === 涨跌语义 === */
.text-up   { color: rgb(var(--up)); }
.text-down { color: rgb(var(--down)); }
.bg-up     { background: rgb(var(--up)); }
.bg-down   { background: rgb(var(--down)); }

/* === 链接 === */
a { color: rgb(var(--brand)); }
a:hover { color: rgb(var(--brand-2)); }
```

- [ ] **Step 2: 验证 CSS 语法**

Run: `cd frontend && npx stylelint src/app/globals.css --allow-empty-input 2>&1 || echo "stylelint not installed, skipping"`
Expected: stylelint 通过或跳过（项目未装 stylelint）

- [ ] **Step 3: Commit**

```bash
cd frontend && git add src/app/globals.css && git commit -m "style: define light/dark CSS variables, selection, focus ring, scrollbar,涨跌语义"
```

---

### Task 3: 改写 layout.tsx

**Files:**
- Modify: `frontend/src/app/layout.tsx`

- [ ] **Step 1: 完整替换 layout.tsx**

```tsx
import type { Metadata } from 'next'
import { Inter, JetBrains_Mono } from 'next/font/google'
import './globals.css'
import { IdleTimeoutProvider } from '@/components/IdleTimeoutProvider'

const inter = Inter({
  subsets: ['latin'],
  display: 'swap',
  variable: '--font-inter',
})

const mono = JetBrains_Mono({
  subsets: ['latin'],
  display: 'swap',
  variable: '--font-mono',
})

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

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="zh-CN" className={`${inter.variable} ${mono.variable}`}>
      <head><script dangerouslySetInnerHTML={{ __html: themeScript }} /></head>
      <body className="font-sans bg-bg text-ink antialiased">
        <IdleTimeoutProvider>
          {children}
        </IdleTimeoutProvider>
      </body>
    </html>
  )
}
```

- [ ] **Step 2: 验证 TypeScript 编译**

Run: `cd frontend && npx tsc --noEmit --skipLibCheck 2>&1 | head -20`
Expected: 无报错

- [ ] **Step 3: Commit**

```bash
cd frontend && git add src/app/layout.tsx && git commit -m "style: add Inter+JetBrains Mono fonts, theme init script, token body classes"
```

---

### Task 4: 新增 theme.ts

**Files:**
- Create: `frontend/src/lib/theme.ts`

- [ ] **Step 1: 创建 theme.ts**

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

- [ ] **Step 2: 验证导出**

Run: `cd frontend && node -e "const t = require('./src/lib/theme.ts'); console.log(typeof t.getTheme)" 2>&1 || echo "TS file, skipping require"`
Expected: 跳过（TypeScript）

- [ ] **Step 3: Commit**

```bash
cd frontend && git add src/lib/theme.ts && git commit -m "feat: add theme.ts API for runtime light/dark switching"
```

---

### Task 5: 构建验证

**Files:**
- 无改动

- [ ] **Step 1: 运行 Next.js 构建**

Run: `cd frontend && npm run build`
Expected: 构建成功，无报错。可能的 warning（如 "Using external image URL"）不影响。

- [ ] **Step 2: 检查输出目录**

Run: `Get-ChildItem frontend\.next`
Expected: `.next/` 目录存在且包含构建产物

---

### Task 6: 现有测试验证

**Files:**
- 无改动

- [ ] **Step 1: 跑现有测试**

Run: `cd frontend && node --test tests/apply-adjust.test.mjs tests/auth.test.mjs tests/export.test.mjs tests/idle-timeout.test.mjs tests/invite.test.mjs tests/parquet.test.mjs tests/resample.test.mjs`
Expected: 全部 PASS（现有测试为纯函数，不依赖 CSS/Tailwind）

---

### Task 7: 视觉冒烟（人工）

- [ ] **Step 1: 浅色模式验证**
  - 浏览器打开 `/login`，确认页面加载无闪烁
  - 确认 body 背景为浅色（slate-50 等效）
  - Tab 键浏览输入框，确认焦点环可见（蓝色 outline）
  - 检查文字渲染（Inter 字体生效）

- [ ] **Step 2: 深色模式验证**
  - 浏览器 DevTools → Emulation → prefers-color-scheme: dark → 刷新
  - 确认背景为深蓝黑（#0b1220 等效）
  - 确认文字为浅色（#e6edf7 等效）
  - Tab 焦点环仍然可见

- [ ] **Step 3: 主题持久化验证**
  - 在 DevTools Console 执行 `toggleTheme()`（需要先 import）
  - 刷新页面，确认主题保持

- [ ] **Step 4: 主页面不回归**
  - 访问 `/`（需要登录态）
  - 确认 K 线图、Watchlist、StrategyList 视觉无异常
  - 现有 `bg-slate-50` / `bg-white` 等 utility 仍然生效（token 作为新增选项，不覆盖现有 utility）
