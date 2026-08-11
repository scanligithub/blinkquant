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
