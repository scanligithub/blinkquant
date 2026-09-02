export type Theme = 'light' | 'dark';

export function setTheme(t: Theme) {
  document.documentElement.classList.remove('light', 'dark');
  document.documentElement.classList.add(t);
  document.documentElement.setAttribute('data-theme', t);
  localStorage.setItem('bq-theme', t);
}

export function getTheme(): Theme {
  if (typeof document === 'undefined') return 'light';
  if (document.documentElement.classList.contains('dark')) return 'dark';
  const stored = localStorage.getItem('bq-theme');
  if (stored === 'dark') return 'dark';
  return 'light';
}

export function toggleTheme() {
  setTheme(getTheme() === 'light' ? 'dark' : 'light');
}