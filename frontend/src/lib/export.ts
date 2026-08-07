export function toCSV(headers: string[], rows: (string | number | null | undefined)[][]): string {
  const esc = (v: string | number | null | undefined): string => {
    if (v === null || v === undefined) return '';
    const s = String(v);
    return /[",\n\r]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  };
  const lines = [headers.map(esc).join(',')];
  for (const row of rows) lines.push(row.map(esc).join(','));
  return '\uFEFF' + lines.join('\r\n');
}

export function buildUserExport(user: any, watchlist: any[], strategies: any[]): object {
  return {
    exported_at: new Date().toISOString(),
    user: {
      id: user.id,
      email: user.email,
      role: user.role,
      status: user.status,
      created_at: user.created_at,
      last_login_at: user.last_login_at ?? null,
    },
    watchlist: watchlist.map((w) => ({ code: w.code, created_at: w.created_at })),
    strategies: strategies.map((s) => ({
      name: s.name, formula: s.formula, timeframe: s.timeframe,
      created_at: s.created_at, updated_at: s.updated_at,
    })),
  };
}

export function parseExportFilename(header: string | null, fallback: string): string {
  if (!header) return fallback;
  const star = /filename\*=UTF-8''([^;]+)/i.exec(header);
  if (star) {
    try { return decodeURIComponent(star[1]); } catch { /* ignore */ }
  }
  const plain = /filename="?([^";]+)"?/i.exec(header);
  if (plain) return plain[1];
  return fallback;
}

export function sanitizeFilename(email: string, ext: string): string {
  const date = new Date().toISOString().slice(0, 10);
  const name = email.replace(/[^a-z0-9]/gi, '_');
  return `${name}_${date}.${ext}`;
}

export function buildUserExports(
  users: any[],
  watchlistByUser: Map<string, any[]>,
  strategiesByUser: Map<string, any[]>,
): Array<{ filename: string; content: string }> {
  const out: Array<{ filename: string; content: string }> = [];
  for (const u of users) {
    const watchlist = watchlistByUser.get(u.id) || [];
    const strategies = strategiesByUser.get(u.id) || [];
    const content = JSON.stringify(buildUserExport(u, watchlist, strategies), null, 2);
    const filename = sanitizeFilename(u.email, 'json');
    out.push({ filename, content });
  }
  return out;
}
