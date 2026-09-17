import { parquetReadObjects } from 'hyparquet';
import { compressors } from 'hyparquet-compressors';

export type ArtifactName = 'equity_curve' | 'trades' | 'positions_daily';

export const PREVIEW_PAGE_SIZE = 500;

export interface EquityPoint {
  date: string;
  equity: number;
  cash?: number;
  positions_value?: number;
}

export interface TradeRow {
  signal_date: string;
  execution_date: string;
  code: string;
  side: string;
  qty: number;
  price: number;
  fee: number;
}

export interface PositionRow {
  date: string;
  code: string;
  qty: number;
  cost: number;
  market_value: number;
}

export interface PageResult<T> {
  rows: T[];
  total: number;
  offset: number;
  limit: number;
}

function toIsoDate(v: unknown): string {
  if (v == null) return '';
  if (v instanceof Date) return v.toISOString().slice(0, 10);
  if (typeof v === 'number') {
    const d = new Date(v);
    if (!Number.isNaN(d.getTime()) && v > 1e11) return d.toISOString().slice(0, 10);
  }
  const s = String(v);
  return s.length >= 10 ? s.slice(0, 10) : s;
}

export async function fetchArtifactBuffer(
  taskId: number,
  name: ArtifactName
): Promise<ArrayBuffer> {
  const res = await fetch(
    `/api/v1/tasks/${taskId}/artifact?name=${encodeURIComponent(name)}`,
    { cache: 'no-store' }
  );
  if (res.status === 401) {
    window.location.href = '/login';
    throw new Error('Unauthorized');
  }
  if (!res.ok) throw new Error(`artifact ${name} failed: HTTP ${res.status}`);
  const buf = await res.arrayBuffer();
  if (!buf.byteLength) throw new Error(`artifact ${name} empty`);
  return buf;
}

export async function loadArtifactRows<T extends Record<string, unknown>>(
  taskId: number,
  name: ArtifactName
): Promise<T[]> {
  const buffer = await fetchArtifactBuffer(taskId, name);
  const records = await parquetReadObjects({ file: buffer, compressors });
  return (records || []) as T[];
}

export async function loadEquityCurve(taskId: number): Promise<EquityPoint[]> {
  const rows = await loadArtifactRows<Record<string, unknown>>(taskId, 'equity_curve');
  return rows.map((r) => ({
    date: toIsoDate(r.date),
    equity: Number(r.equity),
    cash: r.cash != null ? Number(r.cash) : undefined,
    positions_value:
      r.positions_value != null ? Number(r.positions_value) : undefined,
  }));
}

export async function loadTradesPage(
  taskId: number,
  opts: {
    limit?: number;
    offset?: number;
    code?: string;
    side?: string;
    dateFrom?: string;
    dateTo?: string;
  } = {}
): Promise<PageResult<TradeRow>> {
  const limit = opts.limit ?? PREVIEW_PAGE_SIZE;
  const offset = opts.offset ?? 0;
  const qs = new URLSearchParams({
    name: 'trades',
    fmt: 'json',
    limit: String(limit),
    offset: String(offset),
  });
  if (opts.code) qs.set('code', opts.code);
  if (opts.side) qs.set('side', opts.side);
  if (opts.dateFrom) qs.set('date_from', opts.dateFrom);
  if (opts.dateTo) qs.set('date_to', opts.dateTo);

  const res = await fetch(`/api/v1/tasks/${taskId}/artifact?${qs}`, {
    cache: 'no-store',
  });
  if (res.status === 401) {
    window.location.href = '/login';
    throw new Error('Unauthorized');
  }
  if (!res.ok) throw new Error(`trades page HTTP ${res.status}`);
  const data = await res.json();
  const rows: TradeRow[] = (data.rows || []).map((r: any) => ({
    signal_date: toIsoDate(r.signal_date),
    execution_date: toIsoDate(r.execution_date),
    code: String(r.code ?? ''),
    side: String(r.side ?? ''),
    qty: Number(r.qty),
    price: Number(r.price),
    fee: Number(r.fee ?? 0),
  }));
  return {
    rows,
    total: Number(data.total ?? rows.length),
    offset: Number(data.offset ?? offset),
    limit: Number(data.limit ?? limit),
  };
}

export async function loadPositionsPage(
  taskId: number,
  opts: {
    limit?: number;
    offset?: number;
    code?: string;
    dateFrom?: string;
    dateTo?: string;
  } = {}
): Promise<PageResult<PositionRow>> {
  const limit = opts.limit ?? PREVIEW_PAGE_SIZE;
  const offset = opts.offset ?? 0;
  const qs = new URLSearchParams({
    name: 'positions_daily',
    fmt: 'json',
    limit: String(limit),
    offset: String(offset),
  });
  if (opts.code) qs.set('code', opts.code);
  if (opts.dateFrom) qs.set('date_from', opts.dateFrom);
  if (opts.dateTo) qs.set('date_to', opts.dateTo);

  const res = await fetch(`/api/v1/tasks/${taskId}/artifact?${qs}`, {
    cache: 'no-store',
  });
  if (res.status === 401) {
    window.location.href = '/login';
    throw new Error('Unauthorized');
  }
  if (!res.ok) throw new Error(`positions page HTTP ${res.status}`);
  const data = await res.json();
  const rows: PositionRow[] = (data.rows || []).map((r: any) => ({
    date: toIsoDate(r.date),
    code: String(r.code ?? ''),
    qty: Number(r.qty),
    cost: Number(r.cost ?? 0),
    market_value: Number(r.market_value ?? 0),
  }));
  return {
    rows,
    total: Number(data.total ?? rows.length),
    offset: Number(data.offset ?? offset),
    limit: Number(data.limit ?? limit),
  };
}

export async function downloadArtifact(taskId: number, name: ArtifactName) {
  const res = await fetch(
    `/api/v1/tasks/${taskId}/artifact?name=${encodeURIComponent(name)}`,
    { cache: 'no-store' }
  );
  if (!res.ok) {
    alert('下载失败');
    return;
  }
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `task_${taskId}_${name}.parquet`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}
