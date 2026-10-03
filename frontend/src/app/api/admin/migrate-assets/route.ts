import { NextRequest, NextResponse } from 'next/server';
import { sql } from '@/lib/db';
import { requireAdmin } from '@/lib/auth';
import { node1Json } from '@/lib/node1Internal';

export const runtime = 'nodejs';

async function tableExists(name: string): Promise<boolean> {
  const result = await sql`
    SELECT EXISTS (
      SELECT 1 FROM information_schema.tables
      WHERE table_schema = 'public' AND table_name = ${name}
    ) AS exists
  `;
  return !!result.rows[0]?.exists;
}

export async function POST(req: NextRequest) {
  const auth = await requireAdmin(req);
  if (!auth.user) return NextResponse.json({ error: auth.status === 403 ? '无权限' : '未登录' }, { status: auth.status });

  try {
    const strategiesResult = await sql`
      SELECT id,user_id,name,formula,timeframe,
             source_backtest_strategy_id,source_backtest_strategy_version,
             source_backtest_strategy_name,source_backtest_strategy_trigger,
             source_backtest_artifact_id,source_backtest_artifact_title,created_at,updated_at
      FROM strategies ORDER BY user_id,id
    `;
    const versionsResult = await sql`
      SELECT strategy_id,version_no,name,formula,timeframe,
             source_backtest_strategy_id,source_backtest_strategy_version,
             source_backtest_strategy_name,source_backtest_strategy_trigger,created_at
      FROM strategy_versions ORDER BY strategy_id,version_no
    `;

    const byStrategy = new Map<string, any>();
    for (const row of strategiesResult.rows) {
      byStrategy.set(String(row.id), {
        user_id: String(row.user_id), name: row.name, formula: row.formula, timeframe: row.timeframe,
        source_backtest_strategy_id: row.source_backtest_strategy_id,
        source_backtest_strategy_version: row.source_backtest_strategy_version,
        source_backtest_strategy_name: row.source_backtest_strategy_name,
        source_backtest_strategy_trigger: row.source_backtest_strategy_trigger,
        source_backtest_artifact_id: row.source_backtest_artifact_id,
        source_backtest_artifact_title: row.source_backtest_artifact_title,
        created_at: row.created_at, updated_at: row.updated_at, versions: [],
      });
    }
    for (const row of versionsResult.rows) {
      const target = byStrategy.get(String(row.strategy_id));
      if (!target) continue;
      target.versions.push({
        version_no: row.version_no, name: row.name, formula: row.formula, timeframe: row.timeframe,
        source_backtest: row.source_backtest_strategy_id != null ? {
          strategy_id: row.source_backtest_strategy_id, version_no: row.source_backtest_strategy_version,
          name: row.source_backtest_strategy_name, trigger: row.source_backtest_strategy_trigger,
        } : null,
        created_at: row.created_at,
      });
    }

    const byWatchlist = new Map<string, any>();
    if (await tableExists('watchlists') && await tableExists('watchlist_items')) {
      const rows = await sql`
        SELECT w.id,w.user_id,w.name,w.is_default,w.created_at,w.updated_at,wi.code
        FROM watchlists w LEFT JOIN watchlist_items wi ON wi.watchlist_id=w.id
        ORDER BY w.user_id,w.created_at,wi.created_at
      `;
      for (const row of rows.rows) {
        const key = String(row.user_id) + '::' + String(row.id);
        const target = byWatchlist.get(key) || {
          user_id: String(row.user_id), name: row.name, is_default: !!row.is_default,
          created_at: row.created_at, updated_at: row.updated_at, codes: [],
        };
        if (row.code != null) target.codes.push(String(row.code));
        byWatchlist.set(key, target);
      }
    }
    if (await tableExists('watchlist')) {
      const oldRows = await sql`SELECT user_id,code,created_at FROM watchlist ORDER BY user_id,created_at,code`;
      const oldByUser = new Map<string,string[]>();
      for (const row of oldRows.rows) {
        const key=String(row.user_id); const arr=oldByUser.get(key)||[]; arr.push(String(row.code)); oldByUser.set(key,arr);
      }
      for (const [uid,codes] of oldByUser) {
        if (!Array.from(byWatchlist.values()).some((x:any)=>x.user_id===uid && x.is_default)) {
          byWatchlist.set(uid+'::legacy-default',{user_id:uid,name:'默认自选',is_default:true,created_at:null,updated_at:null,codes:Array.from(new Set(codes))});
        }
      }
    }

    const payload={strategies:Array.from(byStrategy.values()),watchlists:Array.from(byWatchlist.values())};
    const migrated=await node1Json('/user-assets/migrate-legacy',{method:'POST',body:JSON.stringify(payload)});
    if (!migrated.response.ok) return NextResponse.json({error:String(migrated.data?.detail||migrated.data?.error||'Node1 资产迁移失败')},{status:migrated.response.status});
    return NextResponse.json({ok:true,source:{strategies:payload.strategies.length,watchlists:payload.watchlists.length},target:migrated.data});
  } catch (error) {
    console.error('[admin/migrate-assets] error:',error);
    return NextResponse.json({error:error instanceof Error?error.message:'资产迁移失败'},{status:500});
  }
}
