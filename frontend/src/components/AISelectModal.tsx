'use client';
import { useState } from 'react';

interface AISelectModalProps {
  onClose: () => void;
  onRun: (formula: string, timeframe: string, date?: string) => void;
}

interface AnalyzeResult {
  restatement: string;
  conditions: string[];
  logic: string;
  timeframe: string;
  date?: string;
}

interface TranslateResult {
  formula: string;
  timeframe: string;
  explanation: string;
}

const TIMEFRAMES = [
  { label: '日', value: 'D' },
  { label: '周', value: 'W' },
  { label: '月', value: 'M' },
];

const MAX_CORRECTION_ROUNDS = 3;

type Phase = 'input' | 'confirm' | 'result';

export default function AISelectModal({ onClose, onRun }: AISelectModalProps) {
  const [query, setQuery] = useState('');
  const [phase, setPhase] = useState<Phase>('input');
  const [loading, setLoading] = useState(false);
  const [loadingLabel, setLoadingLabel] = useState('');
  const [error, setError] = useState('');
  const [analysis, setAnalysis] = useState<AnalyzeResult | null>(null);
  const [correction, setCorrection] = useState('');
  const [correctionRounds, setCorrectionRounds] = useState(0);
  const [timeframe, setTimeframe] = useState('D');
  const [result, setResult] = useState<TranslateResult | null>(null);

  const resetError = () => setError('');

  const handleError = (res: Response, json: { code?: string; retryAfterMs?: number; error?: string }) => {
    if (res.status === 429 && json.code === 'RATE_LIMITED' && json.retryAfterMs) {
      setError(`调用过于频繁，请 ${Math.ceil(json.retryAfterMs / 1000)} 秒后重试`);
    } else {
      setError(json.error || '操作失败，请重试');
    }
  };

  const analyze = async (withCorrection: boolean) => {
    if (!query.trim() || loading) return;
    setLoading(true);
    setLoadingLabel('分析中…');
    resetError();
    try {
      const res = await fetch('/api/select-nl/analyze', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          query: query.trim(),
          ...(withCorrection && analysis ? { correction: correction.trim(), previous: analysis } : {}),
        }),
      });
      const json = await res.json();
      if (!res.ok) {
        handleError(res, json);
        return;
      }
      const data = json.data as AnalyzeResult;
      setAnalysis(data);
      setTimeframe(data.timeframe);
      if (withCorrection) {
        setCorrectionRounds((r) => r + 1);
        // 纠正轮后端返回的是「修订后的语义」，把顶部输入框同步为最新复述，
        // 避免「描述你的选股条件」与下方语义确认/条件清单不一致。
        setQuery(data.restatement);
      }
      setCorrection('');
      setPhase('confirm');
    } catch {
      setError('AI 选股服务暂不可用，请稍后再试');
    } finally {
      setLoading(false);
      setLoadingLabel('');
    }
  };

  const translate = async () => {
    if (!analysis || loading) return;
    setLoading(true);
    setLoadingLabel('翻译中…');
    resetError();
    try {
      const res = await fetch('/api/select-nl', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ analysis: { ...analysis, timeframe } }),
      });
      const json = await res.json();
      if (!res.ok) {
        handleError(res, json);
        return;
      }
      setResult(json.data as TranslateResult);
      setTimeframe(json.data.timeframe);
      setPhase('result');
    } catch {
      setError('AI 选股服务暂不可用，请稍后再试');
    } finally {
      setLoading(false);
      setLoadingLabel('');
    }
  };

  return (
    <div className="fixed inset-0 z-50 bg-black/40 flex items-center justify-center p-4" onClick={onClose}>
      <div className="bg-white rounded-2xl w-full max-w-lg shadow-xl p-6" onClick={(e) => e.stopPropagation()}>
        <h2 className="font-bold text-slate-700 mb-4">AI 选股</h2>

        <label className="text-xs font-bold text-slate-500 uppercase tracking-wider">描述你的选股条件</label>
        <textarea
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          rows={2}
          disabled={loading}
          className="mt-1 w-full bg-slate-50 border border-slate-200 rounded-xl px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 disabled:opacity-50"
          placeholder="例如：市盈率低于20且总市值大于100亿的股票"
        />

        <div className="mt-3 flex justify-end gap-2">
          {phase !== 'result' && (
            <button
              onClick={() => analyze(false)}
              disabled={loading || !query.trim()}
              className="px-4 py-2 text-sm bg-blue-600 hover:bg-blue-700 text-white rounded-xl font-bold disabled:opacity-50 flex items-center gap-2"
            >
              {loading && loadingLabel === '分析中…' && (
                <div className="w-3 h-3 border-2 border-white/30 border-t-white rounded-full animate-spin"></div>
              )}
              {phase === 'confirm' ? '重新分析' : '分析'}
            </button>
          )}
        </div>

        {error && (
          <div className="mt-3 text-sm text-red-600 bg-red-50 border border-red-200 rounded-lg px-3 py-2">{error}</div>
        )}

        {phase === 'confirm' && analysis && (
          <div className="mt-4">
            <label className="text-xs font-bold text-slate-500 uppercase tracking-wider">语义确认</label>
            <div className="mt-1 bg-slate-50 border border-slate-200 rounded-xl px-3 py-2 text-sm text-slate-700">
              {analysis.restatement}
            </div>

            <div className="mt-2 bg-slate-50 border border-slate-200 rounded-xl px-3 py-2 text-sm text-slate-700">
              <div className="font-bold text-slate-600 mb-1">条件清单</div>
              <ol className="list-decimal list-inside space-y-1">
                {analysis.conditions.map((c, i) => (
                  <li key={i}>{c}</li>
                ))}
              </ol>
              <div className="mt-1 text-slate-500">
                逻辑关系：<span className="font-mono text-slate-700">{analysis.logic}</span>
              </div>
              {analysis.date && (
                <div className="mt-1 text-slate-500">
                  查询交易日：<span className="font-mono text-slate-700">{analysis.date}</span>
                  （非交易日自动回退到最近交易日）
                </div>
              )}
            </div>

            <div className="mt-2 flex items-center gap-1">
              {TIMEFRAMES.map((tf) => (
                <button
                  key={tf.value}
                  onClick={() => setTimeframe(tf.value)}
                  disabled={loading}
                  className={`px-2 py-1 text-xs font-bold rounded-md ${timeframe === tf.value ? 'bg-blue-600 text-white' : 'text-slate-500 hover:bg-slate-100'}`}
                >
                  {tf.label}
                </button>
              ))}
            </div>

            {correctionRounds < MAX_CORRECTION_ROUNDS && (
              <details className="mt-3">
                <summary className="text-xs text-blue-600 font-bold cursor-pointer select-none">
                  语义不对？指出问题后重新分析（剩余 {MAX_CORRECTION_ROUNDS - correctionRounds} 次）
                </summary>
                <div className="mt-2">
                  <textarea
                    value={correction}
                    onChange={(e) => setCorrection(e.target.value)}
                    rows={2}
                    disabled={loading}
                    className="mt-1 w-full bg-slate-50 border border-slate-200 rounded-xl px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 disabled:opacity-50"
                    placeholder="例如：振幅指的是 PCT_CHANGE，不是最高最低价差；或者逻辑关系应该是 1 OR 2"
                  />
                  <div className="mt-2 flex justify-end">
                    <button
                      onClick={() => analyze(true)}
                      disabled={loading || !correction.trim()}
                      className="px-4 py-2 text-sm bg-slate-700 hover:bg-slate-800 text-white rounded-xl font-bold disabled:opacity-50 flex items-center gap-2"
                    >
                      {loading && loadingLabel === '分析中…' && (
                        <div className="w-3 h-3 border-2 border-white/30 border-t-white rounded-full animate-spin"></div>
                      )}
                      按纠正重新分析
                    </button>
                  </div>
                </div>
              </details>
            )}

            {correctionRounds >= MAX_CORRECTION_ROUNDS && (
              <div className="mt-3 text-xs text-slate-500 bg-slate-50 border border-slate-200 rounded-lg px-3 py-2">
                已达最大纠正次数，可直接翻译，或在公式预览中手动编辑。
              </div>
            )}

            <div className="mt-4 flex justify-end gap-2">
              <button
                onClick={() => setPhase('input')}
                disabled={loading}
                className="px-4 py-2 text-sm border border-slate-200 rounded-xl text-slate-600 hover:bg-slate-50 disabled:opacity-50"
              >
                返回
              </button>
              <button
                onClick={translate}
                disabled={loading}
                className="px-4 py-2 text-sm bg-blue-600 hover:bg-blue-700 text-white rounded-xl font-bold disabled:opacity-50 flex items-center gap-2"
              >
                {loading && loadingLabel === '翻译中…' && (
                  <div className="w-3 h-3 border-2 border-white/30 border-t-white rounded-full animate-spin"></div>
                )}
                确认并翻译
              </button>
            </div>
          </div>
        )}

        {phase === 'result' && result && (
          <div className="mt-4">
            <div className="flex items-center justify-between gap-2">
              <label className="text-xs font-bold text-slate-500 uppercase tracking-wider">公式预览（可编辑）</label>
              <div className="flex items-center gap-1">
                {TIMEFRAMES.map((tf) => (
                  <button
                    key={tf.value}
                    onClick={() => setTimeframe(tf.value)}
                    className={`px-2 py-1 text-xs font-bold rounded-md ${timeframe === tf.value ? 'bg-blue-600 text-white' : 'text-slate-500 hover:bg-slate-100'}`}
                  >
                    {tf.label}
                  </button>
                ))}
              </div>
            </div>
            <textarea
              value={result.formula}
              onChange={(e) => setResult({ ...result, formula: e.target.value })}
              rows={2}
              className="mt-1 w-full bg-slate-50 border border-slate-200 rounded-xl px-3 py-2 font-mono text-sm focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500"
            />
            {result.explanation && (
              <div className="mt-2 text-xs text-slate-500 bg-slate-50 border border-slate-200 rounded-lg px-3 py-2">
                {result.explanation}
              </div>
            )}
            <div className="mt-4 flex justify-end gap-2">
              <button
                onClick={() => setPhase('confirm')}
                className="px-4 py-2 text-sm border border-slate-200 rounded-xl text-slate-600 hover:bg-slate-50"
              >
                返回语义
              </button>
              <button
                onClick={onClose}
                className="px-4 py-2 text-sm border border-slate-200 rounded-xl text-slate-600 hover:bg-slate-50"
              >
                取消
              </button>
              <button
                onClick={() => onRun(result.formula, timeframe, analysis?.date)}
                disabled={!result.formula.trim()}
                className="px-4 py-2 text-sm bg-blue-600 hover:bg-blue-700 text-white rounded-xl font-bold disabled:opacity-50"
              >
                运行选股
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
