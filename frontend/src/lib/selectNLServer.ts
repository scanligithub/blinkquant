// src/lib/selectNLServer.ts
// 服务端运行时：节点元数据拉取 / LLM 调用 / 共享限流存储。
// 纯函数（selectNL.ts）与运行时（本文件）分离，供 /api/select-nl 与 /api/select-nl/analyze 复用。

import { resolveLlmTimeout, type NLMeta } from './selectNL';

// 测试模式：设置 NL_TEST_MODE=true 时跳过限流（仅供自动化测试，生产勿开）。
// 仅在 Vercel 环境变量显式开启，默认关闭。
export const NL_TEST_MODE = process.env.NL_TEST_MODE === 'true';

export const NODES = [
  'https://scanli-blinkquant-node1.hf.space',
  'https://scanli-blinkquant-node2.hf.space',
  'https://scanli-blinkquant-node3.hf.space',
];

// Nemotron 系列模型（含 Lightning）在 NVIDIA NIM 上以 chat_template_kwargs 控制 thinking，
// 与 OpenAI 系 gpt-oss 的 reasoning_effort 参数语义不同，需区分处理。
function isNemotron(): boolean {
  return /nemotron|lightning/i.test(LLM_MODEL || '');
}

export const LLM_ENDPOINT = process.env.LLM_ENDPOINT;
export const LLM_API_KEY = process.env.LLM_API_KEY;
export const LLM_MODEL = process.env.LLM_MODEL;
const LLM_TIMEOUT_MS = resolveLlmTimeout(process.env.LLM_TIMEOUT_MS);
const LLM_REASONING_EFFORT = process.env.LLM_REASONING_EFFORT || 'low';
const LLM_MAX_TOKENS = Number(process.env.LLM_MAX_TOKENS || 1024);
// Nemotron 系列（Lightning 等）在 NVIDIA NIM 上默认开启 thinking，
// 会把思维链直接写入 content 导致 JSON 解析失败。结构化输出需关闭：
// https://docs.nvidia.com/nim/large-language-models/latest/get-started/advanced/get-started-nemotron-3.5-lightning.html
const LLM_TEMPLATE_KWARGS = process.env.LLM_TEMPLATE_KWARGS || '{"enable_thinking": false}';

const META_TTL_MS = 24 * 60 * 60 * 1000;
let metaCache: { at: number; data: NLMeta } | null = null;

// 限流存储共享：分析端点与翻译端点使用同一 Map，但 key 前缀不同（见各自 route）。
// Vercel 每实例内存 Map 的近似性在两端点间一致。
export const rateStore = new Map<string, { timestamps: number[] }>();

export async function fetchNlMeta(): Promise<NLMeta> {
  if (metaCache && Date.now() - metaCache.at < META_TTL_MS) return metaCache.data;
  const result = await Promise.any(
    NODES.map(async (nodeUrl) => {
      const res = await fetch(`${nodeUrl}/api/v1/nl-meta`, { signal: AbortSignal.timeout(8000) });
      if (!res.ok) throw new Error(`Node responded with ${res.status}`);
      return res.json();
    })
  );
  metaCache = { at: Date.now(), data: result as NLMeta };
  return result as NLMeta;
}

export async function callLlm(systemPrompt: string, query: string): Promise<string> {
  const llmRes = await fetch(LLM_ENDPOINT!, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${LLM_API_KEY!}` },
    body: JSON.stringify({
      model: LLM_MODEL,
      messages: [
        { role: 'system', content: systemPrompt },
        { role: 'user', content: query },
      ],
      temperature: 0,
      ...(isNemotron() ? {} : { reasoning_effort: LLM_REASONING_EFFORT }),
      ...(isNemotron() ? { chat_template_kwargs: JSON.parse(LLM_TEMPLATE_KWARGS) } : {}),
      max_tokens: LLM_MAX_TOKENS,
    }),
    signal: AbortSignal.timeout(LLM_TIMEOUT_MS),
  });
  if (!llmRes.ok) {
    throw new Error(`LLM HTTP ${llmRes.status}`);
  }
  const llmJson = await llmRes.json();
  return llmJson?.choices?.[0]?.message?.content ?? '';
}
