const NODE1_URL = process.env.NODE1_URL || 'https://scanli-blinkquant-node1.hf.space';
const INTERNAL_TOKEN = process.env.INTERNAL_TOKEN || 'internal-secret-change-me';

export function node1Path(path: string, params?: Record<string, string | number | undefined | null>) {
  const url = new URL('/internal' + path, NODE1_URL);
  for (const [key, value] of Object.entries(params || {})) {
    if (value !== undefined && value !== null && String(value) !== '') url.searchParams.set(key, String(value));
  }
  return url.toString();
}

export async function node1Fetch(path: string, init: RequestInit = {}) {
  return fetch(NODE1_URL + '/internal' + path, {
    ...init,
    headers: {
      ...(init.body && !(init.body instanceof FormData) ? { 'Content-Type': 'application/json' } : {}),
      Authorization: 'Bearer ' + INTERNAL_TOKEN,
      ...(init.headers || {}),
    },
    cache: 'no-store',
  });
}

export async function node1Json(path: string, init: RequestInit = {}) {
  const response = await node1Fetch(path, init);
  const data = await response.json().catch(() => ({}));
  return { response, data };
}
