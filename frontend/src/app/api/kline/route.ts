import { NextResponse, NextRequest } from 'next/server';

const NODE_COUNT = 3; // As per whitepaper, 3 backend nodes

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const code = searchParams.get('code');
  const timeframe = searchParams.get('timeframe') || 'D';

  if (!code) {
    return NextResponse.json({ error: 'Stock code is required' }, { status: 400 });
  }

  const fetchPromises = [];
  for (let i = 0; i < NODE_COUNT; i++) {
    // Construct the URL for each backend node via Next.js reverse proxy
    // Example: /api/node0/api/v1/kline?code=...&timeframe=...
    const nodeUrl = `/api/node${i}/api/v1/kline?code=${code}&timeframe=${timeframe}`;
    fetchPromises.push(
      fetch(nodeUrl, { signal: AbortSignal.timeout(5000) }) // 5-second timeout
        .then(async res => {
          if (!res.ok) {
            // If the response is not ok, throw an error to be caught by Promise.any
            // This allows Promise.any to continue to the next promise
            const errorText = await res.text();
            throw new Error(`Node ${i} error: ${res.status} - ${errorText}`);
          }
          const json = await res.json();
          // Check if data is empty or invalid
          if (!json.data || (Array.isArray(json.data) && json.data.length === 0)) {
            throw new Error(`Node ${i} returned empty data`);
          }
          return json;
        })
    );
  }

  try {
    // Use Promise.any to get the first successful response
    const data = await Promise.any(fetchPromises);
    return NextResponse.json(data, {
      headers: {
        'Cache-Control': 'public, max-age=300, stale-while-revalidate=600', // Cache for 5 minutes
      },
    });
  } catch (err: any) {
    console.error('Failed to fetch K-line data from any node:', err);
    // If all promises reject, Promise.any throws an AggregateError.
    // We'll return a generic "Stock not found" or "Cluster error" message.
    return NextResponse.json({ error: 'Stock not found in cluster or cluster data unavailable' }, { status: 404 });
  }
}
