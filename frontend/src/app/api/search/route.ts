import { NextResponse } from 'next/server';

const NODE_COUNT = 3; // As per whitepaper, 3 backend nodes

export async function GET(request: Request) {
  const { searchParams } = new URL(request.url);
  const q = searchParams.get('q');

  if (!q) {
    return NextResponse.json([]);
  }

  const fetchPromises = [];
  for (let i = 0; i < NODE_COUNT; i++) {
    // Construct the URL for each backend node via Next.js reverse proxy
    // Example: /api/node0/api/v1/search?q=query
    const nodeUrl = `/api/node${i}/api/v1/search?q=${encodeURIComponent(q)}`;
    fetchPromises.push(
      fetch(nodeUrl)
        .then(res => {
          if (!res.ok) {
            console.error(`Error fetching from node ${i}: ${res.statusText}`);
            return []; // Return empty array on error for this node
          }
          return res.json();
        })
        .catch(error => {
          console.error(`Network error fetching from node ${i}:`, error);
          return []; // Return empty array on network error
        })
    );
  }

  const allResults = await Promise.all(fetchPromises);

  // Aggregate and deduplicate results
  const uniqueResultsMap = new Map();
  allResults.forEach(nodeResults => {
    nodeResults.forEach((stock: { code: string; name: string }) => {
      if (!uniqueResultsMap.has(stock.code)) {
        uniqueResultsMap.set(stock.code, stock);
      }
    });
  });

  const finalResults = Array.from(uniqueResultsMap.values()).slice(0, 10); // Limit to 10 results for frontend

  return NextResponse.json(finalResults);
}