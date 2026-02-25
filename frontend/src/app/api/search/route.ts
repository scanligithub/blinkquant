import { NextResponse } from 'next/server';

const NODE_COUNT = 3; // As per whitepaper, 3 backend nodes

export async function GET(request: Request) {
  console.log('API Search Route: Request received');
  const { searchParams } = new URL(request.url);
  const q = searchParams.get('q');
  console.log(`API Search Route: Query parameter 'q': ${q}`);

  if (!q) {
    return NextResponse.json([]);
  }

  const fetchPromises = [];
  for (let i = 0; i < NODE_COUNT; i++) {
    // Construct the URL for each backend node via Next.js reverse proxy
    // Example: /api/node0/api/v1/search?q=query
    const nodeUrl = `/api/node${i}/api/v1/search?q=${encodeURIComponent(q)}`;
    console.log(`API Search Route: Fetching from node ${i} with URL: ${nodeUrl}`);
    fetchPromises.push(
      fetch(nodeUrl)
        .then(res => {
          if (!res.ok) {
            console.error(`API Search Route: Error fetching from node ${i}: ${res.statusText}`);
            return []; // Return empty array on error for this node
          }
          return res.json();
        })
        .catch(error => {
          console.error(`API Search Route: Network error fetching from node ${i}:`, error);
          return []; // Return empty array on network error
        })
    );
  }

  const allResults = await Promise.all(fetchPromises);
  console.log(`API Search Route: Received ${allResults.length} results from all nodes.`);

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
  console.log(`API Search Route: Final results count: ${finalResults.length}`);

  return NextResponse.json(finalResults);
}