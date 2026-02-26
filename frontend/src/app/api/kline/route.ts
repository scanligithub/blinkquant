import { NextResponse, NextRequest } from 'next/server';
import * as parquet from 'parquetjs'; // Added for Parquet data parsing

export const runtime = 'edge';

const NODES = [
  'https://scanli-blinkquant-node1.hf.space',
  'https://scanli-blinkquant-node2.hf.space',
  'https://scanli-blinkquant-node3.hf.space'
];

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const code = searchParams.get('code');
  const timeframe = searchParams.get('timeframe') || 'D';

  if (!code) {
    return new NextResponse(JSON.stringify({ error: 'Stock code is required' }), { status: 400, headers: { 'Content-Type': 'application/json' } });
  }

  try {
    const resultBuffer = await Promise.any(
      NODES.map(async (nodeUrl, i) => {
        const url = `${nodeUrl}/api/v1/kline?code=${code}&timeframe=${timeframe}`;
        const res = await fetch(url, { signal: AbortSignal.timeout(5000) });

        if (!res.ok) {
          const errorText = await res.text();
          console.error(`Backend node ${nodeUrl} responded with status ${res.status}: ${errorText}`);
          throw new Error(`Not on this node or backend error: ${errorText}`);
        }

        const arrayBuffer = await res.arrayBuffer();
        if (!arrayBuffer || arrayBuffer.byteLength < 100) { // Parquet files have a magic number PAR1 at start, min size
            throw new Error('Empty or invalid Parquet data received');
        }
        return arrayBuffer;
      })
    );

    const parquet = await import('parquetjs');
    const reader = await parquet.ParquetReader.openBuffer(resultBuffer);
    const cursor = reader.get == undefined ? reader.getRecordReader() : reader.getRecordReader();

    const records: any[] = [];
    while (true) {
      const record = await cursor.read();
      if (record === null) {
        break;
      }
      records.push(record);
    }
    await reader.close();

    if (records.length === 0) {
      throw new Error('Empty or invalid Parquet data after parsing');
    }

    const formattedData = records.map(record => ({
      time: record.date,
      open: record.open,
      high: record.high,
      low: record.low,
      close: record.close,
      volume: record.volume,
    }));
    
    const stockName = records[0].name || records[0].code_name || 'N/A';

    return NextResponse.json({
      code: code,
      name: stockName,
      data: formattedData
    }, {
      status: 200,
      headers: {
        'Cache-Control': 'public, s-maxage=3600, stale-while-revalidate=60'
      }
    });

  } catch (error: any) {
    console.error("Error fetching kline data:", error);
    if (error instanceof AggregateError) {
        return new NextResponse(JSON.stringify({ error: 'Stock not found in cluster or data unavailable' }), { status: 404, headers: { 'Content-Type': 'application/json' } });
    }
    return new NextResponse(JSON.stringify({ error: 'Failed to fetch kline data' }), { status: 500, headers: { 'Content-Type': 'application/json' } });
  }
}
