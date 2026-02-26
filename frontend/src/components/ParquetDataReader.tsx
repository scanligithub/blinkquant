'use client';

import { useEffect, useState } from 'react';

interface ParquetDataReaderProps {
  arrayBuffer: ArrayBuffer | null;
  onDataParsed: (data: any[], stockName: string) => void;
  onParseError: (error: string) => void;
}

export default function ParquetDataReader({ arrayBuffer, onDataParsed, onParseError }: ParquetDataReaderProps) {
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    async function parseParquet() {
      if (!arrayBuffer) {
        onDataParsed([], ''); // No data to parse
        return;
      }

      setLoading(true);
      try {
        console.log('ParquetDataReader: Received arrayBuffer with byteLength:', arrayBuffer.byteLength);

        // Dynamically import parquetjs to ensure it's client-side only
        const { ParquetReader } = await import('parquetjs');
        console.log('ParquetDataReader: parquetjs imported successfully via dynamic import.');

        const reader = await ParquetReader.openBuffer(arrayBuffer);
        console.log('ParquetDataReader: ParquetReader opened buffer.');

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
        console.log('ParquetDataReader: ParquetReader closed. Total records read:', records.length);

        if (records.length > 0) {
          console.log('ParquetDataReader: Sample of first record:', records[0]);
          const formattedData = records.map(record => ({
            time: record.date,
            open: record.open,
            high: record.high,
            low: record.low,
            close: record.close,
            volume: record.volume,
          }));
          console.log('ParquetDataReader: Sample of first formatted data point:', formattedData[0]);

          const stockName = records[0].name || records[0].code_name || 'N/A';
          onDataParsed(formattedData, stockName);
        } else {
          console.warn('ParquetDataReader: Stock data empty or invalid Parquet data after parsing.');
          onParseError('Stock data empty or invalid Parquet data.');
        }
      } catch (err: any) {
        console.error('ParquetDataReader: Failed to parse Parquet data:', err);
        console.error('ParquetDataReader: Full error object:', err);
        onParseError(`Failed to parse Parquet data: ${err.message || err}`);
      } finally {
        setLoading(false);
      }
    }

    parseParquet();
  }, [arrayBuffer, onDataParsed, onParseError]);

  if (loading) {
    return <div className="text-slate-500 italic">Loading Parquet data...</div>;
  }

  return null;
}
