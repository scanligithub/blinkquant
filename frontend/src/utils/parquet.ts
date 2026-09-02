// 将 hyparquet 解析出的 Parquet records 规范化为 K 线组件需要的格式
export function parseParquetRecords(records: any[]): any[] {
  return records.map((record) => {
    let timeValue;
    if (record.date instanceof Date) timeValue = Math.floor(record.date.getTime() / 1000);
    else throw new Error('Invalid date');
    return {
      time: timeValue,
      open: record.open,
      high: record.high,
      low: record.low,
      close: record.close,
      volume: record.volume,
      amount: record.amount,
      turn: record.turn,
      peTTM: record.peTTM,
      total_mv: record.total_mv,
      float_mv: record.float_mv,
      main_net: record.main_net || 0,
      adjustFactor: record.adjustFactor,
    };
  });
}
