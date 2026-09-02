'use client';

interface Position {
  date: string;
  code: string;
  qty: number;
  cost: number;
  market_value: number;
}

interface PositionsTableProps {
  positions: Position[];
  date?: string;
}

export default function PositionsTable({ positions, date }: PositionsTableProps) {
  const filtered = date
    ? positions.filter((p) => p.date === date)
    : positions.filter((p) => p.date === positions[positions.length - 1]?.date);

  if (filtered.length === 0) {
    return <div className="text-center text-gray-400 text-sm py-4">暂无持仓</div>;
  }

  return (
    <div className="overflow-x-auto max-h-[300px] overflow-y-auto">
      <table className="w-full text-xs">
        <thead className="sticky top-0 bg-white">
          <tr className="border-b border-gray-100">
            <th className="text-left py-2 px-2 font-medium text-gray-500">代码</th>
            <th className="text-right py-2 px-2 font-medium text-gray-500">数量</th>
            <th className="text-right py-2 px-2 font-medium text-gray-500">成本</th>
            <th className="text-right py-2 px-2 font-medium text-gray-500">市值</th>
            <th className="text-right py-2 px-2 font-medium text-gray-500">盈亏</th>
          </tr>
        </thead>
        <tbody>
          {filtered.map((p, i) => {
            const pnl = p.market_value - p.cost * p.qty;
            return (
              <tr key={i} className="border-b border-gray-50 hover:bg-gray-50">
                <td className="py-1.5 px-2 font-mono">{p.code}</td>
                <td className="py-1.5 px-2 text-right">{p.qty.toLocaleString()}</td>
                <td className="py-1.5 px-2 text-right">{p.cost.toFixed(2)}</td>
                <td className="py-1.5 px-2 text-right">{p.market_value.toFixed(2)}</td>
                <td className={`py-1.5 px-2 text-right ${pnl >= 0 ? 'text-red-600' : 'text-green-600'}`}>
                  {pnl >= 0 ? '+' : ''}{pnl.toFixed(2)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
