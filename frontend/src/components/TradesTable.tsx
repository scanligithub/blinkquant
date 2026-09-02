'use client';

interface Trade {
  signal_date: string;
  execution_date: string;
  code: string;
  side: string;
  qty: number;
  price: number;
  fee: number;
}

interface TradesTableProps {
  trades: Trade[];
}

export default function TradesTable({ trades }: TradesTableProps) {
  if (trades.length === 0) {
    return <div className="text-center text-gray-400 text-sm py-4">暂无交易</div>;
  }

  return (
    <div className="overflow-x-auto max-h-[300px] overflow-y-auto">
      <table className="w-full text-xs">
        <thead className="sticky top-0 bg-white">
          <tr className="border-b border-gray-100">
            <th className="text-left py-2 px-2 font-medium text-gray-500">执行日</th>
            <th className="text-left py-2 px-2 font-medium text-gray-500">代码</th>
            <th className="text-left py-2 px-2 font-medium text-gray-500">方向</th>
            <th className="text-right py-2 px-2 font-medium text-gray-500">数量</th>
            <th className="text-right py-2 px-2 font-medium text-gray-500">价格</th>
            <th className="text-right py-2 px-2 font-medium text-gray-500">费用</th>
          </tr>
        </thead>
        <tbody>
          {trades.map((t, i) => (
            <tr key={i} className="border-b border-gray-50 hover:bg-gray-50">
              <td className="py-1.5 px-2">{t.execution_date}</td>
              <td className="py-1.5 px-2 font-mono">{t.code}</td>
              <td className="py-1.5 px-2">
                <span className={t.side === 'BUY' ? 'text-red-600' : 'text-green-600'}>
                  {t.side === 'BUY' ? '买入' : '卖出'}
                </span>
              </td>
              <td className="py-1.5 px-2 text-right">{t.qty.toLocaleString()}</td>
              <td className="py-1.5 px-2 text-right">{t.price.toFixed(2)}</td>
              <td className="py-1.5 px-2 text-right">{t.fee.toFixed(2)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
