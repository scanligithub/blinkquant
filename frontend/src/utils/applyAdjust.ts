// 前端复权转换工具（无复权 / 前复权 / 后复权）
// 依据后端 data_manager._apply_forward_adjustment 的公式实现，保持行为完全一致。

export type AdjustMode = 'none' | 'qfq' | 'hfq';

export const ADJUST_OPTIONS = [
  { value: 'none' as AdjustMode, label: '无复权' },
  { value: 'qfq' as AdjustMode, label: '前复权' },
  { value: 'hfq' as AdjustMode, label: '后复权' },
];

export const ADJUST_LABELS: Record<AdjustMode, string> = {
  none: '无复权',
  qfq: '前复权',
  hfq: '后复权',
};

/**
 * 将已前复权（QFQ）过的日线数据转换为指定的复权模式。
 * 输入 bars 必须包含 `adjustFactor`（可能为 null），以及 OHLCV、main_net、time。
 * 输出保持相同结构，只在 price/volume 上做变换，`main_net` 与 `time` 不变。
 */
export function applyAdjust(bars: any[], mode: AdjustMode): any[] {
  if (!Array.isArray(bars) || bars.length === 0) return [];

  // 步骤 1：提取因子并前向填充，前导 null => 1.0
  const factors: number[] = [];
  let last = 1.0; // 前导填充值
  for (const bar of bars) {
    const raw = bar.adjustFactor;
    if (raw == null) {
      factors.push(last);
    } else {
      last = Number(raw);
      factors.push(last);
    }
  }

  // 步骤 2：计算首尾因子
  const first = factors[0] ?? 1.0;
  const latest = factors[factors.length - 1] ?? 1.0;

  // 若 latest <= 0，后端会视为未复权，等价于因子全 1.0
  const safeLatest = latest > 0 ? latest : 1.0;
  const safeFirst = safeLatest > 0 ? first : 1.0;

  // 步骤 3：根据模式计算倍率
  const result = bars.map((bar, idx) => {
    const f = factors[idx];
    let priceMul = 1;
    let volMul = 1;
    if (mode === 'none') {
      // 无复权 = raw = qfq * latest / f
      priceMul = safeLatest / f;
      volMul = f / safeLatest;
    } else if (mode === 'hfq') {
      // 后复权 = 常数倍 = qfq * latest / first
      priceMul = safeLatest / safeFirst;
      volMul = safeFirst / safeLatest;
    } // else 'qfq' → 1
    return {
      time: bar.time,
      open: bar.open * priceMul,
      high: bar.high * priceMul,
      low: bar.low * priceMul,
      close: bar.close * priceMul,
      volume: bar.volume * volMul,
      main_net: bar.main_net, // 资金流不受复权影响
      adjustFactor: bar.adjustFactor,
    };
  });

  return result;
}
