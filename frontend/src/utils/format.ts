export function formatMoney(value: number | null | undefined): string {
  if (!value || isNaN(value)) return '--';
  const absVal = Math.abs(value);
  if (absVal >= 100000000) return (value / 100000000).toFixed(2) + '亿';
  else if (absVal >= 10000) return (value / 10000).toFixed(2) + '万';
  return value.toFixed(2);
}

export function formatVolume(volume: number | null | undefined): string {
  if (!volume || isNaN(volume)) return '--';
  if (volume >= 100000000) return (volume / 100000000).toFixed(2) + '亿';
  else if (volume >= 10000) return (volume / 10000).toFixed(2) + '万';
  return volume.toString();
}
