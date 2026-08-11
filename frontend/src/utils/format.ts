// Shared money and volume formatting helpers

/**
 * Format monetary values (in yuan) to a readable string with units.
 * Returns '--' for null/undefined/NaN values.
 */
export function formatMoney(value: number | null | undefined): string {
  if (value == null || isNaN(value)) return '--';
  const absVal = Math.abs(value);
  if (absVal >= 100000000) return (value / 100000000).toFixed(2) + '亿';
  if (absVal >= 10000) return (value / 10000).toFixed(2) + '万';
  return value.toFixed(2);
}

/**
 * Format volume numbers to readable units.
 * Returns '--' for null/undefined/NaN values.
 */
export function formatVolume(volume: number | null | undefined): string {
  if (volume == null || isNaN(volume)) return '--';
  if (volume >= 100000000) return (volume / 100000000).toFixed(2) + '亿';
  if (volume >= 10000) return (volume / 10000).toFixed(2) + '万';
  return volume.toString();
}
