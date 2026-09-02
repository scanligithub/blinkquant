// utils/cleanInput.ts
/**
 * 清理用户输入的搜索关键字。
 * - 去除空格、全角空格、换行等空白字符
 * - 移除常见标点（中英文全角/半角）
 * - 返回小写字符串，便于后续匹配
 */
export function cleanSearchInput(text: string): string {
  if (!text) return '';
  // 移除空格、全角空格、换行等
  let cleaned = text.replace(/[\s\u3000]/g, '');
  // 移除常见标点（包括全角/半角）
  cleaned = cleaned.replace(/[‘’“”、，。,.!！?？;；:：\-—_—\[\]{}()<>【】《》]/g, '');
  return cleaned.toLowerCase();
}
