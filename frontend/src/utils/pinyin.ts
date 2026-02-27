import { pinyin } from 'pinyin-pro';

/**
 * 将中文字符串转换为拼音首字母（小写）。
 * 与后端 `_get_pinyin_initials` 的实现保持一致。
 *   - 若字符串不包含中文，直接返回 `text.toLowerCase()`。
 *   - 若包含中文，使用 pinyin‑pro 的 `pattern: 'first'` 选项。
 */
export function getPinyinInitials(text: string): string {
  if (!text) return '';
  // 检测是否包含中文字符（Unicode 区间）
  const hasChinese = /[\u4e00-\u9fff]/.test(text);
  if (!hasChinese) return text.toLowerCase();

  // pinyin‑pro 使用 pattern: 'first' 获取首字母
  const initials = pinyin(text, { pattern: 'first', toneType: 'none' })
    .toLowerCase();
  return initials;
}
