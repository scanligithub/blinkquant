// src/utils/stockSearch.ts
import { getPinyinInitials } from './pinyin';
import { cleanSearchInput } from './cleanInput';

export interface StockItem {
  code: string;
  name: string;
}

export function searchStocks(stockList: StockItem[], query: string): StockItem[] {
  if (query.length < 1 || stockList.length === 0) return [];
  const cleanedQuery = cleanSearchInput(query);
  const qLower = cleanedQuery;
  const qPinyin = getPinyinInitials(cleanedQuery);

  const scoredResults = stockList.map((stock) => {
    const { code, name } = stock;
    if (!name || !name.trim()) return { ...stock, score: 0 };

    const nameClean = name.trim().toLowerCase();
    const codeClean = code.trim().toLowerCase();
    const codeNum = codeClean.replace(/^(sh|sz|bj)\./, '');
    const namePinyin = getPinyinInitials(name);
    let score = 0;

    if (codeClean === qLower || codeNum === qLower || nameClean === qLower) score += 1000;
    if (codeClean.startsWith(qLower) || codeNum.startsWith(qLower)) score += 100;
    if (namePinyin.startsWith(qPinyin)) score += 80;
    if (nameClean.startsWith(qLower)) score += 80;
    if (codeClean.includes(qLower) || codeNum.includes(qLower)) score += 10;
    if (namePinyin.includes(qPinyin)) score += 5;
    if (nameClean.includes(qLower)) score += 5;

    return { ...stock, score };
  });

  return scoredResults
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score)
    .map(({ code, name }) => ({ code, name }))
    .slice(0, 10);
}
