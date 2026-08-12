// frontend/tests/stock-search.test.mjs
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { pinyin } from 'pinyin-pro';

// 与 src/utils/pinyin.ts 保持一致（复制实现以绕过 TS import）
function getPinyinInitials(text) {
  if (!text) return '';
  const parts = text.split('.');
  let target = text;
  if (parts.length === 2 && (parts[0] === 'sh' || parts[0] === 'sz' || parts[0] === 'bj')) {
    target = parts[1];
  }
  const hasChinese = /[\u4e00-\u9fff]/.test(target);
  if (!hasChinese) return text.toLowerCase();
  const initials = pinyin(target, { pattern: 'first', toneType: 'none', type: 'array' }).join('').toLowerCase();
  return initials.replace(/[^a-z]/g, '');
}

// 与 src/utils/cleanInput.ts 保持一致
function cleanSearchInput(text) {
  if (!text) return '';
  let cleaned = text.replace(/[\s\u3000]/g, '');
  cleaned = cleaned.replace(/[‘’“”、，。,.!！?？;；:：\-—_—\[\]{}()<>【】《》]/g, '');
  return cleaned.toLowerCase();
}

// 与 src/utils/stockSearch.ts 保持一致（复制实现以绕过 TS import）
function searchStocks(stockList, query) {
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

test('searchStocks: 空查询返回空数组', () => {
  assert.deepEqual(searchStocks([{ code: 'sh.600000', name: '浦发银行' }], ''), []);
});

test('searchStocks: 代码精确匹配优先', () => {
  const list = [
    { code: 'sh.600000', name: '浦发银行' },
    { code: 'sz.000001', name: '平安银行' },
  ];
  assert.deepEqual(searchStocks(list, '600000'), [{ code: 'sh.600000', name: '浦发银行' }]);
});

test('searchStocks: 去前缀数字匹配', () => {
  const list = [
    { code: 'sh.600000', name: '浦发银行' },
    { code: 'sz.000001', name: '平安银行' },
  ];
  assert.deepEqual(searchStocks(list, '000001'), [{ code: 'sz.000001', name: '平安银行' }]);
});

test('searchStocks: 名称精确匹配', () => {
  const list = [
    { code: 'sh.600000', name: '浦发银行' },
    { code: 'sz.000001', name: '平安银行' },
  ];
  assert.deepEqual(searchStocks(list, '平安银行'), [{ code: 'sz.000001', name: '平安银行' }]);
});

test('searchStocks: 拼音首字母匹配', () => {
  const list = [
    { code: 'sh.600000', name: '浦发银行' },
    { code: 'sz.000001', name: '平安银行' },
  ];
  // 浦发银行 → pfyh，平安银行 → payh
  assert.deepEqual(searchStocks(list, 'pf'), [{ code: 'sh.600000', name: '浦发银行' }]);
});

test('searchStocks: 无匹配返回空数组', () => {
  const list = [
    { code: 'sh.600000', name: '浦发银行' },
    { code: 'sz.000001', name: '平安银行' },
  ];
  assert.deepEqual(searchStocks(list, 'zzz'), []);
});

test('searchStocks: 结果截断前10且保持输入顺序', () => {
  const list = Array.from({ length: 12 }, (_, i) => ({ code: `sh.6000${i}`, name: `股票${i}` }));
  const results = searchStocks(list, '股票');
  assert.equal(results.length, 10);
  assert.equal(results[0].name, '股票0');
  assert.equal(results[9].name, '股票9');
});
