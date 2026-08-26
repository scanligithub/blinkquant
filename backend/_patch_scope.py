# -*- coding: utf-8 -*-
p = 'core/backtest_engine.py'
s = open(p, encoding='utf-8').read()
pairs = [
 ('            if pend_sig is not None and t == pend_exec:',
  '            if self._pend_sig is not None and t == self._pend_exec:'),
 ('                cur_signal_date = pend_sig',
  '                cur_signal_date = self._pend_sig'),
 ('                    intents=pend_intents,',
  '                    intents=self._pend_intents,'),
 ('                    raw_prices=pend_prices,',
  '                    raw_prices=self._pend_prices,'),
 ('                for it in pend_intents:',
  '                for it in self._pend_intents:'),
 ('                    pend_sig, pend_exec = new_sig, new_exec',
  '                    self._pend_sig, self._pend_exec = new_sig, new_exec'),
 ('                    pend_intents, pend_prices = new_intents, new_prices',
  '                    self._pend_intents, self._pend_prices = new_intents, new_prices'),
 ('                pend_sig = pend_exec = None',
  '                self._pend_sig = self._pend_exec = None'),
 ('                pend_intents, pend_prices = [], {}',
  '                self._pend_intents, self._pend_prices = [], {}'),
 ('"signal_date": pend_sig,', '"signal_date": self._pend_sig,'),
 ('"execution_date": pend_exec,', '"execution_date": self._pend_exec,'),
 ('for o in pend_intents', 'for o in self._pend_intents'),
 ('for c, v in pend_prices.items()', 'for c, v in self._pend_prices.items()'),
 ('} if pend_sig is not None else None,', '} if self._pend_sig is not None else None,'),
]
missing = [a for a, b in pairs if a not in s]
assert not missing, missing
for a, b in pairs:
    s = s.replace(a, b)
open(p, 'w', encoding='utf-8').write(s)
print('scoped to instance attrs OK')