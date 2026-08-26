# -*- coding: utf-8 -*-
p = 'core/backtest_engine.py'
s = open(p, encoding='utf-8').read()
pairs = [
 ('            pend_sig, pend_exec = ps, pe\n            pend_intents = [OrderIntent(**o) for o in st_pending.get("intents", [])]\n            pend_prices = st_pending.get("prices", {})',
  '            self._pend_sig, self._pend_exec = ps, pe\n            self._pend_intents = [OrderIntent(**o) for o in st_pending.get("intents", [])]\n            self._pend_prices = st_pending.get("prices", {})'),
 ('                intent_codes = [i.code for i in pend_intents]',
  '                intent_codes = [i.code for i in self._pend_intents]'),
]
missing = [a for a, b in pairs if a not in s]
assert not missing, missing
for a, b in pairs:
    s = s.replace(a, b)
open(p, 'w', encoding='utf-8').write(s)
print('restore+exec scoped OK')