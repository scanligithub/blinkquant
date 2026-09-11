with open(r'E:\数据中台\blinkquant\frontend\src\app\page.tsx', 'r', encoding='utf-8') as f:
    content = f.read()

import re

# Find all occurrences of the pattern ) : null)}
for m in re.finditer(r'\)\s*:\s*null\}\)', content):
    start = max(0, m.start()-50)
    end = min(len(content), m.end()+50)
    print('Position {}: ...{}...'.format(m.start(), repr(content[start:end])))
    print()

# Also check for the reverse - correct pattern
for m in re.finditer(r'\)\s*:\s*null\}\)', content):
    start = max(0, m.start()-50)
    end = min(len(content), m.end()+50)
    print('Correct pattern at {}: ...{}...'.format(m.start(), repr(content[start:end])))
    print()