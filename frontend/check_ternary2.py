with open(r'E:\数据中台\blinkquant\frontend\src\app\page.tsx', 'r', encoding='utf-8') as f:
    content = f.read()

# Search for the exact string
search_str = ') : null)}'
count = content.count(search_str)
print('Count of "{}": {}'.format(search_str, count))

# Find positions
start = 0
while True:
    pos = content.find(search_str, start)
    if pos == -1:
        break
    context = content[max(0,pos-30):pos+30]
    print('Position {}: ...{}...'.format(pos, repr(context)))
    start = pos + 1

# Also check for the correct pattern
search_str2 = ') : null)}'
count2 = content.count(search_str2)
print('\nCount of "{}": {}'.format(search_str2, count2))