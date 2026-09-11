with open(r'E:\数据中台\blinkquant\frontend\src\app\page.tsx', 'r', encoding='utf-8') as f:
    content = f.read()

# Check what's between first_map_end (28444) and broken ternary (29883)
print('Content between 28444 and 29883:')
print(repr(content[28444:29883]))
print('---')

# Also check what's at the header end
header_end = content.find('</header>')
print('Header end:', content.find('</header>'))

# Check what's after the first map end
print('\nAfter first map end (28444):')
print(repr(content[28444:29000]))