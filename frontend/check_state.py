with open(r'E:\数据中台\blinkquant\frontend\src\app\page.tsx', 'r', encoding='utf-8') as f:
    content = f.read()

# Find the cluster status section
idx = content.find('clusterStatus')
if idx != -1:
    print('clusterStatus at:', idx)
    print(repr(content[idx:idx+200]))

# Find the map
idx = content.find('nodes?.map')
while idx != -1:
    print('nodes?.map at:', idx)
    idx = content.find('nodes?.map', idx+1)

# Find </header>
idx = content.find('</header>')
print('</header> at:', idx)

# Show the end of the file
print('\nLast 500 chars:')
print(repr(content[-500:]))