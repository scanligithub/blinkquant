with open(r'E:\数据中台\blinkquant\frontend\src\app\page.tsx', 'r', encoding='utf-8') as f:
    content = f.read()

# Find the header section
header_start = content.find('<header')
print('Header start at:', header_end if 'header_end' in dir() else 'N/A')

idx = content.find('<header')
if idx != -1:
    print('Header start at:', idx)
    # Show from header start to header end
    header_end = content.find('</header>', idx)
    print('Header end at:', header_end)
    if header_end != -1:
        print('Header content:')
        print(repr(content[idx:header_end+8]))

# Also check the area around the first map
map_idx = content.find('{clusterStatus?.nodes?.map((node: any, idx: number) => (')
if map_idx != -1:
    print('\nMap at:', map_idx)
    # Show from map to 500 chars after
    print(repr(content[map_idx:map_idx+500]))