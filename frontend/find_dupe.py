with open(r'E:\数据中台\blinkquant\frontend\src\app\page.tsx', 'r', encoding='utf-8') as f:
    content = f.read()

# Search for the second map container
# Look for "flex flex-wrap justify-center gap-3" after the first map
first_map_end = content.find('))}\n            </div>', 26513)
print('First map end:', first_map_end)

# Search for the second container div
search_text = 'flex flex-wrap justify-center gap-3'
pos = content.find(search_text, first_map_end)
while pos != -1:
    print('Found at:', pos)
    # Show context
    context = content[max(0,pos-50):pos+100]
    print('Context:', repr(context))
    print('---')
    pos = content.find(search_text, pos + 1)

# Also check for the second map directly
second_map = content.find('{clusterStatus?.nodes?.map((node: any, idx: number) => (', first_map_end)
print('\nSecond map at:', second_map)

# Find the broken ternary
broken = content.find(') : null)}', first_map_end)
print('Broken ternary at:', broken)
if broken != -1:
    context = content[max(0,broken-100):broken+100]
    print('Broken context:', repr(context))