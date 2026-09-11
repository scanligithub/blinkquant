with open(r'E:\数据中台\blinkquant\frontend\src\app\page.tsx', 'r', encoding='utf-8') as f:
    content = f.read()

# Find the first map
first_map = content.find('{clusterStatus?.nodes?.map((node: any, idx: number) => (')
print('First map at:', first_map)

# Find the end of the first map (the `))}` that closes it)
first_map_end = content.find('))}\n            </div>', first_map)
if first_map_end == -1:
    first_map_end = content.find('))}\n            </div>', first_map)
print('First map end at:', first_map_end)

# The duplicate section starts with the container div for the second map
# It starts with `<div className="flex flex-wrap justify-center gap-3">` after the first map
second_container = content.find('<div className="flex flex-wrap justify-center gap-3">', first_map_end)
print('Second container at:', second_container)

# The header ends at </header>
header_end = content.find('</header>', second_container if second_container != -1 else first_map_end)
print('Header end at:', header_end)

# The duplicate section is from second_container to header_end
# We want to remove everything from second_container to header_end
# But we need to keep the </header> tag

if second_container != -1 and header_end != -1:
    # Remove the duplicate section
    new_content = content[:second_container] + content[header_end:]
    with open(r'E:\数据中台\blinkquant\frontend\src\app\page.tsx', 'w', encoding='utf-8') as f:
        f.write(new_content)
    print('Removed duplicate section')
else:
    print('Could not locate sections')