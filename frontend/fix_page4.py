with open(r'E:\数据中台\blinkquant\frontend\src\app\page.tsx', 'r', encoding='utf-8') as f:
    content = f.read()

first_map_start = content.find('{clusterStatus?.nodes?.map((node: any, idx: number) => (')
second_map_start = content.find('{node.online ? (', first_map_start)
header_end = content.find('</header>', second_map_start)

# The section to replace is from the first map start to the header end
# But we want to keep the first map and fix the second part

# Let's extract the section we want to replace
# From the end of the first map (after `))}`) to the header end
first_map_end = content.find('))}\n            </div>', first_map_start)
if first_map_end == -1:
    first_map_end = content.find('))}\n            </div>', first_map_start)

print('First map end:', first_map_end)

# The second map starts with `{node.online ? (`
# It ends before the header
# Let's find the exact end of the second map
# It should end with `))}` before the closing divs

# Let's just replace from the second map start to the header end with a fixed version
# But we need to keep the first map and remove the duplicate second map

# Actually, the structure should be:
# 1. First map (node badges) - KEEP
# 2. Second map (node.online details) - REMOVE, merge into first map
# 3. Header end

# Let's extract the part we want to replace: from after first map to header end
replace_start = content.find('))}\n            </div>', first_map_start) + len('))}\n            </div>')
if replace_start < len('))}\n            </div>'):
    replace_start = content.find('\n            </div>\n            <div className="flex flex-wrap justify-center gap-3">', first_map_start)
    if replace_start != -1:
        replace_start += len('\n            </div>\n            <div className="flex flex-wrap justify-center gap-3">')

print('Replace start:', replace_start)

# Find the end of the section to replace (before </header>)
# The section ends with the closing of the outer divs before </header>
# Let's find the last `))}` before </header>
import re
# Find all `))}` positions after second_map_start
positions = [m.start() for m in re.finditer(r'\)\)\}', content[second_map_start:header_end])]
if positions:
    last_map_end = second_map_start + positions[-1] + 3  # include the `)}`
    print('Last map end in header:', last_map_end)
    # The section to replace is from the start of the second map container to last_map_end
    # But we need to find where the second map container starts
    # It starts with `<div className="flex flex-wrap justify-center gap-3">` before the second map
    container_start = content.rfind('<div className="flex flex-wrap justify-center gap-3">', first_map_start, second_map_start)
    if container_start == -1:
        container_start = content.rfind('<div className="flex flex-wrap justify-center gap-3">', 0, second_map_start)
    print('Container start:', container_start)

# Actually, let's just do a simple fix: remove the duplicate second map and fix the first map to include online details
# We'll replace from the container_start to header_end with our fixed version

# For now, let's just do a targeted fix on the broken ternary
# The issue is `) : null)}` should be `) : null)}`
# And the second map should be removed

# Let's do a simpler approach: fix the specific syntax error
# The error is at `) : null)}` - change to `) : null)}`
# But we also need to remove the second map entirely since it's a duplicate

# Let's just do a surgical fix: remove everything from the second map's container start to before </header>
# and replace with nothing (since we'll add online details to the first map)

# Find the container div for the second map
second_container_start = content.find('<div className="flex flex-wrap justify-center gap-3">', first_map_end)
if second_container_start == -1:
    # Try alternative
    second_container_start = content.find('<div className="flex flex-wrap justify-center gap-3">', first_map_start)

print('Second container start:', second_container_start)

# The section to replace is from second_container_start to header_end (but not including </header>)
# We'll replace it with nothing (empty string) since we'll add online details to first map

if second_container_start != -1 and header_end != -1:
    # Remove the duplicate second map section
    new_content = content[:second_container_start] + content[header_end:]
    with open(r'E:\数据中台\blinkquant\frontend\src\app\page.tsx', 'w', encoding='utf-8') as f:
        f.write(new_content)
    print('Removed duplicate second map section')
else:
    print('Could not locate sections to remove')