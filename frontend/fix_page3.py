with open(r'E:\数据中台\blinkquant\frontend\src\app\page.tsx', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace by position - from first map to before the second map
first_map_start = content.find('{clusterStatus?.nodes?.map((node: any, idx: number) => (')
second_map_start = content.find('{node.online ? (', first_map_start)

# Find the end of the second map (the `))}` that closes it)
# Search for the pattern that ends the second map
search_start = content.find(') : null)}', first_map_start)
if search_start == -1:
    search_start = content.find(') : null)}', first_map_start)

# Actually, let's find the end of the second map by looking for the closing of the outer divs
# The second map ends with `))}` followed by `            </div>` (the header end)
# Let's find the end of the header section

# Find the end of the header - look for `</header>` after the second map
header_end = content.find('</header>', second_map_start)
if header_end == -1:
    print('Header end not found')
else:
    print('Header ends at', header_end)

# The problematic section is from first_map_start to header_end
# But we want to replace from first_map_start to just before the second map's broken part
# Actually, let's just replace from the first map to the end of the second map

# Find the end of the second map - it should end with `))}` followed by `</div>` (closing the outer div)
# Let's find the `))}` that's followed by `</div>` and then `</header>`

# Let's just replace the whole section from first_map_start to the end of the second map
# The second map ends with `))}` followed by whitespace and then `            </div>` (line 744)

# Find the end of the second map - look for `))}` followed by newlines and `            </div>`
import re
match = re.search(r'\)\)\}\s*\n\s*</div>\s*\n\s*</header>', content[second_map_start:])
if match:
    second_map_end = second_map_start + match.end()
    print('Second map ends at', second_map_end)
else:
    print('Pattern not found')

# Let's just do a more surgical fix - replace the broken ternary part
# The issue is specifically at `) : null)}` which should be `) : null)}`
# But the real issue is the second map shouldn't exist at all

print('First map starts at:', first_map_start)
print('Second map starts at:', second_map_start)