with open(r'E:\数据中台\blinkquant\frontend\src\app\page.tsx', 'r', encoding='utf-8') as f:
    content = f.read()

# The issue is that after the first map ends at 28444, there's orphaned JSX
# that references `node` but is outside the map callback.
# We need to move the `node.online` conditional INSIDE the map callback.

# Find the first map start
first_map_start = content.find('{clusterStatus?.nodes?.map((node: any, idx: number) => (')
print('First map start:', first_map_start)

# Find the end of the map callback (where `))}` closes the map)
# The map callback should end with the closing of the JSX fragment
map_end = content.find('))}\n            </div>', 26513)
print('Map end at:', map_end)

# The problematic content is between map_end and the header
# We need to move the {node.online ? ... : null} INSIDE the map callback

# Let's find the exact structure by reading from map start to header
header_end = content.find('</header>')
map_start = content.find('{clusterStatus?.nodes?.map((node: any, idx: number) => (')
header_end = content.find('</header>')

# Extract the entire header section
header_section = content[map_start:header_end]

# The fix: we need to insert the node.online conditional INSIDE the map callback
# Currently the structure is:
# map(...) => (
#   <div>badge</div>
#   {status === running && ...}
# ))
# {node.online ? ... : null}  <- ORPHANED!
#
# Should be:
# map(...) => (
#   <>
#     <div>badge</div>
#     {status === running && ...}
#     {node.online && <div>...</div>}
#   </>
# )

# Let's do a targeted fix: replace the orphaned {node.online ? ... : null} with nothing
# and add {node.online && ...} inside the map callback

# First, remove the orphaned section after the map
# It starts right after the map's closing `))}\n            </div>`
# and ends before the next major section

map_end_pos = content.find('))}\n            </div>', 26513)
print('Map end at:', map_end)

# The orphaned section starts right after the map's closing div
orphan_start = content.find('))}\n            </div>\n                  {node.online ? (', 28444)
print('Orphan start:', orphan_start)

# The orphaned section ends before the header
header_end = content.find('</header>')
orphan_end = content.find('</header>', 28444)
print('Header end:', orphan_end)

# Let's do a simple fix: remove the orphaned section entirely
# The node.online details will be lost from the header but the map will work
# We can add them back inside the map callback later

if orphan_start != -1:
    # Find the exact end of the orphaned section
    # It ends at the header
    header_pos = content.find('</header>', 28444)
    if orphan_start != -1:
        new_content = content[:orphan_start] + content[orphan_end:]
        with open(r'E:\数据中台\blinkquant\frontend\src\app\page.tsx', 'w', encoding='utf-8') as f:
            f.write(new_content)
        print('Removed orphaned section')
    else:
        print('Could not find header end')
else:
    print('Could not find orphan start')