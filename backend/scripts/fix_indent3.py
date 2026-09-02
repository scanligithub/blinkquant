with open('backend/core/engine.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

fixed = []
for i, line in enumerate(lines):
    stripped = line.lstrip()
    indent = len(line) - len(stripped)
    
    # Fix blank lines with indent 9 (should be 8 for method body)
    if stripped == '' and indent == 9:
        fixed.append('        \n')  # 8 spaces
        continue
    
    # Fix blank line before first method (indent 9 -> 0 or 4)
    if i > 0 and stripped == '' and indent == 9:
        prev_line = lines[i-1]
        prev_stripped = prev_line.lstrip()
        prev_indent = len(prev_line) - len(prev_stripped)
        if prev_indent == 4 and prev_stripped.startswith('#'):
            fixed.append('\n')  # Class-level blank line
            continue
    
    fixed.append(line)

with open('backend/core/engine.py', 'w', encoding='utf-8') as f:
    f.writelines(fixed)
print('Fixed indent 9 blank lines')