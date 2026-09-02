with open('backend/core/engine.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

fixed = []
in_method = False
method_indent = 0

for i, line in enumerate(lines):
    stripped = line.lstrip()
    indent = len(line) - len(stripped)
    
    # Detect method definition
    if indent == 4 and stripped.startswith('def '):
        in_method = True
        method_indent = 4
        fixed.append(line)
        continue
    
    # Inside a method
    if in_method:
        # Blank line with wrong indent (1 space)
        if stripped == '' and indent == 1:
            fixed.append('        \n')  # 8 spaces for method body blank line
            continue
        # Method body continuation
        if indent == 1 and stripped:
            # This shouldn't happen - method body should be at least 8 spaces
            fixed.append('        ' + stripped + '\n')
            continue
        # End of method - next method or class level
        if stripped.startswith('def ') and indent == 4:
            in_method = False
            fixed.append(line)
            continue
        elif stripped.startswith('class ') and indent == 0:
            in_method = False
            fixed.append(line)
            continue
        # Normal line in method body
        fixed.append(line)
        continue
    
    # Not in method
    fixed.append(line)

with open('backend/core/engine.py', 'w', encoding='utf-8') as f:
    f.writelines(fixed)
print('Fixed method blank lines')