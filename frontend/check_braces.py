with open(r'E:\数据中台\blinkquant\frontend\src\app\page.tsx', 'r', encoding='utf-8') as f:
    lines = f.readlines()

brace_count = 0
paren_count = 0
bracket_count = 0
in_string = False
string_char = None

for i, line in enumerate(lines):
    for j, char in enumerate(line):
        if in_string:
            if char == string_char and (j == 0 or line[j-1] != '\\'):
                in_string = False
            continue
        if char in ('"', "'", '`'):
            in_string = True
            string_char = char
            continue
        if char == '{':
            brace_count += 1
        elif char == '}':
            brace_count -= 1
        elif char == '(':
            paren_count += 1
        elif char == ')':
            paren_count -= 1
        elif char == '[':
            bracket_count += 1
        elif char == ']':
            bracket_count -= 1
    
    if brace_count < 0:
        print('Line {}: Negative brace count ({})'.format(i+1, brace_count))
        print('  {}'.format(line.rstrip()))
        break
    if paren_count < 0:
        print('Line {}: Negative paren count ({})'.format(i+1, paren_count))
        break
    if bracket_count < 0:
        print('Line {}: Negative bracket count ({})'.format(i+1, bracket_count))
        break

print('Final counts - braces: {}, parens: {}, brackets: {}'.format(brace_count, paren_count, bracket_count))