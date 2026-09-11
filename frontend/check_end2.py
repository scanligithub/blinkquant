with open(r'E:\数据中台\blinkquant\frontend\src\app\page.tsx', 'r', encoding='utf-8') as f:
    content = f.read()

# Find the exact end of the component
# Look for the export default function Home() and its closing
idx = content.rfind('export default function Home')
print('Function start at:', idx)

# Find the end of the function
func_end = content.rfind('}')
print('Last } at:', content.rfind('}'))

# Check the last 50 lines
lines = content.split('\n')
for i, line in enumerate(lines[-30:]):
    print('{}: {}'.format(len(lines)-30+i, line))

# Also check brace/paren balance line by line near the end
print('\n--- Checking last 50 lines for balance ---')
brace_count = 0
paren_count = 0
bracket_count = 0
in_string = False
string_char = None

for i, line in enumerate(lines[-50:]):
    line_num = len(lines) - 50 + i + 1
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
    
    if paren_count < 0:
        print('Line {}: Negative paren count ({})'.format(line_num, paren_count))
        print('  {}'.format(line.rstrip()))
    if brace_count < 0:
        print('Line {}: Negative brace count ({})'.format(line_num, brace_count))
        print('  {}'.format(line.rstrip()))
    if bracket_count < 0:
        print('Line {}: Negative bracket count ({})'.format(line_num, bracket_count))
        print('  {}'.format(line.rstrip()))
    print('Line {}: braces={}, parens={}, brackets={} | {}'.format(len(lines)-50+i+1, brace_count, paren_count, bracket_count, line.rstrip()[:80]))