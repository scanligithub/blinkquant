with open(r'E:\数据中台\blinkquant\frontend\src\app\page.tsx', 'r', encoding='utf-8') as f:
    content = f.read()

header_end = content.find('</header>')
remaining = content[header_end:]

# Find where the negative counts occur
brace_count = 0
paren_count = 0
bracket_count = 0
in_string = False
string_char = None

for i, char in enumerate(remaining):
    if in_string:
        if char == string_char:
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
        if brace_count < 0:
            print('Negative brace at position {}: ...{}...'.format(i, repr(remaining[max(0,i-50):i+50])))
            break
    elif char == '(':
        paren_count += 1
    elif char == ')':
        paren_count -= 1
        if paren_count < 0:
            print('Negative paren at position {}: ...{}...'.format(i, repr(remaining[max(0,i-50):i+50])))
            break
    elif char == '[':
        bracket_count += 1
    elif char == ']':
        bracket_count -= 1
        if bracket_count < 0:
            print('Negative bracket at position {}: ...{}...'.format(i, repr(remaining[max(0,i-50):i+50])))
            break