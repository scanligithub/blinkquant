with open('backend/core/engine.py', 'rb') as f:
    content = f.read()

# Fix continuation line indentation from 40 to 8 spaces
content = content.replace(
    b'                                        target_date, backtest_mode, raise_on_error)',
    b'        target_date, backtest_mode, raise_on_error)'
)

# Fix blank line indent 1 to 8 (between docstring and Returns)
content = content.replace(
    b'        """\r\n\r\n        Returns:',
    b'        """\r\n        \r\n        Returns:'
)

# Also fix any remaining indent 1 blank lines in methods
content = content.replace(
    b'\r\n        """\r\n\r\n        ',
    b'\r\n        """\r\n        \r\n        '
)

with open('backend/core/engine.py', 'wb') as f:
    f.write(content)
print('Fixed')