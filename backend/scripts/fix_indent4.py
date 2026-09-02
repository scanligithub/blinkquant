with open('backend/core/engine.py', 'rb') as f:
    content = f.read()

# Fix 1: blank line after self._set_cache[cache_key] = result_codes (indent 9 -> 8)
content = content.replace(
    b'self._set_cache[cache_key] = result_codes\r\n        \r\n        return result_codes',
    b'self._set_cache[cache_key] = result_codes\r\n        \r\n        return result_codes'
)

# Fix 2: blank line in docstring
content = content.replace(
    b'        """\r\n        \r\n        Returns:',
    b'        """\r\n        \r\n        Returns:'
)

with open('backend/core/engine.py', 'wb') as f:
    f.write(content)
print('Fixed')