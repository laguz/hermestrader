import re

symbols = ["AAPL", "BRK.B", "BRK/B", "../../../etc/passwd"]

for s in symbols:
    sanitized = re.sub(r'[^A-Z0-9]', '_', s.upper())
    print(f"{s} -> {sanitized}")
