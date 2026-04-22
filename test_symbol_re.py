import re
print(re.sub(r'[^A-Z0-9_\-]', '_', "AAPL"))
print(re.sub(r'[^A-Z0-9_\-]', '_', "BRK.B"))
print(re.sub(r'[^A-Z0-9_\-]', '_', "BRK/B"))
print(re.sub(r'[^A-Z0-9_\-]', '_', "../../../etc/passwd"))
