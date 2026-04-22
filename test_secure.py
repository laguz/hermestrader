import re

def sanitize_filename(filename: str) -> str:
    """Sanitize filename by replacing unsafe characters with underscore."""
    return re.sub(r'[^a-zA-Z0-9_\-]', '_', filename)

print(sanitize_filename("AAPL"))
print(sanitize_filename("BRK.B"))
print(sanitize_filename("BRK/B"))
print(sanitize_filename("../../../etc/passwd"))
