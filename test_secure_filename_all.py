from werkzeug.utils import secure_filename
import re

symbols = ["AAPL", "BRK.B", "BRK/B", "../../../etc/passwd"]

for s in symbols:
    wz = secure_filename(s)
    rx = re.sub(r'[^A-Z0-9_\-\.]', '_', s.upper())
    print(f"{s} -> WZ: {wz} | RX: {rx}")
