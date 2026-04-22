from werkzeug.utils import secure_filename
symbols = ["AAPL", "BRK.B", "BRK/B", "../../../etc/passwd"]

for s in symbols:
    print(f"{s} -> {secure_filename(s)}")
