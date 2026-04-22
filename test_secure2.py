from werkzeug.utils import secure_filename
print(secure_filename("AAPL"))
print(secure_filename("BRK.B"))
print(secure_filename("BRK/B"))
print(secure_filename("../../../etc/passwd"))
