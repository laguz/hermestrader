import sys
import os
sys.path.append(os.getcwd())
from app import app

print(app.url_map)
