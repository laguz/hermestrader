import sys
import os
sys.path.insert(0, os.path.abspath('.'))

from exceptions import AppError
from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/test')
def test():
    err = AppError("Test message", status_code=400)
    return jsonify(err.to_dict()), err.status_code

with app.test_client() as client:
    response = client.get('/test')
    print(f"Status: {response.status_code}")
    print(f"Data: {response.get_json()}")
