import requests

# Stop
try:
    requests.post('http://localhost:8080/api/bot/stop')
    print("Sent STOP signal")
except Exception as e:
    print(e)
    
# Start
try:
    requests.post('http://localhost:8080/api/bot/start')
    print("Sent START signal")
except Exception as e:
    print(e)

