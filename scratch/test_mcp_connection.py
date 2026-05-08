
import json
import subprocess
import os

def test_mcp():
    # Load env vars from .env
    env = os.environ.copy()
    with open(".env", "r") as f:
        for line in f:
            if "=" in line and not line.startswith("#"):
                key, val = line.strip().split("=", 1)
                env[key] = val.strip('"')

    # Start the MCP server
    process = subprocess.Popen(
        ["python3", "-m", "hermes.mcp.server"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env
    )

    # 1. Initialize
    init_req = {
        "jsonrpc": "2.0",
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test-client", "version": "1.0.0"}
        },
        "id": 0
    }
    process.stdin.write(json.dumps(init_req) + "\n")
    process.stdin.flush()
    
    # Read init response
    try:
        init_res = process.stdout.readline()
        print("Init Response:", init_res)

        # 2. Call tool: get_account_balances
        call_req = {
            "jsonrpc": "2.0",
            "method": "tools/call",
            "params": {
                "name": "get_account_balances",
                "arguments": {}
            },
            "id": 1
        }
        process.stdin.write(json.dumps(call_req) + "\n")
        process.stdin.flush()

        # Read call response
        call_res = process.stdout.readline()
        print("Call Response:", call_res)
    except BrokenPipeError:
        print("Server crashed.")
        stderr = process.stderr.read()
        print("Stderr:", stderr)

    # Clean up
    process.terminate()

if __name__ == "__main__":
    test_mcp()
