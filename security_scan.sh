#!/bin/bash
echo "--- Security Scan ---"

echo "1. Checking for hardcoded secrets..."
grep -rEwi 'password|secret|key|token' \
  --include=\*.py \
  --exclude-dir=venv \
  --exclude-dir=.git | grep -v 'os.getenv' | grep -v 'request.form' | grep -v '@' | head -n 20

echo -e "\n2. Checking for eval/exec usage..."
grep -rnEwi 'eval\(|exec\(' --include=\*.py --exclude-dir=venv .

echo -e "\n3. Checking for subprocess usages (Command Injection risk)..."
grep -rnEwi 'subprocess\.|os\.system|os\.popen' --include=\*.py --exclude-dir=venv .

echo -e "\n4. Checking for raw template injection risks (rendering raw HTML)..."
grep -rn ' | safe' templates/

echo -e "\n5. Checking MongoDB queries for NoSQL Injection patterns..."
grep -rn 'find(' services/ | head -n 10
grep -rn 'update(' services/ | head -n 10
