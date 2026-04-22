import sys

with open("services/tradier_service.py", "r") as f:
    content = f.read()

content = content.replace("acct_id = self._get_account_id()", "acct_id = getattr(self, 'account_id', None) or os.getenv('TRADIER_ACCOUNT_ID')")
content = content.replace("current_account_id = self._get_account_id()", "current_account_id = getattr(self, 'account_id', None) or os.getenv('TRADIER_ACCOUNT_ID')")

with open("services/tradier_service.py", "w") as f:
    f.write(content)
