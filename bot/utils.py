import logging
import re
from datetime import datetime

class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def is_match(pos, target):
    """
    Check if a position matches a target symbol.
    - Equity: AAPL
    - Option: AAPL230616C00150000 or similar
    """
    symbol = pos.get('symbol', '')
    if symbol == target:
        return True
    
    # Try explicit underlying field if available
    if pos.get('underlying') == target:
        return True
        
    # Regex for Option Symbol: [ROOT][YYMMDD][CP][STRIKE]
    # Root can be 1-6 chars. Strike usually 8 digits, but tests might use less.
    match = re.match(r'^([A-Z]+)\d{6}[CP]\d+', symbol)
    if match and match.group(1) == target:
        return True
        
    # Absolute Fallback: starts with target and followed by digit
    if symbol.startswith(target):
         suffix = symbol[len(target):]
         if suffix and suffix[0].isdigit():
             return True
             
    return False

def get_op_type(pos):
    """Determine if a position is a PUT or CALL."""
    # Use explicit field if exists
    otype = pos.get('option_type', '').lower()
    if otype in ['put', 'call']:
        return otype
        
    symbol = pos.get('symbol', '')
    # Match YYMMDD followed by C or P
    match = re.search(r'\d{6}([CP])', symbol)
    if match:
        return 'call' if match.group(1) == 'C' else 'put'
    return None

def get_underlying(symbol):
    """Extract underlying symbol from an option symbol."""
    # Pattern: [A-Z]{1,6}\d{6}[CP]\d+
    match = re.match(r'^([A-Z]+)\d{6}[CP]', symbol)
    if match:
        return match.group(1)
    return symbol

def get_expiry_str(symbol):
    """Extract expiry string (YYYY-MM-DD) from an option symbol."""
    match = re.search(r'(\d{6})[CP]', symbol)
    if match:
        date_part = match.group(1)
        try:
            return datetime.strptime(date_part, "%y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None
