import sys
import re
from datetime import datetime, date
from dotenv import load_dotenv

load_dotenv()
from services.tradier_service import TradierService
from bot.utils import get_op_type, get_expiry_str, get_underlying

tradier = TradierService()
try:
    positions = tradier.get_positions() or []
except Exception as e:
    print(f"Error getting positions: {e}")
    sys.exit(1)

riot_positions = [p for p in positions if 'RIOT' in p.get('symbol', '')]

if not riot_positions:
    print("No RIOT positions found.")
    sys.exit(0)

today = date.today()
print(f"Today is {today}")

for p in riot_positions:
    symbol = p.get('symbol')
    if symbol == 'RIOT':
        print(f"Stock: {symbol} Qty: {p.get('quantity')}")
        continue
        
    underlying = get_underlying(symbol)
    qty = p.get('quantity')
    cb = p.get('cost_basis')
    op_type = get_op_type(p)
    
    match = re.match(r'^([A-Z]+)(\d{6})[CP](\d{8})$', symbol)
    if not match:
        continue
        
    strike = float(match.group(3)) / 1000.0
    expiry_str = get_expiry_str(symbol)
    expiry_date = datetime.strptime(expiry_str, "%Y-%m-%d").date()
    dte = (expiry_date - today).days
    
    quote = tradier.get_quote(symbol)
    ask_price = float(quote.get('ask', 0)) if quote else 0
    
    und_quote = tradier.get_quote(underlying)
    current_price = float(und_quote.get('last', 0)) if und_quote else 0
    
    is_itm = (op_type == 'put' and current_price < strike) or \
             (op_type == 'call' and current_price > strike)
             
    entry_price = abs(cb) / (abs(qty) * 100) if abs(qty) > 0 else 0
    profit_pct = (entry_price - ask_price) / entry_price if entry_price > 0 else 0
    
    print(f"Option: {symbol} Qty: {qty} Strike: {strike} Expiry: {expiry_str} DTE: {dte}")
    print(f"  Current stock price: {current_price}")
    print(f"  Is ITM: {is_itm}")
    print(f"  Entry Price: {entry_price:.2f}, Current Ask: {ask_price:.2f}, Profit: {profit_pct*100:.1f}%")
    
    if profit_pct >= 0.8:
        print("  -> Would trigger Take Profit (>= 80%)")
    
    if dte <= 7:
        if is_itm:
            print("  -> Would trigger ROLL (DTE <= 7 and ITM)")
        else:
            print("  -> DTE <= 7 but OTM, expire worthless")
    else:
        print(f"  -> NO ROLL (DTE {dte} > 7)")

