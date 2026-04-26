from unittest.mock import MagicMock

# We do not need to mock sys modules if we avoid importing Container!
from bot.strategies.wheel import WheelStrategy

tradier_mock = MagicMock()
tradier_mock.get_positions.return_value = [
    # A short put (1 lot)
    {'symbol': 'AAPL260515P00150000', 'quantity': -1},
    # A put credit spread (1 lot = 1 short, 1 long)
    {'symbol': 'MSFT260515P00400000', 'quantity': -1},
    {'symbol': 'MSFT260515P00390000', 'quantity': 1},
    # 2-lot call credit spread
    {'symbol': 'TSLA260515C00200000', 'quantity': -2},
    {'symbol': 'TSLA260515C00210000', 'quantity': 2},
]
tradier_mock.get_orders.return_value = []

db_mock = MagicMock()
analysis_mock = MagicMock()

strategy = WheelStrategy(tradier_mock, db_mock, dry_run=True, analysis_service=analysis_mock)

exclusions, counts = strategy._check_expiry_constraints('AAPL')
print("AAPL constraints:", counts)
assert counts.get('2026-05-15', 0) == 1, f"Expected 1, got {counts}"

exclusions, counts = strategy._check_expiry_constraints('MSFT')
print("MSFT constraints:", counts)
assert counts.get('2026-05-15', 0) == 1, f"Expected 1, got {counts}"

exclusions, counts = strategy._check_expiry_constraints('TSLA')
print("TSLA constraints:", counts)
assert counts.get('2026-05-15', 0) == 2, f"Expected 2, got {counts}"

print("All assertions passed!")
