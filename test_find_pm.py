import sys
import inspect
from bot.portfolio_manager import PortfolioManager

print("Methods in PortfolioManager:")
for name, method in inspect.getmembers(PortfolioManager, predicate=inspect.isfunction):
    print(name)
