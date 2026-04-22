from services.container import Container

tradier = Container.get_tradier_service()
positions = tradier.get_positions()
riot_positions = [p for p in positions if 'RIOT' in p.get('symbol', '')]

print("RIOT Positions:")
for p in riot_positions:
    print(f"- {p.get('symbol')} | Qty: {p.get('quantity')} | Cost Basis: {p.get('cost_basis')}")

