from services.container import Container
from services.analysis_service import AnalysisService
from dotenv import load_dotenv
import os

load_dotenv()

db = Container.get_db()
tradier = Container.get_tradier_service()
ml = Container.get_ml_service()

if db is not None:
    print("Connected to DB:", db.name)
    
    # Instantiate Service with DB
    import sys
    import services.analysis_service
    print(f"DEBUG: AnalysisService module file: {services.analysis_service.__file__}")
    print(f"DEBUG: sys.path: {sys.path}")
    service = AnalysisService(tradier, ml, db)
    
    print("Analyzing SPY...")
    result = service.analyze_symbol('SPY', period='3m')
    
    print("Analysis done. Checking DB...")
    entries = list(db.entries.find({'symbol': 'SPY'}))
    print(f"Entries count for SPY: {len(entries)}")
    if entries:
        print("Success! Entry found.")
        print(entries[0].keys())
    else:
        print("Failure. No entry found.")

else:
    print("Could not connect to DB.")
