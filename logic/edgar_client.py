import requests
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SEC requires a proper User-Agent
HEADERS = {
    'User-Agent': 'Rule1App/1.0 (contact@example.com)',
    'Accept-Encoding': 'gzip, deflate',
    'Host': 'data.sec.gov'
}

def get_cik_from_ticker(ticker):
    """
    Fetches the CIK for a given ticker symbol using the SEC's company_tickers.json.
    """
    url = "https://www.sec.gov/files/company_tickers.json"
    logger.info(f"Fetching CIK mapping from {url}")
    
    try:
        response = requests.get(url, headers={'User-Agent': HEADERS['User-Agent']}) # Headers on sec.gov main site are also strict
        response.raise_for_status()
        data = response.json()
        
        ticker = ticker.upper()
        
        # Structure is {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}, ...}
        for _, entry in data.items():
            if entry['ticker'] == ticker:
                # CIK must be 10 digits, padded with leading zeros
                return str(entry['cik_str']).zfill(10)
        
        logger.warning(f"Ticker {ticker} not found in SEC mapping.")
        return None
        
    except Exception as e:
        logger.error(f"Error fetching CIK mapping: {e}")
        return None

def get_company_facts(ticker):
    """
    Fetches the 'Company Facts' JSON for a given ticker from SEC EDGAR.
    """
    cik = get_cik_from_ticker(ticker)
    if not cik:
        return None
    
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    logger.info(f"Fetching company facts for {ticker} (CIK: {cik}) from {url}")
    
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Error fetching company facts for {ticker}: {e}")
        return None
