import os
import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)

# SEC requires a proper User-Agent with contact info
SEC_USER_AGENT = os.getenv(
    'SEC_USER_AGENT',
    'Rule1App/1.0 (contact@example.com)'
)

HEADERS = {
    'User-Agent': SEC_USER_AGENT,
    'Accept-Encoding': 'gzip, deflate',
    'Host': 'data.sec.gov'
}

# Default timeout for SEC API requests
REQUEST_TIMEOUT = 10


def get_cik_from_ticker(ticker: str) -> Optional[str]:
    """Fetch the CIK for a given ticker symbol using the SEC's company_tickers.json."""
    url = "https://www.sec.gov/files/company_tickers.json"
    logger.info(f"Fetching CIK mapping from {url}")
    
    try:
        response = requests.get(
            url,
            headers={'User-Agent': SEC_USER_AGENT},
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()
        
        ticker = ticker.upper()
        
        for _, entry in data.items():
            if entry['ticker'] == ticker:
                return str(entry['cik_str']).zfill(10)
        
        logger.warning(f"Ticker {ticker} not found in SEC mapping.")
        return None
        
    except Exception as e:
        logger.error(f"Error fetching CIK mapping: {e}")
        return None


def get_company_facts(ticker: str) -> Optional[dict]:
    """Fetch the 'Company Facts' JSON for a given ticker from SEC EDGAR."""
    cik = get_cik_from_ticker(ticker)
    if not cik:
        return None
    
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    logger.info(f"Fetching company facts for {ticker} (CIK: {cik}) from {url}")
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Error fetching company facts for {ticker}: {e}")
        return None
