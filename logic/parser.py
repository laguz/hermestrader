import pandas as pd
import logging

logger = logging.getLogger(__name__)

def extract_financials(facts_json):
    """
    Extracts key historical financial metrics from the SEC Company Facts JSON.
    Returns a DataFrame with columns: Year, Revenue, NetIncome, EPS, Equity, LongTermDebt, Cash, OperatingCashFlow.
    """
    if not facts_json or 'facts' not in facts_json or 'us-gaap' not in facts_json['facts']:
        logger.error("Invalid facts JSON structure")
        return None
        
    us_gaap = facts_json['facts']['us-gaap']
    
    # Helper to extract a specific concept series, returning both Value and Date
    def get_concept(concept_name, taxonomy='us-gaap'):
        if taxonomy not in facts_json['facts'] or concept_name not in facts_json['facts'][taxonomy]:
            return pd.DataFrame()
        
        units = facts_json['facts'][taxonomy][concept_name]['units']
        if not units:
            return pd.DataFrame()
            
        key = list(units.keys())[0] 
        data = units[key]
        
        clean_data = []
        for entry in data:
            if entry.get('fp') == 'FY' or (entry.get('form') == '10-K'):
                clean_data.append({
                    'Year': entry['fy'],
                    'Value': entry['val'],
                    'Date': entry['end']
                })
        
        df = pd.DataFrame(clean_data)
        if df.empty:
            return pd.DataFrame()
        
        df = df.sort_values(by=['Year', 'Date'])
        df = df.drop_duplicates(subset=['Year'], keep='last')
        df = df.set_index('Year')
        return df[['Value', 'Date']]

    def get_val(concept_name, taxonomy='us-gaap'):
        df = get_concept(concept_name, taxonomy)
        return df['Value'] if not df.empty else pd.Series(dtype='float64')

    # Revenue
    revenue = get_val('Revenues')
    if revenue.empty:
        revenue = get_val('SalesRevenueNet')
    if revenue.empty:
        revenue = get_val('RevenueFromContractWithCustomerExcludingAssessedTax')

    # Net Income
    net_income = get_val('NetIncomeLoss')

    # EPS (Diluted)
    # We want dates from EPS for split adjustment matching
    eps_df = get_concept('EarningsPerShareDiluted')
    eps = eps_df['Value'] if not eps_df.empty else pd.Series(dtype='float64')
    # Use EPS dates as primary dates
    dates = eps_df['Date'] if not eps_df.empty else pd.Series(dtype='object')
    
    # Book Value / Equity
    equity = get_val('StockholdersEquity')
    if equity.empty:
         equity = get_val('StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest')

    # Long Term Debt
    lt_debt = get_val('LongTermDebt')
    if lt_debt.empty:
        lt_debt = get_val('LongTermDebtNoncurrent')

    # Cash
    cash = get_val('CashAndCashEquivalentsAtCarryingValue')
    
    # OCF
    ocf = get_val('NetCashProvidedByUsedInOperatingActivities')
    
    # Shares Outstanding (for BVPS calc and Split checks)
    shares = get_val('CommonStockSharesOutstanding')
    if shares.empty:
        shares = get_val('EntityCommonStockSharesOutstanding') # DEI taxonomy usually

    # Combine
    df = pd.DataFrame({
        'Revenue': revenue,
        'NetIncome': net_income,
        'EPS': eps,
        'Equity': equity,
        'LongTermDebt': lt_debt,
        'Cash': cash,
        'OCF': ocf,
        'Shares': shares,
        'FilingDate': dates
    })
    
    # Propagate dates if missing from EPS?
    # If EPS is missing, we might have dates from others.
    # But usually EPS is critical. Rule #1 fails without EPS.
    
    df = df.sort_index(ascending=True)
    return df
