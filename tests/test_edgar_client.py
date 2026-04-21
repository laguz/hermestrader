import pytest
from unittest.mock import patch, Mock
import sys

def test_get_company_facts_happy_path():
    mock_requests = Mock()
    with patch.dict('sys.modules', {'requests': mock_requests}):
        from logic.edgar_client import get_company_facts

        with patch('logic.edgar_client.get_cik_from_ticker') as mock_get_cik, \
             patch('logic.edgar_client.requests.get') as mock_get:

            mock_get_cik.return_value = '0000320193'

            mock_response = Mock()
            mock_response.json.return_value = {'facts': {'us-gaap': {}}}
            mock_get.return_value = mock_response

            result = get_company_facts('AAPL')

            assert result == {'facts': {'us-gaap': {}}}
            mock_get_cik.assert_called_once_with('AAPL')
            mock_get.assert_called_once()

            args, kwargs = mock_get.call_args
            assert args[0] == "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json"
            mock_response.raise_for_status.assert_called_once()

def test_get_company_facts_no_cik():
    mock_requests = Mock()
    with patch.dict('sys.modules', {'requests': mock_requests}):
        from logic.edgar_client import get_company_facts

        with patch('logic.edgar_client.get_cik_from_ticker') as mock_get_cik:
            mock_get_cik.return_value = None

            result = get_company_facts('INVALID')

            assert result is None
            mock_get_cik.assert_called_once_with('INVALID')

def test_get_company_facts_api_error():
    mock_requests = Mock()
    with patch.dict('sys.modules', {'requests': mock_requests}):
        from logic.edgar_client import get_company_facts

        with patch('logic.edgar_client.get_cik_from_ticker') as mock_get_cik, \
             patch('logic.edgar_client.requests.get') as mock_get:

            mock_get_cik.return_value = '0000320193'
            mock_get.side_effect = Exception("API Error")

            result = get_company_facts('AAPL')

            assert result is None
            mock_get_cik.assert_called_once_with('AAPL')
            mock_get.assert_called_once()
