from unittest.mock import patch, MagicMock

# Mock pandas before importing logic.parser to avoid ModuleNotFoundError
mock_pd = MagicMock()
with patch.dict('sys.modules', {'pandas': mock_pd}):
    import logic.parser

def test_extract_financials_invalid_json():
    """Test extract_financials with invalid JSON structures."""
    extract_financials = logic.parser.extract_financials

    # None input
    assert extract_financials(None) is None

    # Empty dict
    assert extract_financials({}) is None

    # Missing 'facts'
    assert extract_financials({'not_facts': {}}) is None

    # Missing 'us-gaap'
    assert extract_financials({'facts': {'not_us-gaap': {}}}) is None

def test_extract_financials_happy_path():
    """Test extract_financials with a minimal valid structure."""
    mock_pd.reset_mock()
    extract_financials = logic.parser.extract_financials

    valid_json = {
        'facts': {
            'us-gaap': {
                'Revenues': {
                    'units': {
                        'USD': [
                            {'fy': 2020, 'val': 100, 'end': '2020-12-31', 'fp': 'FY'}
                        ]
                    }
                }
            }
        }
    }

    # Configure mock_pd to return a mock DataFrame when called
    mock_df = MagicMock()
    mock_pd.DataFrame.return_value = mock_df
    # Mocking sort_index to return the same mock_df for simplicity
    mock_df.sort_index.return_value = mock_df

    result = extract_financials(valid_json)

    assert result is not None
    assert mock_pd.DataFrame.called
