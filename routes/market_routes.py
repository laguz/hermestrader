from flask import Blueprint, jsonify
from services.container import Container

market_bp = Blueprint('market', __name__)

@market_bp.route('/api/market/status', methods=['GET'])
def market_status():
    """Get current market status (open/closed) and time."""
    try:
        tradier_service = Container.get_tradier_service()
        clock = tradier_service.get_clock()
        
        if clock:
            return jsonify({'clock': clock})
        else:
            return jsonify({'error': 'Failed to fetch market clock'}), 500
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@market_bp.route('/api/market/sentiment', methods=['GET'])
def market_sentiment():
    """Get market sentiment (VIX)."""
    try:
        tradier_service = Container.get_tradier_service()
        vix_quote = tradier_service.get_quote('VIX')
        
        # Fallback for Sandbox/Missing VIX
        if not vix_quote:
             # Check if we are in dry run or sandbox? 
             # Just return a simulated neutral value so UI works
             return jsonify({
                'symbol': 'VIX (Simulated)',
                'value': 18.50,
                'change_percent': 1.25, # Simulated +1.25%
                'sentiment': 'Normal (Simulated)',
                'color': 'var(--md-sys-color-secondary)',
                'description': 'Simulated VIX for Sandbox'
             })
             
        vix_val = vix_quote.get('last')
        if vix_val is None: vix_val = vix_quote.get('close')
        
        # Get Percentage Change (Real Time)
        change_pct = vix_quote.get('change_percentage', 0.0)
        
        # Simple Sentiment Logic
        sentiment = "Neutral"
        color = "var(--md-sys-color-on-surface)"
        
        if vix_val:
            if vix_val < 15:
                sentiment = "Risk On (Complacent)"
                color = "var(--md-sys-color-primary)" # Green-ish/Primary
            elif vix_val < 20:
                sentiment = "Normal"
                color = "var(--md-sys-color-secondary)"
            elif vix_val < 30:
                sentiment = "Fear (Elevated)"
                color = "orange"
            else:
                sentiment = "Extreme Fear"
                color = "var(--md-sys-color-error)"
                
        return jsonify({
            'symbol': 'VIX',
            'value': vix_val,
            'change_percent': change_pct,
            'sentiment': sentiment,
            'color': color,
            'description': vix_quote.get('description', 'CBOE Volatility Index')
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500
