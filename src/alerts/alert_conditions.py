from typing import Dict, List
import logging

logger = logging.getLogger(__name__)

class AlertConditions:
    def __init__(self):
        self.alert_rules = {
            # RSI overbought/oversold conditions
            'rsi_overbought': {'indicator': 'RSI_14', 'condition': '>', 'threshold': 70},
            'rsi_oversold': {'indicator': 'RSI_14', 'condition': '<', 'threshold': 30},
            
            # SMA crossover signals
            'sma_golden_cross': {'indicators': ['SMA_5', 'SMA_10'], 'condition': 'crossover_above'},
            'sma_death_cross': {'indicators': ['SMA_5', 'SMA_10'], 'condition': 'crossover_below'},
            
            # EMA momentum signals
            'ema_bullish': {'indicators': ['EMA_12', 'EMA_26'], 'condition': 'crossover_above'},
            'ema_bearish': {'indicators': ['EMA_12', 'EMA_26'], 'condition': 'crossover_below'},
            
            # Price vs moving average
            'price_above_sma20': {'price_vs_indicator': 'SMA_20', 'condition': '>', 'threshold_pct': 2},
            'price_below_sma20': {'price_vs_indicator': 'SMA_20', 'condition': '<', 'threshold_pct': -2}
        }
        
        self.previous_values = {}  # Store previous indicator values for crossover detection
    
    def check_alerts(self, symbol: str, current_price: float, indicators: Dict) -> List[Dict]:
        """Check all alert conditions and return triggered alerts"""
        alerts = []
        
        # RSI alerts
        if 'RSI_14' in indicators:
            rsi_alerts = self._check_rsi_alerts(symbol, indicators['RSI_14'])
            alerts.extend(rsi_alerts)
        
        # SMA crossover alerts
        sma_alerts = self._check_crossover_alerts(symbol, indicators, 'SMA')
        alerts.extend(sma_alerts)
        
        # EMA crossover alerts  
        ema_alerts = self._check_crossover_alerts(symbol, indicators, 'EMA')
        alerts.extend(ema_alerts)
        
        # Price vs SMA alerts
        price_alerts = self._check_price_alerts(symbol, current_price, indicators)
        alerts.extend(price_alerts)
        
        # Update previous values for next comparison
        self._update_previous_values(symbol, indicators)
        
        return alerts
    
    def _check_rsi_alerts(self, symbol: str, rsi_value: float) -> List[Dict]:
        alerts = []
        
        if rsi_value > 70:
            alerts.append({
                'symbol': symbol,
                'alert_type': 'rsi_overbought',
                'message': f'{symbol} RSI is overbought at {rsi_value:.2f}',
                'severity': 'warning',
                'value': rsi_value
            })
        elif rsi_value < 30:
            alerts.append({
                'symbol': symbol,
                'alert_type': 'rsi_oversold', 
                'message': f'{symbol} RSI is oversold at {rsi_value:.2f}',
                'severity': 'warning',
                'value': rsi_value
            })
        
        return alerts
    
    def _check_crossover_alerts(self, symbol: str, indicators: Dict, indicator_type: str) -> List[Dict]:
        alerts = []
        
        if indicator_type == 'SMA':
            short_key, long_key = 'SMA_5', 'SMA_10'
            alert_prefix = 'sma'
        else:  # EMA
            short_key, long_key = 'EMA_12', 'EMA_26'
            alert_prefix = 'ema'
        
        if short_key in indicators and long_key in indicators:
            short_val = indicators[short_key]
            long_val = indicators[long_key]
            
            # Check for crossovers
            prev_key = f"{symbol}_{indicator_type}"
            if prev_key in self.previous_values:
                prev_short = self.previous_values[prev_key].get(short_key)
                prev_long = self.previous_values[prev_key].get(long_key)
                
                if prev_short and prev_long:
                    # Golden cross: short MA crosses above long MA
                    if prev_short <= prev_long and short_val > long_val:
                        alerts.append({
                            'symbol': symbol,
                            'alert_type': f'{alert_prefix}_golden_cross',
                            'message': f'{symbol} {short_key} crossed above {long_key} - Bullish signal',
                            'severity': 'info',
                            'values': {short_key: short_val, long_key: long_val}
                        })
                    
                    # Death cross: short MA crosses below long MA  
                    elif prev_short >= prev_long and short_val < long_val:
                        alerts.append({
                            'symbol': symbol,
                            'alert_type': f'{alert_prefix}_death_cross',
                            'message': f'{symbol} {short_key} crossed below {long_key} - Bearish signal',
                            'severity': 'warning',
                            'values': {short_key: short_val, long_key: long_val}
                        })
        
        return alerts
    
    def _check_price_alerts(self, symbol: str, current_price: float, indicators: Dict) -> List[Dict]:
        alerts = []
        
        if 'SMA_20' in indicators:
            sma20 = indicators['SMA_20']
            price_diff_pct = ((current_price - sma20) / sma20) * 100
            
            if price_diff_pct > 2:
                alerts.append({
                    'symbol': symbol,
                    'alert_type': 'price_above_sma20',
                    'message': f'{symbol} price ${current_price:.2f} is {price_diff_pct:.1f}% above SMA20',
                    'severity': 'info',
                    'price_diff_pct': price_diff_pct
                })
            elif price_diff_pct < -2:
                alerts.append({
                    'symbol': symbol,
                    'alert_type': 'price_below_sma20',
                    'message': f'{symbol} price ${current_price:.2f} is {abs(price_diff_pct):.1f}% below SMA20',
                    'severity': 'warning',
                    'price_diff_pct': price_diff_pct
                })
        
        return alerts
    
    def _update_previous_values(self, symbol: str, indicators: Dict):
        """Store current values for next crossover comparison"""
        for indicator_type in ['SMA', 'EMA']:
            key = f"{symbol}_{indicator_type}"
            if key not in self.previous_values:
                self.previous_values[key] = {}
            
            # Store current values as previous for next iteration
            if indicator_type == 'SMA':
                for sma_key in ['SMA_5', 'SMA_10', 'SMA_20']:
                    if sma_key in indicators:
                        self.previous_values[key][sma_key] = indicators[sma_key]
            else:  # EMA
                for ema_key in ['EMA_12', 'EMA_26']:
                    if ema_key in indicators:
                        self.previous_values[key][ema_key] = indicators[ema_key]
