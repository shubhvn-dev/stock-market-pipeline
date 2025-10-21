import pandas as pd
import numpy as np
from typing import List, Dict
from collections import deque
import logging

logger = logging.getLogger(__name__)

class TechnicalIndicators:
    def __init__(self):
        self.price_history = {}
        self.max_history = 200
        
    def add_price_data(self, symbol: str, price_data: Dict):
        if symbol not in self.price_history:
            self.price_history[symbol] = deque(maxlen=self.max_history)
        
        self.price_history[symbol].append({
            'timestamp': price_data['timestamp'],
            'open': price_data['open'],
            'high': price_data['high'],
            'low': price_data['low'],
            'close': price_data['close'],
            'volume': price_data['volume']
        })
    
    def calculate_sma(self, symbol: str, period: int = 20) -> float:
        if symbol not in self.price_history:
            return None
            
        prices = list(self.price_history[symbol])
        if len(prices) < period:
            return None
        
        recent_closes = [p['close'] for p in prices[-period:]]
        sma = sum(recent_closes) / period
        
        logger.info(f"SMA-{period} for {symbol}: {sma:.2f}")
        return round(sma, 2)
    
    def calculate_multiple_sma(self, symbol: str, periods: List[int] = [5, 10, 20, 50]) -> Dict:
        sma_results = {}
        for period in periods:
            sma_value = self.calculate_sma(symbol, period)
            if sma_value is not None:
                sma_results[f'SMA_{period}'] = sma_value
        return sma_results

    def calculate_ema(self, symbol: str, period: int = 20) -> float:
        """Calculate Exponential Moving Average"""
        if symbol not in self.price_history:
            return None
            
        prices = list(self.price_history[symbol])
        if len(prices) < period:
            return None
        
        closes = [p['close'] for p in prices]
        
        # Calculate EMA using standard formula
        multiplier = 2 / (period + 1)
        ema = closes[0]  # Start with first price
        
        for price in closes[1:]:
            ema = (price * multiplier) + (ema * (1 - multiplier))
        
        logger.info(f"EMA-{period} for {symbol}: {ema:.2f}")
        return round(ema, 2)
    
    def calculate_multiple_ema(self, symbol: str, periods: List[int] = [12, 26, 50]) -> Dict:
        """Calculate multiple EMA periods"""
        ema_results = {}
        for period in periods:
            ema_value = self.calculate_ema(symbol, period)
            if ema_value is not None:
                ema_results[f'EMA_{period}'] = ema_value
        return ema_results
    
    def calculate_all_indicators(self, symbol: str) -> Dict:
        """Calculate both SMA and EMA indicators"""
        indicators = {}
        
        # SMA calculations
        sma_results = self.calculate_multiple_sma(symbol)
        indicators.update(sma_results)
        
        # EMA calculations  
        ema_results = self.calculate_multiple_ema(symbol)
        indicators.update(ema_results)
        
        return indicators
