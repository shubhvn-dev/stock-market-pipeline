import google.generativeai as genai
import os
import logging
from typing import Dict, List
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

class GeminiAnalyzer:
    def __init__(self):
        api_key = os.getenv('GEMINI_API_KEY')
        if not api_key:
            raise ValueError("GEMINI_API_KEY not found in environment variables")
        
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel('models/gemini-2.0-flash')  # Fastest model
        
    def analyze_technical_indicators(self, symbol: str, indicators: Dict, current_price: float) -> str:
        """Generate natural language explanation of technical indicators"""
        
        prompt = f"""
        Analyze the following technical indicators for stock {symbol} (current price: ${current_price:.2f}):
        
        Technical Indicators:
        {self._format_indicators(indicators)}
        
        Provide a concise analysis covering:
        1. Current trend direction
        2. Key support/resistance levels
        3. Momentum indicators interpretation
        4. Risk assessment
        5. Brief outlook
        
        Keep the response under 150 words and use professional trading terminology.
        """
        
        try:
            response = self.model.generate_content(prompt)
            return response.text
        except Exception as e:
            logger.error(f"Gemini API error: {e}")
            return f"Unable to generate analysis for {symbol} due to API error."
    
    def _format_indicators(self, indicators: Dict) -> str:
        """Format indicators for the prompt"""
        formatted = []
        
        for key, value in indicators.items():
            if value is not None:
                formatted.append(f"- {key}: {value}")
        
        return "\n".join(formatted)
    
    def analyze_alert(self, alert: Dict) -> str:
        """Generate explanation for an alert"""
        
        prompt = f"""
        Explain this trading alert in simple terms:
        
        Alert: {alert['message']}
        Type: {alert['alert_type']}
        Symbol: {alert['symbol']}
        
        Explain what this means for traders and potential next steps.
        Keep response under 100 words.
        """
        
        try:
            response = self.model.generate_content(prompt)
            return response.text
        except Exception as e:
            logger.error(f"Alert analysis error: {e}")
            return "Unable to analyze alert."
