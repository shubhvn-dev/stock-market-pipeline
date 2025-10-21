import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.llm.gemini_analyzer import GeminiAnalyzer

def test_gemini():
    try:
        analyzer = GeminiAnalyzer()
        
        # Test data
        test_indicators = {
            'SMA_5': 247.43,
            'SMA_10': 247.43,
            'EMA_12': 247.43,
            'RSI_14': 65.2
        }
        
        analysis = analyzer.analyze_technical_indicators(
            'AAPL', test_indicators, 247.43
        )
        
        print("LLM Analysis:")
        print(analysis)
        
    except Exception as e:
        print(f"Test failed: {e}")

if __name__ == "__main__":
    test_gemini()
