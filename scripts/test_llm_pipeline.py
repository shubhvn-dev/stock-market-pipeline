import sys
import os
import json
import time
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from kafka import KafkaConsumer
from src.llm.gemini_analyzer import GeminiAnalyzer

def test_llm_pipeline():
    print("Testing complete LLM pipeline...")
    
    # Test 1: Direct analyzer test
    print("\n1. Testing Gemini analyzer directly:")
    analyzer = GeminiAnalyzer()
    
    test_indicators = {
        'SMA_5': 247.43,
        'SMA_10': 247.43,
        'EMA_12': 247.43,
        'RSI_14': 65.0
    }
    
    analysis = analyzer.analyze_technical_indicators('AAPL', test_indicators, 247.43)
    print(f"Direct analysis result:\n{analysis}\n")
    
    # Test 2: Monitor stock-analysis topic for real-time LLM output
    print("2. Monitoring stock-analysis topic for LLM output...")
    print("(Make sure your LLM consumer is running in another terminal)")
    
    consumer = KafkaConsumer(
        'stock-analysis',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        consumer_timeout_ms=15000
    )
    
    analysis_count = 0
    try:
        for message in consumer:
            analysis_data = message.value
            symbol = analysis_data['symbol']
            current_price = analysis_data['current_price']
            llm_analysis = analysis_data['llm_analysis']
            
            print(f"\nLLM Analysis for {symbol} (${current_price:.2f}):")
            print(f"{llm_analysis}")
            print("-" * 50)
            
            analysis_count += 1
            if analysis_count >= 3:
                break
                
    except Exception as e:
        print(f"Timeout or error monitoring topic: {e}")
    
    consumer.close()
    
    print(f"\nTesting complete. Processed {analysis_count} LLM analyses.")
    
    if analysis_count > 0:
        print("LLM pipeline is working correctly!")
    else:
        print("No LLM analyses detected. Check if LLM consumer is running.")

if __name__ == "__main__":
    test_llm_pipeline()
