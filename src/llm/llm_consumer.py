import json
from kafka import KafkaConsumer, KafkaProducer
import logging
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.llm.gemini_analyzer import GeminiAnalyzer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LLMConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'stock-indicators',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        self.analyzer = GeminiAnalyzer()
        
    def process_indicators(self):
        logger.info("Starting LLM Consumer for indicator analysis...")
        
        try:
            for message in self.consumer:
                indicator_data = message.value
                symbol = indicator_data['symbol']
                current_price = indicator_data['current_price']
                indicators = indicator_data['indicators']
                
                # Generate LLM analysis
                analysis = self.analyzer.analyze_technical_indicators(
                    symbol, indicators, current_price
                )
                
                # Create analysis message
                analysis_msg = {
                    'symbol': symbol,
                    'timestamp': indicator_data['timestamp'],
                    'current_price': current_price,
                    'indicators': indicators,
                    'llm_analysis': analysis
                }
                
                # Send to analysis topic
                self.producer.send('stock-analysis', key=symbol, value=analysis_msg)
                logger.info(f"Generated LLM analysis for {symbol}")
                
        except KeyboardInterrupt:
            logger.info("LLM consumer stopped")
        finally:
            self.consumer.close()
            self.producer.close()

if __name__ == "__main__":
    consumer = LLMConsumer()
    consumer.process_indicators()
