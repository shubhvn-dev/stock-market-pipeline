import json
from kafka import KafkaConsumer, KafkaProducer
import logging
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.indicators.technical_indicators import TechnicalIndicators

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IndicatorConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'stock-quotes',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        self.indicators = TechnicalIndicators()
        
    def process_messages(self):
        logger.info("Starting Indicator Consumer (SMA + EMA)...")
        
        try:
            for message in self.consumer:
                stock_data = message.value
                symbol = stock_data['symbol']
                
                self.indicators.add_price_data(symbol, stock_data)
                all_indicators = self.indicators.calculate_all_indicators(symbol)
                
                if all_indicators:
                    indicator_msg = {
                        'symbol': symbol,
                        'timestamp': stock_data['timestamp'],
                        'current_price': stock_data['close'],
                        'indicators': all_indicators
                    }
                    
                    self.producer.send('stock-indicators', key=symbol, value=indicator_msg)
                    logger.info(f"Indicators calculated for {symbol}: {all_indicators}")
                
        except KeyboardInterrupt:
            logger.info("Indicator consumer stopped")
        finally:
            self.consumer.close()
            self.producer.close()

if __name__ == "__main__":
    consumer = IndicatorConsumer()
    consumer.process_messages()
