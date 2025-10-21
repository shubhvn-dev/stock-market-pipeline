import json
from kafka import KafkaConsumer
import logging
import sys
import os
from datetime import datetime
from collections import defaultdict
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.storage.cloud_storage import CloudStorage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IndicatorStorageConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'stock-indicators',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
        
        self.storage = CloudStorage(provider='aws')
        self.batch_data = defaultdict(list)
        self.batch_size = 5
        
    def process_messages(self):
        logger.info("Starting Storage Consumer for processed indicators...")
        
        try:
            for message in self.consumer:
                indicator_data = message.value
                symbol = indicator_data['symbol']
                
                # Flatten the data structure for easier querying
                flattened_data = {
                    'symbol': symbol,
                    'timestamp': indicator_data['timestamp'],
                    'current_price': indicator_data['current_price']
                }
                
                # Add all indicators as separate columns
                indicators = indicator_data.get('indicators', {})
                flattened_data.update(indicators)
                
                # Add to batch
                self.batch_data[symbol].append(flattened_data)
                
                # Save batch when size reached
                if len(self.batch_data[symbol]) >= self.batch_size:
                    self.save_batch(symbol)
                
        except KeyboardInterrupt:
            # Save remaining data before exit
            for symbol in self.batch_data:
                if self.batch_data[symbol]:
                    self.save_batch(symbol)
            logger.info("Indicator storage consumer stopped")
        finally:
            self.consumer.close()
    
    def save_batch(self, symbol):
        today = datetime.now().strftime('%Y-%m-%d')
        data = self.batch_data[symbol]
        
        try:
            self.storage.save_processed_indicators(data, symbol, today)
            logger.info(f"Saved indicator batch of {len(data)} records for {symbol}")
            self.batch_data[symbol] = []
        except Exception as e:
            logger.error(f"Failed to save indicator batch for {symbol}: {e}")

if __name__ == "__main__":
    consumer = IndicatorStorageConsumer()
    consumer.process_messages()
