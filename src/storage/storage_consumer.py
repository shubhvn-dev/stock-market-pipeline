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

class StorageConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'stock-quotes',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
        
        self.storage = CloudStorage(provider='aws')
        self.batch_data = defaultdict(list)
        self.batch_size = 10
        
    def process_messages(self):
        logger.info("Starting Storage Consumer for raw data...")
        
        try:
            for message in self.consumer:
                stock_data = message.value
                symbol = stock_data['symbol']
                
                # Add to batch
                self.batch_data[symbol].append(stock_data)
                
                # Save batch when size reached
                if len(self.batch_data[symbol]) >= self.batch_size:
                    self.save_batch(symbol)
                
        except KeyboardInterrupt:
            # Save remaining data before exit
            for symbol in self.batch_data:
                if self.batch_data[symbol]:
                    self.save_batch(symbol)
            logger.info("Storage consumer stopped")
        finally:
            self.consumer.close()
    
    def save_batch(self, symbol):
        today = datetime.now().strftime('%Y-%m-%d')
        data = self.batch_data[symbol]
        
        try:
            self.storage.save_raw_data(data, symbol, today)
            logger.info(f"Saved batch of {len(data)} records for {symbol}")
            self.batch_data[symbol] = []
        except Exception as e:
            logger.error(f"Failed to save batch for {symbol}: {e}")

if __name__ == "__main__":
    consumer = StorageConsumer()
    consumer.process_messages()
