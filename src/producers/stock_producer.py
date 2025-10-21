import json
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
import yfinance as yf
from typing import List, Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StockDataProducer:
    def __init__(self, bootstrap_servers: str = 'localhost:9092', topic: str = 'stock-quotes'):
        self.producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        self.topic = topic
        
    def get_stock_data(self, symbols: List[str]) -> List[Dict]:
        stock_data = []
        for symbol in symbols:
            try:
                ticker = yf.Ticker(symbol)
                info = ticker.history(period="1d", interval="1m").tail(1)
                
                if not info.empty:
                    data = {
                        'symbol': symbol,
                        'timestamp': datetime.now().isoformat(),
                        'open': float(info['Open'].iloc[0]),
                        'high': float(info['High'].iloc[0]),
                        'low': float(info['Low'].iloc[0]),
                        'close': float(info['Close'].iloc[0]),
                        'volume': int(info['Volume'].iloc[0])
                    }
                    stock_data.append(data)
                    logger.info(f"Fetched data for {symbol}: ${data['close']:.2f}")
            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {e}")
        return stock_data
    
    def send_data(self, data: Dict):
        try:
            future = self.producer.send(self.topic, key=data['symbol'], value=data)
            future.get(timeout=10)
            logger.info(f"Sent {data['symbol']} data to Kafka")
        except Exception as e:
            logger.error(f"Failed to send data: {e}")
    
    def run_producer(self, symbols: List[str], interval: int = 30):
        logger.info(f"Starting producer for symbols: {symbols}")
        try:
            while True:
                stock_data = self.get_stock_data(symbols)
                for data in stock_data:
                    self.send_data(data)
                logger.info(f"Waiting {interval} seconds...")
                time.sleep(interval)
        except KeyboardInterrupt:
            logger.info("Producer stopped by user")
        finally:
            self.producer.close()
