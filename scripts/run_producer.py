import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.producers.stock_producer import StockDataProducer

def main():
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN']
    producer = StockDataProducer(topic='stock-quotes')
    producer.run_producer(symbols, interval=15)

if __name__ == "__main__":
    main()
