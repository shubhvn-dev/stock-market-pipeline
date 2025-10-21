import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.storage.indicator_storage_consumer import IndicatorStorageConsumer

def main():
    consumer = IndicatorStorageConsumer()
    consumer.process_messages()

if __name__ == "__main__":
    main()
