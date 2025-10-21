import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.storage.storage_consumer import StorageConsumer

def main():
    consumer = StorageConsumer()
    consumer.process_messages()

if __name__ == "__main__":
    main()
