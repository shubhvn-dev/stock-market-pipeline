import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.consumers.alert_consumer import AlertConsumer

def main():
    consumer = AlertConsumer()
    consumer.process_indicators()

if __name__ == "__main__":
    main()
