import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.consumers.indicator_consumer import IndicatorConsumer

def main():
    consumer = IndicatorConsumer()
    consumer.process_messages()

if __name__ == "__main__":
    main()
