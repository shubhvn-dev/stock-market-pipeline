import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.llm.llm_consumer import LLMConsumer

def main():
    consumer = LLMConsumer()
    consumer.process_indicators()

if __name__ == "__main__":
    main()
