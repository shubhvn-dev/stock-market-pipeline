import json
from kafka import KafkaProducer
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

# Send test indicator data that should trigger RSI overbought alert
test_data = {
    'symbol': 'TEST',
    'timestamp': '2025-10-17T04:30:00',
    'current_price': 100.0,
    'indicators': {
        'RSI_14': 75.0,  # This should trigger overbought alert
        'SMA_5': 95.0,
        'SMA_10': 90.0   # SMA_5 > SMA_10 could trigger crossover
    }
}

producer.send('stock-indicators', key='TEST', value=test_data)
print("Sent test data with RSI 75 - should trigger overbought alert")

producer.close()
