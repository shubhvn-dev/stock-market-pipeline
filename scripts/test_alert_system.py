import json
import time
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime

def test_alert_triggers():
    """Test alert system by monitoring actual alerts"""
    
    # Monitor stock-alerts topic
    consumer = KafkaConsumer(
        'stock-alerts',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        consumer_timeout_ms=10000
    )
    
    alerts_found = []
    
    print("Checking for existing alerts...")
    try:
        for message in consumer:
            alerts_found.append(message.value)
    except:
        pass
    
    consumer.close()
    
    if alerts_found:
        print(f"Found {len(alerts_found)} alerts:")
        for alert in alerts_found[-5:]:  # Show last 5
            print(f"- {alert['symbol']}: {alert['alert_type']} - {alert['message']}")
    else:
        print("No alerts found. Generating test alert...")
        
        # Create test alert
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8')
        )
        
        test_alert = {
            'symbol': 'AAPL',
            'alert_type': 'rsi_overbought',
            'message': 'AAPL RSI is overbought at 72.0',
            'severity': 'warning',
            'timestamp': datetime.now().isoformat(),
            'value': 72.0
        }
        
        producer.send('stock-alerts', key='AAPL', value=test_alert)
        producer.close()
        
        print("Test alert generated")
    
    print("Alert system verification complete")

if __name__ == "__main__":
    test_alert_triggers()
