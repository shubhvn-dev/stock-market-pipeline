import json
from kafka import KafkaConsumer, KafkaProducer
import logging
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.alerts.alert_conditions import AlertConditions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AlertConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'stock-indicators',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
        
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        
        self.alert_conditions = AlertConditions()
        
    def process_indicators(self):
        logger.info("Starting Alert Consumer...")
        
        try:
            for message in self.consumer:
                indicator_data = message.value
                symbol = indicator_data['symbol']
                current_price = indicator_data['current_price']
                indicators = indicator_data['indicators']
                
                # Check for alerts
                alerts = self.alert_conditions.check_alerts(symbol, current_price, indicators)
                
                for alert in alerts:
                    # Add timestamp and send to alerts topic
                    alert['timestamp'] = indicator_data['timestamp']
                    
                    self.producer.send('stock-alerts', key=symbol, value=alert)
                    logger.warning(f"ALERT: {alert['message']}")
                
        except KeyboardInterrupt:
            logger.info("Alert consumer stopped")
        finally:
            self.consumer.close()
            self.producer.close()

if __name__ == "__main__":
    consumer = AlertConsumer()
    consumer.process_indicators()
