from kafka import KafkaConsumer
import json

def check_alert_system():
    topics = ['stock-quotes', 'stock-indicators', 'stock-alerts', 'stock-analysis']
    
    for topic in topics:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=['localhost:9092'],
                consumer_timeout_ms=2000,
                auto_offset_reset='latest'
            )
            
            message_count = 0
            for message in consumer:
                message_count += 1
                if message_count >= 1:
                    break
            
            status = "ACTIVE" if message_count > 0 else "INACTIVE"
            print(f"{topic}: {status}")
            consumer.close()
            
        except Exception as e:
            print(f"{topic}: ERROR - {e}")

if __name__ == "__main__":
    check_alert_system()
