import json
import boto3
from kafka import KafkaConsumer
import logging
import os
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

class LambdaAlertForwarder:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'stock-alerts',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
        
        self.lambda_client = boto3.client(
            'lambda',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_DEFAULT_REGION')
        )
        
        self.function_name = 'stock-alert-processor'
        
    def forward_alerts(self):
        logger.info("Starting Lambda Alert Forwarder...")
        
        try:
            for message in self.consumer:
                alert_data = message.value
                
                # Invoke Lambda function
                try:
                    response = self.lambda_client.invoke(
                        FunctionName=self.function_name,
                        InvocationType='Event',  # Asynchronous
                        Payload=json.dumps(alert_data)
                    )
                    
                    logger.info(f"Forwarded alert to Lambda: {alert_data['symbol']} - {alert_data['alert_type']}")
                    
                except Exception as e:
                    logger.error(f"Failed to invoke Lambda: {e}")
                
        except KeyboardInterrupt:
            logger.info("Lambda forwarder stopped")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    forwarder = LambdaAlertForwarder()
    forwarder.forward_alerts()
