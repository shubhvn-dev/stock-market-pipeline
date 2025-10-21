import json
import boto3
from kafka import KafkaConsumer
import logging
import os
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

class EmailNotifier:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'stock-alerts',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
        
        self.sns_client = boto3.client(
            'sns',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_DEFAULT_REGION')
        )
        
        self.topic_arn = os.getenv('SNS_TOPIC_ARN')
        self.notification_threshold = {
            'warning': True,
            'critical': True,
            'info': False  # Only send warnings and critical alerts
        }
        
    def process_alerts(self):
        logger.info("Starting Email Notifier for stock alerts...")
        
        if not self.topic_arn:
            logger.error("SNS_TOPIC_ARN not configured")
            return
        
        try:
            for message in self.consumer:
                alert_data = message.value
                
                # Check if alert should trigger email
                severity = alert_data.get('severity', 'info')
                
                if self.notification_threshold.get(severity, False):
                    self.send_email_alert(alert_data)
                
        except KeyboardInterrupt:
            logger.info("Email notifier stopped")
        finally:
            self.consumer.close()
    
    def send_email_alert(self, alert):
        """Send email notification via SNS"""
        
        symbol = alert['symbol']
        alert_type = alert['alert_type']
        message = alert['message']
        severity = alert['severity']
        timestamp = alert['timestamp']
        
        # Format email content
        subject = f"Stock Alert: {symbol} - {alert_type.upper()}"
        
        email_body = f"""
Stock Market Alert

Symbol: {symbol}
Alert Type: {alert_type}
Severity: {severity.upper()}
Time: {timestamp}

Message: {message}

This is an automated alert from your stock market monitoring system.
        """
        
        try:
            response = self.sns_client.publish(
                TopicArn=self.topic_arn,
                Message=email_body.strip(),
                Subject=subject
            )
            
            logger.info(f"Email alert sent for {symbol}: {alert_type}")
            
        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")

if __name__ == "__main__":
    notifier = EmailNotifier()
    notifier.process_alerts()
