import json
import boto3
import os
from datetime import datetime

def lambda_handler(event, context):
    """
    AWS Lambda function to handle stock alerts
    Triggered by Kafka messages or API Gateway
    """
    
    # Initialize services
    sns_client = boto3.client('sns')
    dynamodb = boto3.resource('dynamodb')
    
    # Parse alert data
    try:
        if 'Records' in event:
            # Triggered by SQS/Kinesis
            alerts = []
            for record in event['Records']:
                alert_data = json.loads(record['body'])
                alerts.append(alert_data)
        else:
            # Direct invocation
            alerts = [event]
        
        for alert in alerts:
            process_alert(alert, sns_client, dynamodb)
            
        return {
            'statusCode': 200,
            'body': json.dumps(f'Processed {len(alerts)} alerts successfully')
        }
        
    except Exception as e:
        print(f"Error processing alerts: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }

def process_alert(alert, sns_client, dynamodb):
    """Process individual alert"""
    
    symbol = alert['symbol']
    alert_type = alert['alert_type']
    message = alert['message']
    severity = alert['severity']
    timestamp = alert['timestamp']
    
    # Store alert in DynamoDB
    table = dynamodb.Table('stock-alerts')
    table.put_item(
        Item={
            'alert_id': f"{symbol}_{alert_type}_{timestamp}",
            'symbol': symbol,
            'alert_type': alert_type,
            'message': message,
            'severity': severity,
            'timestamp': timestamp,
            'processed_at': datetime.utcnow().isoformat()
        }
    )
    
    # Send notification based on severity
    if severity in ['warning', 'critical']:
        send_notification(alert, sns_client)
    
    print(f"Processed alert: {symbol} - {alert_type}")

def send_notification(alert, sns_client):
    """Send SNS notification"""
    
    topic_arn = os.environ.get('SNS_TOPIC_ARN')
    if not topic_arn:
        return
    
    notification_message = f"""
    Stock Alert: {alert['symbol']}
    Type: {alert['alert_type']}
    Severity: {alert['severity']}
    Message: {alert['message']}
    Time: {alert['timestamp']}
    """
    
    sns_client.publish(
        TopicArn=topic_arn,
        Message=notification_message,
        Subject=f"Stock Alert: {alert['symbol']} - {alert['alert_type']}"
    )
