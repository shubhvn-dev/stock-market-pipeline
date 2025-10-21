import boto3
import os
from dotenv import load_dotenv

load_dotenv()

def setup_sns_notifications():
    """Create SNS topic and email subscription for stock alerts"""
    
    sns_client = boto3.client(
        'sns',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION')
    )
    
    try:
        # Create SNS topic
        topic_response = sns_client.create_topic(
            Name='stock-market-alerts'
        )
        
        topic_arn = topic_response['TopicArn']
        print(f"SNS topic created: {topic_arn}")
        
        # Subscribe email to topic
        email = input("Enter email address for alerts: ")
        
        subscription_response = sns_client.subscribe(
            TopicArn=topic_arn,
            Protocol='email',
            Endpoint=email
        )
        
        print(f"Email subscription created: {subscription_response['SubscriptionArn']}")
        print("Check your email and confirm the subscription")
        
        # Update environment file
        with open('.env', 'a') as f:
            f.write(f'\nSNS_TOPIC_ARN={topic_arn}\n')
        
        return topic_arn
        
    except Exception as e:
        print(f"Error setting up SNS: {e}")
        return None

if __name__ == "__main__":
    setup_sns_notifications()
