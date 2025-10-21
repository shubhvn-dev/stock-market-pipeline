import boto3
import zipfile
import os
from dotenv import load_dotenv

load_dotenv()

def create_lambda_deployment():
    """Create and deploy Lambda function"""
    
    # Create deployment package
    with zipfile.ZipFile('lambda_deployment.zip', 'w') as zip_file:
        zip_file.write('src/alerts/lambda_alert_handler.py', 'lambda_function.py')
    
    # Upload to Lambda
    lambda_client = boto3.client(
        'lambda',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION')
    )
    
    try:
        # Create Lambda function
        with open('lambda_deployment.zip', 'rb') as zip_file:
            response = lambda_client.create_function(
                FunctionName='stock-alert-processor',
                Runtime='python3.9',
                Role='arn:aws:iam::YOUR_ACCOUNT:role/lambda-execution-role',  # Update with your role
                Handler='lambda_function.lambda_handler',
                Code={'ZipFile': zip_file.read()},
                Description='Process stock market alerts',
                Timeout=30,
                Environment={
                    'Variables': {
                        'SNS_TOPIC_ARN': 'arn:aws:sns:us-east-1:YOUR_ACCOUNT:stock-alerts'
                    }
                }
            )
        
        print(f"Lambda function created: {response['FunctionArn']}")
        
    except Exception as e:
        print(f"Error creating Lambda function: {e}")
    
    # Clean up
    os.remove('lambda_deployment.zip')

if __name__ == "__main__":
    create_lambda_deployment()
