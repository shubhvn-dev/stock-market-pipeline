import boto3
import os
from dotenv import load_dotenv

load_dotenv()

def check_rds_status():
    rds_client = boto3.client(
        'rds',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION')
    )
    
    try:
        response = rds_client.describe_db_instances(
            DBInstanceIdentifier='stock-pipeline-db'
        )
        
        instance = response['DBInstances'][0]
        print(f"Status: {instance['DBInstanceStatus']}")
        print(f"Endpoint: {instance.get('Endpoint', {}).get('Address', 'Not available yet')}")
        print(f"Port: {instance.get('Endpoint', {}).get('Port', 'Not available yet')}")
        
    except Exception as e:
        print(f"Error checking RDS status: {e}")

if __name__ == "__main__":
    check_rds_status()
