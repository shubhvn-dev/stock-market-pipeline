import boto3
import os
from dotenv import load_dotenv

load_dotenv()

def create_s3_bucket():
    # Generate unique bucket name
    import time
    bucket_name = f"stock-pipeline-{int(time.time())}"
    
    # Create S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    )
    
    try:
        # Create bucket
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Created S3 bucket: {bucket_name}")
        
        # Update .env file
        with open('.env', 'a') as f:
            f.write(f'\nS3_BUCKET_NAME={bucket_name}\n')
        
        return bucket_name
        
    except Exception as e:
        print(f"Error creating bucket: {e}")
        return None

if __name__ == "__main__":
    create_s3_bucket()
