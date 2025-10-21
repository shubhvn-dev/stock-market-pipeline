import sys
import os
import pandas as pd
import io
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.storage.cloud_storage import CloudStorage
import boto3
from dotenv import load_dotenv

load_dotenv()

def verify_stored_data():
    storage = CloudStorage(provider='aws')
    
    # List and count files
    raw_files = storage.list_files('raw/')
    processed_files = storage.list_files('processed/')
    
    print(f"Raw data files: {len(raw_files)}")
    print(f"Processed data files: {len(processed_files)}")
    
    # Download and inspect a sample file
    if raw_files:
        sample_file = raw_files[0]
        print(f"\nInspecting raw data file: {sample_file}")
        
        # Download file content
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_DEFAULT_REGION')
        )
        
        response = s3_client.get_object(
            Bucket=os.getenv('S3_BUCKET_NAME'),
            Key=sample_file
        )
        
        # Read parquet data from bytes
        parquet_data = response['Body'].read()
        df = pd.read_parquet(io.BytesIO(parquet_data))
        print(f"Records in file: {len(df)}")
        print(f"Columns: {list(df.columns)}")
        print("\nSample data:")
        print(df.head(2).to_string())
    
    if processed_files:
        sample_file = processed_files[0]
        print(f"\nInspecting processed data file: {sample_file}")
        
        response = s3_client.get_object(
            Bucket=os.getenv('S3_BUCKET_NAME'),
            Key=sample_file
        )
        
        parquet_data = response['Body'].read()
        df = pd.read_parquet(io.BytesIO(parquet_data))
        print(f"Records in file: {len(df)}")
        print(f"Columns: {list(df.columns)}")
        print("\nSample indicators:")
        print(df.head(2).to_string())

if __name__ == "__main__":
    verify_stored_data()
