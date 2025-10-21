import os
import json
import pandas as pd
from datetime import datetime
from typing import Dict, List
import logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

class CloudStorage:
    def __init__(self, provider='aws'):
        self.provider = provider
        
        if provider == 'aws':
            import boto3
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
            )
            self.bucket_name = os.getenv('S3_BUCKET_NAME')
        
    def save_raw_data(self, data: List[Dict], symbol: str, date: str):
        """Save raw stock data in Parquet format"""
        df = pd.DataFrame(data)
        
        # Create partition path: raw/symbol=AAPL/date=2025-10-17/
        partition_path = f"raw/symbol={symbol}/date={date}/"
        filename = f"{partition_path}stock_data_{datetime.now().strftime('%H%M%S')}.parquet"
        
        # Convert to parquet bytes
        parquet_buffer = df.to_parquet(index=False)
        
        self._upload_data(filename, parquet_buffer)
        logger.info(f"Saved raw data to {filename}")
        
    def save_processed_indicators(self, data: List[Dict], symbol: str, date: str):
        """Save processed technical indicators in Parquet format"""
        df = pd.DataFrame(data)
        
        partition_path = f"processed/symbol={symbol}/date={date}/"
        filename = f"{partition_path}indicators_{datetime.now().strftime('%H%M%S')}.parquet"
        
        parquet_buffer = df.to_parquet(index=False)
        
        self._upload_data(filename, parquet_buffer)
        logger.info(f"Saved indicators to {filename}")
    
    def save_alerts(self, data: List[Dict], date: str):
        """Save alerts data"""
        df = pd.DataFrame(data)
        
        partition_path = f"alerts/date={date}/"
        filename = f"{partition_path}alerts_{datetime.now().strftime('%H%M%S')}.parquet"
        
        parquet_buffer = df.to_parquet(index=False)
        
        self._upload_data(filename, parquet_buffer)
        logger.info(f"Saved alerts to {filename}")
    
    def _upload_data(self, filename: str, data: bytes):
        """Upload data to S3"""
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=filename,
            Body=data
        )
    
    def list_files(self, prefix: str = ""):
        """List files in S3 bucket"""
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket_name,
            Prefix=prefix
        )
        return [obj['Key'] for obj in response.get('Contents', [])]
