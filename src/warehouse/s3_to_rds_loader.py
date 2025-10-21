import boto3
import pandas as pd
import io
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.warehouse.rds_warehouse_manager import RDSWarehouseManager
from src.storage.cloud_storage import CloudStorage

load_dotenv()
logger = logging.getLogger(__name__)

class S3ToRDSLoader:
    def __init__(self):
        self.warehouse = RDSWarehouseManager()
        self.storage = CloudStorage(provider='aws')
        
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_DEFAULT_REGION')
        )
        self.bucket_name = os.getenv('S3_BUCKET_NAME')
    
    def load_raw_stock_data(self, limit=None):
        """Load raw stock data from S3 to RDS"""
        raw_files = self.storage.list_files('raw/')
        
        if limit:
            raw_files = raw_files[:limit]
        
        total_records = 0
        
        for file_key in raw_files:
            try:
                # Download file from S3
                response = self.s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=file_key
                )
                
                # Read parquet data
                parquet_data = response['Body'].read()
                df = pd.read_parquet(io.BytesIO(parquet_data))
                
                # Load to database
                self._load_stock_prices(df)
                total_records += len(df)
                
                print(f"Loaded {len(df)} records from {file_key}")
                
            except Exception as e:
                print(f"Error loading {file_key}: {e}")
        
        print(f"Total raw records loaded: {total_records}")
    
    def load_processed_indicators(self, limit=None):
        """Load processed indicators from S3 to RDS"""
        processed_files = self.storage.list_files('processed/')
        
        if limit:
            processed_files = processed_files[:limit]
        
        total_records = 0
        
        for file_key in processed_files:
            try:
                # Download file from S3
                response = self.s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=file_key
                )
                
                # Read parquet data
                parquet_data = response['Body'].read()
                df = pd.read_parquet(io.BytesIO(parquet_data))
                
                # Load to database
                self._load_indicators(df)
                total_records += len(df)
                
                print(f"Loaded {len(df)} indicator records from {file_key}")
                
            except Exception as e:
                print(f"Error loading {file_key}: {e}")
        
        print(f"Total indicator records loaded: {total_records}")
    
    def _load_stock_prices(self, df):
        """Load stock price data to RDS"""
        conn = self.warehouse.get_connection()
        try:
            cursor = conn.cursor()
            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO stock_prices (symbol, timestamp, open_price, high_price, low_price, close_price, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    row['symbol'], row['timestamp'], row['open'],
                    row['high'], row['low'], row['close'], row['volume']
                ))
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Failed to load stock prices: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def _load_indicators(self, df):
        """Load technical indicators to RDS"""
        conn = self.warehouse.get_connection()
        try:
            cursor = conn.cursor()
            for _, row in df.iterrows():
                cursor.execute("""
                    INSERT INTO technical_indicators 
                    (symbol, timestamp, current_price, sma_5, sma_10, sma_20, sma_50, ema_12, ema_26, ema_50, rsi_14)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    row['symbol'], row['timestamp'], row['current_price'],
                    row.get('SMA_5'), row.get('SMA_10'), row.get('SMA_20'), row.get('SMA_50'),
                    row.get('EMA_12'), row.get('EMA_26'), row.get('EMA_50'), row.get('RSI_14')
                ))
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Failed to load indicators: {e}")
            conn.rollback()
        finally:
            conn.close()
