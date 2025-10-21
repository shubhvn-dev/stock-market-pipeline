import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.storage.cloud_storage import CloudStorage
from datetime import datetime

def test_s3_connection():
    print("Testing S3 connection...")
    
    try:
        # Initialize cloud storage
        storage = CloudStorage(provider='aws')
        
        # Test data
        test_data = [
            {
                'symbol': 'TEST',
                'timestamp': datetime.now().isoformat(),
                'open': 100.0,
                'high': 105.0,
                'low': 98.0,
                'close': 102.0,
                'volume': 50000
            }
        ]
        
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Test raw data upload
        print("Uploading test raw data...")
        storage.save_raw_data(test_data, 'TEST', today)
        print("Raw data upload successful")
        
        # Test indicator data upload
        indicator_data = [
            {
                'symbol': 'TEST',
                'timestamp': datetime.now().isoformat(),
                'current_price': 102.0,
                'SMA_5': 101.0,
                'SMA_10': 100.5,
                'RSI_14': 55.0
            }
        ]
        
        print("Uploading test indicator data...")
        storage.save_processed_indicators(indicator_data, 'TEST', today)
        print("Indicator data upload successful")
        
        # List files to verify
        print("Listing files in bucket...")
        raw_files = storage.list_files('raw/')
        processed_files = storage.list_files('processed/')
        
        print(f"Raw files: {len(raw_files)} found")
        print(f"Processed files: {len(processed_files)} found")
        
        if raw_files:
            print(f"Latest raw file: {raw_files[-1]}")
        if processed_files:
            print(f"Latest processed file: {processed_files[-1]}")
            
        print("S3 integration test completed successfully")
        
    except Exception as e:
        print(f"S3 test failed: {e}")
        print("Check your AWS credentials in .env file")

if __name__ == "__main__":
    test_s3_connection()
