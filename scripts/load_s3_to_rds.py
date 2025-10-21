import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.warehouse.s3_to_rds_loader import S3ToRDSLoader

def main():
    loader = S3ToRDSLoader()
    
    print("Loading data from S3 to RDS...")
    
    # Load raw stock data (limit to first 10 files for testing)
    print("Loading raw stock data...")
    loader.load_raw_stock_data(limit=10)
    
    # Load processed indicators (limit to first 10 files for testing)
    print("Loading processed indicators...")
    loader.load_processed_indicators(limit=10)
    
    print("Data loading complete!")

if __name__ == "__main__":
    main()
