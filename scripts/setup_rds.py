import boto3
import os
from dotenv import load_dotenv

load_dotenv()

def create_rds_instance():
    rds_client = boto3.client(
        'rds',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION')
    )
    
    try:
        response = rds_client.create_db_instance(
            DBInstanceIdentifier='stock-pipeline-db',
            DBInstanceClass='db.t3.micro',
            Engine='postgres',
            MasterUsername='stockuser',  # Changed from admin
            MasterUserPassword='stockpipeline123',
            AllocatedStorage=20,
            DBName='stockdata',
            PubliclyAccessible=True,
            StorageType='gp2',
            BackupRetentionPeriod=0,
            MultiAZ=False
        )
        
        print(f"RDS instance creation initiated: {response['DBInstance']['DBInstanceIdentifier']}")
        print("Instance will take 5-10 minutes to become available")
        
    except Exception as e:
        print(f"Failed to create RDS instance: {e}")

if __name__ == "__main__":
    create_rds_instance()
