import boto3
import os
import requests
from dotenv import load_dotenv

load_dotenv()

def fix_rds_security():
    ec2_client = boto3.client(
        'ec2',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION')
    )
    
    rds_client = boto3.client(
        'rds',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_DEFAULT_REGION')
    )
    
    try:
        # Get your public IP
        my_ip = requests.get('https://checkip.amazonaws.com').text.strip()
        print(f"Your IP: {my_ip}")
        
        # Get RDS security groups
        response = rds_client.describe_db_instances(
            DBInstanceIdentifier='stock-pipeline-db'
        )
        
        security_groups = response['DBInstances'][0]['VpcSecurityGroups']
        
        for sg in security_groups:
            sg_id = sg['VpcSecurityGroupId']
            print(f"Adding rule to security group: {sg_id}")
            
            # Add rule to allow your IP on port 5432
            ec2_client.authorize_security_group_ingress(
                GroupId=sg_id,
                IpPermissions=[
                    {
                        'IpProtocol': 'tcp',
                        'FromPort': 5432,
                        'ToPort': 5432,
                        'IpRanges': [{'CidrIp': f'{my_ip}/32', 'Description': 'Allow PostgreSQL access'}]
                    }
                ]
            )
            print(f"Security group {sg_id} updated")
        
    except Exception as e:
        print(f"Error fixing security group: {e}")

if __name__ == "__main__":
    fix_rds_security()
