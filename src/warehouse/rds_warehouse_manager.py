import psycopg2
import pandas as pd
import os
import logging
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

class RDSWarehouseManager:
    def __init__(self):
        self.connection_params = {
            'host': 'stock-pipeline-db.cg18g6e0wizx.us-east-1.rds.amazonaws.com',
            'database': 'stockdata',
            'user': 'stockuser',
            'password': 'stockpipeline123',
            'port': 5432
        }
        
    def get_connection(self):
        return psycopg2.connect(**self.connection_params)
    
    def test_connection(self):
        """Test RDS connection"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            result = cursor.fetchone()
            print(f"Connected to RDS PostgreSQL: {result[0]}")
            conn.close()
            return True
        except Exception as e:
            print(f"RDS connection failed: {e}")
            return False
    
    def initialize_schema(self):
        """Create tables and indexes"""
        with open('src/warehouse/schema.sql', 'r') as f:
            schema_sql = f.read()
        
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(schema_sql)
            conn.commit()
            print("RDS warehouse schema initialized successfully")
        except Exception as e:
            print(f"Failed to initialize RDS schema: {e}")
            conn.rollback()
        finally:
            conn.close()
