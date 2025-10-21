import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.warehouse.rds_warehouse_manager import RDSWarehouseManager

def main():
    warehouse = RDSWarehouseManager()
    
    if warehouse.test_connection():
        print("Initializing database schema...")
        warehouse.initialize_schema()
    else:
        print("Connection failed - check RDS status")

if __name__ == "__main__":
    main()
