import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.warehouse.warehouse_manager import WarehouseManager

def main():
    warehouse = WarehouseManager()
    warehouse.initialize_schema()
    print("Warehouse schema created successfully")

if __name__ == "__main__":
    main()
