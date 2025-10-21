import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.warehouse.rds_warehouse_manager import RDSWarehouseManager
import pandas as pd

def verify_data():
    warehouse = RDSWarehouseManager()
    
    # Test stock prices query
    print("Testing stock prices table...")
    conn = warehouse.get_connection()
    
    # Query stock prices
    stock_query = """
        SELECT symbol, COUNT(*) as record_count, 
               MIN(timestamp) as earliest_date,
               MAX(timestamp) as latest_date,
               AVG(close_price) as avg_price
        FROM stock_prices 
        GROUP BY symbol 
        ORDER BY symbol;
    """
    
    df_stocks = pd.read_sql(stock_query, conn)
    print("Stock Prices Summary:")
    print(df_stocks.to_string(index=False))
    
    # Query technical indicators
    print("\nTesting technical indicators table...")
    indicator_query = """
        SELECT symbol, COUNT(*) as record_count,
               AVG(sma_5) as avg_sma5,
               AVG(ema_12) as avg_ema12,
               AVG(rsi_14) as avg_rsi
        FROM technical_indicators 
        GROUP BY symbol 
        ORDER BY symbol;
    """
    
    df_indicators = pd.read_sql(indicator_query, conn)
    print("Technical Indicators Summary:")
    print(df_indicators.to_string(index=False))
    
    # Test specific stock query
    print("\nLatest AAPL data:")
    latest_query = """
        SELECT sp.symbol, sp.timestamp, sp.close_price,
               ti.sma_5, ti.ema_12, ti.rsi_14
        FROM stock_prices sp
        LEFT JOIN technical_indicators ti ON sp.symbol = ti.symbol 
                                           AND sp.timestamp = ti.timestamp
        WHERE sp.symbol = 'AAPL'
        ORDER BY sp.timestamp DESC
        LIMIT 5;
    """
    
    df_latest = pd.read_sql(latest_query, conn)
    print(df_latest.to_string(index=False))
    
    conn.close()
    print("\nWarehouse verification complete!")

if __name__ == "__main__":
    verify_data()
