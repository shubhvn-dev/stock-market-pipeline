import psycopg2
import pandas as pd
import os
import logging
from dotenv import load_dotenv
from typing import List, Dict

load_dotenv()
logger = logging.getLogger(__name__)

class WarehouseManager:
    def __init__(self):
        self.connection_params = {
            'host': 'localhost',
            'database': 'stock_data',
            'user': 'admin',
            'password': 'password123',
            'port': 5432
        }
        
    def get_connection(self):
        return psycopg2.connect(**self.connection_params)
    
    def initialize_schema(self):
        """Create tables and indexes"""
        with open('src/warehouse/schema.sql', 'r') as f:
            schema_sql = f.read()
        
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(schema_sql)
            conn.commit()
            print("Warehouse schema initialized successfully")
        except Exception as e:
            print(f"Failed to initialize schema: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def load_stock_prices(self, df: pd.DataFrame):
        """Load stock price data from DataFrame"""
        conn = self.get_connection()
        try:
            df_clean = df.rename(columns={
                'open': 'open_price',
                'high': 'high_price', 
                'low': 'low_price',
                'close': 'close_price'
            })
            
            cursor = conn.cursor()
            for _, row in df_clean.iterrows():
                cursor.execute("""
                    INSERT INTO stock_prices (symbol, timestamp, open_price, high_price, low_price, close_price, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    row['symbol'], row['timestamp'], row['open_price'],
                    row['high_price'], row['low_price'], row['close_price'], row['volume']
                ))
            
            conn.commit()
            print(f"Loaded {len(df_clean)} stock price records")
            
        except Exception as e:
            print(f"Failed to load stock prices: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def load_indicators(self, df: pd.DataFrame):
        """Load technical indicators from DataFrame"""
        conn = self.get_connection()
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
            print(f"Loaded {len(df)} indicator records")
            
        except Exception as e:
            print(f"Failed to load indicators: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def query_latest_prices(self, symbol: str, limit: int = 100):
        """Query latest prices for a symbol"""
        conn = self.get_connection()
        try:
            query = """
                SELECT * FROM stock_prices 
                WHERE symbol = %s 
                ORDER BY timestamp DESC 
                LIMIT %s
            """
            df = pd.read_sql(query, conn, params=(symbol, limit))
            return df
        finally:
            conn.close()
