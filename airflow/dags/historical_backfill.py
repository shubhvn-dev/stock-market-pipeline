from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import yfinance as yf
import pandas as pd
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'stock-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10)
}

dag = DAG(
    'historical_data_backfill',
    default_args=default_args,
    description='Backfill historical stock data for analysis',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['backfill', 'historical', 'stock-data']
)

def backfill_stock_data(**context):
    """Download and load historical stock data"""
    symbols = ['AAPL', 'AMZN', 'GOOGL', 'MSFT', 'TSLA']
    period = '6mo'  # 6 months of historical data
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    total_records = 0
    
    for symbol in symbols:
        try:
            logger.info(f"Downloading {period} of data for {symbol}")
            
            # Download historical data
            ticker = yf.Ticker(symbol)
            hist_data = ticker.history(period=period)
            
            if hist_data.empty:
                logger.warning(f"No data found for {symbol}")
                continue
            
            # Prepare data for insertion
            hist_data.reset_index(inplace=True)
            hist_data['symbol'] = symbol
            
            # Insert data
            for _, row in hist_data.iterrows():
                insert_query = """
                    INSERT INTO stock_prices 
                    (symbol, timestamp, open_price, high_price, low_price, close_price, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, timestamp) DO NOTHING
                """
                
                postgres_hook.run(insert_query, parameters=[
                    symbol,
                    row['Date'],
                    float(row['Open']),
                    float(row['High']),
                    float(row['Low']),
                    float(row['Close']),
                    int(row['Volume'])
                ])
            
            total_records += len(hist_data)
            logger.info(f"Loaded {len(hist_data)} records for {symbol}")
            
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            continue
    
    logger.info(f"Backfill complete. Total records loaded: {total_records}")
    context['task_instance'].xcom_push(key='total_records', value=total_records)

def calculate_historical_indicators(**context):
    """Calculate technical indicators for historical data"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Get symbols to process
    symbols_query = "SELECT DISTINCT symbol FROM stock_prices ORDER BY symbol"
    symbols = [row[0] for row in postgres_hook.get_records(symbols_query)]
    
    processed_count = 0
    
    for symbol in symbols:
        try:
            # Get historical data for symbol
            data_query = """
                SELECT timestamp, close_price, volume
                FROM stock_prices 
                WHERE symbol = %s 
                ORDER BY timestamp
            """
            
            df = postgres_hook.get_pandas_df(data_query, parameters=[symbol])
            
            if len(df) < 50:  # Need sufficient data for indicators
                logger.warning(f"Insufficient data for {symbol}: {len(df)} records")
                continue
            
            # Calculate technical indicators
            df['sma_5'] = df['close_price'].rolling(window=5).mean()
            df['sma_10'] = df['close_price'].rolling(window=10).mean()
            df['sma_20'] = df['close_price'].rolling(window=20).mean()
            df['sma_50'] = df['close_price'].rolling(window=50).mean()
            
            # RSI calculation
            delta = df['close_price'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['rsi_14'] = 100 - (100 / (1 + rs))
            
            # Insert indicators
            for _, row in df.dropna(subset=['sma_50']).iterrows():
                insert_query = """
                    INSERT INTO technical_indicators 
                    (symbol, timestamp, current_price, sma_5, sma_10, sma_20, sma_50, rsi_14)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, timestamp) DO UPDATE SET
                        sma_5 = EXCLUDED.sma_5,
                        sma_10 = EXCLUDED.sma_10,
                        sma_20 = EXCLUDED.sma_20,
                        sma_50 = EXCLUDED.sma_50,
                        rsi_14 = EXCLUDED.rsi_14
                """
                
                postgres_hook.run(insert_query, parameters=[
                    symbol,
                    row['timestamp'],
                    row['close_price'],
                    float(row['sma_5']) if pd.notna(row['sma_5']) else None,
                    float(row['sma_10']) if pd.notna(row['sma_10']) else None,
                    float(row['sma_20']) if pd.notna(row['sma_20']) else None,
                    float(row['sma_50']) if pd.notna(row['sma_50']) else None,
                    float(row['rsi_14']) if pd.notna(row['rsi_14']) else None
                ])
                processed_count += 1
            
            logger.info(f"Processed indicators for {symbol}")
            
        except Exception as e:
            logger.error(f"Error processing indicators for {symbol}: {e}")
            continue
    
    logger.info(f"Historical indicators complete. Processed: {processed_count}")

def generate_backfill_report(**context):
    """Generate summary report of backfill operation"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Get comprehensive statistics
    stats = {}
    
    # Total records
    stats['total_prices'] = postgres_hook.get_first("SELECT COUNT(*) FROM stock_prices")[0]
    stats['total_indicators'] = postgres_hook.get_first("SELECT COUNT(*) FROM technical_indicators")[0]
    
    # Date range
    date_range = postgres_hook.get_first("""
        SELECT MIN(timestamp), MAX(timestamp) 
        FROM stock_prices
    """)
    stats['date_range'] = f"{date_range[0]} to {date_range[1]}"
    
    # Symbols
    symbols = postgres_hook.get_records("""
        SELECT symbol, COUNT(*) as records 
        FROM stock_prices 
        GROUP BY symbol 
        ORDER BY records DESC
    """)
    stats['symbols'] = {row[0]: row[1] for row in symbols}
    
    # Processing stats from this run
    total_records = context['task_instance'].xcom_pull(
        task_ids='backfill_stock_data',
        key='total_records'
    ) or 0
    
    stats['backfill_records_added'] = total_records
    
    logger.info(f"Backfill Report: {stats}")
    context['task_instance'].xcom_push(key='backfill_report', value=stats)

# Define tasks
backfill_task = PythonOperator(
    task_id='backfill_stock_data',
    python_callable=backfill_stock_data,
    dag=dag
)

indicators_task = PythonOperator(
    task_id='calculate_historical_indicators',
    python_callable=calculate_historical_indicators,
    dag=dag
)

report_task = PythonOperator(
    task_id='generate_backfill_report',
    python_callable=generate_backfill_report,
    dag=dag
)

# Set dependencies
backfill_task >> indicators_task >> report_task
