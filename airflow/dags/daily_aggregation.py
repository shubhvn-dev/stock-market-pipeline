from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
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
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'daily_stock_aggregation',
    default_args=default_args,
    description='Daily aggregation and summary of stock market data',
    schedule_interval='0 6 * * *',
    catchup=False,
    tags=['daily', 'aggregation', 'stock-data']
)

def create_daily_summary(**context):
    """Create daily OHLCV summaries using correct SQL"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Create table
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS daily_stock_summary (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(10) NOT NULL,
            date DATE NOT NULL,
            open_price DECIMAL(10,2),
            high_price DECIMAL(10,2),
            low_price DECIMAL(10,2),
            close_price DECIMAL(10,2),
            volume BIGINT,
            avg_price DECIMAL(10,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, date)
        )
    """
    postgres_hook.run(create_table_sql)
    
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Fixed SQL - separate the window functions from GROUP BY
    aggregation_sql = """
        WITH daily_prices AS (
            SELECT 
                symbol,
                DATE(timestamp) as date,
                close_price,
                volume,
                timestamp,
                ROW_NUMBER() OVER (PARTITION BY symbol, DATE(timestamp) ORDER BY timestamp) as rn_first,
                ROW_NUMBER() OVER (PARTITION BY symbol, DATE(timestamp) ORDER BY timestamp DESC) as rn_last
            FROM stock_prices 
            WHERE DATE(timestamp) = %s
        ),
        ohlc_data AS (
            SELECT 
                symbol,
                date,
                MAX(CASE WHEN rn_first = 1 THEN close_price END) as open_price,
                MAX(close_price) as high_price,
                MIN(close_price) as low_price,
                MAX(CASE WHEN rn_last = 1 THEN close_price END) as close_price,
                SUM(volume) as volume,
                AVG(close_price) as avg_price
            FROM daily_prices
            GROUP BY symbol, date
        )
        INSERT INTO daily_stock_summary 
        (symbol, date, open_price, high_price, low_price, close_price, volume, avg_price)
        SELECT * FROM ohlc_data
        ON CONFLICT (symbol, date) DO UPDATE SET
            open_price = EXCLUDED.open_price,
            high_price = EXCLUDED.high_price,
            low_price = EXCLUDED.low_price,
            close_price = EXCLUDED.close_price,
            volume = EXCLUDED.volume,
            avg_price = EXCLUDED.avg_price
    """
    
    postgres_hook.run(aggregation_sql, parameters=[yesterday])
    logger.info(f"Created daily summaries for {yesterday}")

def generate_simple_alerts(**context):
    """Generate alerts for significant movements"""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Simple volatility check
    alert_query = """
        SELECT symbol, open_price, close_price, high_price, low_price
        FROM daily_stock_summary
        WHERE date = %s 
        AND close_price > 0
        AND ((high_price - low_price) / close_price) > 0.05
    """
    
    volatile_stocks = postgres_hook.get_records(alert_query, parameters=[yesterday])
    
    for symbol, open_price, close_price, high_price, low_price in volatile_stocks:
        volatility = ((high_price - low_price) / close_price) * 100
        price_change = close_price - open_price if open_price else 0
        
        alert_message = f"{symbol} volatility: {volatility:.1f}% range, ${close_price:.2f}"
        
        insert_alert_sql = """
            INSERT INTO alerts (symbol, alert_type, message, severity, timestamp, value)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        postgres_hook.run(insert_alert_sql, parameters=[
            symbol, 'daily_volatility', alert_message, 'medium',
            datetime.now(), float(volatility)
        ])
    
    logger.info(f"Generated {len(volatile_stocks)} volatility alerts for {yesterday}")

# Define tasks
summary_task = PythonOperator(
    task_id='create_daily_summary',
    python_callable=create_daily_summary,
    dag=dag
)

alerts_task = PythonOperator(
    task_id='generate_simple_alerts',
    python_callable=generate_simple_alerts,
    dag=dag
)

# Dependencies
summary_task >> alerts_task
