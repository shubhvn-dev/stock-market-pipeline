from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
import json
from io import StringIO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'stock-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1
}

dag = DAG(
    'postgres_warehouse_loading',
    default_args=default_args,
    description='Load and process stock data directly in PostgreSQL',
    schedule_interval='@hourly',
    catchup=False,
    tags=['warehouse', 'etl', 'postgres']
)

def check_data_availability(**context):
    """Check if we have recent stock data in PostgreSQL"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Check for recent data (last 2 hours)
        recent_data_query = """
            SELECT COUNT(*) as recent_count 
            FROM stock_prices 
            WHERE created_at >= NOW() - INTERVAL '2 hours'
        """
        
        result = postgres_hook.get_first(recent_data_query)
        recent_count = result[0] if result else 0
        
        logger.info(f"Found {recent_count} recent stock price records")
        
        # Store count for downstream tasks
        context['task_instance'].xcom_push(key='recent_data_count', value=recent_count)
        
        return recent_count > 0
        
    except Exception as e:
        logger.error(f"Error checking data availability: {e}")
        raise

def process_stock_indicators(**context):
    """Calculate technical indicators for recent stock data"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Get recent stock data without indicators
        data_query = """
            SELECT symbol, timestamp, close_price, volume
            FROM stock_prices 
            WHERE created_at >= NOW() - INTERVAL '2 hours'
            AND symbol NOT IN (
                SELECT DISTINCT symbol 
                FROM technical_indicators 
                WHERE created_at >= NOW() - INTERVAL '1 hour'
            )
            ORDER BY symbol, timestamp
        """
        
        df = postgres_hook.get_pandas_df(data_query)
        
        if df.empty:
            logger.info("No new data to process")
            return
        
        processed_count = 0
        
        # Process each symbol
        for symbol in df['symbol'].unique():
            symbol_data = df[df['symbol'] == symbol].sort_values('timestamp')
            
            if len(symbol_data) < 5:  # Need at least 5 data points for SMA_5
                continue
                
            # Calculate simple moving averages
            symbol_data['sma_5'] = symbol_data['close_price'].rolling(window=5, min_periods=5).mean()
            symbol_data['sma_10'] = symbol_data['close_price'].rolling(window=10, min_periods=10).mean()
            symbol_data['sma_20'] = symbol_data['close_price'].rolling(window=20, min_periods=20).mean()
            
            # Calculate RSI (simplified)
            price_changes = symbol_data['close_price'].diff()
            gains = price_changes.where(price_changes > 0, 0)
            losses = -price_changes.where(price_changes < 0, 0)
            
            if len(gains) >= 14:
                avg_gains = gains.rolling(window=14, min_periods=14).mean()
                avg_losses = losses.rolling(window=14, min_periods=14).mean()
                rs = avg_gains / avg_losses
                symbol_data['rsi_14'] = 100 - (100 / (1 + rs))
            
            # Insert indicators for records that have them
            for _, row in symbol_data.dropna(subset=['sma_5']).iterrows():
                insert_query = """
                    INSERT INTO technical_indicators 
                    (symbol, timestamp, current_price, sma_5, sma_10, sma_20, rsi_14)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (symbol, timestamp) DO NOTHING
                """
                
                postgres_hook.run(insert_query, parameters=[
                    row['symbol'], row['timestamp'], row['close_price'],
                    float(row['sma_5']) if pd.notna(row['sma_5']) else None,
                    float(row['sma_10']) if pd.notna(row['sma_10']) else None,
                    float(row['sma_20']) if pd.notna(row['sma_20']) else None,
                    float(row['rsi_14']) if pd.notna(row['rsi_14']) else None
                ])
                processed_count += 1
        
        logger.info(f"Processed {processed_count} indicator records")
        context['task_instance'].xcom_push(key='indicators_processed', value=processed_count)
        
    except Exception as e:
        logger.error(f"Error processing indicators: {e}")
        raise

def cleanup_old_data(**context):
    """Clean up old data to maintain database size"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Clean data older than 30 days
        cleanup_queries = [
            "DELETE FROM stock_prices WHERE created_at < NOW() - INTERVAL '30 days'",
            "DELETE FROM technical_indicators WHERE created_at < NOW() - INTERVAL '30 days'",
            "DELETE FROM alerts WHERE created_at < NOW() - INTERVAL '7 days'"
        ]
        
        for query in cleanup_queries:
            result = postgres_hook.run(query)
            logger.info(f"Cleanup query executed: {query}")
        
        # Update table statistics
        postgres_hook.run("ANALYZE stock_prices")
        postgres_hook.run("ANALYZE technical_indicators")
        
        logger.info("Database cleanup completed")
        
    except Exception as e:
        logger.error(f"Error in cleanup: {e}")
        raise

def generate_warehouse_report(**context):
    """Generate warehouse status and data quality report"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Get comprehensive statistics
        stats_queries = {
            'total_stock_records': "SELECT COUNT(*) FROM stock_prices",
            'total_indicators': "SELECT COUNT(*) FROM technical_indicators",
            'unique_symbols': "SELECT COUNT(DISTINCT symbol) FROM stock_prices",
            'latest_data': "SELECT MAX(created_at) FROM stock_prices",
            'data_by_symbol': """
                SELECT symbol, COUNT(*) as records, 
                       MIN(created_at) as earliest, 
                       MAX(created_at) as latest 
                FROM stock_prices 
                GROUP BY symbol 
                ORDER BY records DESC
            """
        }
        
        report = {
            'execution_date': context['execution_date'].isoformat(),
            'warehouse_stats': {}
        }
        
        for stat_name, query in stats_queries.items():
            try:
                if stat_name == 'data_by_symbol':
                    results = postgres_hook.get_records(query)
                    report['warehouse_stats'][stat_name] = [
                        {
                            'symbol': r[0], 
                            'records': r[1], 
                            'earliest': str(r[2]), 
                            'latest': str(r[3])
                        } for r in results
                    ]
                else:
                    result = postgres_hook.get_first(query)
                    report['warehouse_stats'][stat_name] = str(result[0]) if result else 'No data'
            except Exception as e:
                report['warehouse_stats'][stat_name] = f'Error: {str(e)}'
        
        # Get processing stats from XCom
        recent_data_count = context['task_instance'].xcom_pull(
            task_ids='check_data_availability',
            key='recent_data_count'
        ) or 0
        
        indicators_processed = context['task_instance'].xcom_pull(
            task_ids='process_stock_indicators',
            key='indicators_processed'
        ) or 0
        
        report['processing_stats'] = {
            'recent_data_found': recent_data_count,
            'indicators_processed_this_run': indicators_processed
        }
        
        # Store report
        context['task_instance'].xcom_push(key='warehouse_report', value=report)
        
        logger.info(f"Warehouse Report: {json.dumps(report, indent=2)}")
        
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        raise

# Define tasks
check_data_task = PythonOperator(
    task_id='check_data_availability',
    python_callable=check_data_availability,
    dag=dag
)

process_indicators_task = PythonOperator(
    task_id='process_stock_indicators',
    python_callable=process_stock_indicators,
    dag=dag
)

cleanup_task = PythonOperator(
    task_id='cleanup_old_data',
    python_callable=cleanup_old_data,
    dag=dag
)

report_task = PythonOperator(
    task_id='generate_warehouse_report',
    python_callable=generate_warehouse_report,
    dag=dag
)

# Create performance indexes
create_indexes_task = PostgresOperator(
    task_id='optimize_database',
    postgres_conn_id='postgres_default',
    sql="""
        -- Create indexes if they don't exist
        CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_created 
        ON stock_prices(symbol, created_at DESC);
        
        CREATE INDEX IF NOT EXISTS idx_indicators_symbol_created 
        ON technical_indicators(symbol, created_at DESC);
        
        CREATE INDEX IF NOT EXISTS idx_stock_prices_timestamp 
        ON stock_prices(timestamp DESC);
        
        -- Update table statistics
        ANALYZE stock_prices;
        ANALYZE technical_indicators;
    """,
    dag=dag
)

# Set task dependencies
check_data_task >> process_indicators_task >> cleanup_task >> create_indexes_task >> report_task
