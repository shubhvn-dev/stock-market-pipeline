# Stock Market Data Pipeline

A real-time stock market data streaming and analytics pipeline built with Apache Kafka, Apache Spark, Apache Airflow, and AWS RDS.

## Architecture
```
Yahoo Finance API → Kafka Producer → Kafka Broker → Spark Consumer → AWS RDS PostgreSQL
                                                                            ↓
                                                     Apache Airflow DAGs ← RDS
                                                            ↓
                                                    Daily Aggregations
```

## Tech Stack

- **Streaming**: Apache Kafka (Confluent Platform)
- **Processing**: Apache Spark
- **Orchestration**: Apache Airflow
- **Database**: AWS RDS PostgreSQL
- **Data Source**: Yahoo Finance API (yfinance)
- **Containerization**: Docker & Docker Compose

## Features

- Real-time stock price streaming from Yahoo Finance
- Kafka-based message queue for reliable data ingestion
- Spark streaming for data processing
- AWS RDS PostgreSQL for data warehousing
- Airflow DAGs for daily aggregations and data quality checks
- Technical indicators calculation
- Daily OHLCV (Open, High, Low, Close, Volume) summaries

## Prerequisites

- Docker Desktop
- Docker Compose
- AWS Account (for RDS)
- Python 3.11+
- Git

## Project Structure
```
stock-market-pipeline/
├── airflow/
│   ├── dags/
│   │   ├── daily_aggregation.py
│   │   ├── postgres_warehouse_loading.py
│   │   └── historical_backfill.py
│   ├── logs/
│   └── plugins/
├── src/
│   ├── kafka_producer.py
│   ├── spark_consumer.py
│   └── technical_indicators.py
├── docker-compose.yml
├── Dockerfile.airflow
├── requirements.txt
└── README.md
```

## Setup Instructions

### 1. Clone the Repository
```bash
git clone <your-repo-url>
cd stock-market-pipeline
```

### 2. Configure AWS RDS

Create a PostgreSQL RDS instance with the following settings:
- Database name: `stockdata`
- Username: `stockuser`
- Password: `stockpipeline123`
- Port: `5432`

Update security group to allow inbound connections from your IP on port 5432.

### 3. Update Environment Variables

Update `docker-compose.yml` with your RDS endpoint:
```yaml
AIRFLOW_CONN_POSTGRES_DEFAULT: 'postgresql://stockuser:stockpipeline123@YOUR-RDS-ENDPOINT:5432/stockdata'
```

### 4. Build and Start Services
```bash
# Build custom Airflow image
docker-compose build

# Start all services
docker-compose up -d

# Check services are running
docker-compose ps
```

### 5. Initialize Airflow
```bash
# Create Airflow admin user (if not exists)
docker-compose run --rm airflow-webserver airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

### 6. Access Services

- **Airflow UI**: http://localhost:8070 (admin/admin)
- **Spark Master UI**: http://localhost:8090
- **Kafka**: localhost:9092

## Running the Pipeline

### Start the Kafka Producer
```bash
python src/kafka_producer.py
```

This will start streaming real-time stock prices for AAPL, GOOGL, MSFT, AMZN, and TSLA.

### Start the Spark Consumer
```bash
python src/spark_consumer.py
```

This processes the Kafka stream and writes to the RDS database.

### Monitor Airflow DAGs

1. Go to http://localhost:8070
2. Enable the DAGs:
   - `daily_stock_aggregation` - Creates daily OHLCV summaries
   - `postgres_warehouse_loading` - Loads processed data into warehouse

## Database Schema

### stock_prices
```sql
- id (SERIAL PRIMARY KEY)
- symbol (VARCHAR)
- timestamp (TIMESTAMP)
- open_price, high_price, low_price, close_price (DECIMAL)
- volume (BIGINT)
- created_at (TIMESTAMP)
```

### daily_stock_summary
```sql
- id (SERIAL PRIMARY KEY)
- symbol (VARCHAR)
- date (DATE)
- open_price, high_price, low_price, close_price (DECIMAL)
- volume (BIGINT)
- avg_price (DECIMAL)
- created_at (TIMESTAMP)
- UNIQUE(symbol, date)
```

### technical_indicators
```sql
- id (SERIAL PRIMARY KEY)
- symbol (VARCHAR)
- timestamp (TIMESTAMP)
- rsi (DECIMAL)
- macd (DECIMAL)
- signal_line (DECIMAL)
- created_at (TIMESTAMP)
```

## Stopping the Pipeline
```bash
# Stop all services
docker-compose down

# Stop and remove volumes (WARNING: This deletes local data)
docker-compose down -v
```

## Troubleshooting

### Airflow Connection Issues

If DAGs fail with connection errors:
```bash
# Delete old connection
docker exec stock-market-pipeline-airflow-webserver-1 airflow connections delete postgres_default

# Restart services
docker-compose restart airflow-webserver airflow-scheduler
```

### RDS Connection Refused

Check your security group allows inbound traffic on port 5432 from your IP:
```bash
# Test RDS connection
docker run --rm postgres:15 psql postgresql://stockuser:stockpipeline123@YOUR-RDS-ENDPOINT:5432/stockdata -c "SELECT 1;"
```

### View Logs
```bash
# Airflow webserver logs
docker-compose logs -f airflow-webserver

# Airflow scheduler logs
docker-compose logs -f airflow-scheduler

# All logs
docker-compose logs -f
```

## Environment Variables

Key environment variables in `docker-compose.yml`:

- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` - Airflow metadata database
- `AIRFLOW_CONN_POSTGRES_DEFAULT` - Default PostgreSQL connection
- `AIRFLOW_CONN_POSTGRES_WAREHOUSE` - Warehouse PostgreSQL connection

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License.

## Acknowledgments

- Yahoo Finance for providing stock market data
- Apache Software Foundation for Kafka, Spark, and Airflow
- Confluent Platform for Kafka distribution

## Contact

Shubhan Kadam - dev.shubhankadam@gmail.com
