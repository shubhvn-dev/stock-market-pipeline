-- Create database schema for stock market data warehouse

-- Stock prices table (OHLCV data)
CREATE TABLE IF NOT EXISTS stock_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open_price DECIMAL(10,2) NOT NULL,
    high_price DECIMAL(10,2) NOT NULL,
    low_price DECIMAL(10,2) NOT NULL,
    close_price DECIMAL(10,2) NOT NULL,
    volume BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Technical indicators table
CREATE TABLE IF NOT EXISTS technical_indicators (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    current_price DECIMAL(10,2) NOT NULL,
    sma_5 DECIMAL(10,2),
    sma_10 DECIMAL(10,2),
    sma_20 DECIMAL(10,2),
    sma_50 DECIMAL(10,2),
    ema_12 DECIMAL(10,2),
    ema_26 DECIMAL(10,2),
    ema_50 DECIMAL(10,2),
    rsi_14 DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Alerts table
CREATE TABLE IF NOT EXISTS alerts (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    alert_type VARCHAR(50) NOT NULL,
    message TEXT NOT NULL,
    severity VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    value DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_stock_prices_symbol_timestamp ON stock_prices(symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_indicators_symbol_timestamp ON technical_indicators(symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_alerts_symbol_timestamp ON alerts(symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_alerts_type ON alerts(alert_type);
