import dash
from dash import dcc, html, Input, Output, dash_table
import plotly.graph_objs as go
import pandas as pd
import json
from datetime import datetime
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.warehouse.rds_warehouse_manager import RDSWarehouseManager
from src.dashboard.kafka_data_provider import KafkaDataProvider

app = dash.Dash(__name__)

# Initialize connections
warehouse = RDSWarehouseManager()
kafka_provider = KafkaDataProvider()
kafka_provider.start_consumers()

app.layout = html.Div([
    # Header
    html.Div([
        html.H1("Real-Time Stock Market Pipeline Dashboard", 
                style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': 30})
    ]),
    
    # Control Panel
    html.Div([
        html.Div([
            html.Label("Select Stock Symbol:"),
            dcc.Dropdown(
                id='symbol-dropdown',
                options=[
                    {'label': 'AAPL', 'value': 'AAPL'},
                    {'label': 'GOOGL', 'value': 'GOOGL'},
                    {'label': 'MSFT', 'value': 'MSFT'},
                    {'label': 'TSLA', 'value': 'TSLA'},
                    {'label': 'AMZN', 'value': 'AMZN'}
                ],
                value='AAPL'
            )
        ], style={'width': '30%', 'display': 'inline-block'}),
        
        html.Div([
            html.Label("Auto-refresh (15 sec)"),
            dcc.Interval(
                id='interval-component',
                interval=15*1000,  # 15 seconds
                n_intervals=0
            )
        ], style={'width': '20%', 'display': 'inline-block', 'marginLeft': '20px'})
    ], style={'marginBottom': 30}),
    
    # Main Content
    html.Div([
        # Left Column - Price Chart
        html.Div([
            dcc.Graph(id='price-chart')
        ], style={'width': '60%', 'display': 'inline-block'}),
        
        # Right Column - Current Metrics
        html.Div([
            html.H3("Current Metrics"),
            html.Div(id='current-metrics'),
            
            html.H3("Technical Indicators", style={'marginTop': 20}),
            html.Div(id='indicators-table')
        ], style={'width': '35%', 'display': 'inline-block', 'verticalAlign': 'top', 'marginLeft': '5%'})
    ]),
    
    # Bottom Section - LLM Analysis and Alerts
    html.Div([
        # LLM Analysis
        html.Div([
            html.H3("AI Analysis"),
            html.Div(id='llm-analysis', 
                    style={'backgroundColor': '#f8f9fa', 'padding': 15, 'borderRadius': 5, 'minHeight': 200})
        ], style={'width': '48%', 'display': 'inline-block'}),
        
        # Recent Alerts
        html.Div([
            html.H3("Recent Alerts"),
            html.Div(id='alerts-table')
        ], style={'width': '48%', 'display': 'inline-block', 'marginLeft': '4%'})
    ], style={'marginTop': 30})
])

@app.callback(
    [Output('price-chart', 'figure'),
     Output('current-metrics', 'children'),
     Output('indicators-table', 'children'),
     Output('llm-analysis', 'children'),
     Output('alerts-table', 'children')],
    [Input('symbol-dropdown', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_dashboard(selected_symbol, n):
    # Get stock price data from warehouse
    conn = warehouse.get_connection()
    
    price_query = f"""
        SELECT timestamp, close_price, volume
        FROM stock_prices 
        WHERE symbol = '{selected_symbol}'
        ORDER BY timestamp DESC
        LIMIT 50
    """
    
    df_prices = pd.read_sql(price_query, conn)
    
    # Get technical indicators
    indicators_query = f"""
        SELECT timestamp, sma_5, sma_10, ema_12, rsi_14
        FROM technical_indicators 
        WHERE symbol = '{selected_symbol}'
        ORDER BY timestamp DESC
        LIMIT 10
    """
    
    df_indicators = pd.read_sql(indicators_query, conn)
    conn.close()
    
    # Create price chart
    if not df_prices.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_prices['timestamp'],
            y=df_prices['close_price'],
            mode='lines+markers',
            name='Close Price',
            line=dict(color='#3498db', width=2)
        ))
        
        fig.update_layout(
            title=f'{selected_symbol} Price Chart (Last 50 Records)',
            xaxis_title='Time',
            yaxis_title='Price ($)',
            height=400
        )
    else:
        fig = go.Figure()
        fig.update_layout(title=f'No data available for {selected_symbol}')
    
    # Current metrics
    if not df_prices.empty:
        latest_price = df_prices.iloc[0]['close_price']
        latest_volume = df_prices.iloc[0]['volume']
        
        metrics = html.Div([
            html.P(f"Current Price: ${latest_price:.2f}", 
                  style={'fontSize': 18, 'fontWeight': 'bold', 'color': '#27ae60'}),
            html.P(f"Volume: {latest_volume:,}"),
            html.P(f"Last Updated: {df_prices.iloc[0]['timestamp']}")
        ])
    else:
        metrics = html.P("No current data available")
    
    # Indicators table
    if not df_indicators.empty:
        latest_indicators = df_indicators.iloc[0]
        indicators_display = html.Div([
            html.P(f"SMA 5: {latest_indicators['sma_5']:.2f}" if pd.notna(latest_indicators['sma_5']) else "SMA 5: N/A"),
            html.P(f"SMA 10: {latest_indicators['sma_10']:.2f}" if pd.notna(latest_indicators['sma_10']) else "SMA 10: N/A"),
            html.P(f"EMA 12: {latest_indicators['ema_12']:.2f}" if pd.notna(latest_indicators['ema_12']) else "EMA 12: N/A"),
            html.P(f"RSI 14: {latest_indicators['rsi_14']:.2f}" if pd.notna(latest_indicators['rsi_14']) else "RSI 14: N/A")
        ])
    else:
        indicators_display = html.P("No indicator data available")
    
    # Get live LLM analysis
    llm_analysis_data = kafka_provider.get_latest_llm_analysis(selected_symbol)
    if llm_analysis_data:
        llm_display = html.Div([
            html.P(f"Analysis for {selected_symbol}:", style={'fontWeight': 'bold'}),
            html.P(llm_analysis_data['llm_analysis'], style={'lineHeight': 1.6}),
            html.P(f"Generated: {llm_analysis_data['timestamp']}", 
                  style={'fontSize': 12, 'color': '#7f8c8d', 'marginTop': 10})
        ])
    else:
        llm_display = html.P("Waiting for AI analysis... (Make sure LLM consumer is running)")
    
    # Get recent alerts
    recent_alerts = kafka_provider.get_recent_alerts(5)
    if recent_alerts:
        alerts_display = html.Div([
            html.Div([
                html.P(f"{alert['symbol']}: {alert['alert_type']}", 
                      style={'fontWeight': 'bold', 'marginBottom': 5}),
                html.P(alert['message'], style={'fontSize': 14}),
                html.P(f"Severity: {alert['severity']} | {alert['timestamp']}", 
                      style={'fontSize': 12, 'color': '#7f8c8d'}),
                html.Hr()
            ]) for alert in reversed(recent_alerts)
        ])
    else:
        alerts_display = html.P("No recent alerts")
    
    return fig, metrics, indicators_display, llm_display, alerts_display

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8050)
