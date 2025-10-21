import dash
from dash import dcc, html, Input, Output, dash_table
import plotly.graph_objs as go
import plotly.express as px
import pandas as pd
import json
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.warehouse.rds_warehouse_manager import RDSWarehouseManager

# Modern color palette
COLORS = {
    'primary': '#1a1a2e',
    'secondary': '#16213e', 
    'accent': '#0f3460',
    'success': '#00d4aa',
    'warning': '#ff6b6b',
    'info': '#4dabf7',
    'background': '#0a0a0a',
    'surface': '#1e1e1e',
    'text': '#ffffff',
    'text_secondary': '#a0a0a0'
}

app = dash.Dash(__name__)
warehouse = RDSWarehouseManager()

# Custom CSS styles
METRIC_CARD_STYLE = {
    'background': 'rgba(255, 255, 255, 0.08)',
    'border': '1px solid rgba(255, 255, 255, 0.2)',
    'borderRadius': '12px',
    'padding': '15px',
    'margin': '8px 0',
    'transition': 'all 0.3s ease'
}

# Enhanced layout
app.layout = html.Div([
    # Background with gradient
    html.Div(style={
        'position': 'fixed',
        'top': 0,
        'left': 0,
        'width': '100%',
        'height': '100%',
        'background': f'linear-gradient(135deg, {COLORS["primary"]} 0%, {COLORS["secondary"]} 50%, {COLORS["accent"]} 100%)',
        'zIndex': -1
    }),
    
    # Header with glassmorphism effect
    html.Div([
        html.H1("Stock Market Intelligence", style={
            'textAlign': 'center',
            'color': COLORS['text'],
            'marginBottom': '10px',
            'fontSize': '3rem',
            'fontWeight': '700',
            'letterSpacing': '2px',
            'textShadow': '0 0 20px rgba(255,255,255,0.3)'
        }),
        html.P("Real-time AI-powered stock analysis with 6 months of historical data", style={
            'textAlign': 'center',
            'color': COLORS['text_secondary'],
            'fontSize': '1.2rem',
            'marginBottom': '30px'
        })
    ], style={
        'background': 'rgba(255, 255, 255, 0.1)',
        'backdropFilter': 'blur(20px)',
        'border': '1px solid rgba(255, 255, 255, 0.2)',
        'borderRadius': '20px',
        'padding': '30px',
        'margin': '20px',
        'boxShadow': '0 20px 40px rgba(0,0,0,0.3)'
    }),
    
    # Control Panel
    html.Div([
        html.Div([
            html.Label("Select Stock", style={'color': COLORS['text'], 'fontWeight': 'bold', 'marginBottom': '10px', 'display': 'block'}),
            dcc.Dropdown(
                id='symbol-dropdown',
                options=[
                    {'label': 'Apple (AAPL)', 'value': 'AAPL'},
                    {'label': 'Google (GOOGL)', 'value': 'GOOGL'},
                    {'label': 'Microsoft (MSFT)', 'value': 'MSFT'},
                    {'label': 'Tesla (TSLA)', 'value': 'TSLA'},
                    {'label': 'Amazon (AMZN)', 'value': 'AMZN'}
                ],
                value='AAPL',
                style={
                    'backgroundColor': COLORS['surface'],
                    'color': COLORS['text'],
                    'border': 'none'
                }
            )
        ], style={
            'width': '30%',
            'display': 'inline-block',
            'background': 'rgba(255, 255, 255, 0.05)',
            'backdropFilter': 'blur(10px)',
            'border': '1px solid rgba(255, 255, 255, 0.1)',
            'borderRadius': '15px',
            'padding': '20px',
            'margin': '10px'
        }),
        
        html.Div([
            html.Label("Auto-refresh", style={'color': COLORS['text'], 'fontWeight': 'bold'}),
            html.P("Updates every 15 seconds", style={'color': COLORS['text_secondary'], 'fontSize': '0.9rem', 'margin': '5px 0'}),
            dcc.Interval(
                id='interval-component',
                interval=15*1000,
                n_intervals=0
            ),
            html.Div([
                html.Div(style={
                    'width': '10px',
                    'height': '10px',
                    'backgroundColor': COLORS['success'],
                    'borderRadius': '50%',
                    'display': 'inline-block',
                    'marginRight': '8px'
                }),
                html.Span("LIVE", style={'color': COLORS['success'], 'fontWeight': 'bold', 'fontSize': '0.8rem'})
            ])
        ], style={
            'width': '25%',
            'display': 'inline-block',
            'background': 'rgba(255, 255, 255, 0.05)',
            'backdropFilter': 'blur(10px)',
            'border': '1px solid rgba(255, 255, 255, 0.1)',
            'borderRadius': '15px',
            'padding': '20px',
            'margin': '10px',
            'marginLeft': '5%'
        }),
        
        html.Div([
            html.Div(id='key-metrics-summary')
        ], style={
            'width': '30%',
            'display': 'inline-block',
            'background': 'rgba(255, 255, 255, 0.05)',
            'backdropFilter': 'blur(10px)',
            'border': '1px solid rgba(255, 255, 255, 0.1)',
            'borderRadius': '15px',
            'padding': '20px',
            'margin': '10px',
            'marginLeft': '5%'
        })
    ], style={'margin': '20px'}),
    
    # Main Content
    html.Div([
        html.Div([
            html.H3("Price Chart & Technical Analysis", style={
                'color': COLORS['text'],
                'marginBottom': '20px',
                'fontSize': '1.5rem'
            }),
            dcc.Graph(id='price-chart', style={'height': '500px'})
        ], style={
            'width': '68%',
            'display': 'inline-block',
            'background': 'rgba(255, 255, 255, 0.05)',
            'backdropFilter': 'blur(10px)',
            'border': '1px solid rgba(255, 255, 255, 0.1)',
            'borderRadius': '20px',
            'padding': '25px',
            'margin': '10px'
        }),
        
        html.Div([
            html.H3("Real-time Metrics", style={
                'color': COLORS['text'],
                'marginBottom': '20px',
                'fontSize': '1.5rem'
            }),
            html.Div(id='current-metrics'),
            
            html.H3("Technical Indicators", style={
                'color': COLORS['text'],
                'marginTop': '30px',
                'marginBottom': '20px',
                'fontSize': '1.5rem'
            }),
            html.Div(id='indicators-table')
        ], style={
            'width': '28%',
            'display': 'inline-block',
            'verticalAlign': 'top',
            'background': 'rgba(255, 255, 255, 0.05)',
            'backdropFilter': 'blur(10px)',
            'border': '1px solid rgba(255, 255, 255, 0.1)',
            'borderRadius': '20px',
            'padding': '25px',
            'margin': '10px',
            'marginLeft': '2%'
        })
    ]),
    
    # Bottom section
    html.Div([
        html.Div([
            html.H3("AI Market Analysis", style={
                'color': COLORS['text'],
                'marginBottom': '20px',
                'fontSize': '1.5rem'
            }),
            html.Div(id='llm-analysis', style={
                'backgroundColor': 'rgba(255, 255, 255, 0.03)',
                'border': '1px solid rgba(255, 255, 255, 0.1)',
                'borderRadius': '15px',
                'padding': '20px',
                'minHeight': '250px',
                'color': COLORS['text'],
                'lineHeight': '1.6'
            })
        ], style={
            'width': '48%',
            'display': 'inline-block',
            'background': 'rgba(255, 255, 255, 0.05)',
            'backdropFilter': 'blur(10px)',
            'border': '1px solid rgba(255, 255, 255, 0.1)',
            'borderRadius': '20px',
            'padding': '25px',
            'margin': '10px'
        }),
        
        html.Div([
            html.H3("Smart Alerts", style={
                'color': COLORS['text'],
                'marginBottom': '20px',
                'fontSize': '1.5rem'
            }),
            html.Div(id='alerts-table')
        ], style={
            'width': '48%',
            'display': 'inline-block',
            'background': 'rgba(255, 255, 255, 0.05)',
            'backdropFilter': 'blur(10px)',
            'border': '1px solid rgba(255, 255, 255, 0.1)',
            'borderRadius': '20px',
            'padding': '25px',
            'margin': '10px',
            'marginLeft': '2%'
        })
    ], style={'margin': '20px'})
], style={
    'fontFamily': '"Inter", "Segoe UI", "Roboto", sans-serif',
    'minHeight': '100vh',
    'margin': 0,
    'padding': 0
})

@app.callback(
    [Output('price-chart', 'figure'),
     Output('current-metrics', 'children'),
     Output('indicators-table', 'children'),
     Output('llm-analysis', 'children'),
     Output('alerts-table', 'children'),
     Output('key-metrics-summary', 'children')],
    [Input('symbol-dropdown', 'value'),
     Input('interval-component', 'n_intervals')]
)
def update_dashboard(selected_symbol, n):
    try:
        conn = warehouse.get_connection()
        
        price_query = """
            SELECT timestamp, close_price, volume
            FROM stock_prices 
            WHERE symbol = %s 
            ORDER BY timestamp DESC 
            LIMIT 100
        """
        df_prices = pd.read_sql(price_query, conn, params=[selected_symbol])
        
        indicators_query = """
            SELECT timestamp, current_price, sma_5, sma_10, sma_20, sma_50, rsi_14
            FROM technical_indicators 
            WHERE symbol = %s 
            ORDER BY timestamp DESC 
            LIMIT 50
        """
        df_indicators = pd.read_sql(indicators_query, conn, params=[selected_symbol])
        
        conn.close()
        
        # Enhanced price chart
        fig = go.Figure()
        
        if not df_prices.empty:
            fig.add_trace(go.Scatter(
                x=df_prices['timestamp'],
                y=df_prices['close_price'],
                mode='lines',
                name='Price',
                line=dict(color=COLORS['info'], width=3),
                fill='tonexty',
                fillcolor='rgba(77, 171, 247, 0.1)'
            ))
            
            if not df_indicators.empty:
                fig.add_trace(go.Scatter(
                    x=df_indicators['timestamp'],
                    y=df_indicators['sma_20'],
                    mode='lines',
                    name='SMA 20',
                    line=dict(color=COLORS['success'], width=2, dash='dot')
                ))
        
        fig.update_layout(
            template='plotly_dark',
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color=COLORS['text'], family='Inter'),
            xaxis=dict(gridcolor='rgba(255,255,255,0.1)', showgrid=True),
            yaxis=dict(gridcolor='rgba(255,255,255,0.1)', showgrid=True),
            legend=dict(
                bgcolor='rgba(255,255,255,0.1)',
                bordercolor='rgba(255,255,255,0.2)',
                borderwidth=1
            ),
            margin=dict(l=0, r=0, t=0, b=0)
        )
        
        # Enhanced metrics
        if not df_prices.empty:
            latest_price = df_prices.iloc[0]['close_price']
            price_change = latest_price - df_prices.iloc[1]['close_price'] if len(df_prices) > 1 else 0
            price_change_pct = (price_change / df_prices.iloc[1]['close_price'] * 100) if len(df_prices) > 1 else 0
            
            metrics = html.Div([
                html.Div([
                    html.H4(f"${latest_price:.2f}", style={'color': COLORS['text'], 'margin': '0', 'fontSize': '2rem'}),
                    html.P(f"{'+' if price_change >= 0 else ''}{price_change:.2f} ({price_change_pct:+.2f}%)", 
                          style={'color': COLORS['success'] if price_change >= 0 else COLORS['warning'], 'margin': '5px 0', 'fontWeight': 'bold'})
                ], style=METRIC_CARD_STYLE),
                
                html.Div([
                    html.P("24h Volume", style={'color': COLORS['text_secondary'], 'margin': '0'}),
                    html.H5(f"{df_prices['volume'].iloc[0]:,}" if pd.notna(df_prices['volume'].iloc[0]) else "N/A", 
                           style={'color': COLORS['text'], 'margin': '5px 0'})
                ], style=METRIC_CARD_STYLE)
            ])
            
            key_metrics = html.Div([
                html.P("Portfolio Overview", style={'color': COLORS['text'], 'fontWeight': 'bold', 'marginBottom': '10px'}),
                html.P(f"Current: ${latest_price:.2f}", style={'color': COLORS['text'], 'margin': '3px 0'}),
                html.P(f"Change: {price_change_pct:+.2f}%", 
                      style={'color': COLORS['success'] if price_change >= 0 else COLORS['warning'], 'margin': '3px 0', 'fontWeight': 'bold'})
            ])
        else:
            metrics = html.P("No price data available", style={'color': COLORS['text_secondary']})
            key_metrics = html.P("No data", style={'color': COLORS['text_secondary']})
        
        # Enhanced indicators
        if not df_indicators.empty:
            latest_indicators = df_indicators.iloc[0]
            indicators_display = html.Div([
                html.Div([
                    html.P("SMA 5", style={'color': COLORS['text_secondary'], 'margin': '0'}),
                    html.H5(f"${latest_indicators['sma_5']:.2f}" if pd.notna(latest_indicators['sma_5']) else "N/A", 
                           style={'color': COLORS['text'], 'margin': '5px 0'})
                ], style=METRIC_CARD_STYLE),
                
                html.Div([
                    html.P("RSI 14", style={'color': COLORS['text_secondary'], 'margin': '0'}),
                    html.H5(f"{latest_indicators['rsi_14']:.1f}" if pd.notna(latest_indicators['rsi_14']) else "N/A", 
                           style={'color': COLORS['warning'] if pd.notna(latest_indicators['rsi_14']) and latest_indicators['rsi_14'] > 70 else COLORS['text'], 'margin': '5px 0'})
                ], style=METRIC_CARD_STYLE)
            ])
        else:
            indicators_display = html.P("Calculating indicators...", style={'color': COLORS['text_secondary']})
        
        llm_analysis = html.Div([
            html.P("Generating AI analysis...", style={'color': COLORS['info'], 'fontStyle': 'italic'}),
            html.P("Connect to Kafka stream for real-time AI insights", style={'color': COLORS['text_secondary'], 'marginTop': '10px'})
        ])
        
        alerts = html.Div([
            html.P("Monitoring for alerts...", style={'color': COLORS['info'], 'fontStyle': 'italic'}),
            html.P("No recent alerts", style={'color': COLORS['text_secondary'], 'marginTop': '10px'})
        ])
        
        return fig, metrics, indicators_display, llm_analysis, alerts, key_metrics
        
    except Exception as e:
        error_msg = html.Div([
            html.P(f"Error loading data: {str(e)}", style={'color': COLORS['warning']})
        ])
        return {}, error_msg, error_msg, error_msg, error_msg, error_msg

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8051)
