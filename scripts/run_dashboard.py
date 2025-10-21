import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.dashboard.stock_dashboard import app

if __name__ == '__main__':
    print("Starting Stock Market Dashboard...")
    print("Access the dashboard at: http://localhost:8050")
    app.run(debug=True, host='0.0.0.0', port=8050)
