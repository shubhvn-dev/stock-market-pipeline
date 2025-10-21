import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.dashboard.enhanced_stock_dashboard import app

if __name__ == '__main__':
    print("Starting Enhanced Stock Market Dashboard...")
    print("Access the beautiful dashboard at: http://localhost:8051")
    app.run(debug=True, host='0.0.0.0', port=8051)
