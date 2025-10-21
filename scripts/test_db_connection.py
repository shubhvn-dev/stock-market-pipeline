import psycopg2

# Try different connection approaches
connection_attempts = [
    {'host': '127.0.0.1', 'database': 'stock_data', 'user': 'admin', 'password': 'password123', 'port': 5432},
    {'host': 'localhost', 'database': 'stock_data', 'user': 'admin', 'port': 5432},
    {'host': '127.0.0.1', 'database': 'stock_data', 'user': 'admin', 'port': 5432}
]

for i, params in enumerate(connection_attempts):
    try:
        print(f"Attempt {i+1}: {params}")
        conn = psycopg2.connect(**params)
        print("Connection successful!")
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()
        print(f"Test query result: {result[0]}")
        conn.close()
        print("This connection method works!")
        break
    except Exception as e:
        print(f"Failed: {e}")
        print()

print("Testing complete")
