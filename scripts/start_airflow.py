import os
import subprocess

def start_airflow():
    # Set Airflow home
    os.environ['AIRFLOW_HOME'] = '/c/Data/Projects/stock-market-pipeline/airflow'
    
    print("Initializing Airflow database...")
    subprocess.run(['airflow', 'db', 'init'])
    
    print("Creating admin user...")
    subprocess.run([
        'airflow', 'users', 'create',
        '--username', 'admin',
        '--firstname', 'Admin',
        '--lastname', 'User',
        '--role', 'Admin',
        '--email', 'admin@example.com',
        '--password', 'admin'
    ])
    
    print("Starting Airflow webserver...")
    print("Access Airflow UI at: http://localhost:8085")
    print("Username: admin, Password: admin")
    
    subprocess.run(['airflow', 'webserver', '--port', '8085'])

if __name__ == "__main__":
    start_airflow()
