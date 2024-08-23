import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Add the project directory to the Python path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

def fetch_and_store_data(league_code):
    import requests  # Import requests to handle API calls
    import pandas as pd  # Import pandas for data manipulation
    from sqlalchemy import create_engine  # Import SQLAlchemy to handle database connections
    from config import DATABASE_URL  # Import the database URL from the config file

    API_URL = "https://api.football-data.org/v2/"  # Base URL for the football data API
    API_KEY = "749f20e7479343c597d0062ca3c102d1"  # Your API key for accessing the football data API
    headers = {"X-Auth-Token": API_KEY}  # Headers required by the API for authentication

    def fetch_data(endpoint):
        response = requests.get(API_URL + endpoint, headers=headers)  # Make the API request
        data = response.json()  # Parse the JSON response
        return data  # Return the parsed data

    # Fetch matches data from the API
    matches = fetch_data(f"competitions/{league_code}/matches")
    matches_df = pd.json_normalize(matches['matches'])  # Normalize the JSON data into a DataFrame

    # Store data in PostgreSQL
    engine = create_engine(DATABASE_URL)  # Create a SQLAlchemy engine
    matches_df.to_sql(f'{league_code}_matches', engine, if_exists='replace', index=False)  # Write the DataFrame to the PostgreSQL table

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,  # DAG does not depend on past runs
    'start_date': datetime(2023, 1, 1),  # Start date for the DAG
    'email_on_failure': False,  # Do not email on failure
    'email_on_retry': False,  # Do not email on retry
    'retries': 1,  # Number of retries in case of failure
    'retry_delay': timedelta(minutes=5),  # Delay between retries
}

# Define the DAG
dag = DAG(
    'football_data_ingestion',  # Name of the DAG
    default_args=default_args,  # Default arguments for the DAG
    description='A DAG to ingest football data from top leagues',  # Description of the DAG
    schedule=timedelta(days=1),  # Schedule interval for the DAG (daily)
)

# Create tasks for each league
leagues = {
    'PL': 'fetch_epl_data',
    'PD': 'fetch_laliga_data',
    'BL1': 'fetch_bundesliga_data',
    'SA': 'fetch_seriea_data',
    'FL1': 'fetch_ligue1_data'
}

tasks = []

for league_code, task_id in leagues.items():
    task = PythonOperator(
        task_id=task_id,
        python_callable=fetch_and_store_data,
        op_args=[league_code],
        dag=dag,
    )
    tasks.append(task)

# Optionally, set dependencies between tasks (if needed)
for i in range(len(tasks) - 1):
    tasks[i] >> tasks[i + 1]
