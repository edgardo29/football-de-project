from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def fetch_and_store_data():
    import requests  # Import requests to handle API calls
    import pandas as pd  # Import pandas for data manipulation
    from sqlalchemy import create_engine  # Import SQLAlchemy to handle database connections
    from config import DATABASE_URL  # Import the database URL from the config file

    API_URL = "https://api.football-data.org/v2/"  # Base URL for the football data API
    API_KEY = "YOUR_API_KEY"  # Your API key for accessing the football data API
    headers = {"X-Auth-Token": API_KEY}  # Headers required by the API for authentication

    def fetch_data(endpoint):
        response = requests.get(API_URL + endpoint, headers=headers)  # Make the API request
        data = response.json()  # Parse the JSON response
        return data  # Return the parsed data

    # Fetch matches data from the API
    matches = fetch_data("competitions/PL/matches")
    matches_df = pd.json_normalize(matches['matches'])  # Normalize the JSON data into a DataFrame

    # Store data in PostgreSQL
    engine = create_engine(DATABASE_URL)  # Create a SQLAlchemy engine
    matches_df.to_sql('matches', engine, if_exists='replace', index=False)  # Write the DataFrame to the PostgreSQL table

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
    description='A simple DAG to ingest football data',  # Description of the DAG
    schedule_interval=timedelta(days=1),  # Schedule interval for the DAG (daily)
)

# Define the task using PythonOperator
t1 = PythonOperator(
    task_id='fetch_and_store_data',  # Task ID
    python_callable=fetch_and_store_data,  # Python function to call
    dag=dag,  # DAG to which the task belongs
)