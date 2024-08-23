import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Replace with your actual DATABASE_URL
DATABASE_URL = "postgresql+psycopg2://postgres:Ei123nte@localhost:5432/football_data"

# Example DataFrame
data = {
    'id': [1, 2],
    'name': ['Test Team 1', 'Test Team 2'],
    'score': [3, 2]
}

df = pd.DataFrame(data)

# Create a SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Use a direct connection from the engine
# Write DataFrame to SQL table
try:
    df.to_sql('test_table', engine, if_exists='replace', index=False)
    print("DataFrame written to SQL table successfully.")
except SQLAlchemyError as e:
    print(f"Error writing DataFrame to SQL table: {e}")