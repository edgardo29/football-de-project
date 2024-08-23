# run_data_ingestion.py
from sqlalchemy import create_engine
from config import DATABASE_URL, API_KEY
import requests
import pandas as pd


def fetch_and_store_data(league_code):
    API_URL = "https://api.football-data.org/v4/"
    headers = {"X-Auth-Token": API_KEY}

    # Step 1: Fetch Data
    print(f"Fetching data for league: {league_code}...")

    def fetch_data(endpoint):
        response = requests.get(API_URL + endpoint, headers=headers)
        return response.json()

    matches = fetch_data(f"competitions/{league_code}/matches")
    matches_df = pd.json_normalize(matches['matches'])
    print(f"Fetched {len(matches_df)} records.")

    print(f"Processing data for league: {league_code}...")
    matches_df = matches_df.rename(columns={
        'id': 'match_id',
        'season.id': 'season_id',
        'utcDate': 'utcDate',
        'status': 'status',
        'matchday': 'matchday',
        'stage': 'stage',
        'group': 'group_name',
        'lastUpdated': 'lastUpdated',
        'homeTeam.id': 'homeTeam_id',
        'homeTeam.name': 'homeTeam_name',
        'awayTeam.id': 'awayTeam_id',
        'awayTeam.name': 'awayTeam_name'
    })

    # Drop or transform columns that contain complex data types (e.g., dictionaries or lists)
    if 'referees' in matches_df.columns:
        matches_df.drop(columns=['referees'], inplace=True)

    if 'score' in matches_df.columns:
        matches_df.drop(columns=['score'], inplace=True)

    print(f"Connecting to PostgreSQL database at: {DATABASE_URL}")
    engine = create_engine(DATABASE_URL)
    table_name = f'{league_code}_matches'
    print(f"Inserting data into table: {table_name}...")

    matches_df.to_sql(f'{league_code}_matches', engine, if_exists='replace', index=False)
    print(f"Data successfully inserted into {table_name}.")


# Run the function
fetch_and_store_data('PL')
