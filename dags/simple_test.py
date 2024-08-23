from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData

# Replace with your actual DATABASE_URL
DATABASE_URL = "postgresql+psycopg2://postgres:Ei123nte@localhost:5432/football_data"

# Create a SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Define a simple metadata and table
metadata = MetaData()

test_table = Table('test_matches', metadata,
                   Column('match_id', Integer, primary_key=True),
                   Column('season_id', Integer),
                   Column('utcDate', String),
                   Column('status', String),
                   Column('matchday', Integer),
                   Column('stage', String),
                   Column('group_name', String),
                   Column('lastUpdated', String),
                   Column('homeTeam_id', Integer),
                   Column('homeTeam_name', String),
                   Column('awayTeam_id', Integer),
                   Column('awayTeam_name', String)
                   )

# Create the table
metadata.create_all(engine)

# Insert data into the table
with engine.connect() as connection:
    connection.execute(test_table.insert(), [
        {'match_id': 1, 'season_id': 2021, 'utcDate': '2021-08-13T19:00:00Z', 'status': 'SCHEDULED', 'matchday': 1, 'stage': 'Regular Season', 'group_name': None, 'lastUpdated': '2021-08-11T09:00:00Z', 'homeTeam_id': 66, 'homeTeam_name': 'Manchester United FC', 'awayTeam_id': 57, 'awayTeam_name': 'Leeds United FC'},
        {'match_id': 2, 'season_id': 2021, 'utcDate': '2021-08-14T11:30:00Z', 'status': 'SCHEDULED', 'matchday': 1, 'stage': 'Regular Season', 'group_name': None, 'lastUpdated': '2021-08-11T09:00:00Z', 'homeTeam_id': 64, 'homeTeam_name': 'Arsenal FC', 'awayTeam_id': 61, 'awayTeam_name': 'Tottenham Hotspur FC'}
    ])

print("Data inserted successfully using SQLAlchemy directly.")