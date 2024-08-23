import pandas as pd
from sqlalchemy import create_engine
from config import DATABASE_URL

# Establish a connection to the PostgreSQL database using SQLAlchemy
engine = create_engine(DATABASE_URL)
connection = engine.connect()

def get_current_matchday():
    """Function to get the latest matchday from the database."""
    query = """
    SELECT MAX("matchday") AS current_matchday
    FROM "PL_matches"
    """
    result = connection.execute(query).fetchone()
    return result['current_matchday'] if result else None

def calculate_team_form():
    print("Calculating team form...")

    # Dynamically get the current matchday
    current_matchday = get_current_matchday()

    if current_matchday is None:
        print("No matchday data available.")
        return

    print(f"Using current matchday: {current_matchday}")

    query = f"""
    WITH Last5Matches AS (
        SELECT
            "homeTeam_id" AS team_id,
            CASE
                WHEN "score.fullTime.homeTeam" > "score.fullTime.awayTeam" THEN 1
                WHEN "score.fullTime.homeTeam" = "score.fullTime.awayTeam" THEN 0
                ELSE -1
            END AS result,
            "matchday"
        FROM "PL_matches"
        WHERE "matchday" <= {current_matchday}

        UNION ALL

        SELECT
            "awayTeam_id" AS team_id,
            CASE
                WHEN "score.fullTime.awayTeam" > "score.fullTime.homeTeam" THEN 1
                WHEN "score.fullTime.awayTeam" = "score.fullTime.homeTeam" THEN 0
                ELSE -1
            END AS result,
            "matchday"
        FROM "PL_matches"
        WHERE "matchday" <= {current_matchday}
    )

    SELECT
        team_id,
        SUM(CASE WHEN result = 1 THEN 1 ELSE 0 END) AS recent_wins,
        SUM(CASE WHEN result = 0 THEN 1 ELSE 0 END) AS recent_draws,
        SUM(CASE WHEN result = -1 THEN 1 ELSE 0 END) AS recent_losses
    FROM (
        SELECT
            team_id,
            result,
            ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY "matchday" DESC) AS rn
        FROM Last5Matches
    ) AS subquery
    WHERE rn <= 5
    GROUP BY team_id;
    """

    print("Executing query...")
    df = pd.read_sql(query, connection)
    print("Query executed successfully.")

    print(f"Result DataFrame shape: {df.shape}")
    print("First few rows of the result:")
    print(df.head())

# Call the function to run the calculation
calculate_team_form()