import os
from dotenv import load_dotenv
from postgres_api import PostgresDatabaseAPI
import pandas as pd

# Load the .env file
load_dotenv()

# Fetch Postgres Configuration Keys
PG_HOST = os.getenv("PGRE_HOST")
PG_PORT = os.getenv("PGRE_PORT")
PG_DATABASE = os.getenv("PGRE_DB")
PG_USER = os.getenv("PGRE_UNAME")
PG_PASSWORD = os.getenv("PGRE_PWD")

# Optional: Print to verify they are loading (Delete this in production!)
print(f"Connecting to host: {PG_HOST} as user: {PG_USER}")
postgres_db_api = PostgresDatabaseAPI("uat")
records = postgres_db_api.read("wa_dropoff")
pd.DataFrame(records).to_csv("dummy_data.csv")
