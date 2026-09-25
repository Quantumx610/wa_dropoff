import os
import sys
import logging
from .postgres_api import PostgresDatabaseAPI
from .cosmosdb_api import CosmosDatabaseAPI
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

COSMOS_ENDPOINT = os.getenv("COSMOS_ENDPOINT")
COSMOS_KEY = os.getenv("COSMOS_KEY")
COSMOS_DATABASE = os.getenv("COSMOS_DATABASE")
DB_ENV = os.getenv('CONFIG')


def get_cosmos_connection() -> CosmosDatabaseAPI:
    try:
        instance = CosmosDatabaseAPI(url=COSMOS_ENDPOINT, key=COSMOS_KEY, db_name=COSMOS_DATABASE)
        logger.info("Cosmos DB instance initialized successfully.")
        return instance
    except Exception as e:
        logger.critical(f"Cosmos DB initialization failure: {e}", exc_info=True)
        sys.exit(1)


def get_postgres_connection() -> PostgresDatabaseAPI:
    try:
        instance = PostgresDatabaseAPI(db_prefix=DB_ENV)
        logger.info("PostgreSQL instance initialized successfully.")
        return instance
    except Exception as e:
        logger.critical(f"PostgreSQL DB initialization failure: {e}", exc_info=True)
        sys.exit(1)


# --- OPTION A: Pre-instantiated Singletons (Eager Loading) ---
# If credentials are directly accessible on startup, instantiate them here:
#
# pg_db_api = get_postgres_connection("DEV")
# cosmos_db_api = get_cosmos_connection(COSMOS_ENDPOINT, COSMOS_KEY, COSMOS_DATABASE)
#
# __all__ = ["pg_db_api", "cosmos_db_api", "get_postgres_connection", "get_cosmos_connection"]