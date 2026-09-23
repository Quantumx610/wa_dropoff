import sys
import logging
from .postgres_api import PostgresDatabaseAPI
from .cosmosdb_api import CosmosDatabaseAPI


logger = logging.getLogger(__name__)


def get_cosmos_connection(endpoint: str, key: str, db_name: str) -> CosmosDatabaseAPI:
    try:
        instance = CosmosDatabaseAPI(url=endpoint, key=key, db_name=db_name)
        logger.info("Cosmos DB instance initialized successfully.")
        return instance
    except Exception as e:
        logger.critical(f"Cosmos DB initialization failure: {e}", exc_info=True)
        sys.exit(1)


def get_postgres_connection(db_env: str) -> PostgresDatabaseAPI:
    try:
        instance = PostgresDatabaseAPI(db_prefix=db_env)
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