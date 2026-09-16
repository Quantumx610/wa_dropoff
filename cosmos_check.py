import os
from cosmosdb_api import CosmosDatabaseAPI
from dotenv import load_dotenv
load_dotenv()
cosmos_db_manager = CosmosDatabaseAPI(
    url=os.getenv("COSMOS_ENDPOINT"),
    key=os.getenv("COSMOS_KEY"),
    # db_name=os.getenv("COSMOS_DB_NAME"),
    db_name="testdb"
)

event = {
    "event_name": "test",
    "event_type": "ABC",
    "source": "VMSandbox-44",
    "created_by": "100008239"
}

event_inserted = cosmos_db_manager.dbInsert("kafka-superapp-events", event)

print("Event Inserted: ", event_inserted)