import json
import uuid
import logging
import os
import signal
import sys
import pandas as pd
from datetime import datetime, timedelta, timezone
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from cosmosdb_api import CosmosDatabaseAPI
from postgres_api import PostgresDatabaseAPI
from zoneinfo import ZoneInfo

load_dotenv()

db_api = CosmosDatabaseAPI(url=os.getenv("COSMOS_ENDPOINT"), key=os.getenv("COSMOS_KEY"), db_name=os.getenv("COSMOS_DATABASE"))

# 2. Define your container names
COSMOS_LOG_CONTAINER = "kafka_input_log"
COSMOS_DEDUPE_CONTAINER = "kafka_dedupe_log"
# 2. Fetch all items in the container
# Note: You can optimize by only selecting the id and partition key
all_items = db_api.dbGet(COSMOS_LOG_CONTAINER, {})

if all_items:
    deleted_count = 0
    for item in all_items:
        item_id = item.get("id")
        
        # IMPORTANT: Replace "id" below with your actual partition key field name if it's different.
        # Cosmos DB requires the exact partition key value to delete an item.
        partition_key_value = item.get("id") 
        
        # 3. Call your dbDelete method for each item
        success = db_api.dbDelete(
            container_name=COSMOS_LOG_CONTAINER, 
            item_id=item_id, 
            partition_key=partition_key_value
        )
        if success:
            deleted_count += 1
            
    print(f"Successfully deleted {deleted_count} items from {COSMOS_LOG_CONTAINER}.")
else:
    print(f"Container {COSMOS_LOG_CONTAINER} is already empty.")

all_items = db_api.dbGet(COSMOS_DEDUPE_CONTAINER, {})

if all_items:
    deleted_count = 0
    for item in all_items:
        item_id = item.get("id")
        
        # IMPORTANT: Replace "id" below with your actual partition key field name if it's different.
        # Cosmos DB requires the exact partition key value to delete an item.
        partition_key_value = item.get("id") 
        
        # 3. Call your dbDelete method for each item
        success = db_api.dbDelete(
            container_name=COSMOS_DEDUPE_CONTAINER, 
            item_id=item_id, 
            partition_key=partition_key_value
        )
        
        if success:
            deleted_count += 1
            
    print(f"Successfully deleted {deleted_count} items from {COSMOS_DEDUPE_CONTAINER}.")
else:
    print(f"Container {COSMOS_DEDUPE_CONTAINER} is already empty.")