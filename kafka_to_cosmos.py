from datetime import datetime, timedelta, timezone
import json
import time
from confluent_kafka import Consumer, KafkaError
from cosmosdb_api import CosmosDatabaseAPI
from postgres_api import PostgresDatabaseAPI 
import os
from dotenv import load_file, load_dotenv

# ============================================================
# 1. CONFIGURATION
# ============================================================

# -------------------------
# COSMOS DB CONFIGURATION
# -------------------------

# Load environment variables from the .env file
load_dotenv()

# Retrieve the variables
COSMOS_ENDPOINT = os.getenv("COSMOS_ENDPOINT")
COSMOS_KEY = os.getenv("COSMOS_KEY")
COSMOS_DATABASE = os.getenv("COSMOS_DATABASE")
COSMOS_CONTAINER = os.getenv("COSMOS_CONTAINER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")

try:
    # Initialize your custom wrapper class
    cosmos_db_api = CosmosDatabaseAPI(
        url=COSMOS_ENDPOINT, 
        key=COSMOS_KEY, 
        db_name=COSMOS_DATABASE
    )
    print("Cosmos DB configured successfully via CosmosDatabaseAPI.")
except Exception as e:
    print(f"Failed to configure Cosmos DB: {e}")
    exit(1)


kafka_conf = {
    'bootstrap.servers': 'b-1.sitmskcluster.cymah8.c4.kafka.ap-south-1.amazonaws.com:9096,b-2.sitmskcluster.cymah8.c4.kafka.ap-south-1.amazonaws.com:9096',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': 'mmfsl-dna-sit',
    'sasl.password': KAFKA_PASSWORD,
    'group.id': 'superapp-cosmos-writer-group',  
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True                   
}

try:
    consumer = Consumer(kafka_conf)
    consumer.subscribe([KAFKA_TOPIC])
    print("Kafka configuration set successfully. Consumer subscribed.")
except Exception as e:
    print(f"Failed to configure Kafka: {e}")
    exit(1)

# ============================================================
# 2. STREAM PROCESSING LOOP
# ============================================================

print("Starting to read Kafka stream...")

try:
    while True:
        # Poll Kafka for new messages every 1 second
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
            
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Reached end of partition, continue waiting
                continue
            else:
                print(f"Kafka Consumer Error: {msg.error()}")
                break

        # Decode Kafka message payload
        key_str = msg.key().decode('utf-8') if msg.key() else None
        value_str = msg.value().decode('utf-8') if msg.value() else "{}"

        # Parse JSON
        try:
            val_json = json.loads(value_str)
        except json.JSONDecodeError:
            print(f"Invalid JSON received at offset {msg.offset()}. Skipping.")
            continue

        # Filter SUPERAPP events 
        source = val_json.get("Source", "")
        if source and source.strip().upper() == "SUPERAPP":
            
            # Extract metadata
            topic = msg.topic()
            partition = msg.partition()
            offset = msg.offset()
            timestamp = msg.timestamp()[1]

            # Generate Cosmos DB unique ID (topic-partition-offset)
            doc_id = f"{topic}-{partition}-{offset}"

            # Prepare the document for Cosmos DB
            cosmos_document = {
                "id": doc_id,
                "key": key_str,
                "value": value_str,
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "timestamp": timestamp,
                "superAppEventsTopic": KAFKA_TOPIC  # Configured partition key
            }

            # Write to Cosmos DB using your wrapper API
            # Note: Using the newly added dbUpsert method to prevent duplicate ID crashes
            result = cosmos_db_api.dbUpsert(COSMOS_CONTAINER, cosmos_document)
            
            if result:
                print(f"Inserted SUPERAPP event into Cosmos DB | Offset: {offset}")
            else:
                print(f"Failed to insert document into Cosmos DB at Offset: {offset}")

except KeyboardInterrupt:
    print("Streaming process stopped by user.")
finally:
    # Ensure graceful shutdown to commit final offsets
    print("Closing Kafka consumer...")
    consumer.close()



def insert_input_log(event: dict):
    cosmos_db_api.dbInsert("input_log", event)
    return 1


def dedupe_log_check(mobile_no: str, event_name: str):
    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
    thirty_days_query = """
        SELECT TOP 1 * FROM c
        WHERE c.mobile_no = @mobile_no
        AND c.event_name = @event_name
        AND c.timestamp >= @thirty_days_ago
        """
    thirty_days_params = [
        {"name": "@mobile_no", "value": mobile_no},
        {"name": "@event_name", "value": event_name},
        {"name": "@thirty_days_ago_timestamp", "value": thirty_days_ago},
    ]
    if cosmos_db_api.dbGetOne(collection="dedupe_log", keyValues=thirty_days_query, params=thirty_days_params):
        return 0

    return 1


def check_postgres_entry(mobile_no: str):
    
    return 1


def main(event):
    postgres_db_api = PostgresDatabaseAPI("uat")

    # event processsing logic    
    event = {}

    # step 1
    insert_input_log(event)

    # step 2 Dedupe check
    eligible = dedupe_log_check("mobile_no", "event_name")

    if not eligible:
        return 0

    # format yyyy-mm-ddTHH:MM:SSZ
    event["created_at"] = datetime.now(timezone.utc)
    new_event = cosmos_db_api.dbInsert(event)

    # step 3 | Store in postgres for calling
    record = postgres_db_api.read("whatsapp_dropoff", filters={"mobile_no": event.get("mobile_no")})

    if not record:
        postgres_db_api.insert("whatsapp_dropoff", event)
        return 1

    if record and len(record) > 1:
        return 0

    existing_record = dict(record[0])

    if existing_record["is_process"] == True:
        return 0

    
    if existing_record["event_name"] == event.get("event_name"):
        return 0
    
    postgres_db_api.update("whatsapp_dropoff", {"event_name": event.get("event_name"), "call_triggered": False})

    return 1
        
