import json
import logging
import os
import signal
import sys
from datetime import datetime, timedelta, timezone
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

from cosmosdb_api import CosmosDatabaseAPI
from postgres_api import PostgresDatabaseAPI

# ============================================================
# 1. SETUP & CONFIGURATION
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("EventProcessor")

load_dotenv()

COSMOS_ENDPOINT = os.getenv("COSMOS_ENDPOINT")
COSMOS_KEY = os.getenv("COSMOS_KEY")
COSMOS_DATABASE = os.getenv("COSMOS_DATABASE")

# Retained only the two required containers
COSMOS_LOG_CONTAINER = "kafka_input_log"
COSMOS_DEDUPE_CONTAINER = "kafka_dedupe_log"

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
DB_ENV = os.getenv("FLASK_CONFIG", "uat")

try:
    cosmos_db_api = CosmosDatabaseAPI(
        url=COSMOS_ENDPOINT, 
        key=COSMOS_KEY, 
        db_name=COSMOS_DATABASE
    )
    postgres_db_api = PostgresDatabaseAPI(DB_ENV)
    logger.info("Cosmos DB and PostgreSQL connections initialized successfully.")
except Exception as e:
    logger.critical(f"Database initialization failure: {e}", exc_info=True)
    sys.exit(1)

kafka_conf = {
    'bootstrap.servers': 'b-1.sitmskcluster.cymah8.c4.kafka.ap-south-1.amazonaws.com:9096,b-2.sitmskcluster.cymah8.c4.kafka.ap-south-1.amazonaws.com:9096',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': 'mmfsl-dna-sit',
    'sasl.password': KAFKA_PASSWORD,
    'group.id': 'superapp-cosmos-writer-group',  
    'auto.offset.reset': 'latest',
    'enable.auto.commit': False
}

try:
    consumer = Consumer(kafka_conf)
    consumer.subscribe([KAFKA_TOPIC])
    logger.info(f"Kafka consumer subscribed to {KAFKA_TOPIC}.")
except Exception as e:
    logger.critical(f"Kafka connection failed: {e}", exc_info=True)
    sys.exit(1)

# ============================================================
# 2. BUSINESS LOGIC & HELPERS
# ============================================================

def insert_input_log(event: dict) -> bool:
    """Logs incoming payload into the input log container."""
    res = cosmos_db_api.dbInsert(COSMOS_LOG_CONTAINER, event)
    return res is not None


def dedupe_log_check(mobile_no: str, event_name: str) -> bool:
    """
    Returns True if user is ELIGIBLE for processing (no duplicate found within 30 days).
    Returns False if an entry exists within the 30-day window.
    """
    thirty_days_ago = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    
    query = """
        SELECT TOP 1 c.id FROM c
        WHERE c.mobile_no = @mobile_no
        AND c.event_name = @event_name
        AND c.timestamp >= @thirty_days_ago
    """
    params = [
        {"name": "@mobile_no", "value": mobile_no},
        {"name": "@event_name", "value": event_name},
        {"name": "@thirty_days_ago", "value": thirty_days_ago},
    ]
    
    existing = cosmos_db_api.dbGetOne(collection=COSMOS_DEDUPE_CONTAINER, keyValues=query, params=params)
    return existing is None


def process_event(event: dict) -> bool:
    """
    Handles step-by-step pipeline processing:
    1. Input logging (kafka_input_log)
    2. 30-day deduplication check & write (kafka_dedupe_log)
    3. Postgres state checks and updates
    """
    mobile_no = event.get("mobile_no")
    event_name = event.get("event_name")

    if not mobile_no or not event_name:
        logger.warning("Event missing critical fields: mobile_no or event_name.")
        return False

    # Step 1: Input Log
    insert_input_log(event)

    # Step 2: Deduplication Check
    if not dedupe_log_check(mobile_no, event_name):
        logger.info(f"Dedupe hit: {mobile_no} / {event_name} seen within 30 days.")
        return False

    # Store event in dedupe tracking container
    event["timestamp"] = datetime.now(timezone.utc).isoformat()
    cosmos_db_api.dbInsert(COSMOS_DEDUPE_CONTAINER, event)

    # Step 3: PostgreSQL Calling Ledger
    records = postgres_db_api.read("whatsapp_dropoff", filters={"mobile_no": mobile_no})

    pg_payload = {
        "mobile_no": mobile_no,
        "event_name": event_name,
        "is_process": False,
        "call_triggered": False
    }

    if not records:
        postgres_db_api.insert("whatsapp_dropoff", pg_payload)
        logger.info(f"Created new drop-off record for {mobile_no}.")
        return True

    existing_record = dict(records[0])

    if existing_record.get("is_process") is True:
        logger.info(f"Skipping {mobile_no}: Record already processed.")
        return False

    if existing_record.get("event_name") == event_name:
        logger.info(f"Skipping {mobile_no}: Event name unchanged.")
        return False

    # Update state for changed event
    postgres_db_api.update(
        table_name="whatsapp_dropoff",
        update_data={"event_name": event_name, "call_triggered": False},
        filters={"mobile_no": mobile_no}
    )
    logger.info(f"Updated drop-off record for {mobile_no} with event {event_name}.")
    return True

# ============================================================
# 3. STREAM PROCESSING LOOP
# ============================================================

running = True

def handle_shutdown(signum, frame):
    global running
    logger.info("Shutdown signal received. Stopping consumer...")
    running = False

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

logger.info("Starting Kafka processing loop...")

try:
    while running:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            logger.error(f"Kafka Consumer Error: {msg.error()}")
            break

        value_str = msg.value().decode('utf-8') if msg.value() else "{}"

        try:
            val_json = json.loads(value_str)
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON at offset {msg.offset()}. Committing and skipping.")
            consumer.commit(message=msg, asynchronous=False)
            continue

        source = val_json.get("Source", "")
        if source and source.strip().upper() == "SUPERAPP":
            # Direct processing down the pipeline (no raw document upsert)
            process_event(val_json)

        # Commit offset after successful consumption
        consumer.commit(message=msg, asynchronous=False)

finally:
    logger.info("Closing Kafka consumer safely.")
    consumer.close()