import json
import uuid
import logging
import os
import signal
import sys
from datetime import datetime, timedelta, timezone
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from db import get_postgres_connection, get_cosmos_connection
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

# ============================================================
# 1. SETUP & CONFIGURATION
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)


# Suppress verbose Azure HTTP logs
logging.getLogger("azure.cosmos._cosmos_http_logging_policy").setLevel(logging.WARNING)
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

# (Optional) Broadly silence all non-critical Azure logs
logging.getLogger("azure").setLevel(logging.WARNING)

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

SARVAM_TOTAL_ATTEMPTS = os.getenv("SARVAM_TOTAL_ATTEMPTS")

try:
    cosmos_db_api = get_cosmos_connection()
    postgres_db_api = get_postgres_connection()
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

# try:
#     consumer = Consumer(kafka_conf)
#     consumer.subscribe([KAFKA_TOPIC])
#     logger.info(f"Kafka consumer subscribed to {KAFKA_TOPIC}.")
# except Exception as e:
#     logger.critical(f"Kafka connection failed: {e}", exc_info=True)
#     sys.exit(1)

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
        AND c.received_at_utc >= @thirty_days_ago
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

    correlation_id = str(uuid.uuid4())
    received_at_ist = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
    received_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    event["id"] = correlation_id
    event["correlation_id"] = correlation_id
    event["received_at_ist"] = received_at_ist
    event["received_at_utc"] = received_at_utc

    # Step 1: Input Log
    insert_input_log(event)

    mobile_no = event.get("MobileNumber", "")
    event_name = event.get("EventName", "")
    enquiry_id = event.get("EnquiryNo", "")

    if not mobile_no or not event_name or not enquiry_id :
        logger.warning("Exit: Event missing critical fields: mobile_no or event_name or enquiry_id. ")
        return False

    cosmos_event = {}
    cosmos_event["id"] = correlation_id
    cosmos_event["source"] = event.get("Source", "")
    cosmos_event["app_version"] = event.get("AppVersion", "")
    cosmos_event["platform"] = event.get("Platform", "")
    cosmos_event["application_id"] = event.get("Application_ID", "")
    cosmos_event["hpa"] = event.get("HPA", "")
    cosmos_event["mandate_mode"] = event.get("MandateMode", "")
    cosmos_event["penny_drop_failure_reason"] = event.get("PennyDropFailureReason", "")
    cosmos_event["offer"] = event.get("Offer", "")
    cosmos_event["enquiry_id"] = event.get("EnquiryNo", "")
    cosmos_event["superapp_id"] = event.get("SuperAppid", "")
    cosmos_event["journey"] = event.get("Journey", "")
    cosmos_event["event_name"] = event.get("EventName", "")
    cosmos_event["mobile_no"] = event.get("MobileNumber", "")
    cosmos_event["customer_flag"] = event.get("CustomerFlag", "")
    cosmos_event["ucic"] = event.get("UCIC", "")
    cosmos_event["ucic_value"] = event.get("UCIC_VALUE", "")
    cosmos_event["lan_verified"] = event.get("LAN_VERIFIED", "")
    cosmos_event["timestamp"] = event.get("Timestamp", "")
    cosmos_event["response_code"] = event.get("Response Code", "")
    cosmos_event["loan_amount"] = event.get("LoanAmount", "")
    cosmos_event["correlation_id"] = correlation_id
    cosmos_event["received_at_ist"] = received_at_ist
    cosmos_event["received_at_utc"] = received_at_utc

    # Step 2: Deduplication Check
    if not dedupe_log_check(mobile_no, event_name):
        logger.info(f"Dedupe hit: {mobile_no} / {event_name} seen within 30 days.")
        return False

    # Step 3: Store Eligible
    cosmos_db_api.dbInsert(COSMOS_DEDUPE_CONTAINER, cosmos_event)

    # Step 3: PostgreSQL Calling Ledger
    records = postgres_db_api.read("wa_dropoff", filters={"mobile_no": mobile_no})

    # Step 4: is_processed = True for this particular mobile_no

    pg_payload = {
        "correlation_id": correlation_id,
        "source": event.get("Source", "NA"),
        "customer_name": event.get("Name", "Priya Grahak"),
        "loan_amount": event.get("LoanAmount", "NA"),
        "loan_tenure": event.get("Tenure", "NA"),
        "enquiry_id": event.get("EnquiryNo", ""),
        "superapp_id": event.get("SuperAppid", ""),
        "event_timestamp":event.get("Timestamp",""),
        "mobile_no": mobile_no,
        "event_name": event_name,
        "received_at_ist": received_at_ist,
        "received_at_utc": received_at_utc
    }

    if not records:
        postgres_db_api.insert("wa_dropoff", pg_payload)
        logger.info(f"Created new drop-off record for {mobile_no}.")
        return True

    existing_record = dict(records[0])
    call_count = existing_record.get("call_count") or 0
    if existing_record.get("is_processed") is True or call_count >= SARVAM_TOTAL_ATTEMPTS:
        if call_count >= 3:
            logger.info(f"Skipping {mobile_no}: call_count>=3")
        logger.info(f"Skipping {mobile_no}: Record already processed.")

    if existing_record.get("event_name") == event_name:
        logger.info(f"Skipping {mobile_no}: Event name unchanged.")
        return False

    # Update state for changed event
    postgres_db_api.update(
        table_name="wa_dropoff",
        update_data={"event_name": event_name, "call_triggered": False},
        filters={"mobile_no": mobile_no}
    )
    logger.info(f"Updated drop-off record for {mobile_no} with event {event_name}.")
    return True

# ============================================================
# 3. STREAM PROCESSING LOOP
# ============================================================

# running = True

# def handle_shutdown(signum, frame):
#     global running
#     logger.info("Shutdown signal received. Stopping consumer...")
#     running = False

# signal.signal(signal.SIGINT, handle_shutdown)
# signal.signal(signal.SIGTERM, handle_shutdown)

# logger.info("Starting Kafka processing loop...")

# try:
#     while running:
#         msg = consumer.poll(timeout=1.0)
#         if msg is None:
#             continue

#         if msg.error():
#             if msg.error().code() == KafkaError._PARTITION_EOF:
#                 continue
#             logger.error(f"Kafka Consumer Error: {msg.error()}")
#             break

#         value_str = msg.value().decode('utf-8') if msg.value() else "{}"

#         try:
#             val_json = json.loads(value_str)
#         except json.JSONDecodeError:
#             logger.warning(f"Invalid JSON at offset {msg.offset()}. Committing and skipping.")
#             consumer.commit(message=msg, asynchronous=False)
#             continue

#         source = val_json.get("Source", "")
#         if source and source.strip().upper() == "SUPERAPP":
#             # Direct processing down the pipeline (no raw document upsert)
#             process_event(val_json)

#         # Commit offset after successful consumption
#         consumer.commit(message=msg, asynchronous=False)

# finally:
#     logger.info("Closing Kafka consumer safely.")
#     consumer.close()