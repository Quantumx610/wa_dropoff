import csv
import json
import uuid
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
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

COSMOS_LOG_CONTAINER = os.getenv("COSMOS_LOG_CONTAINER")
COSMOS_DEDUPE_CONTAINER = os.getenv("COSMOS_DEDUPE_CONTAINER")

DB_ENV = os.getenv("CONFIG", "uat")
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH", "dummy_data.csv") 

cosmos_db_api = get_cosmos_connection()
postgres_db_api = get_postgres_connection()

# ============================================================
# 2. TRACKING STATS DICTIONARY
# ============================================================

stats = {
    "total_rows": 0,
    "cosmos_input_inserted": 0,
    "cosmos_dedupe_inserted": 0,
    "pg_inserted": 0,
    "pg_updated": 0,
    "skipped_source": 0,
    "skipped_missing_fields": 0,
    "skipped_dedupe": 0,
    "skipped_pg_processed": 0,
    "skipped_pg_unchanged": 0,
    "skip_reasons": [] # Will store tuples of (mobile_no, reason)
}

# ============================================================
# 3. BUSINESS LOGIC & HELPERS
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
    Handles step-by-step pipeline processing.
    """
    correlation_id = str(uuid.uuid4())
    received_at_ist = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
    received_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    event["id"] = correlation_id  # Cosmos DB strictly requires 'id'
    event["correlation_id"] = correlation_id
    event["received_at_ist"] = received_at_ist
    event["received_at_utc"] = received_at_utc

    # Step 1: Input Log
    insert_input_log(event)
    stats["cosmos_input_inserted"] += 1

    mobile_no = event.get("MobileNumber", None)
    event_name = event.get("EventName", None)
    enquiry_id = event.get("EnquiryNo", None)
    display_mobile = mobile_no if mobile_no else "UNKNOWN"

    if not mobile_no or not event_name or not enquiry_id:
        logger.warning(f"Exit: Event missing critical fields. Mobile: {display_mobile}")
        stats["skipped_missing_fields"] += 1
        stats["skip_reasons"].append((display_mobile, "Missing mobile_no or event_name or enquiry_id"))
        return False

    cosmos_event = {}
    cosmos_event["id"] = correlation_id
    cosmos_event["source"] = event.get("Source", None)
    cosmos_event["app_version"] = event.get("AppVersion", None)
    cosmos_event["platform"] = event.get("Platform", None)
    cosmos_event["application_id"] = event.get("Application_ID", None)
    cosmos_event["hpa"] = event.get("HPA", None)
    cosmos_event["mandate_mode"] = event.get("MandateMode", None)
    cosmos_event["penny_drop_failure_reason"] = event.get("PennyDropFailureReason", None)
    cosmos_event["offer"] = event.get("Offer", None)
    cosmos_event["enquiry_id"] = event.get("EnquiryNo", None)
    cosmos_event["superapp_id"] = event.get("SuperAppid", None)
    cosmos_event["journey"] = event.get("Journey", None)
    cosmos_event["event_name"] = event.get("EventName", None)
    cosmos_event["mobile_no"] = event.get("MobileNumber", None)
    cosmos_event["customer_flag"] = event.get("CustomerFlag", None)
    cosmos_event["ucic"] = event.get("UCIC", None)
    cosmos_event["ucic_value"] = event.get("UCIC_VALUE", None)
    cosmos_event["lan_verified"] = event.get("LAN_VERIFIED", None)
    cosmos_event["timestamp"] = event.get("Timestamp", None)
    cosmos_event["response_code"] = event.get("Response Code", None)
    cosmos_event["loan_amount"] = event.get("LoanAmount", None)
    cosmos_event["correlation_id"] = correlation_id
    cosmos_event["received_at_ist"] = received_at_ist
    cosmos_event["received_at_utc"] = received_at_utc
    
    # Step 2: Deduplication Check
    if not dedupe_log_check(mobile_no, event_name):
        logger.info(f"Dedupe hit: {mobile_no} / {event_name} seen within 30 days.")
        stats["skipped_dedupe"] += 1
        stats["skip_reasons"].append((display_mobile, "Dedupe hit (Seen in last 30 days)"))
        return False

    # Step 3: Store Eligible
    cosmos_db_api.dbInsert(COSMOS_DEDUPE_CONTAINER, cosmos_event)
    stats["cosmos_dedupe_inserted"] += 1

    # Step 4: PostgreSQL Calling Ledger
    records = postgres_db_api.read("wa_dropoff", filters={"mobile_no": mobile_no})
#     pg_schema = [
#     "correlation_id",
#     "source",
#     "customer_name",
#     "offer",
#     "loan_amount",
#     "loan_tenure",
#     "enquiry_id",
#     "superapp_id",
#     "event_timestamp",
#     "pennydropfailurereason",
#     "mandate_mode",
#     "application_id",
#     "hpa",
#     "mobile_no",
#     "event_name",
#     "received_at_ist",
#     "received_at_utc"
# ]

    pg_payload = {
        "correlation_id": correlation_id,
        "source": event.get("Source", None),
        "customer_name": event.get("Name", "Priya Grahak"),
        "offer":event.get("Offer", None),
        "loan_amount": event.get("LoanAmount", None),
        "loan_tenure": event.get("Tenure", None),
        "enquiry_id": event.get("EnquiryNo", None),
        "superapp_id": event.get("SuperAppid", None),
        "event_timestamp":event.get("Timestamp",None),
        "penny_drop_failure_reason":event.get("PennyDropFailureReason",None),
        "mandate_mode": event.get("MandateMode", None),
        "application_id": event.get("Application_ID", None),
        "hpa" : event.get("HPA", None),
        "mobile_no": mobile_no,
        "event_name": event_name,
        "received_at_ist": received_at_ist,
        "received_at_utc": received_at_utc
    }

    if event_name in ["CheckChildFailure", "JourneyCompleted", "PennyDropFailure", "AMLCheckFailure"]:
        # Add the is_processed flag to the payload before inserting
        if not records:
            pg_payload["is_processed"] = True     
            postgres_db_api.insert("wa_dropoff", pg_payload)
            logger.info(f"User reached failure or completion for mobile no {mobile_no}.")
            stats["pg_inserted"] += 1
            return True
            
        else:
            postgres_db_api.update(table_name="wa_dropoff",
                            update_data={"event_name": event_name, "call_triggered": False, "is_processed": True},
                            filters={"mobile_no": mobile_no})
            logger.info(f"Updated drop-off record for {mobile_no} with event {event_name}, and completed the journey")
            stats["pg_updated"] += 1
            return True
        
    if not records:
        postgres_db_api.insert("wa_dropoff", pg_payload)
        logger.info(f"Created new drop-off record for {mobile_no}.")
        stats["pg_inserted"] += 1
        return True

    existing_record = dict(records[0])
    call_count = existing_record.get("call_count") or 0
    if existing_record.get("is_processed") is True or call_count >= 3:
        if call_count >= 3:
            logger.info(f"Skipping {mobile_no}: call_count>=3")
        logger.info(f"Skipping {mobile_no}: Record already processed.")
        stats["skipped_pg_processed"] += 1
        stats["skip_reasons"].append((display_mobile, "Postgres: Record already processed"))
        return False


    if existing_record.get("event_name") == event_name:
        logger.info(f"Skipping {mobile_no}: Event name unchanged.")
        stats["skipped_pg_unchanged"] += 1
        stats["skip_reasons"].append((display_mobile, "Postgres: Event name unchanged"))
        return False
   
    
    postgres_db_api.update(
    table_name="wa_dropoff",
    update_data={"event_name": event_name, "call_triggered": False},
    filters={"mobile_no": mobile_no}
)
    logger.info(f"Updated drop-off record for {mobile_no} with event {event_name}.")
    stats["pg_updated"] += 1
    return True

# ============================================================
# 4. CSV MAPPING & SIMULATION LOOP
# ============================================================

def map_csv_row_to_event(csv_row: dict) -> dict:
    return {
        "MobileNumber": csv_row.get("mobile_no", None),
        "EventName": csv_row.get("event_name", None),
        "EnquiryNo": csv_row.get("enquiry_id", None),
        "Source": csv_row.get("source", None),
        "LoanAmount": csv_row.get("loan_amount", None),
        "SuperAppid": csv_row.get("superapp_id", None),
        "Tenure": csv_row.get("loan_tenure", None),
        "Name": csv_row.get("customer_name", None),
        "Timestamp": csv_row.get("event_timestamp", None),
        "AppVersion": None, "Platform": None, "OS": None, "journey": None,
        "CustomerFlag": None, "UCIC": None, "UCIC_VALUE": None,
        "LAN_VERIFIED": None, "Response Code": None
    }

def print_summary():
    """Generates a clean tabular summary of the execution."""
    print("\n\n")
    print("="*65)
    print(" "*22 + "EXECUTION SUMMARY")
    print("="*65)
    print(f"{'Total Rows Read from CSV':<45}: {stats['total_rows']}")
    print("-" * 65)
    
    print("[ COSMOS DB ]")
    print(f"{'Inserted into Input Log (kafka_input_log)':<45}: {stats['cosmos_input_inserted']}")
    print(f"{'Inserted into Dedupe Log (kafka_dedupe_log)':<45}: {stats['cosmos_dedupe_inserted']}")
    print("-" * 65)
    
    print("[ POSTGRESQL ]")
    print(f"{'New Records Inserted':<45}: {stats['pg_inserted']}")
    print(f"{'Existing Records Updated':<45}: {stats['pg_updated']}")
    print("-" * 65)
    
    print("[ SKIPPED RECORDS AGGREGATE ]")
    print(f"{'Source not SUPERAPP':<45}: {stats['skipped_source']}")
    print(f"{'Missing Critical Fields (Mobile/Event)':<45}: {stats['skipped_missing_fields']}")
    print(f"{'Dedupe Hit (Seen in 30 days)':<45}: {stats['skipped_dedupe']}")
    print(f"{'Postgres: Already Processed':<45}: {stats['skipped_pg_processed']}")
    print(f"{'Postgres: Event Unchanged':<45}: {stats['skipped_pg_unchanged']}")
    print("="*65)
    
    if stats["skip_reasons"]:
        print("\n--- SKIPPED MOBILE NUMBERS & REASONS ---")
        print(f"| {'Mobile Number':<15} | {'Reason':<40} |")
        print(f"|{'-'*17}|{'-'*42}|")
        for mobile, reason in stats["skip_reasons"]:
            print(f"| {mobile:<15} | {reason:<40} |")
        print("-" * 65)
    print("\n")

# Graceful shutdown flag
running = True

def handle_shutdown(signum, frame):
    global running
    logger.info("\nShutdown signal received. Stopping CSV simulation...")
    running = False

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

logger.info(f"Starting CSV processing loop from {CSV_FILE_PATH}...")

try:
    if not os.path.exists(CSV_FILE_PATH):
        logger.error(f"CSV file '{CSV_FILE_PATH}' not found. Exiting.")
        sys.exit(1)

    with open(CSV_FILE_PATH, mode='r', encoding='utf-8-sig') as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row in reader:
            if not running:
                break
            
            stats["total_rows"] += 1
            mobile_no = row.get('mobile_no', 'UNKNOWN')
            logger.info(f"Read row for mobile_no: {mobile_no}")
            
            event_payload = map_csv_row_to_event(row)
            
            source = event_payload.get("Source", "")
            if source and source.strip().upper() == "SUPERAPP":
                process_event(event_payload)
            else:
                logger.info(f"Skipping row (Source is not SUPERAPP): {source}")
                stats["skipped_source"] += 1
                stats["skip_reasons"].append((mobile_no, f"Source not SUPERAPP ({source})"))
            
            if running:
                logger.info("Sleeping for 1 seconds...\n")
                for _ in range(1):
                    if not running:
                        break
                    time.sleep(0.1) #TODO change to 10

    logger.info("Finished reading all rows in the CSV file.")

except Exception as e:
    logger.error(f"Error during CSV processing: {e}", exc_info=True)
finally:
    logger.info("CSV simulation safely stopped. Generating summary...")
    print_summary()