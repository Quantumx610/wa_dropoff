import os
import uuid
import logging
from datetime import datetime, timezone, timedelta
from db import get_cosmos_connection, get_postgres_connection
logger = logging.getLogger(__name__)

# Config & Constants
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
SARVAM_TOTAL_ATTEMPTS = int(os.getenv("SARVAM_TOTAL_ATTEMPTS", "3"))

COSMOS_LOG_CONTAINER = os.getenv("COSMOS_LOG_CONTAINER", "kafka_input_log")
COSMOS_DEDUPE_CONTAINER = os.getenv("COSMOS_DEDUPE_CONTAINER", "kafka_dedupe_log")

IST = timezone(timedelta(hours=5, minutes=30))


# ==========================================
# PIPELINE BUSINESS LOGIC
# ==========================================


def insert_input_log(event: dict) -> bool:
    """Logs incoming payload into the input log container."""
    cosmos_db_api = get_cosmos_connection()
    res = cosmos_db_api.dbInsert(COSMOS_LOG_CONTAINER, event)
    return res is not None


def dedupe_log_check(mobile_no: str, event_name: str) -> bool:
    """
    Returns True if user is ELIGIBLE for processing (no duplicate found within 30 days).
    Returns False if an entry exists within the 30-day window.
    """
    cosmos_db_api = get_cosmos_connection()
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
    cosmos_db_api = get_cosmos_connection()
    postgres_db_api = get_postgres_connection()

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

    if not mobile_no or not event_name or not enquiry_id:
        logger.warning("Exit: Event missing critical fields: mobile_no or event_name or enquiry_id.")
        return False

    cosmos_event = {
        "id": correlation_id,
        "source": event.get("Source", None),
        "app_version": event.get("AppVersion", None),
        "platform": event.get("Platform", None),
        "application_id": event.get("Application_ID", None),
        "hpa": event.get("HPA", None),
        "mandate_mode": event.get("MandateMode", None),
        "penny_drop_failure_reason": event.get("PennyDropFailureReason", None),
        "offer": event.get("Offer", None),
        "enquiry_id": enquiry_id,
        "superapp_id": event.get("SuperAppid", None),
        "journey": event.get("Journey", None),
        "event_name": event_name,
        "mobile_no": mobile_no,
        "customer_flag": event.get("CustomerFlag", None),
        "ucic": event.get("UCIC", None),
        "ucic_value": event.get("UCIC_VALUE", None),
        "lan_verified": event.get("LAN_VERIFIED", None),
        "timestamp": event.get("Timestamp", None),
        "response_code": event.get("Response Code", None),
        "loan_amount": event.get("LoanAmount", None),
        "correlation_id": correlation_id,
        "received_at_ist": received_at_ist,
        "received_at_utc": received_at_utc
    }

    # Step 2: Deduplication Check
    if not dedupe_log_check(mobile_no, event_name):
        logger.info(f"Dedupe hit: {mobile_no} / {event_name} seen within 30 days.")
        return False

    # Store Eligible Event in Cosmos
    cosmos_db_api.dbInsert(COSMOS_DEDUPE_CONTAINER, cosmos_event)

    # Step 3: PostgreSQL Calling Ledger Check
    records = postgres_db_api.read("wa_dropoff", filters={"mobile_no": mobile_no})

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

    if not records:
        postgres_db_api.insert("wa_dropoff", pg_payload)
        logger.info(f"Created new drop-off record for {mobile_no}.")
        return True

    existing_record = dict(records[0])
    call_count = existing_record.get("call_count") or 0
    
    # Step 4: Skipping conditions fix
    if existing_record.get("is_processed") is True or call_count >= SARVAM_TOTAL_ATTEMPTS:
        if call_count >= SARVAM_TOTAL_ATTEMPTS:
            logger.info(f"Skipping {mobile_no}: call_count >= {SARVAM_TOTAL_ATTEMPTS}")
        else:
            logger.info(f"Skipping {mobile_no}: Record already processed.")
        return False

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


# ==========================================
# REUSABLE KAFKA CONSUMER WRAPPER
# ==========================================

