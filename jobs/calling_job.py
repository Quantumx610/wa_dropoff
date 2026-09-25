import os
import logging
from datetime import datetime, time as dtime, timedelta, timezone
import pandas as pd
import pytz
from db import get_cosmos_connection, get_postgres_connection
from services import SarvamService
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

total_attempts = os.getenv('SARVAM_TOTAL_ATTEMPTS')
db_env = os.getenv('CONFIG')
calling_delay = int(os.getenv('CALLING_DELAY', 0))

KAFKA_GEN_LOG = os.getenv("KAFKA_GEN_LOG")
IST = pytz.timezone("Asia/Kolkata")


def calling_eligible():
    postgres_db_api = get_postgres_connection()
    cosmos_db_api = get_cosmos_connection()

    start_time_ist = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")

    job_logs = {
        "type": "job_log",
        "job_name": "calling_job",
        "started_at_ist": start_time_ist,
        "status": "SUCCESS",
        "within_allowed_time": "yes",
        "valid_call_count": 0,
        "details": {},
        "errors": []
    }

    validated_df = pd.DataFrame()

    try:
        # 1. Business hours gate check (09:00 - 19:00 IST)
        current_datetime = datetime.now(IST)
        current_time = current_datetime.time()

        if not (dtime(9, 0) <= current_time <= dtime(19, 0)):
            job_logs["within_allowed_time"] = "no"
            job_logs["details"]["message"] = "Outside business hours (09:00 - 19:00 IST). Skipping job."
            logger.info("Outside business hours (09:00 - 19:00 IST). Skipping job.")
            return job_logs

        n_minutes_before = (datetime.now(timezone.utc) - timedelta(minutes=calling_delay)).isoformat()

        records = postgres_db_api.read(
            "wa_dropoff",
            filters={
                "call_triggered": False,
                "is_processed": False,
                "call_count": {"op": "<", "val": str(total_attempts)},
                "created_at": {"op": "<", "val": n_minutes_before},
                "updated_at": {"op": "<", "val": n_minutes_before}
            }
        )

        if not records:
            logger.info("No pending drop-off records to call.")
            job_logs["details"]["message"] = "No pending drop-off records to call."
            return job_logs

        records_df = pd.DataFrame(records)

        # 2. Sort by latest creation date first per mobile number
        records_df = records_df.sort_values(
            by=["mobile_no", "created_at"], ascending=[True, False]
        )

        # 3. Deduplicate to keep only the newest event per mobile number
        records_df["row_count"] = records_df.groupby("mobile_no").cumcount() + 1
        validated_df = records_df[records_df["row_count"] == 1].reset_index(drop=True)

        valid_count = len(validated_df)
        job_logs["valid_call_count"] = valid_count
        logger.info(f"Valid call count: {valid_count}")

        calling_cids = validated_df["correlation_id"].dropna().unique().tolist()

        if calling_cids:
            postgres_db_api.update_bulk(
                "wa_dropoff",
                {"call_triggered": True, "call_state": "pending"},
                "correlation_id",
                calling_cids
            )

    except Exception as e:
        err_msg = f"Error in calling_job before Sarvam service: {str(e)}"
        job_logs["status"] = "FAILED"
        job_logs["errors"].append(err_msg)
        logger.error(err_msg, exc_info=True)

    # Execute Sarvam Service Batch only if there are records to process
    if not validated_df.empty:
        try:
            SarvamService.process_batch(validated_df)
            job_logs["details"]["sarvam_service"] = "success"
        except Exception as e:
            job_logs["status"] = "PARTIAL_FAILURE" if job_logs["status"] == "SUCCESS" else "FAILED"
            job_logs["details"]["sarvam_service"] = "failed"
            err_msg = f"Error during Sarvam service processing: {str(e)}"
            job_logs["errors"].append(err_msg)
            logger.error(err_msg, exc_info=True)

    job_logs["completed_at_ist"] = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")

    try:
        cosmos_db_api.dbInsert(KAFKA_GEN_LOG, job_logs)
    except Exception as cosmos_err:
        logger.error(f"Failed to log execution details to Cosmos DB: {str(cosmos_err)}")

    return job_logs