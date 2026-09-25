import os
import logging
import pytz
from datetime import datetime, timedelta, timezone
from db import get_cosmos_connection, get_postgres_connection
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

total_attempts = os.getenv('SARVAM_TOTAL_ATTEMPTS', 3)
KAFKA_GEN_LOG = os.getenv("KAFKA_GEN_LOG")

IST = pytz.timezone("Asia/Kolkata")


def push_rechurn_queue() -> dict:
    postgres_db_api = get_postgres_connection()
    cosmos_db_api = get_cosmos_connection()

    start_time_ist = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")

    job_logs = {
        "type": "job_log",
        "job_name": "push_rechurn_queue",
        "started_at_ist": start_time_ist,
        "status": "IN_PROGRESS",
        "details": {
            "part1": {"status": "PENDING", "rows_updated": 0},
            "part2": {"status": "PENDING", "rows_updated": 0}
        },
        "errors": []
    }

    # --- Part-1 : Dequeue records that reached max attempts ---
    try:
        update_global_safety = """
            UPDATE wa_dropoff
            SET is_processed = TRUE,
                updated_at = CURRENT_TIMESTAMP
            WHERE call_count >= %s
              AND is_processed = FALSE;
        """

        part1_rows = postgres_db_api.execute_raw_query(update_global_safety, (total_attempts,))

        job_logs["details"]["part1"] = {
            "status": "SUCCESS",
            "rows_updated": part1_rows if isinstance(part1_rows, int) else 0
        }
        logger.info(f"Part-1 completed. Updated {job_logs['details']['part1']['rows_updated']} records.")

    except Exception as e:
        err_msg = f"Part1 Error: {str(e)}"
        job_logs["details"]["part1"] = {
            "status": "FAILED"
        }
        job_logs["errors"].append(err_msg)
        logger.error(f"Caught Exception in Global Safety Job: Part1 - {str(e)}", exc_info=True)

    # --- Part-2 : Atomic SQL Update for Rechurn Queue ---
    try:
        thirty_mins_ago_utc = (datetime.now(timezone.utc) - timedelta(minutes=30)).replace(tzinfo=None)

        optimized_update_query = """
            UPDATE wa_dropoff AS wd
            SET call_triggered = FALSE,
                updated_at = CURRENT_TIMESTAMP
            WHERE wd.call_triggered = TRUE
                AND wd.is_processed = FALSE
                AND wd.call_state != 'pending'
                AND wd.call_count < %s
                AND wd.created_at < %s
                AND wd.updated_at < %s
                AND wd.correlation_id IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 
                    FROM wa_interactions AS wi 
                    WHERE wi.correlation_id = wd.correlation_id
                );
        """

        part2_rows = postgres_db_api.execute_raw_query(
            optimized_update_query, 
            (total_attempts, thirty_mins_ago_utc, thirty_mins_ago_utc)
        )

        job_logs["details"]["part2"] = {
            "status": "SUCCESS",
            "rows_updated": part2_rows if isinstance(part2_rows, int) else 0
        }
        logger.info(f"Part-2 completed. Updated {job_logs['details']['part2']['rows_updated']} records.")

    except Exception as e:
        err_msg = f"Part2 Error: {str(e)}"
        job_logs["details"]["part2"] = {
            "status": "FAILED"
        }
        job_logs["errors"].append(err_msg)
        logger.error(f"Caught Exception in Global Safety Job: Part2 - {str(e)}", exc_info=True)

    job_logs["completed_at_ist"] = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")

    p1_status = job_logs["details"]["part1"]["status"]
    p2_status = job_logs["details"]["part2"]["status"]

    if p1_status == "FAILED" or p2_status == "FAILED":
        job_logs["status"] = "PARTIAL_FAILURE" if "SUCCESS" in (p1_status, p2_status) else "FAILED"
    else:
        job_logs["status"] = "SUCCESS"

    logger.info(f"Global Safety Job Log Summary: {job_logs}")

    try:
        cosmos_db_api.dbInsert(KAFKA_GEN_LOG, job_logs)
    except Exception as cosmos_err:
        logger.error(f"Failed to write job log to Cosmos DB: {cosmos_err}", exc_info=True)

    return job_logs