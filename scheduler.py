import os
from datetime import datetime, time as dtime
import logging
import pandas as pd
import pytz
from postgres_api import PostgresDatabaseAPI
from sarvam_service import SarvamService
from dotenv import load_dotenv
logger = logging.getLogger(__name__)

load_dotenv()

total_attempts = os.getenv('SARVAM_TOTAL_ATTEMPTS')

def calling_job():
    # 1. Business hours gate check
    current_time = datetime.now(pytz.timezone("Asia/Kolkata")).time()
    if not (dtime(9, 0) <= current_time <= dtime(19, 0)):
        logger.info("Outside business hours (09:00 - 19:00 IST). Skipping job.")
        return True

    postgres_db_api = PostgresDatabaseAPI("uat")
    records = postgres_db_api.read(
        "wa_dropoff", filters={"call_triggered": False, "is_processed": False, "is_connected": False, "call_count": {"op": "<", "val": str(total_attempts)}}
    )

    if not records:
        logger.info("No pending drop-off records to call.")
        return True

    records_df = pd.DataFrame(records)

    # 2. Sort by latest creation date first per mobile number
    records_df = records_df.sort_values(
        by=["mobile_no", "created_at"], ascending=[True, False]
    )

    # 3. Deduplicate to keep only the newest event per mobile number
    records_df["row_count"] = records_df.groupby("mobile_no").cumcount() + 1
    validated_df = records_df[records_df["row_count"] == 1].reset_index(
        drop=True
    )

    dict_validated = validated_df.to_dict(orient='records')

    logger.info(f"This is the valid calls: {validated_df.count()}")

    # 4. Execute Sarvam Service Batch
    # SarvamService.process_batch(validated_df)
    return True