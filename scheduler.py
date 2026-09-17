import os
import pandas as pd
from datetime import datetime, timezone, time as dtime
import pytz
from postgres_api import PostgresDatabaseAPI
import logging


logger = logging.getLogger(__name__)


def calling_job():

    # time frame window for calling
    current_time = datetime.now(pytz.timezone("Asia/Kolkata")).time()
    if not (dtime(9, 0) <= current_time <= dtime(19, 0)):
        logger.info(f"Outside business hours. Skipping job.")
        return
    
    # get call_triggered false records
    postgres_db_api = PostgresDatabaseAPI("uat")
    records = postgres_db_api.read("whatsapp_dropoff", filters={"call_triggered": False, "is_processed": False})

    records_df = pd.DataFrame(records)

    records_df = records_df.drop_duplicates(subset=["mobile_no", "event_name"], ignore_index=True)
    records_df.sort_values(by=["mobile_no", "created_at"], ascending=[True, False])
    records_df["row_count"] = records_df.groupby('mobile_no').cumcount() + 1

    validated_df = records_df[records_df["row_count"] == 1].reset_index(drop=True)


    return True