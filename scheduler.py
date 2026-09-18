import os
from datetime import datetime, time as dtime, timedelta, timezone
import logging
import pandas as pd
import pytz
import requests
from postgres_api import PostgresDatabaseAPI
from sarvam_service import SarvamService
from dotenv import load_dotenv
logger = logging.getLogger(__name__)

load_dotenv()

total_attempts = os.getenv('SARVAM_TOTAL_ATTEMPTS')
sarvam_api_key = os.getenv('SARVAM_API_KEY')
org_id = os.getenv('SARVAM_ORG_ID')
app_id = os.getenv('SARVAM_APP_ID')
workspace_id = os.getenv('SARVAM_WORKSPACE_ID')
db_env = os.getenv('CONFIG')

postgres_db_api = PostgresDatabaseAPI(db_env)

def calling_job():
    # 1. Business hours gate check
    current_time = datetime.now(pytz.timezone("Asia/Kolkata")).time()
    if not (dtime(9, 0) <= current_time <= dtime(19, 0)):
        logger.info("Outside business hours (09:00 - 19:00 IST). Skipping job.")
        return True

    postgres_db_api = PostgresDatabaseAPI("uat")

    ten_minutes_before = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()

    records = postgres_db_api.read(
        "wa_dropoff", filters={"call_triggered": False, "is_processed": False, "is_connected": False, "call_count": {"op": "<", "val": str(total_attempts)}, "created_at": {"op": "<", "val": ten_minutes_before}, "updated_at": {"op": "<", "val": ten_minutes_before}}
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
    SarvamService.process_batch(validated_df)
    return True


def get_interactions():
    logger.info(f"Running get attempts task at {datetime.now(timezone.utc)}")

    headers = {"X-API-Key": sarvam_api_key}
    now_utc = datetime.now(timezone.utc)
    start_time = now_utc - timedelta(minutes=60)
    # start_time = now_utc - timedelta(hours=7)
    fmt = '%Y-%m-%dT%H:%M:%S'
    base_url = f"https://apps.sarvam.ai/api/analytics/v1/{org_id}/{workspace_id}/{app_id}"

    # --- HELPER: Fetch Paginated ---
    def fetch_paginated_data(endpoint_type, start_datetime):
        all_items, current_url = [], f"{base_url}/{endpoint_type}"
        current_params = {"start_datetime": start_datetime, "end_datetime": now_utc.strftime(fmt), "limit": 100}

        while current_url:
            logger.info(f"Giving 1 second break before hitting url: {current_url}")
            response = requests.get(current_url, headers=headers, params=current_params)

            response.raise_for_status()
            data = response.json()

            all_items.extend(data.get("items", []))
            next_uri = data.get("next_page_uri")

            if next_uri and next_uri.strip():
                current_params = {}
                current_url = f"{base_url}/{endpoint_type}{next_uri}" if next_uri.startswith("?") else next_uri
            else: current_url = None

        return pd.DataFrame(all_items)

    try:
        # 1. FETCH & INITIAL CLEANING
        created_at = "SELECT MAX(created_at) FROM wa_interactions"

        if created_at is None:
            created_at = now_utc

        less_10_min = (created_at - timedelta(minutes=10)).strftime(fmt)

        df_int = fetch_paginated_data("interactions", less_10_min)

        # =====================================================================
        # FETCH & INITIAL CLEANING
        # =====================================================================
        if 'channel_direction' in df_int.columns:
            df_int = df_int[df_int['channel_direction'] != 'inbound']

        if df_int.empty:
            logger.info("No attempts found.")
            return

        select_correlation_id = f"SELECT correlation_id FROM wa_interactions WHERE created_at BETWEEN '{less_10_min}' AND '{now_utc}'"

        correlation_ids = set(select_correlation_id)

        merge_df = pd.merge(df_int, correlation_ids, how='left', indicator=True)

        cleaned_df = merge_df[merge_df['_merge'] == 'left_only'].reset_index(drop=True)

        cleaned_df['vars'] = cleaned_df['agent_variables'].apply(lambda x: x if isinstance(x, dict) else {})

        # CRITICAL FIX: Normalize while explicitly clamping the index to merged_df to stop row-shifting!
        normalized_vars = pd.json_normalize(cleaned_df['vars']).set_index(cleaned_df.index)

        # Strip complex JSON structures and perform horizontal concatenation safely
        base_columns_df = cleaned_df.drop(['agent_variables', 'vars'], axis=1, errors='ignore')

        final_df = pd.concat([base_columns_df, normalized_vars], axis=1)

        columns_to_insert = ["customer_name", "mobile_no", "event_name"]

        # Execute bulk insert
        inserted_count = postgres_db_api.insert_bulk_df(
            table_name="wa_dropoff", 
            df=final_df, 
            allowed_columns=columns_to_insert
        )

        logger.info(f"Total inserted: {inserted_count}")

        # Clean out empty/missing attempt keys and ignore unassigned worker rows
        df_int = df_int.dropna(subset=['attempt_id']).query("attempt_id != 'NO_JOB_ID'").drop_duplicates('attempt_id')
    except Exception as e:
        logger.error(f"this is error: {str(e)}")

