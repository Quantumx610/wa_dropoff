import json
import os
import numpy as np
from psycopg2 import sql
from psycopg2.extras import execute_values
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

    calling_cids = validated_df["correlation_id"].dropna().unique().tolist()

    postgres_db_api.update_bulk("wa_dropoff", {"call_triggered": True, "call_state": "pending"}, "correlation_id", calling_cids)

    # 4. Execute Sarvam Service Batch
    SarvamService.process_batch(validated_df)
    return True


def get_interactions():
    logger.info(f"Running get attempts task at {datetime.now(timezone.utc)}")

    headers = {"X-API-Key": sarvam_api_key}
    now_utc = datetime.now(timezone.utc)
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    base_url = f"https://apps.sarvam.ai/api/analytics/v1/{org_id}/{workspace_id}/{app_id}"

    # --- HELPER: Fetch Paginated ---
    def fetch_paginated_data(endpoint_type, start_datetime):
        all_items, current_url = [], f"{base_url}/{endpoint_type}"
        current_params = {
            "start_datetime": start_datetime,
            "end_datetime": now_utc.strftime(fmt),
            "limit": 100,
        }

        while current_url:
            logger.info(f"Going to hit url: {current_url}")
            response = requests.get(
                current_url, headers=headers, params=current_params
            )
            response.raise_for_status()
            data = response.json()

            all_items.extend(data.get("items", []))
            next_uri = data.get("next_page_uri")

            if next_uri and next_uri.strip():
                current_params = {}
                current_url = (
                    f"{base_url}/{endpoint_type}{next_uri}"
                    if next_uri.startswith("?")
                    else next_uri
                )
            else:
                current_url = None

        return pd.DataFrame(all_items)

    try:
        # =====================================================================
        # 1. DATABASE LOOKUP & API FETCH
        # =====================================================================
        max_created_res = postgres_db_api.execute_raw_query("SELECT MAX(created_at) FROM wa_interactions;")

        created_at = (max_created_res[0]["max"] if max_created_res and max_created_res[0]["max"] else (now_utc - timedelta(minutes=60)))

        less_10_min = (created_at - timedelta(minutes=10)).strftime(fmt)

        df_int = fetch_paginated_data("interactions", "2026-09-18T03:10:59Z")

        if df_int.empty:
            logger.info("No attempts found from API.")
            return

        if "channel_direction" in df_int.columns:
            df_int = df_int[df_int["channel_direction"] != "inbound"]

        if df_int.empty:
            logger.info("No outbound interactions after filtering.")
            return

        # =====================================================================
        # 2. FLATTEN AGENT VARIABLES (PRE-MERGE)
        # =====================================================================
        # Normalize agent_variables first to extract 'correlation_id' for deduplication
        if "agent_variables" in df_int.columns:
            df_int["vars"] = df_int["agent_variables"].apply(lambda x: x if isinstance(x, dict) else {})
            normalized_vars = pd.json_normalize(df_int["vars"]).set_index(df_int.index)
            base_columns_df = df_int.drop(["agent_variables", "vars"], axis=1, errors="ignore")
            flattened_df = pd.concat([base_columns_df, normalized_vars], axis=1)
        else:
            flattened_df = df_int.copy()

        # act immediately is_processed to True if user said do not call me
        if "do_not_call" in flattened_df.columns:
            do_not_call_df = flattened_df[flattened_df["do_not_call"] == "yes"]
            do_not_call_correlation_ids = do_not_call_df["correlation_id"].dropna().unique().tolist()

            postgres_db_api.update_bulk("wa_dropoff", {"is_processed": True}, "correlation_id", do_not_call_correlation_ids)

        # =====================================================================
        # 3. PANDAS MERGE ANTI-JOIN FOR CORRELATION IDs
        # =====================================================================
        existing_db_records = postgres_db_api.execute_raw_query(
            "SELECT correlation_id FROM wa_interactions WHERE created_at BETWEEN %s AND %s AND correlation_id IS NOT NULL;",
            (less_10_min, now_utc.strftime(fmt)),
        )

        if existing_db_records and "correlation_id" in flattened_df.columns:
            db_df = pd.DataFrame(existing_db_records)[["correlation_id"]].drop_duplicates()
            
            merged_df = pd.merge(
                flattened_df, 
                db_df, 
                on="correlation_id", 
                how="left", 
                indicator=True
            )
            cleaned_df = (
                merged_df[merged_df["_merge"] == "left_only"]
                .drop(columns=["_merge"])
                .reset_index(drop=True)
            )
        else:
            cleaned_df = flattened_df.reset_index(drop=True)

        if cleaned_df.empty:
            logger.info("All fetched records already exist in database.")
            return

        # =====================================================================
        # 4. PREPARE ALL COLUMNS FOR DATABASE INSERTION
        # =====================================================================
        all_table_columns = [
            "interaction_id", "user_identifier", "duration_in_seconds", "start_datetime",
            "end_datetime", "language_name", "num_messages", "average_agent_response_time_in_seconds",
            "average_user_response_time_in_seconds", "user_contact_masked", "user_contact_hashed",
            "user_contact", "channel_direction", "retry_attempt", "campaign_id", "cohort_id",
            "is_debug_call", "audio_url", "job_id", "channel_type", "channel_provider",
            "channel_protocol", "server_retry_attempt", "failure_reason", "ended_by",
            "has_log_issues", "attempted_at", "correlation_id", "customer_name",
            "current_dropoff_state", "loan_amount", "loan_tenure", "reschedule_date",
            "reschedule_time", "next_emi_ptp_date", "customer_verification_type",
            "customer_verification", "call_disposition", "wrong_number", "do_not_call",
            "journey_progressed_during_call", "bank_verification_status", "emandate_status",
            "esign_status"
        ]

        valid_cols = [c for c in all_table_columns if c in cleaned_df.columns]

        if not valid_cols:
            logger.warning("No valid columns found to insert into wa_interactions.")
            return

        # Step A: Filter down to valid columns
        cleaned_df = cleaned_df[valid_cols].copy()

        # Step B: Clean function to catch float NaNs, empty strings, and stringified nulls
        def clean_val(val):
            if pd.isna(val) or val is None:
                return None
            val_str = str(val).strip()
            if val_str == "" or val_str.lower() in ("nan", "none", "<na>", "null"):
                return None
            return val_str

        cleaned_df = cleaned_df[valid_cols].copy()

        # Standardize string representations ("nan", "none", "", etc.) to true NaN
        cleaned_df = cleaned_df.replace(
            to_replace=r"^(?i:\s*|nan|none|<na>|null)\s*$", 
            value=np.nan, 
            regex=True
        )

        # Replace all np.nan/None across the whole DataFrame with Python None
        # using object casting via .to_numpy() to bypass Pandas auto-converting None back to np.nan
        cleaned_df = pd.DataFrame(
            np.where(pd.isna(cleaned_df), None, cleaned_df),
            columns=cleaned_df.columns,
            index=cleaned_df.index
        )
        # ----------------------------------

        # =====================================================================
        # 5. CHUNKED ATOMIC BULK INSERT (MAIN TABLE + API OUTBOX)
        # =====================================================================
        CHUNK_SIZE = 250
        total_rows = len(cleaned_df)
        num_chunks = int(np.ceil(total_rows / CHUNK_SIZE))

        # Build dynamic SQL queries
        insert_interactions_query = sql.SQL(
            "INSERT INTO {table} ({fields}) VALUES %s ON CONFLICT (interaction_id) DO NOTHING"
        ).format(
            table=sql.Identifier("wa_interactions"),
            fields=sql.SQL(", ").join(map(sql.Identifier, valid_cols)),
        )

        insert_outbox_query = sql.SQL(
            "INSERT INTO {table} (interaction_id, correlation_id, event_name, mobile_no, api_name, status) VALUES %s ON CONFLICT (interaction_id) DO NOTHING"
        ).format(table=sql.Identifier("api_outbox"))

        total_inserted = 0

        with postgres_db_api.transaction() as cursor:
            for i in range(num_chunks):
                chunk_df = cleaned_df.iloc[
                    i * CHUNK_SIZE : (i + 1) * CHUNK_SIZE
                ].copy()

                # A. Prepare tuples for wa_interactions (No need for extra cleaning here)
                main_tuples = [
                    tuple(row[col] for col in valid_cols)
                    for _, row in chunk_df.iterrows()
                ]

                # B. Prepare tuples for api_outbox
                outbox_tuples = [
                    (
                        row.get("interaction_id"),
                        row.get("correlation_id"),
                        row.get("current_dropoff_state"),
                        str(row.get("user_contact"))[-10:] if row.get("user_contact") else None,
                        "create_enquiry",
                        "PENDING"
                    )
                    for _, row in chunk_df.iterrows()
                    if row.get("correlation_id")  # Skip rows missing correlation_id
                ]

                # Execute inserts
                execute_values(cursor, insert_interactions_query, main_tuples, page_size=1000)

                if outbox_tuples:
                    execute_values(cursor, insert_outbox_query, outbox_tuples, page_size=1000)

                total_inserted += len(main_tuples)
                logger.info(
                    f"Processed chunk {i + 1}/{num_chunks} ({len(main_tuples)} rows)"
                )

        logger.info(f"Successfully committed total inserted interactions: {total_inserted}")

    except Exception as e:
        logger.error(f"Error executing get_interactions scheduler: {str(e)}", exc_info=True)


def push_rechurn_queue():
    # Part-1 : dequeue the records who reached max attempts
    cycle_ends = postgres_db_api.read("wa_dropoff", "correlation_id", {"call_count": {"op": ">=", "val": str(total_attempts)}}) 
    if cycle_ends:
        unique_cycle_ends = set(cycle_ends)
        postgres_db_api.update_bulk("wa_dropoff", {"is_processed": True}, "correlation_id", unique_cycle_ends)

    # Part-2 : Queue the records who did not 
    thirty_mins_ago = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()

    rechurn_records = postgres_db_api.read("wa_dropoff", "correlation_id", {"is_processed": False, "call_count": {"op": "<", "val": str(total_attempts)}, "created_at": {"op": "<", "val": thirty_mins_ago}, "updated_at": {"op": "<", "val": thirty_mins_ago}})

    if not rechurn_records:
        logger.info("No records for rechurn.")
        return

    unique_ids = set(rechurn_records)

    postgres_db_api.update_bulk("wa_dropoff", {"call_trigger": False}, "correlation_id", unique_ids)
