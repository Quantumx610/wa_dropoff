import os
import time
import numpy as np
from psycopg2 import sql
from psycopg2.extras import execute_values
from datetime import datetime, time as dtime, timedelta, timezone
import logging
import pandas as pd
import pytz
import requests
from cosmosdb_api import CosmosDatabaseAPI
from db import get_cosmos_connection, get_postgres_connection
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
calling_delay = os.getenv('CALLING_DELAY')

COSMOS_ENDPOINT = os.getenv("COSMOS_ENDPOINT")
COSMOS_KEY = os.getenv("COSMOS_KEY")
COSMOS_DATABASE = os.getenv("COSMOS_DATABASE")
KAFKA_GEN_LOG = os.getenv("KAFKA_GEN_LOG")


def calling_job():
    postgres_db_api = get_postgres_connection(db_env or "uat")
    cosmos_db_api = get_cosmos_connection(COSMOS_ENDPOINT, COSMOS_KEY, COSMOS_DATABASE)
    
    job_logs = {
        "type": "job_log", 
        "job_name": "calling_job", 
        "within_allowed_time": "yes"
    }
    errors = ""
    validated_df = pd.DataFrame()  # Initialize to prevent UnboundLocalError

    try:
        # 1. Business hours gate check (09:00 - 19:00 IST)
        current_datetime = datetime.now(pytz.timezone("Asia/Kolkata"))
        current_time = current_datetime.time()
        
        if not (dtime(9, 0) <= current_time <= dtime(19, 0)):
            job_logs["within_allowed_time"] = "no"
            job_logs["executed_at"] = current_datetime.isoformat()
            logger.info("Outside business hours (09:00 - 19:00 IST). Skipping job.")
            cosmos_db_api.dbInsert(KAFKA_GEN_LOG, job_logs)
            return

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
            job_logs["valid_call_count"] = 0
            job_logs["completed_at"] = datetime.now(pytz.timezone("Asia/Kolkata")).isoformat()
            cosmos_db_api.dbInsert(KAFKA_GEN_LOG, job_logs)
            return

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
        errors += f"\n{str(e)}"
        logger.error(f"Caught error in calling_job before Sarvam service: {e}", exc_info=True)

    # Execute Sarvam Service Batch only if there are records to process
    if not validated_df.empty:
        try:
            SarvamService.process_batch(validated_df)
            job_logs["sarvam_service"] = "success"
        except Exception as e:
            job_logs["sarvam_service"] = "failed"
            errors += f"\n{str(e)}"
            logger.error(f"Caught error during Sarvam service processing: {e}", exc_info=True)

    if errors:
        job_logs["errors"] = errors

    job_logs["completed_at"] = datetime.now(pytz.timezone("Asia/Kolkata")).isoformat()
    cosmos_db_api.dbInsert(KAFKA_GEN_LOG, job_logs)

    return True


def get_interactions():
    postgres_db_api = get_postgres_connection(db_env or "uat")
    cosmos_db_api = get_cosmos_connection(COSMOS_ENDPOINT, COSMOS_KEY, COSMOS_DATABASE)

    job_logs = {
        "type": "job_log",
        "job_name": "get_interactions",
        "status": "SUCCESS",
        "inserted_count": 0,
        "executed_at": datetime.now(pytz.timezone("Asia/Kolkata")).isoformat(),
        "errors": []
    }
    errors = ""

    logger.info(f"Running get interactions task at IST: {datetime.now(pytz.timezone("Asia/Kolkata")).isoformat()}")

    headers = {"X-API-Key": sarvam_api_key}
    now_utc = datetime.now(timezone.utc)
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    base_url = f"[https://apps.sarvam.ai/api/analytics/v1/](https://apps.sarvam.ai/api/analytics/v1/){org_id}/{workspace_id}/{app_id}"

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
            try:
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
            except requests.exceptions.RequestException as req_err:
                err_msg = f"API Request failed for URL {current_url}: {str(req_err)}"
                logger.error(err_msg)
                job_logs["errors"].append(err_msg)
                raise

        return pd.DataFrame(all_items)

    try:
        # =====================================================================
        # 1. DATABASE LOOKUP & API FETCH
        # =====================================================================
        try:
            max_created_res = postgres_db_api.execute_raw_query("SELECT MAX(start_datetime) FROM wa_interactions;")
            created_at = (max_created_res[0]["max"] if max_created_res and max_created_res[0]["max"] else (now_utc - timedelta(minutes=60)))
        except Exception as db_err:
            err_msg = f"Database query for MAX(start_datetime) failed: {str(db_err)}"
            logger.error(err_msg)
            job_logs["errors"].append(err_msg)
            raise

        less_10_min = (created_at - timedelta(minutes=10)).strftime(fmt)

        df_int = fetch_paginated_data("interactions", less_10_min)

        if df_int.empty:
            logger.info("No attempts found from API.")
            job_logs["details"] = "No attempts found from API."
            return

        if "channel_direction" in df_int.columns:
            df_int = df_int[df_int["channel_direction"] != "inbound"]

        if df_int.empty:
            logger.info("No outbound interactions after filtering.")
            job_logs["details"] = "No outbound interactions after filtering."
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
        if "dnd_status" in flattened_df.columns:
            dnd_status_df = flattened_df[flattened_df["dnd_status"] == "yes"]
            dnd_status_correlation_ids = dnd_status_df["correlation_id"].dropna().unique().tolist()

            if dnd_status_correlation_ids:
                try:
                    postgres_db_api.update_bulk("wa_dropoff", {"is_processed": True}, "correlation_id", dnd_status_correlation_ids)
                except Exception as dnd_err:
                    err_msg = f"Failed to bulk update dnd_status in wa_dropoff: {str(dnd_err)}"
                    logger.error(err_msg)
                    job_logs["errors"].append(err_msg)

        # =====================================================================
        # 3. PANDAS MERGE ANTI-JOIN FOR CORRELATION IDs
        # =====================================================================
        try:
            existing_db_records = postgres_db_api.execute_raw_query(
                "SELECT correlation_id FROM wa_interactions WHERE created_at BETWEEN %s AND %s AND correlation_id IS NOT NULL;",
                (less_10_min, now_utc.strftime(fmt)),
            )
        except Exception as db_rec_err:
            err_msg = f"Failed to fetch existing database records: {str(db_rec_err)}"
            logger.error(err_msg)
            job_logs["errors"].append(err_msg)
            raise

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
            job_logs["details"] = "All fetched records already exist in database."
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
            "has_log_issues", "attempted_at", "correlation_id", "whatsapp_link", "customer_name",
            "current_dropoff_state", "loan_amount", "loan_tenure", "reschedule_date",
            "reschedule_time", "customer_verification", "call_disposition", "identity_confirmed",
            "dnd_status", "journey_progressed_during_call", "emandate_status",
            "esign_status", "journey_completed", "mandate_mode"
        ]

        valid_cols = [c for c in all_table_columns if c in cleaned_df.columns]

        if not valid_cols:
            logger.warning("No valid columns found to insert into wa_interactions.")
            job_logs["details"] = "No valid columns found to insert into wa_interactions."
            return

        # Step A: Filter down to valid columns
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
        connected_ids = cleaned_df["correlation_id"].dropna().unique().tolist()
        if connected_ids:
            try:
                postgres_db_api.update_bulk("wa_dropoff", {"is_connected": True, "call_state": "completed"}, "correlation_id", connected_ids)
            except Exception as conn_err:
                err_msg = f"Failed to update wa_dropoff status for connected IDs: {str(conn_err)}"
                logger.error(err_msg)
                job_logs["errors"].append(err_msg)
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
        job_logs["inserted_count"] = total_inserted

    except Exception as e:
        err_msg = str(e)
        logger.error(f"Error executing get_interactions scheduler: {err_msg}", exc_info=True)
        job_logs["status"] = "FAILED"
        job_logs["errors"].append(err_msg)
        errors = "; ".join(job_logs["errors"])
    finally:
        job_logs["completed_at"] = datetime.now(pytz.timezone("Asia/Kolkata")).isoformat()
        job_logs["error_summary"] = errors

        try:
            cosmos_db_api.dbInsert(KAFKA_GEN_LOG, job_logs)
        except Exception as log_err:
            logger.error(f"Failed to log execution details to Cosmos DB: {str(log_err)}")


def push_rechurn_queue():
    postgres_db_api = get_postgres_connection("uat")
    cosmos_db_api = get_cosmos_connection(COSMOS_ENDPOINT, COSMOS_KEY, COSMOS_DATABASE)

    # Initialize job execution context
    job_log = {
        "job_name": "push_rechurn_queue",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "status": "IN_PROGRESS",
        "part1": {"status": "PENDING", "rows_updated": 0, "duration_ms": 0},
        "part2": {"status": "PENDING", "rows_updated": 0, "duration_ms": 0},
        "completed_at": None,
        "error": None
    }

    # --- Part-1 : Dequeue records that reached max attempts ---
    part1_start = time.perf_counter()
    try:
        update_global_safety = """
            UPDATE wa_dropoff
            SET is_processed = TRUE
            WHERE call_count >= %s
              AND is_processed = FALSE;
        """

        # Single parameterized query replaces network roundtrips
        part1_rows = postgres_db_api.execute_raw_query(update_global_safety, (total_attempts,))

        job_log["part1"] = {
            "status": "SUCCESS",
            "rows_updated": part1_rows if isinstance(part1_rows, int) else 0,
            "duration_ms": round((time.perf_counter() - part1_start) * 1000, 2)
        }
        logger.info(f"Part-1 completed. Updated {job_log['part1']['rows_updated']} records.")

    except Exception as e:
        part1_duration = round((time.perf_counter() - part1_start) * 1000, 2)
        job_log["part1"] = {
            "status": "FAILED",
            "duration_ms": part1_duration,
            "error": str(e)
        }
        logger.error(f"Caught Exception in Global Safety Job: Part1 - {str(e)}", exc_info=True)

    # --- Part-2 : Atomic SQL Update for Rechurn Queue ---
    part2_start = time.perf_counter()
    try:
        thirty_mins_ago = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()

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
                AND NOT EXISTS (
                    SELECT 1 
                    FROM wa_interactions AS wi 
                    WHERE wi.correlation_id = wd.correlation_id
                );
        """

        part2_rows = postgres_db_api.execute_raw_query(
            optimized_update_query, 
            (total_attempts, thirty_mins_ago, thirty_mins_ago)
        )

        job_log["part2"] = {
            "status": "SUCCESS",
            "rows_updated": part2_rows if isinstance(part2_rows, int) else 0,
            "duration_ms": round((time.perf_counter() - part2_start) * 1000, 2)
        }
        logger.info(f"Part-2 completed. Updated {job_log['part2']['rows_updated']} records.")

    except Exception as e:
        part2_duration = round((time.perf_counter() - part2_start) * 1000, 2)
        job_log["part2"] = {
            "status": "FAILED",
            "duration_ms": part2_duration,
            "error": str(e)
        }
        logger.error(f"Caught Exception in Global Safety Job: Part2 - {str(e)}", exc_info=True)

    # Finalize Job Context
    job_log["completed_at"] = datetime.now(timezone.utc).isoformat()
    if job_log["part1"]["status"] == "FAILED" or job_log["part2"]["status"] == "FAILED":
        job_log["status"] = "PARTIAL_FAILURE" if "SUCCESS" in (job_log["part1"]["status"], job_log["part2"]["status"]) else "FAILED"
    else:
        job_log["status"] = "SUCCESS"

    logger.info(f"Global Safety Job Log Summary: {job_log}")
    return job_log

