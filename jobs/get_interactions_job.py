import os
import numpy as np
from psycopg2 import sql
from psycopg2.extras import execute_values
from datetime import datetime, timedelta, timezone
import logging
import pandas as pd
import pytz
import requests
from db import get_cosmos_connection, get_postgres_connection
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

load_dotenv()

total_attempts = os.getenv('SARVAM_TOTAL_ATTEMPTS')
sarvam_api_key = os.getenv('SARVAM_API_KEY')
org_id = os.getenv('SARVAM_ORG_ID')
app_id = os.getenv('SARVAM_APP_ID')
workspace_id = os.getenv('SARVAM_WORKSPACE_ID')
KAFKA_GEN_LOG = os.getenv("KAFKA_GEN_LOG")

IST = pytz.timezone("Asia/Kolkata")


def get_interactions():
    postgres_db_api = get_postgres_connection()
    cosmos_db_api = get_cosmos_connection()

    start_time_ist = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")

    job_logs = {
        "type": "job_log",
        "job_name": "get_interactions",
        "started_at_ist": start_time_ist,
        "status": "SUCCESS",
        "inserted_count": 0,
        "details": {},
        "errors": []
    }

    logger.info(f"Running get interactions task at IST: {start_time_ist}")

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
            job_logs["details"]["message"] = "No attempts found from API."
            return job_logs

        if "channel_direction" in df_int.columns:
            df_int = df_int[df_int["channel_direction"] != "inbound"]

        if df_int.empty:
            logger.info("No outbound interactions after filtering.")
            job_logs["details"]["message"] = "No outbound interactions after filtering."
            return job_logs

        # =====================================================================
        # 2. FLATTEN AGENT VARIABLES (PRE-MERGE)
        # =====================================================================
        if "agent_variables" in df_int.columns:
            df_int["vars"] = df_int["agent_variables"].apply(lambda x: x if isinstance(x, dict) else {})
            normalized_vars = pd.json_normalize(df_int["vars"]).set_index(df_int.index)
            base_columns_df = df_int.drop(["agent_variables", "vars"], axis=1, errors="ignore")
            flattened_df = pd.concat([base_columns_df, normalized_vars], axis=1)
        else:
            flattened_df = df_int.copy()

        # Act immediately on dnd_status
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
                "SELECT interaction_id FROM wa_interactions WHERE start_datetime BETWEEN %s AND %s AND interaction_id IS NOT NULL;",
                (less_10_min, now_utc.strftime(fmt)),
            )
        except Exception as db_rec_err:
            err_msg = f"Failed to fetch existing database records: {str(db_rec_err)}"
            logger.error(err_msg)
            job_logs["errors"].append(err_msg)
            raise

        if existing_db_records and "interaction_id" in flattened_df.columns:
            db_df = pd.DataFrame(existing_db_records)[["interaction_id"]].drop_duplicates()
            
            merged_df = pd.merge(
                flattened_df, 
                db_df, 
                on="interaction_id", 
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
            job_logs["details"]["message"] = "All fetched records already exist in database."
            return job_logs

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
            "dnd_status", "journey_progressed_during_call", "emandate_status", "voicemail",
            "esign_status", "journey_completed", "mandate_mode" 
        ]

        valid_cols = [c for c in all_table_columns if c in cleaned_df.columns]

        if not valid_cols:
            logger.warning("No valid columns found to insert into wa_interactions.")
            job_logs["details"]["message"] = "No valid columns found to insert into wa_interactions."
            return job_logs

        cleaned_df = cleaned_df[valid_cols].copy()

        cleaned_df = cleaned_df.replace(
            to_replace=r"^(?i:\s*|nan|none|<na>|null)\s*$", 
            value=np.nan, 
            regex=True
        )

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

        # =====================================================================
        # 5. CHUNKED ATOMIC BULK INSERT (MAIN TABLE + API OUTBOX)
        # =====================================================================
        CHUNK_SIZE = 250
        total_rows = len(cleaned_df)
        num_chunks = int(np.ceil(total_rows / CHUNK_SIZE))

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

                main_tuples = [
                    tuple(row[col] for col in valid_cols)
                    for _, row in chunk_df.iterrows()
                ]

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
                    if row.get("correlation_id")
                ]

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

    finally:
        job_logs["completed_at_ist"] = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")

        try:
            cosmos_db_api.dbInsert(KAFKA_GEN_LOG, job_logs)
        except Exception as log_err:
            logger.error(f"Failed to log execution details to Cosmos DB: {str(log_err)}")

    return job_logs
