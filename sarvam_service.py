from datetime import datetime
import os
import time
import pandas as pd
import pytz
import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from limits import strategies, parse
from limits.storage import MemoryStorage
from dotenv import load_dotenv

from postgres_api import PostgresDatabaseAPI


# Initialize the Limiter Engine (Global/Class level)
storage = MemoryStorage()
limiter = strategies.MovingWindowRateLimiter(storage)
SARVAM_LIMIT = parse("200 per 1 minute")

load_dotenv()
env = os.getenv('CONFIG', "uat")


db_api = PostgresDatabaseAPI(env)
rechurn_count = 3

# Fallback logger for standalone use
default_logger = logging.getLogger(__name__)

class SarvamService:
    _session = requests.Session()
    _config = {}
    _logger = default_logger

    @classmethod
    def init_service(cls, config, logger=None):
        cls._session.headers.update({"X-API-Key": os.getenv("SARVAM_API_KEY")})

    @classmethod
    def trigger_outbound_call(cls, name, phone, customer_intent, branch, correlation_id):
        while not limiter.hit(SARVAM_LIMIT, "sarvam_api"):
            time.sleep(0.1)

        meta = cls._config
        url = f"https://apps.sarvam.ai/api/outbounds/v1/orgs/{os.getenv('SARVAM_ORG_ID')}/workspaces/{os.getenv('SARVAM_WORKSPACE_ID')}/outbounds"

        payload = {
            "app_config": {
                "app_id": os.getenv("SARVAM_APP_ID"),
                "app_version": os.getenv("SARVAM_APP_VERSION"),
                "connection_config": {
                    "connection_id": os.getenv("SARVAM_CONNECTION_ID"),
                    "agent_phone_number": os.getenv("SARVAM_AGENT_NUMBER")
                },
                "agent_variables": {
                    "full_name": name, "intent": customer_intent,
                    "branch": branch, "correlation_id": correlation_id,
                },
                "app_type": "agent",
                "app_overrides": {
                    "initial_bot_message": f"नमस्ते, क्या मेरी बात {name} जी से हो रही है?",
                    "initial_language_name": "Hindi"
                }
            },
            "user_config": {"user_phone_number": f"+91{phone}"}
        }

        try:
            response = cls._session.post(url, json=payload, timeout=15)
            if response.status_code == 200:
                cls._logger.info("Sarvam Rechurned Called Successfull.")
                return True, response.json().get("attempt_id"), correlation_id
            return False, response.text, correlation_id
        except Exception as e:
            return False, str(e), correlation_id
    
    @classmethod
    def process_batch(cls, rows, is_rechurn_source=False):
        cls._logger.info(f"Starting process_batch. Total rows: {len(rows)}")
        
        # 1. Extract IDs immediately from the input rows
        all_cids = [row['correlation_id'] for row in rows]
        if not all_cids:
            return

        # --- PRE-INCREMENT (Commit the attempt in DB first) ---
        try:
            # A. ALWAYS increment the count first
            cls._logger.info(f"Pre-incrementing call_count for {len(all_cids)} CIDs")
            db_api.increment_bulk(
                table_name="whatsapp_dropoff", 
                column_to_inc="call_count", 
                where_col="correlation_id", 
                values_list=all_cids
            )

            # B. Mark as is_rechurn=True if they just hit the limit
            # Note: Since we just incremented, we check if count is >= rechurn_count
            limit_query = f"""
                UPDATE whatsapp_dropoff 
                SET is_processed = True 
                WHERE correlation_id IN %s 
                AND call_count >= {rechurn_count}
                AND is_processed = False
            """
            db_api.execute_raw_query(limit_query, (tuple(all_cids),))

        except Exception as e:
            cls._logger.error(f"Failed to pre-update Postgres: {e}")
            cls._logger.error(f"Exiting early process_batch because of postgres failed.")
            return

        # --- EXECUTE API CALLS ---
        with ThreadPoolExecutor(max_workers=10) as executor:
            failed_cids = []
            journey_updates = []

            future_to_cid = {
                executor.submit(
                    cls.trigger_outbound_call,
                    row['name'], row['mobile_no'], row.get('intent', ''), 
                    row.get('brndes', ''), row['correlation_id']
                ): row['correlation_id'] for row in rows
            }

            for future in as_completed(future_to_cid):
                cid = future_to_cid[future]
                try:
                    success, result, _ = future.result()

                    if not success:
                        # API trigger failed - collect CID for bulk reset
                        failed_cids.append(cid)

                    journey_updates.append({
                        "correlation_id": cid, 
                        "sarvam_attempt_id": result if success else None, 
                        "sarvam_outbound": success,
                        "is_rechurn": is_rechurn_source 
                    })
                except Exception as e:
                    cls._logger.error(f"Thread Exception for {cid}: {str(e)}")

            # --- FINAL UPDATE (Journey Logs only) ---
            if journey_updates:
                db_api.update_bulk_mapped("cibil_journey", journey_updates, "correlation_id")

            if failed_cids:
                cls._logger.warning(f"Resetting {len(failed_cids)} failed triggers to 'failed' state.")
                db_api.update_bulk(
                    table_name="whatsapp_dropoff",
                    update_data={"call_state": "failed"},
                    column_name="correlation_id",
                    values_list=failed_cids
                )

        cls._logger.info("Batch processing complete.")