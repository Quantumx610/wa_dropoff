from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import os
import time
from dotenv import load_dotenv
from limits import parse, strategies
from limits.storage import MemoryStorage
import pandas as pd
from postgres_api import PostgresDatabaseAPI

storage = MemoryStorage()
limiter = strategies.MovingWindowRateLimiter(storage)
SARVAM_LIMIT = parse("200 per 1 minute")

load_dotenv()
env = os.getenv("CONFIG", "uat")
db_api = PostgresDatabaseAPI(env)
total_attempts = int(os.getenv("SARVAM_TOTAL_ATTEMPTS", 3))

default_logger = logging.getLogger(__name__)


class SarvamService:

    _session = None
    _logger = default_logger

    @classmethod
    def get_session(cls):
        if cls._session is None:
            import requests

            cls._session = requests.Session()
            cls._session.headers.update(
                {"X-API-Key": os.getenv("SARVAM_API_KEY")}
            )
        return cls._session

    @classmethod
    def trigger_outbound_call(
        cls,
        customer_name,
        phone,
        event_name,
        loan_amount_offered,
        loan_tenure,
        correlation_id,
    ):
        while not limiter.hit(SARVAM_LIMIT, "sarvam_api"):
            time.sleep(0.1)

        url = f"https://apps.sarvam.ai/api/outbounds/v1/orgs/{os.getenv('SARVAM_ORG_ID')}/workspaces/{os.getenv('SARVAM_WORKSPACE_ID')}/outbounds"

        payload = {
            "app_config": {
                "app_id": os.getenv("SARVAM_APP_ID"),
                "app_version": os.getenv("SARVAM_APP_VERSION"),
                "connection_config": {
                    "connection_id": os.getenv("SARVAM_CONNECTION_ID"),
                    "agent_phone_number": os.getenv("SARVAM_AGENT_NUMBER"),
                },
                "agent_variables": {
                    "customer_name": customer_name,
                    "current_dropoff_state": event_name,
                    "loan_amount": loan_amount_offered,
                    "loan_tenure": loan_tenure,
                    "correlation_id": correlation_id,
                },
                "app_type": "agent",
                "app_overrides": {
                    "initial_bot_message": f"नमस्ते, क्या मेरी बात {customer_name} जी से हो रही है?",
                    "initial_language_name": "Hindi",
                },
            },
            "user_config": {"user_phone_number": f"+91{phone}"},
        }

        try:
            session = cls.get_session()
            response = session.post(url, json=payload, timeout=15)
            if response.status_code == 200:
                cls._logger.info(f"Sarvam call triggered for CID: {correlation_id}")
                return True, response.json().get("attempt_id"), correlation_id
            return False, response.text, correlation_id
        except Exception as e:
            return False, str(e), correlation_id

    @classmethod
    def process_batch(cls, rows: pd.DataFrame):
        cls._logger.info(f"Starting process_batch. Total rows: {len(rows)}")

        all_cids = rows["correlation_id"].dropna().unique().tolist()
        if not all_cids:
            return

        dict_rows = rows.to_dict(orient="records")
        # --- PRE-INCREMENT & RECHURN EVALUATION ---
        try:
            cls._logger.info(f"Pre-incrementing call_count for {len(all_cids)} CIDs")
            db_api.increment_bulk(
                table_name="wa_dropoff",
                column_to_inc="call_count",
                where_col="correlation_id",
                values_list=all_cids,
            )

            limit_query = f"""
                UPDATE wa_dropoff 
                UPDATE wa_dropoff 
                SET is_processed = True 
                WHERE correlation_id IN %s 
                AND call_count >= {total_attempts}
                AND is_processed = False
            """
            db_api.execute_raw_query(limit_query, (tuple(all_cids),))

        except Exception as e:
            cls._logger.error(f"Failed to pre-update Postgres: {e}")
            return

        # --- EXECUTE API CALLS ---
        journey_updates = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_cid = {
                executor.submit(
                    cls.trigger_outbound_call,
                    row.get("customer_name", "Priya Grahak"),
                    row["mobile_no"],
                    row.get("event_name", ""),
                    row.get("loan_amount", ""),
                    row.get("loan_tenure", ""),
                    row["correlation_id"],
                ): row["correlation_id"]
                for row in dict_rows
            }

            for future in as_completed(future_to_cid):
                cid = future_to_cid[future]
                try:
                    success, result, _ = future.result()

                    journey_updates.append(
                        {
                            "correlation_id": cid,
                            "sarvam_attempt_id": result if success else None,
                            "call_state": "success" if success else "failed",
                            "call_triggered": True,
                        }
                    )
                except Exception as e:
                    cls._logger.error(f"Thread Exception for {cid}: {str(e)}")
                    journey_updates.append(
                        {
                            "correlation_id": cid,
                            "sarvam_attempt_id": None,
                            "call_state": "failed",
                            "call_triggered": True,
                        }
                    )

        # --- FINAL POSTGRES STATE COMMIT ---
        if journey_updates:
            db_api.update_bulk_mapped(
                "wa_dropoff", journey_updates, "correlation_id"
            )

        cls._logger.info("Batch processing complete.")