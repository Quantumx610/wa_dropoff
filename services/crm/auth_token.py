import os
import pytz
import requests
from datetime import datetime
from db import get_cosmos_connection
from dotenv import load_dotenv

load_dotenv()

URL = os.getenv("AUTH_TOKEN_ENDPOINT")
AUTH_LOG_COLLECTION = os.getenv("COSMOS_AUTH_LOGS")
TOKEN_BASIC_AUTH = os.getenv("TOKEN_BASIC_AUTH")

IST = pytz.timezone("Asia/Kolkata")

def get_access_token():

    cosmos_db = get_cosmos_connection()

    payload = "grant_type=client_credentials"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f'Basic {TOKEN_BASIC_AUTH}'
    }

    log_doc = {
        "event_type": "AUTH_TOKEN",
        "request_time": datetime.now(IST).isoformat()
    }

    try:
        response = requests.post(URL, headers=headers, data=payload, timeout=30)

        log_doc["response_time"] = datetime.now(IST).isoformat()
        log_doc["status_code"] = response.status_code

        response_json = response.json()

        if response.status_code == 200:

            log_doc["status"] = "SUCCESS"
            log_doc["response_body"] = response_json

            cosmos_db.dbInsert( AUTH_LOG_COLLECTION, log_doc)

            return response_json.get("access_token")

        else:

            log_doc["status"] = "FAILED"
            log_doc["response_body"] = response_json

            cosmos_db.dbInsert(AUTH_LOG_COLLECTION, log_doc)

            return None

    except Exception as e:

        log_doc["status"] = "FAILED"
        log_doc["error"] = str(e)

        cosmos_db.dbInsert(AUTH_LOG_COLLECTION, log_doc)

        return None
