from datetime import datetime
import pytz
import os
import requests
from db import get_cosmos_connection
from dotenv import load_dotenv
import logging

load_dotenv()

logger = logging.getLogger(__name__)

URL = os.getenv("CREATE_ENQUIRY_ENDPOINT")
REQ_CLIENT_ID = os.getenv("REQ_CLIENT_ID")
REQ_ORG_ID = os.getenv("REQ_ORG_ID")
COSMOS_KAFKA_CRM_OUTBOX = os.getenv("COSMOS_KAFKA_CRM_OUTBOX")

IST = pytz.timezone("Asia/Kolkata")

def create_enquiry(enquiry_no: str, no_of_chunks: int, access_token: str):

    cosmos_db = get_cosmos_connection()

    payload = {
        "EnquiryNo": enquiry_no,
        "Noofchunks": no_of_chunks
    }

    log_doc = {
        "enquiry_no": enquiry_no,
        "chunk_count": no_of_chunks,
        "request_payload": payload,
        "request_time": datetime.now(IST).isoformat()
    }

    headers = {
        "Cookie": (
            f"ReqClientId={REQ_CLIENT_ID};"
            f"orgId={REQ_ORG_ID}"
        ),
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    try:

        response = requests.post(URL, headers=headers, json=payload, timeout=60)

        log_doc["response_time"] = datetime.now(IST).isoformat()
        log_doc["status_code"] = response.status_code

        try:
            response_body = response.json()
        except:
            response_body = response.text

        log_doc["response_body"] = response_body

        if response.status_code == 200:
            log_doc["status"] = "SUCCESS"
        else:
            log_doc["status"] = "FAILED"

        cosmos_db.dbInsert(COSMOS_KAFKA_CRM_OUTBOX, log_doc)

        return response.status_code, response_body

    except Exception as e:

        log_doc["status"] = "FAILED"
        log_doc["error"] = str(e)

        cosmos_db.dbInsert(COSMOS_KAFKA_CRM_OUTBOX, log_doc)
        logger.error(f"CRM API Failed, Error: {str(e)}")
        return 400, {}
