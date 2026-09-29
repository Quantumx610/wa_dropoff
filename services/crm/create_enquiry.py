import uuid
import json
from datetime import datetime

def create_enquiry(
        enquiry_no: str,
        no_of_chunks: int,
        access_token: str):

    cosmos_db = get_cosmos_connection()

    correlation_id = str(uuid.uuid4())

    payload = {
        "EnquiryNo": enquiry_no,
        "Noofchunks": no_of_chunks
    }

    log_doc = {
        "correlation_id": correlation_id,
        "enquiry_no": enquiry_no,
        "chunk_count": no_of_chunks,
        "request_payload": payload,
        "request_time": datetime.utcnow().isoformat()
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

        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=60
        )

        log_doc["response_time"] = datetime.utcnow().isoformat()
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

        cosmos_db.dbInsert(
            COSMOS_KAFKA_CRM_OUTBOX,
            log_doc
        )

        return response.status_code, response_body

    except Exception as e:

        log_doc["status"] = "FAILED"
        log_doc["error"] = str(e)

        cosmos_db.dbInsert(
            COSMOS_KAFKA_CRM_OUTBOX,
            log_doc
        )

        raise



########################################################

import os
import requests
import json
from db import get_cosmos_connection
from dotenv import load_dotenv
import logging

load_dotenv()


logger = logging.getLogger(__name__)

url = os.getenv("CREATE_ENQUIRY_ENDPOINT")
REQ_CLIENT_ID = os.getenv("REQ_CLIENT_ID")
REQ_ORG_ID = os.getenv("REQ_ORG_ID")
COSMOS_KAFKA_CRM_OUTBOX = os.getenv("COSMOS_KAFKA_CRM_OUTBOX")

def create_enquiry(enquiry_no: str, no_of_chunks: str, access_token: str):

    cosmos_db_api = get_cosmos_connection()

    payload = json.dumps({
        "EnquiryNo": enquiry_no,
        "Noofchunks": no_of_chunks
    })

    cosmos_dump = {"request_payload": payload}

    headers = {
        'Cookie': f'ReqClientId={REQ_CLIENT_ID}; orgId={REQ_ORG_ID}',
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code == 200:
            response_body = response.json()

            cosmos_dump["response_body"] = {"errors":"", "response_json":response_body, "status_code": response.status_code}

            cosmos_db_api.dbInsert(COSMOS_KAFKA_CRM_OUTBOX, cosmos_dump)
        else:
            response_body = response.json()

            cosmos_dump["response_body"] = {"errors":"", "response_json":response_body, "status_code": response.status_code}
            cosmos_db_api.dbInsert(COSMOS_KAFKA_CRM_OUTBOX, cosmos_dump)

    except Exception as e:
        cosmos_dump["response_body"] = {"errors":str(e), "response_json":"", "status_code": ""}
        cosmos_db_api.dbInsert(COSMOS_KAFKA_CRM_OUTBOX, cosmos_dump)
        logger.error(f"CRM API Failed, Error: {str(e)}")
