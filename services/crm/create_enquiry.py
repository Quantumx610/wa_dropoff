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
