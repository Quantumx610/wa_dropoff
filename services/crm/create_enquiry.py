import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

url = os.getenv("CREATE_ENQUIRY_ENDPOINT")
REQ_CLIENT_ID = os.getenv("REQ_CLIENT_ID")
REQ_ORG_ID = os.getenv("REQ_ORG_ID")

def create_enquiry(enquiry_no: str, no_of_chunks: str, access_token: str):

    errors = None

    payload = json.dumps({
    "EnquiryNo": enquiry_no,
    "Noofchunks": no_of_chunks
    })

    headers = {
    'Cookie': f'ReqClientId={REQ_CLIENT_ID}; orgId={REQ_ORG_ID}',
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
    }

    try:
        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code == 200:
            response_body = response.json()
            status = response_body.get("Status_Msg")
            enquiry_no_in_response = response_body.get("EnquiryNo")
        print(response.text)
    except Exception as e:
        print(e)

    