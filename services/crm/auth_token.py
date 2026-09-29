import os
import requests
from datetime import datetime
from db import get_cosmos_connection

AUTH_LOG_COLLECTION = os.getenv("COSMOS_AUTH_LOGS")

def get_access_token():

    cosmos_db = get_cosmos_connection()

    payload = "grant_type=client_credentials"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": os.getenv("AUTH_HEADER")
    }

    log_doc = {
        "event_type": "AUTH_TOKEN",
        "request_time": datetime.utcnow().isoformat()
    }

    try:

        response = requests.post(
            os.getenv("AUTH_TOKEN_ENDPOINT"),
            headers=headers,
            data=payload,
            timeout=30
        )

        log_doc["response_time"] = datetime.utcnow().isoformat()
        log_doc["status_code"] = response.status_code

        response_json = response.json()

        if response.status_code == 200:

            log_doc["status"] = "SUCCESS"
            log_doc["response_body"] = response_json

            cosmos_db.dbInsert(
                AUTH_LOG_COLLECTION,
                log_doc
            )

            return response_json.get("access_token")

        else:

            log_doc["status"] = "FAILED"
            log_doc["response_body"] = response_json

            cosmos_db.dbInsert(
                AUTH_LOG_COLLECTION,
                log_doc
            )

            raise Exception(
                f"Token API Failed : {response.status_code}"
            )

    except Exception as e:

        log_doc["status"] = "FAILED"
        log_doc["error"] = str(e)

        cosmos_db.dbInsert(
            AUTH_LOG_COLLECTION,
            log_doc
        )

        raise

##################################################################################################
import os
import requests
from dotenv import load_dotenv

load_dotenv()

url = os.getenv("AUTH_TOKEN_ENDPOINT")

payload = 'grant_type=client_credentials'
headers = {
  'Content-Type': 'application/x-www-form-urlencoded',
  'Authorization': 'Basic QVdFcGNiZ0RSc1UxVmNXQVdjRU90TTNlQXZtQ2MzR0tmZHVhR3FDM2xHcmd2YXlROjlyOUNuOFl5a0g0UjlWNnNlSTgyT1BzZWdQQmJ2R2FHTkpBdHJaR3Vxb3k3aXQzc2lHWVlXNFBpMWtrVGVteU4='
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
