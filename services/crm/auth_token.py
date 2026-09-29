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
