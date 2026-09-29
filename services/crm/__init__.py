from .auth_token import get_access_token
from .create_enquiry import create_crm_enquiry


def process_crm_enquiries(records):
    access_token = get_access_token()

    if not access_token:
        raise Exception("CRM token generation failed")

    for row in records:
        create_crm_enquiry(
            enquiry_no=str(row["enquiry_id"]),
            no_of_chunks=int(row["call_count"]),
            access_token=access_token
        )
