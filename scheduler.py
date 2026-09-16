
import pandas as pd



from postgres_api import PostgresDatabaseAPI


def calling_job():
    # get call_triggered false records
    postgres_db_api = PostgresDatabaseAPI("uat")
    records = postgres_db_api.read("whatsapp_dropoff", filters={"call_triggered": False, "is_processed": False})

    records_df = pd.DataFrame(records)

    records_df = records_df.drop_duplicates(subset="mobile_no", ignore_index=False)

    

    return True