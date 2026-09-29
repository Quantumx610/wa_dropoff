from db import get_cosmos_connection

try:
    cosmos_db_api = get_cosmos_connection()
    # cosmos_db_api.connect
    cosmos_db_api.dbInsert("kafka_input_log", {"status": "OK", "code": "200"})
    obj = cosmos_db_api.dbGet("kafka_input_log", {"status": "OK", "code": "200"})
    print("data inserted", obj[0])
except Exception as e:
    print(str(e))
