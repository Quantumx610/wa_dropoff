from confluent_kafka import Consumer, KafkaError
import os
from db import get_cosmos_connection
from dotenv import load_dotenv

load_dotenv()
KAFKA_USERNAME_SIT = os.getenv("KAFKA_USERNAME_SIT")
KAFKA_USERNAME_UAT = os.getenv("KAFKA_USERNAME_UAT")
KAFKA_PASSWORD_UAT = os.getenv("KAFKA_PASSWORD_UAT")
KAFKA_PASSWORD_SIT = os.getenv("KAFKA_PASSWORD_SIT")
# 1. Define the Kafka consumer configuration
conf = {
    'bootstrap.servers': 'b-1.sitmskcluster.cymah8.c4.kafka.ap-south-1.amazonaws.com:9096,b-2.sitmskcluster.cymah8.c4.kafka.ap-south-1.amazonaws.com:9096',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': KAFKA_USERNAME_SIT,
    'sasl.password': KAFKA_PASSWORD_SIT, 
    'group.id': 'superapp-cosmos-writer-group', # Required for standard Python consumers
    'auto.offset.reset': 'latest',              # Equivalent to startingOffsets="latest"
    # 'debug': 'broker,security,protocol'
}

# 2. Initialize the Consumer
consumer = Consumer(conf)

# 3. Subscribe to the topic
topic = 'superAppEventsTopic'
consumer.subscribe([topic])

print(f"Subscribed to '{topic}'. Waiting for messages...")

try:
    while True:
        # Poll for new messages every 1 second
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
            
        if msg.error():
            # Ignore EOF errors which just mean we reached the end of a partition
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                break

        # 4. Extract and cast payload to string (equivalent to CAST(key AS STRING), CAST(value AS STRING))
        key = msg.key().decode('utf-8') if msg.key() else None
        value = msg.value().decode('utf-8') if msg.value() else None

        # 5. Extract metadata
        msg_topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()
        # msg.timestamp() returns a tuple (timestamp_type, timestamp_in_ms)
        timestamp = msg.timestamp()[1] 

        # Display the output
        print({
            "key": key,
            "value": value,
            "topic": msg_topic,
            "partition": partition,
            "offset": offset,
            "timestamp": timestamp
        })

except KeyboardInterrupt:
    print("Streaming stopped by user.")
finally:
    # Clean up and commit final offsets
    consumer.close()
