import os
import json
import time
import signal
import logging
import threading
from typing import Callable, Any, Dict, Optional
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")


class KafkaConsumer:
    """
    Production-grade streaming Kafka consumer wrapper.
    Handles connection management, deserialization, manual offset commits, 
    and graceful shutdown signals.
    """
    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        group_id: Optional[str] = None,
        topics: Optional[list] = None,
        security_protocol: str = "SASL_SSL",
        sasl_mechanism: str = "SCRAM-SHA-512",
        sasl_username: Optional[str] = None,
        sasl_password: Optional[str] = None,
        auto_offset_reset: str = "latest",
        extra_config: Optional[Dict[str, Any]] = None
    ):
        self.running = True
        self.topics = topics or [KAFKA_TOPIC]
        
        self.conf = {
            'bootstrap.servers': bootstrap_servers or 'b-1.sitmskcluster.cymah8.c4.kafka.ap-south-1.amazonaws.com:9096,b-2.sitmskcluster.cymah8.c4.kafka.ap-south-1.amazonaws.com:9096',
            'security.protocol': security_protocol,
            'sasl.mechanism': sasl_mechanism,
            'sasl.username': sasl_username or 'mmfsl-dna-sit',
            'sasl.password': sasl_password or KAFKA_PASSWORD,
            'group.id': group_id or 'superapp-cosmos-writer-group',  
            'auto.offset.reset': auto_offset_reset,
            'enable.auto.commit': False,
            'session.timeout.ms': 45000,
            'max.poll.interval.ms': 300000
        }

        if extra_config:
            self.conf.update(extra_config)

        self.consumer: Optional[Consumer] = None
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Catch termination signals cleanly if instantiated in the main thread."""
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, self._shutdown)
            signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, signum, frame):
        logger.warning(f"Received signal {signum}. Initiating graceful shutdown...")
        self.running = False

    def connect(self):
        """Initializes consumer connection and subscribes to topics."""
        try:
            self.consumer = Consumer(self.conf)
            self.consumer.subscribe(self.topics)
            logger.info(f"Kafka Consumer connected and subscribed to topics: {self.topics}")
        except Exception as e:
            logger.critical(f"Failed to connect Kafka consumer: {str(e)}", exc_info=True)
            raise e

    def start_listening(self, handler_func: Callable[[Dict[str, Any]], bool]):
        """Continuously polls Kafka and passes processed events to handler_func."""
        if not self.consumer:
            self.connect()

        logger.info("Starting continuous message stream polling loop...")

        while self.running:
            try:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition: {msg.topic()} [{msg.partition()}]")
                    else:
                        logger.error(f"Kafka consumer error: {msg.error()}")
                    continue

                # 1. Deserialize Payload
                payload = None
                if msg.value():
                    try:
                        raw_str = msg.value().decode("utf-8")
                        payload = json.loads(raw_str)
                        logger.info(f"Incoming Event payload received: Partition {msg.partition()} Offset {msg.offset()}")
                    except (UnicodeDecodeError, json.JSONDecodeError) as parse_err:
                        logger.error(
                            f"Skipping corrupt message at Partition {msg.partition()} "
                            f"Offset {msg.offset()}: {parse_err}"
                        )
                        self.consumer.commit(msg, asynchronous=False)
                        continue

                # 2. Execute Business Logic Handler
                try:
                    if payload:
                        processed_status = handler_func(payload)
                        logger.info(f"Event processing finished with status: {processed_status}")
                    
                    # 3. Commit offset
                    self.consumer.commit(msg, asynchronous=False)

                except Exception as handler_err:
                    logger.error(
                        f"Error in handler processing Partition {msg.partition()} "
                        f"Offset {msg.offset()}: {handler_err}", 
                        exc_info=True
                    )

            except Exception as loop_err:
                logger.error(f"Unexpected error in streaming consumption loop: {loop_err}", exc_info=True)
                time.sleep(1)

        self.close()

    def close(self):
        """Cleanly close connection and rebalance partition consumer group."""
        if self.consumer:
            logger.info("Closing Kafka consumer connection...")
            try:
                self.consumer.close()
                logger.info("Kafka consumer closed successfully.")
            except Exception as e:
                logger.error(f"Error closing consumer: {e}")
